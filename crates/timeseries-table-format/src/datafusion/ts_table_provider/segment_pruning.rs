use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use chrono::{DateTime, Timelike, Utc};
use datafusion::common::pruning::PrunableStatistics;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::scalar::ScalarValue;

use crate::metadata::table_metadata::{IndexKind, IndexSpec, IndexValue};
use crate::transaction_log::{SegmentEntityLayout, SegmentMeta};

pub(super) fn segment_pruning_statistics(
    schema: &SchemaRef,
    index: &IndexSpec,
    segments: &[&SegmentMeta],
) -> DFResult<PrunableStatistics> {
    let index_position = schema.index_of(&index.column).map_err(|source| {
        DataFusionError::Plan(format!(
            "ordered index column {} is missing from the Arrow schema: {source}",
            index.column
        ))
    })?;
    let index_type = schema.field(index_position).data_type();
    validate_index_type(index, index_type)?;
    let entity_positions = index
        .entity_columns
        .iter()
        .map(|column| {
            schema.index_of(column).map_err(|source| {
                DataFusionError::Plan(format!(
                    "entity column {column} is missing from the Arrow schema: {source}"
                ))
            })
        })
        .collect::<DFResult<Vec<_>>>()?;

    let statistics = segments
        .iter()
        .map(|segment| {
            segment.validate_bounds(&index.kind).map_err(|source| {
                DataFusionError::Execution(format!(
                    "cannot build segment pruning statistics: {source}"
                ))
            })?;

            let min = index_scalar(&segment.index_min, index_type, segment, "minimum")?;
            let max = index_scalar(&segment.index_max, index_type, segment, "maximum")?;
            let mut statistics = Statistics::new_unknown(schema);
            statistics.column_statistics[index_position] = ColumnStatistics::new_unknown()
                .with_min_value(Precision::Exact(min))
                .with_max_value(Precision::Exact(max));
            if let SegmentEntityLayout::Single(identity) = &segment.entity_layout {
                if identity.components().len() != entity_positions.len() {
                    return Err(DataFusionError::Execution(format!(
                        "cannot build entity pruning statistics for segment {}: expected {} identity components, found {}",
                        segment.path,
                        entity_positions.len(),
                        identity.components().len()
                    )));
                }
                for (position, component) in entity_positions.iter().zip(identity.components()) {
                    let value = entity_scalar(
                        component,
                        schema.field(*position).data_type(),
                        segment,
                        schema.field(*position).name(),
                    )?;
                    statistics.column_statistics[*position] = ColumnStatistics::new_unknown()
                        .with_null_count(Precision::Exact(0))
                        .with_min_value(Precision::Exact(value.clone()))
                        .with_max_value(Precision::Exact(value));
                }
            } else if matches!(segment.entity_layout, SegmentEntityLayout::Mixed) {
                for position in &entity_positions {
                    // PrunableStatistics needs a typed null to combine unknown and exact
                    // per-segment values in one Arrow array.
                    let value = ScalarValue::try_from(schema.field(*position).data_type())?;
                    statistics.column_statistics[*position] = ColumnStatistics::new_unknown()
                        .with_min_value(Precision::Exact(value.clone()))
                        .with_max_value(Precision::Exact(value));
                }
            }
            Ok(Arc::new(statistics))
        })
        .collect::<DFResult<Vec<_>>>()?;

    Ok(PrunableStatistics::new(statistics, Arc::clone(schema)))
}

fn entity_scalar(
    component: &str,
    data_type: &DataType,
    segment: &SegmentMeta,
    column: &str,
) -> DFResult<ScalarValue> {
    match data_type {
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(component.to_string()))),
        DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8(Some(component.to_string()))),
        _ => Err(DataFusionError::Execution(format!(
            "cannot build entity pruning statistics for column {column} in segment {}: Arrow type {data_type} is unsupported",
            segment.path
        ))),
    }
}

pub(super) fn prune_segments<'a>(
    schema: &SchemaRef,
    index: &IndexSpec,
    segments: Vec<&'a SegmentMeta>,
    predicate: &Arc<dyn PhysicalExpr>,
) -> DFResult<Vec<&'a SegmentMeta>> {
    let statistics = segment_pruning_statistics(schema, index, &segments)?;
    let pruning_predicate = PruningPredicate::try_new(Arc::clone(predicate), Arc::clone(schema))
        .map_err(|source| {
            DataFusionError::Execution(format!(
                "cannot create segment pruning predicate for ordered index {}: {source}",
                index.column
            ))
        })?;
    let keep = pruning_predicate.prune(&statistics).map_err(|source| {
        DataFusionError::Execution(format!(
            "cannot evaluate segment pruning predicate for ordered index {}: {source}",
            index.column
        ))
    })?;
    if keep.len() != segments.len() {
        return Err(DataFusionError::Internal(format!(
            "segment pruning returned {} decisions for {} segments",
            keep.len(),
            segments.len()
        )));
    }

    Ok(segments
        .into_iter()
        .zip(keep)
        .filter_map(|(segment, keep)| keep.then_some(segment))
        .collect())
}

fn validate_index_type(index: &IndexSpec, data_type: &DataType) -> DFResult<()> {
    let matches = matches!(
        (&index.kind, data_type),
        (IndexKind::Timestamp { .. }, DataType::Timestamp(_, _))
            | (IndexKind::Int64 { .. }, DataType::Int64)
            | (IndexKind::UInt64 { .. }, DataType::UInt64)
    );
    if matches {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "ordered index column {} has registered {} domain but Arrow type {data_type}",
            index.column,
            index.kind.name()
        )))
    }
}

fn index_scalar(
    value: &IndexValue,
    data_type: &DataType,
    segment: &SegmentMeta,
    bound: &str,
) -> DFResult<ScalarValue> {
    match (value, data_type) {
        (IndexValue::Int64(value), DataType::Int64) => Ok(ScalarValue::Int64(Some(*value))),
        (IndexValue::UInt64(value), DataType::UInt64) => Ok(ScalarValue::UInt64(Some(*value))),
        (IndexValue::Timestamp(value), DataType::Timestamp(unit, timezone)) => timestamp_scalar(
            *value,
            unit,
            timezone.clone(),
        )
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "cannot represent {bound} timestamp {value} from segment {} exactly as {unit:?}",
                segment.path
            ))
        }),
        _ => Err(DataFusionError::Execution(format!(
            "{bound} ordered-index value {} from segment {} is incompatible with Arrow type {data_type}",
            value, segment.path
        ))),
    }
}

pub(super) fn timestamp_scalar(
    value: DateTime<Utc>,
    unit: &TimeUnit,
    timezone: Option<Arc<str>>,
) -> Option<ScalarValue> {
    let subsecond_nanos = value.nanosecond();
    match unit {
        TimeUnit::Second if subsecond_nanos == 0 => Some(ScalarValue::TimestampSecond(
            Some(value.timestamp()),
            timezone,
        )),
        TimeUnit::Millisecond if subsecond_nanos.is_multiple_of(1_000_000) => Some(
            ScalarValue::TimestampMillisecond(Some(value.timestamp_millis()), timezone),
        ),
        TimeUnit::Microsecond if subsecond_nanos.is_multiple_of(1_000) => Some(
            ScalarValue::TimestampMicrosecond(Some(value.timestamp_micros()), timezone),
        ),
        TimeUnit::Nanosecond => value
            .timestamp_nanos_opt()
            .map(|value| ScalarValue::TimestampNanosecond(Some(value), timezone)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use arrow::datatypes::{Field, Schema};
    use chrono::TimeZone;
    use datafusion::common::Column;
    use datafusion::common::pruning::PruningStatistics;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysicalColumn, Literal};

    use crate::coverage::EntityIdentity;
    use crate::metadata::table_metadata::TimeBucket;
    use crate::transaction_log::{FileFormat, SegmentEntityLayout};

    use super::*;

    fn index(kind: IndexKind) -> IndexSpec {
        IndexSpec {
            column: "idx".to_string(),
            entity_columns: vec![],
            kind,
        }
    }

    fn schema(index_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("idx", index_type, false),
            Field::new("payload", DataType::Utf8, true),
        ]))
    }

    fn segment(path: &str, min: IndexValue, max: IndexValue) -> SegmentMeta {
        SegmentMeta {
            path: path.to_string(),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: min,
            index_max: max,
            row_count: 10,
            file_size: Some(100),
            coverage_path: None,
        }
    }

    fn scalar_at(
        statistics: &PrunableStatistics,
        column: &str,
        position: usize,
        minimum: bool,
    ) -> ScalarValue {
        let column = Column::from_name(column);
        let values = if minimum {
            statistics.min_values(&column)
        } else {
            statistics.max_values(&column)
        }
        .expect("known statistics");
        ScalarValue::try_from_array(values.as_ref(), position).expect("scalar value")
    }

    fn null_count_at(
        statistics: &PrunableStatistics,
        column: &str,
        position: usize,
    ) -> ScalarValue {
        let values = statistics
            .null_counts(&Column::from_name(column))
            .expect("known null count");
        ScalarValue::try_from_array(values.as_ref(), position).expect("scalar value")
    }

    #[test]
    fn exposes_exact_entity_statistics_only_for_single_entity_segments() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("device", DataType::Utf8, false),
            Field::new("region", DataType::LargeUtf8, false),
        ]));
        let index = IndexSpec {
            column: "idx".to_string(),
            entity_columns: vec!["region".to_string(), "device".to_string()],
            kind: IndexKind::Int64 {
                bucket_width: NonZeroU64::new(1).unwrap(),
            },
        };
        let mut single = segment(
            "data/single.parquet",
            IndexValue::Int64(0),
            IndexValue::Int64(1),
        );
        single.entity_layout = SegmentEntityLayout::Single(
            EntityIdentity::try_new(vec!["west".to_string(), "sensor-a".to_string()]).unwrap(),
        );
        let mut mixed = segment(
            "data/mixed.parquet",
            IndexValue::Int64(2),
            IndexValue::Int64(3),
        );
        mixed.entity_layout = SegmentEntityLayout::Mixed;

        let statistics = segment_pruning_statistics(&schema, &index, &[&single, &mixed]).unwrap();

        assert_eq!(
            scalar_at(&statistics, "device", 0, true),
            ScalarValue::Utf8(Some("sensor-a".to_string()))
        );
        assert_eq!(
            scalar_at(&statistics, "device", 0, false),
            ScalarValue::Utf8(Some("sensor-a".to_string()))
        );
        assert_eq!(
            scalar_at(&statistics, "region", 0, true),
            ScalarValue::LargeUtf8(Some("west".to_string()))
        );
        assert_eq!(
            scalar_at(&statistics, "region", 0, false),
            ScalarValue::LargeUtf8(Some("west".to_string()))
        );
        assert_eq!(
            null_count_at(&statistics, "device", 0),
            ScalarValue::UInt64(Some(0))
        );
        assert_eq!(
            scalar_at(&statistics, "device", 1, true),
            ScalarValue::Utf8(None)
        );
        assert_eq!(
            scalar_at(&statistics, "device", 1, false),
            ScalarValue::Utf8(None)
        );
        assert_eq!(
            null_count_at(&statistics, "device", 1),
            ScalarValue::UInt64(None)
        );
        assert_eq!(
            scalar_at(&statistics, "region", 1, true),
            ScalarValue::LargeUtf8(None)
        );
    }

    #[test]
    fn rejects_single_entity_statistics_for_unsupported_arrow_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("device", DataType::Int64, false),
        ]));
        let index = IndexSpec {
            column: "idx".to_string(),
            entity_columns: vec!["device".to_string()],
            kind: IndexKind::Int64 {
                bucket_width: NonZeroU64::new(1).unwrap(),
            },
        };
        let mut segment = segment(
            "data/unsupported.parquet",
            IndexValue::Int64(0),
            IndexValue::Int64(1),
        );
        segment.entity_layout = SegmentEntityLayout::Single(
            EntityIdentity::try_new(vec!["sensor-a".to_string()]).unwrap(),
        );

        let error = segment_pruning_statistics(&schema, &index, &[&segment])
            .err()
            .expect("unsupported entity type must fail");

        assert!(error.to_string().contains("data/unsupported.parquet"));
        assert!(error.to_string().contains("device"));
        assert!(error.to_string().contains("Int64"));
    }

    #[test]
    fn preserves_signed_bounds_and_leaves_other_statistics_unknown() {
        let schema = schema(DataType::Int64);
        let index = index(IndexKind::Int64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        });
        let segments = [
            segment(
                "data/min.parquet",
                IndexValue::Int64(i64::MIN),
                IndexValue::Int64(-1),
            ),
            segment(
                "data/max.parquet",
                IndexValue::Int64(0),
                IndexValue::Int64(i64::MAX),
            ),
        ];
        let segments = segments.iter().collect::<Vec<_>>();

        let statistics = segment_pruning_statistics(&schema, &index, &segments).unwrap();

        assert_eq!(statistics.num_containers(), 2);
        assert_eq!(
            scalar_at(&statistics, "idx", 0, true),
            ScalarValue::Int64(Some(i64::MIN))
        );
        assert_eq!(
            scalar_at(&statistics, "idx", 1, false),
            ScalarValue::Int64(Some(i64::MAX))
        );
        assert!(
            statistics
                .min_values(&Column::from_name("payload"))
                .is_none()
        );
        assert!(
            statistics
                .max_values(&Column::from_name("payload"))
                .is_none()
        );
        assert!(statistics.null_counts(&Column::from_name("idx")).is_none());
        assert!(statistics.row_counts(&Column::from_name("idx")).is_none());
    }

    #[test]
    fn preserves_unsigned_bounds_above_signed_range() {
        let schema = schema(DataType::UInt64);
        let index = index(IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        });
        let segment = segment(
            "data/unsigned.parquet",
            IndexValue::UInt64(i64::MAX as u64 + 1),
            IndexValue::UInt64(u64::MAX),
        );

        let statistics = segment_pruning_statistics(&schema, &index, &[&segment]).unwrap();

        assert_eq!(
            scalar_at(&statistics, "idx", 0, true),
            ScalarValue::UInt64(Some(i64::MAX as u64 + 1))
        );
        assert_eq!(
            scalar_at(&statistics, "idx", 0, false),
            ScalarValue::UInt64(Some(u64::MAX))
        );
    }

    #[test]
    fn native_pruning_retains_possible_segments_in_input_order() {
        let schema = schema(DataType::Int64);
        let index = index(IndexKind::Int64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        });
        let segments = [
            segment(
                "data/first.parquet",
                IndexValue::Int64(0),
                IndexValue::Int64(10),
            ),
            segment(
                "data/second.parquet",
                IndexValue::Int64(11),
                IndexValue::Int64(20),
            ),
            segment(
                "data/third.parquet",
                IndexValue::Int64(21),
                IndexValue::Int64(30),
            ),
        ];
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(PhysicalColumn::new("idx", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(15)))),
        ));

        let selected =
            prune_segments(&schema, &index, segments.iter().collect(), &predicate).unwrap();

        assert_eq!(
            selected
                .into_iter()
                .map(|segment| segment.path.as_str())
                .collect::<Vec<_>>(),
            vec!["data/second.parquet", "data/third.parquet"]
        );

        let null_predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(PhysicalColumn::new("idx", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int64(None))),
        ));
        let selected =
            prune_segments(&schema, &index, segments.iter().collect(), &null_predicate).unwrap();
        assert_eq!(selected.len(), segments.len());
    }

    #[test]
    fn converts_exact_negative_timestamps_in_all_arrow_units_and_timezones() {
        let cases = [
            (
                TimeUnit::Second,
                None,
                Utc.timestamp_opt(-1, 0).unwrap(),
                ScalarValue::TimestampSecond(Some(-1), None),
            ),
            (
                TimeUnit::Millisecond,
                Some(Arc::<str>::from("UTC")),
                Utc.timestamp_opt(-1, 999_000_000).unwrap(),
                ScalarValue::TimestampMillisecond(Some(-1), Some(Arc::from("UTC"))),
            ),
            (
                TimeUnit::Microsecond,
                Some(Arc::<str>::from("+05:30")),
                Utc.timestamp_opt(-1, 999_999_000).unwrap(),
                ScalarValue::TimestampMicrosecond(Some(-1), Some(Arc::from("+05:30"))),
            ),
            (
                TimeUnit::Nanosecond,
                Some(Arc::<str>::from("America/Phoenix")),
                Utc.timestamp_opt(-1, 999_999_999).unwrap(),
                ScalarValue::TimestampNanosecond(Some(-1), Some(Arc::from("America/Phoenix"))),
            ),
        ];

        for (unit, timezone, value, expected) in cases {
            let schema = schema(DataType::Timestamp(unit, timezone));
            let index = index(IndexKind::Timestamp {
                bucket: TimeBucket::Seconds(1),
                timezone: None,
            });
            let segment = segment(
                "data/timestamp.parquet",
                IndexValue::Timestamp(value),
                IndexValue::Timestamp(value),
            );

            let statistics = segment_pruning_statistics(&schema, &index, &[&segment]).unwrap();

            assert_eq!(scalar_at(&statistics, "idx", 0, true), expected);
            assert_eq!(scalar_at(&statistics, "idx", 0, false), expected);
        }
    }

    #[test]
    fn rejects_lossy_or_out_of_range_timestamp_conversion() {
        let index = index(IndexKind::Timestamp {
            bucket: TimeBucket::Seconds(1),
            timezone: None,
        });
        let lossy = segment(
            "data/lossy.parquet",
            IndexValue::Timestamp(Utc.timestamp_opt(0, 1).unwrap()),
            IndexValue::Timestamp(Utc.timestamp_opt(0, 1).unwrap()),
        );
        let error = segment_pruning_statistics(
            &schema(DataType::Timestamp(TimeUnit::Millisecond, None)),
            &index,
            &[&lossy],
        )
        .err()
        .expect("lossy conversion must fail");
        assert!(error.to_string().contains("data/lossy.parquet"));
        assert!(error.to_string().contains("exactly"));

        let old = Utc.with_ymd_and_hms(1600, 1, 1, 0, 0, 0).unwrap();
        let out_of_range = segment(
            "data/out-of-range.parquet",
            IndexValue::Timestamp(old),
            IndexValue::Timestamp(old),
        );
        let error = segment_pruning_statistics(
            &schema(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            &index,
            &[&out_of_range],
        )
        .err()
        .expect("out-of-range conversion must fail");
        assert!(error.to_string().contains("data/out-of-range.parquet"));
        assert!(error.to_string().contains("exactly"));
    }

    #[test]
    fn rejects_schema_and_segment_domain_mismatches_with_context() {
        let signed_index = index(IndexKind::Int64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        });
        let schema_error =
            segment_pruning_statistics(&schema(DataType::UInt64), &signed_index, &[])
                .err()
                .expect("schema mismatch must fail");
        assert!(schema_error.to_string().contains("registered int64 domain"));
        assert!(schema_error.to_string().contains("UInt64"));

        let wrong_domain = segment(
            "data/wrong-domain.parquet",
            IndexValue::UInt64(0),
            IndexValue::UInt64(1),
        );
        let segment_error =
            segment_pruning_statistics(&schema(DataType::Int64), &signed_index, &[&wrong_domain])
                .err()
                .expect("segment mismatch must fail");
        assert!(
            segment_error
                .to_string()
                .contains("data/wrong-domain.parquet")
        );
        assert!(segment_error.to_string().contains("expected int64"));

        let reversed = segment(
            "data/reversed.parquet",
            IndexValue::Int64(2),
            IndexValue::Int64(1),
        );
        let reversed_error =
            segment_pruning_statistics(&schema(DataType::Int64), &signed_index, &[&reversed])
                .err()
                .expect("reversed bounds must fail");
        assert!(reversed_error.to_string().contains("data/reversed.parquet"));
        assert!(reversed_error.to_string().contains("expected min <= max"));
    }
}
