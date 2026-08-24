use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use chrono::{DateTime, Timelike, Utc};
use datafusion::common::pruning::PrunableStatistics;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::PruningPredicateBuilder;
use datafusion::scalar::ScalarValue;

use crate::coverage::EntityValue;
use crate::metadata::index::{IndexKind, IndexSpec, IndexValue};
use crate::transaction_log::{SegmentEntityLayout, SegmentMeta};

use super::df_external;

pub(super) fn segment_pruning_statistics(
    schema: &SchemaRef,
    index: &IndexSpec,
    segments: &[&SegmentMeta],
) -> DFResult<PrunableStatistics> {
    let index_position = schema.index_of(&index.column).map_err(|source| {
        DataFusionError::from(source).context(format!(
            "ordered index column {} is missing from the Arrow schema",
            index.column,
        ))
    })?;
    let index_type = schema.field(index_position).data_type();
    validate_index_type(index, index_type)?;
    let entity_positions = index
        .entity_columns
        .iter()
        .map(|column| {
            schema.index_of(column).map_err(|source| {
                DataFusionError::from(source).context(format!(
                    "entity column {column} is missing from the Arrow schema"
                ))
            })
        })
        .collect::<DFResult<Vec<_>>>()?;

    let statistics = segments
        .iter()
        .map(|segment| {
            segment.validate_bounds(&index.kind).map_err(|source| {
                df_external(source).context(format!(
                    "cannot build pruning statistics for segment {}",
                    segment.path
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
    component: &EntityValue,
    data_type: &DataType,
    segment: &SegmentMeta,
    column: &str,
) -> DFResult<ScalarValue> {
    match (component, data_type) {
        (EntityValue::Utf8(value), DataType::Utf8) => Ok(ScalarValue::Utf8(Some(value.clone()))),
        (EntityValue::Utf8(value), DataType::LargeUtf8) => {
            Ok(ScalarValue::LargeUtf8(Some(value.clone())))
        }
        (EntityValue::Int32(value), DataType::Int32) => Ok(ScalarValue::Int32(Some(*value))),
        (EntityValue::Int64(value), DataType::Int64) => Ok(ScalarValue::Int64(Some(*value))),
        (EntityValue::UInt64(value), DataType::UInt64) => Ok(ScalarValue::UInt64(Some(*value))),
        _ => Err(DataFusionError::Execution(format!(
            "cannot build entity pruning statistics for column {column} in segment {}: identity value {component:?} does not match Arrow type {data_type}",
            segment.path,
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
    let pruning_predicate = PruningPredicateBuilder::new()
        .with_file_schema(Arc::clone(schema))
        .try_build(Arc::clone(predicate))
        .map_err(|source| source.context("cannot create segment metadata pruning predicate"))?;
    let keep = pruning_predicate
        .prune(&statistics)
        .map_err(|source| source.context("cannot evaluate segment metadata pruning predicate"))?;
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
    use std::{error::Error as _, num::NonZeroU64};

    use arrow::datatypes::{Field, Schema};
    use chrono::TimeZone;
    use datafusion::common::Column;
    use datafusion::common::pruning::PruningStatistics;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysicalColumn, Literal};

    use crate::coverage::EntityIdentity;
    use crate::metadata::index::TimeIndexGranularity;
    use crate::metadata::segments::SegmentMetaError;
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

    fn single_entity_segment(path: &str, min: i64, max: i64, components: &[&str]) -> SegmentMeta {
        let mut segment = segment(path, IndexValue::Int64(min), IndexValue::Int64(max));
        segment.entity_layout = SegmentEntityLayout::Single(
            EntityIdentity::try_new(
                components
                    .iter()
                    .map(|value| EntityValue::from(*value))
                    .collect(),
            )
            .unwrap(),
        );
        segment
    }

    fn binary(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, op, right))
    }

    fn string_eq(name: &str, position: usize, value: &str) -> Arc<dyn PhysicalExpr> {
        binary(
            Arc::new(PhysicalColumn::new(name, position)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8(Some(value.to_string())))),
        )
    }

    fn scalar_eq(
        name: &str,
        position: usize,
        value: ScalarValue,
        reversed: bool,
    ) -> Arc<dyn PhysicalExpr> {
        let column: Arc<dyn PhysicalExpr> = Arc::new(PhysicalColumn::new(name, position));
        let literal: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(value));
        if reversed {
            binary(literal, Operator::Eq, column)
        } else {
            binary(column, Operator::Eq, literal)
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
                index_granularity: NonZeroU64::new(1).unwrap(),
            },
        };
        let mut single = segment(
            "data/single.parquet",
            IndexValue::Int64(0),
            IndexValue::Int64(1),
        );
        single.entity_layout = SegmentEntityLayout::Single(
            EntityIdentity::try_new(vec!["west".into(), "sensor-a".into()]).unwrap(),
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
                index_granularity: NonZeroU64::new(1).unwrap(),
            },
        };
        let mut segment = segment(
            "data/unsupported.parquet",
            IndexValue::Int64(0),
            IndexValue::Int64(1),
        );
        segment.entity_layout =
            SegmentEntityLayout::Single(EntityIdentity::try_new(vec!["sensor-a".into()]).unwrap());

        let error = segment_pruning_statistics(&schema, &index, &[&segment])
            .err()
            .expect("unsupported entity type must fail");

        assert!(error.to_string().contains("data/unsupported.parquet"));
        assert!(error.to_string().contains("device"));
        assert!(error.to_string().contains("Int64"));
    }

    #[test]
    fn typed_entity_statistics_prune_every_supported_type_in_both_orders() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
            Field::new("large_text", DataType::LargeUtf8, false),
            Field::new("signed_32", DataType::Int32, false),
            Field::new("signed_64", DataType::Int64, false),
            Field::new("unsigned_64", DataType::UInt64, false),
        ]));
        let index = IndexSpec {
            column: "idx".to_string(),
            entity_columns: vec![
                "text".to_string(),
                "large_text".to_string(),
                "signed_32".to_string(),
                "signed_64".to_string(),
                "unsigned_64".to_string(),
            ],
            kind: IndexKind::Int64 {
                index_granularity: NonZeroU64::MIN,
            },
        };
        let mut matching = segment(
            "matching.parquet",
            IndexValue::Int64(0),
            IndexValue::Int64(0),
        );
        matching.entity_layout = SegmentEntityLayout::Single(
            EntityIdentity::try_new(vec![
                EntityValue::from("device"),
                EntityValue::from("region"),
                EntityValue::Int32(-1),
                EntityValue::Int64(i64::MIN),
                EntityValue::UInt64(u64::MAX),
            ])
            .unwrap(),
        );
        let mut conflicting = segment(
            "conflicting.parquet",
            IndexValue::Int64(1),
            IndexValue::Int64(1),
        );
        conflicting.entity_layout = SegmentEntityLayout::Single(
            EntityIdentity::try_new(vec![
                EntityValue::from("other"),
                EntityValue::from("other"),
                EntityValue::Int32(1),
                EntityValue::Int64(i64::MAX),
                EntityValue::UInt64(0),
            ])
            .unwrap(),
        );
        let mut mixed = segment("mixed.parquet", IndexValue::Int64(2), IndexValue::Int64(2));
        mixed.entity_layout = SegmentEntityLayout::Mixed;
        let segments = vec![&matching, &conflicting, &mixed];
        let cases = [
            ("text", 1, ScalarValue::Utf8(Some("device".to_string()))),
            (
                "large_text",
                2,
                ScalarValue::LargeUtf8(Some("region".to_string())),
            ),
            ("signed_32", 3, ScalarValue::Int32(Some(-1))),
            ("signed_64", 4, ScalarValue::Int64(Some(i64::MIN))),
            ("unsigned_64", 5, ScalarValue::UInt64(Some(u64::MAX))),
        ];

        for (column, position, value) in cases {
            for reversed in [false, true] {
                let predicate = scalar_eq(column, position, value.clone(), reversed);
                let selected = prune_segments(&schema, &index, segments.clone(), &predicate)
                    .expect("typed pruning");
                assert_eq!(
                    selected
                        .into_iter()
                        .map(|segment| segment.path.as_str())
                        .collect::<Vec<_>>(),
                    ["matching.parquet", "mixed.parquet"]
                );
            }
        }
    }

    #[test]
    fn composite_and_boolean_predicates_prune_without_false_negatives() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("device", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let index = IndexSpec {
            column: "idx".to_string(),
            entity_columns: vec!["region".to_string(), "device".to_string()],
            kind: IndexKind::Int64 {
                index_granularity: NonZeroU64::new(1).unwrap(),
            },
        };
        let west_a = single_entity_segment("west-a.parquet", 0, 9, &["west", "a"]);
        let east_a = single_entity_segment("east-a.parquet", 10, 19, &["east", "a"]);
        let west_b = single_entity_segment("west-b.parquet", 20, 29, &["west", "b"]);
        let mut mixed = segment(
            "mixed.parquet",
            IndexValue::Int64(30),
            IndexValue::Int64(39),
        );
        mixed.entity_layout = SegmentEntityLayout::Mixed;
        let segments = [&west_a, &east_a, &west_b, &mixed];

        let cases = [
            (
                binary(
                    string_eq("region", 2, "west"),
                    Operator::And,
                    string_eq("device", 1, "a"),
                ),
                vec!["west-a.parquet", "mixed.parquet"],
            ),
            (
                string_eq("device", 1, "a"),
                vec!["west-a.parquet", "east-a.parquet", "mixed.parquet"],
            ),
            (
                binary(
                    string_eq("device", 1, "a"),
                    Operator::And,
                    binary(
                        Arc::new(PhysicalColumn::new("idx", 0)),
                        Operator::GtEq,
                        Arc::new(Literal::new(ScalarValue::Int64(Some(15)))),
                    ),
                ),
                vec!["east-a.parquet", "mixed.parquet"],
            ),
            (
                binary(
                    string_eq("device", 1, "a"),
                    Operator::Or,
                    string_eq("region", 2, "east"),
                ),
                vec!["west-a.parquet", "east-a.parquet", "mixed.parquet"],
            ),
            (
                binary(
                    string_eq("device", 1, "a"),
                    Operator::Or,
                    string_eq("payload", 3, "unknown"),
                ),
                vec![
                    "west-a.parquet",
                    "east-a.parquet",
                    "west-b.parquet",
                    "mixed.parquet",
                ],
            ),
        ];

        for (predicate, expected) in cases {
            let selected = prune_segments(&schema, &index, segments.to_vec(), &predicate).unwrap();
            assert_eq!(
                selected
                    .into_iter()
                    .map(|segment| segment.path.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn preserves_signed_bounds_and_leaves_other_statistics_unknown() {
        let schema = schema(DataType::Int64);
        let index = index(IndexKind::Int64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
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
        assert!(statistics.row_counts().is_none());
    }

    #[test]
    fn preserves_unsigned_bounds_above_signed_range() {
        let schema = schema(DataType::UInt64);
        let index = index(IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
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
            index_granularity: NonZeroU64::new(1).unwrap(),
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
                index_granularity: TimeIndexGranularity::Seconds(1),
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
            index_granularity: TimeIndexGranularity::Seconds(1),
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
            index_granularity: NonZeroU64::new(1).unwrap(),
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
        let external = segment_error
            .source()
            .and_then(|source| source.downcast_ref::<DataFusionError>())
            .expect("contextual DataFusion source");
        assert!(
            external
                .source()
                .is_some_and(|source| source.is::<SegmentMetaError>())
        );

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
