//! Exact row-aligned entity coverage extraction from Parquet segments.

use std::{collections::BTreeMap, path::Path};

use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{
    Array, Int32Array, Int64Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use futures::{Stream, StreamExt};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::ParquetRecordBatchStreamBuilder,
    },
    errors::ParquetError,
};
use roaring::RoaringTreemap;
use snafu::Backtrace;
use tokio::task::JoinSet;

use crate::{
    coverage::{Coverage, EntityCoverage, EntityIdentity, EntityValue},
    metadata::table_metadata::{IndexKind, IndexSpec, IndexValue},
    storage::{TableLocation, open_parquet_reader},
};

use super::{INSPECTION_BATCH_SIZE, resolve_rg_settings};
use super::{
    coverage::{SegmentCoverageError, arrow_index_error, insert_bucket, timestamp_value},
    schema::validate_parquet_index,
};

pub(super) enum EntityArray<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt64(&'a UInt64Array),
}

impl EntityArray<'_> {
    fn value(&self, row: usize) -> EntityValue {
        match self {
            Self::Utf8(array) => EntityValue::Utf8(array.value(row).to_string()),
            Self::LargeUtf8(array) => EntityValue::Utf8(array.value(row).to_string()),
            Self::Int32(array) => EntityValue::Int32(array.value(row)),
            Self::Int64(array) => EntityValue::Int64(array.value(row)),
            Self::UInt64(array) => EntityValue::UInt64(array.value(row)),
        }
    }
}

enum OrderedIndexArray<'a> {
    TimestampSecond(&'a TimestampSecondArray),
    TimestampMillisecond(&'a TimestampMillisecondArray),
    TimestampMicrosecond(&'a TimestampMicrosecondArray),
    TimestampNanosecond(&'a TimestampNanosecondArray),
    Int64(&'a Int64Array),
    UInt64(&'a UInt64Array),
}

impl OrderedIndexArray<'_> {
    fn value(
        &self,
        row: usize,
        path: &str,
        index: &IndexSpec,
    ) -> Result<Option<IndexValue>, SegmentCoverageError> {
        if self.is_null(row) {
            return Ok(None);
        }
        match self {
            Self::TimestampSecond(array) => {
                timestamp_value(path, index, TimeUnit::Second, array.value(row)).map(Some)
            }
            Self::TimestampMillisecond(array) => {
                timestamp_value(path, index, TimeUnit::Millisecond, array.value(row)).map(Some)
            }
            Self::TimestampMicrosecond(array) => {
                timestamp_value(path, index, TimeUnit::Microsecond, array.value(row)).map(Some)
            }
            Self::TimestampNanosecond(array) => {
                timestamp_value(path, index, TimeUnit::Nanosecond, array.value(row)).map(Some)
            }
            Self::Int64(array) => Ok(Some(IndexValue::Int64(array.value(row)))),
            Self::UInt64(array) => Ok(Some(IndexValue::UInt64(array.value(row)))),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::TimestampSecond(array) => array.is_null(row),
            Self::TimestampMillisecond(array) => array.is_null(row),
            Self::TimestampMicrosecond(array) => array.is_null(row),
            Self::TimestampNanosecond(array) => array.is_null(row),
            Self::Int64(array) => array.is_null(row),
            Self::UInt64(array) => array.is_null(row),
        }
    }
}

pub(super) fn entity_arrays<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    path: &str,
    entity_columns: &[String],
) -> Result<Vec<EntityArray<'a>>, SegmentCoverageError> {
    entity_columns
        .iter()
        .map(|column| {
            let array = batch.column_by_name(column).ok_or_else(|| {
                SegmentCoverageError::EntityColumnNotFound {
                    path: path.to_string(),
                    column: column.clone(),
                }
            })?;
            if array.null_count() != 0 {
                return Err(SegmentCoverageError::EntityColumnHasNull {
                    path: path.to_string(),
                    column: column.clone(),
                });
            }
            match array.data_type() {
                DataType::Utf8 => array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(EntityArray::Utf8),
                DataType::LargeUtf8 => array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .map(EntityArray::LargeUtf8),
                DataType::Int32 => array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .map(EntityArray::Int32),
                DataType::Int64 => array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(EntityArray::Int64),
                DataType::UInt64 => array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .map(EntityArray::UInt64),
                _ => None,
            }
            .ok_or_else(|| SegmentCoverageError::EntityColumnUnsupportedType {
                path: path.to_string(),
                column: column.clone(),
                datatype: array.data_type().to_string(),
            })
        })
        .collect()
}

pub(super) fn entity_identity_at(
    arrays: &[EntityArray<'_>],
    row: usize,
    path: &str,
) -> Result<EntityIdentity, SegmentCoverageError> {
    EntityIdentity::try_new(arrays.iter().map(|array| array.value(row)).collect()).map_err(
        |source| SegmentCoverageError::EntityIdentity {
            path: path.to_string(),
            source,
        },
    )
}

fn ordered_index_array<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    path: &str,
    index: &IndexSpec,
) -> Result<OrderedIndexArray<'a>, SegmentCoverageError> {
    let array = batch
        .column_by_name(&index.column)
        .ok_or_else(|| arrow_index_error(path, index, "missing".to_string()))?;

    let typed = match (&index.kind, array.data_type()) {
        (IndexKind::Timestamp { .. }, DataType::Timestamp(TimeUnit::Second, _)) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .map(OrderedIndexArray::TimestampSecond),
        (IndexKind::Timestamp { .. }, DataType::Timestamp(TimeUnit::Millisecond, _)) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(OrderedIndexArray::TimestampMillisecond),
        (IndexKind::Timestamp { .. }, DataType::Timestamp(TimeUnit::Microsecond, _)) => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(OrderedIndexArray::TimestampMicrosecond),
        (IndexKind::Timestamp { .. }, DataType::Timestamp(TimeUnit::Nanosecond, _)) => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(OrderedIndexArray::TimestampNanosecond),
        (IndexKind::Int64 { .. }, DataType::Int64) => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(OrderedIndexArray::Int64),
        (IndexKind::UInt64 { .. }, DataType::UInt64) => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(OrderedIndexArray::UInt64),
        _ => None,
    };

    typed.ok_or_else(|| arrow_index_error(path, index, format!("Arrow {}", array.data_type())))
}

async fn compute_from_stream(
    mut reader: impl Stream<
        Item = Result<arrow::record_batch::RecordBatch, parquet::errors::ParquetError>,
    > + Unpin,
    path: &str,
    index: &IndexSpec,
) -> Result<EntityCoverage, SegmentCoverageError> {
    let mut by_identity = BTreeMap::<EntityIdentity, RoaringTreemap>::new();

    while let Some(batch) = reader.next().await {
        let batch = batch.map_err(|source| SegmentCoverageError::ParquetRead {
            path: path.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })?;
        let entities = entity_arrays(&batch, path, &index.entity_columns)?;
        let ordered_index = ordered_index_array(&batch, path, index)?;

        for row in 0..batch.num_rows() {
            let identity = entity_identity_at(&entities, row, path)?;
            let bitmap = by_identity.entry(identity).or_default();
            if let Some(value) = ordered_index.value(row, path, index)? {
                insert_bucket(bitmap, path, index, value)?;
            }
        }

        tokio::task::yield_now().await;
    }

    let mut coverage = EntityCoverage::empty();
    for (identity, bitmap) in by_identity {
        coverage.union_coverage(identity, Coverage::from_treemap(bitmap));
    }
    Ok(coverage)
}

/// Compute exact row-aligned entity coverage from one Parquet segment.
///
/// Entity columns and the ordered index are projected together and read from
/// the same record-batch row. No sidecars or table state are written.
///
/// # Errors
///
/// Returns [`SegmentCoverageError`] for storage, Parquet, entity, index, or
/// bucket validation failures.
pub async fn compute_segment_entity_coverage(
    location: &TableLocation,
    rel_path: &Path,
    index: &IndexSpec,
) -> Result<EntityCoverage, SegmentCoverageError> {
    if index.entity_columns.is_empty() {
        return Ok(EntityCoverage::empty());
    }

    let path = rel_path.display().to_string();
    let mut file = open_parquet_reader(location.as_ref(), rel_path)
        .await
        .map_err(|source| SegmentCoverageError::Storage {
            path: path.clone(),
            source,
        })?;
    let metadata = ArrowReaderMetadata::load_async(&mut file, ArrowReaderOptions::default())
        .await
        .map_err(|source| SegmentCoverageError::ParquetRead {
            path: path.clone(),
            source,
            backtrace: Backtrace::capture(),
        })?;

    if metadata.metadata().file_metadata().num_rows() == 0 {
        return Err(SegmentCoverageError::EntityColumnEmpty {
            path,
            column: index.entity_columns[0].clone(),
        });
    }
    let validated_index = validate_parquet_index(&path, metadata.parquet_schema(), index)
        .map_err(|source| SegmentCoverageError::OrderedIndexColumn { source })?;
    let parquet_schema = metadata.parquet_schema();
    let root_fields = parquet_schema.root_schema().get_fields();
    let mut projected_roots = Vec::with_capacity(index.entity_columns.len() + 1);
    for column in &index.entity_columns {
        let root_index = root_fields
            .iter()
            .position(|field| field.name() == column)
            .ok_or_else(|| SegmentCoverageError::EntityColumnNotFound {
                path: path.clone(),
                column: column.clone(),
            })?;
        projected_roots.push(root_index);
        let field = metadata.schema().field_with_name(column).map_err(|_| {
            SegmentCoverageError::EntityColumnNotFound {
                path: path.clone(),
                column: column.clone(),
            }
        })?;
        if !matches!(
            field.data_type(),
            DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt64
        ) {
            return Err(SegmentCoverageError::EntityColumnUnsupportedType {
                path,
                column: column.clone(),
                datatype: field.data_type().to_string(),
            });
        }
    }
    projected_roots.push(parquet_schema.get_column_root_idx(validated_index.leaf_index));
    let mask = ProjectionMask::roots(parquet_schema, projected_roots);
    drop(file);

    let row_groups = metadata.metadata().num_row_groups();
    let (max_tasks, row_groups_per_task) = resolve_rg_settings(row_groups);
    let row_groups = (0..row_groups).collect::<Vec<_>>();
    let chunks = row_groups
        .chunks(row_groups_per_task)
        .map(<[usize]>::to_vec)
        .collect::<Vec<_>>();
    debug_assert!(chunks.len() <= max_tasks);

    let mut tasks = JoinSet::new();
    for chunk in chunks {
        let location = location.clone();
        let rel_path = rel_path.to_path_buf();
        let path = path.clone();
        let index = index.clone();
        let metadata = metadata.clone();
        let mask = mask.clone();

        tasks.spawn(async move {
            let file = open_parquet_reader(location.as_ref(), &rel_path)
                .await
                .map_err(|source| SegmentCoverageError::Storage {
                    path: path.clone(),
                    source,
                })?;
            let reader = ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata)
                .with_projection(mask)
                .with_row_groups(chunk)
                .with_batch_size(INSPECTION_BATCH_SIZE)
                .build()
                .map_err(|source| SegmentCoverageError::ParquetRead {
                    path: path.clone(),
                    source,
                    backtrace: Backtrace::capture(),
                })?;
            compute_from_stream(reader, &path, &index).await
        });
    }

    let mut merged = EntityCoverage::empty();
    while let Some(result) = tasks.join_next().await {
        let coverage = result.map_err(|source| SegmentCoverageError::ParquetRead {
            path: path.clone(),
            source: ParquetError::General(format!("row-group scan task failed: {source}")),
            backtrace: Backtrace::capture(),
        })??;
        merged.union_inplace(&coverage);
    }

    if merged.is_empty() {
        return Err(SegmentCoverageError::EntityColumnEmpty {
            path,
            column: index.entity_columns[0].clone(),
        });
    }
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::File, num::NonZeroU64, sync::Arc};

    use arrow::{
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use arrow_array::{
        BooleanArray, Int32Array, Int64Array, LargeStringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use tempfile::TempDir;

    use crate::metadata::table_metadata::TimeBucket;

    type TestResult = Result<(), Box<dyn std::error::Error>>;
    const EPOCH_BUCKET: u64 = 0x8000_0000_0000_0000;

    fn timestamp_index() -> IndexSpec {
        IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec!["entity".to_string()],
            kind: IndexKind::Timestamp {
                bucket: TimeBucket::Hours(1),
                timezone: None,
            },
        }
    }

    fn identity(value: &str) -> EntityIdentity {
        EntityIdentity::try_new(vec![value.into()]).expect("test identity")
    }

    fn write_batch(
        path: &Path,
        batch: &RecordBatch,
        properties: Option<WriterProperties>,
    ) -> TestResult {
        let mut writer = ArrowWriter::try_new(File::create(path)?, batch.schema(), properties)?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }

    fn write_timestamp_segment(
        path: &Path,
        entities: Vec<&str>,
        timestamps: Vec<Option<i64>>,
        max_row_group_size: Option<usize>,
    ) -> TestResult {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("payload", DataType::Int32, false),
            Field::new("entity", DataType::Utf8, false),
        ]));
        let payload = (0..entities.len() as i32).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                Arc::new(Int32Array::from(payload)),
                Arc::new(StringArray::from(entities)),
            ],
        )?;
        let properties = max_row_group_size.map(|size| {
            WriterProperties::builder()
                .set_max_row_group_size(size)
                .build()
        });
        write_batch(path, &batch, properties)
    }

    fn buckets(coverage: &EntityCoverage, entity: &str) -> Vec<u64> {
        coverage
            .get(&identity(entity))
            .expect("entity coverage")
            .present()
            .iter()
            .collect()
    }

    #[tokio::test]
    async fn row_aligned_scan_does_not_invent_entity_bucket_pairs() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        write_timestamp_segment(
            &temp.path().join(rel_path),
            vec!["A", "B", "A", "B", "A"],
            vec![Some(0), Some(7_200_000), Some(3_600_000), None, Some(0)],
            None,
        )?;

        let coverage = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            rel_path,
            &timestamp_index(),
        )
        .await?;

        assert_eq!(coverage.cardinality(), 3);
        assert_eq!(
            buckets(&coverage, "A"),
            vec![EPOCH_BUCKET, EPOCH_BUCKET + 1]
        );
        assert_eq!(buckets(&coverage, "B"), vec![EPOCH_BUCKET + 2]);
        Ok(())
    }

    #[tokio::test]
    async fn same_bucket_remains_independent_for_each_entity() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        write_timestamp_segment(
            &temp.path().join(rel_path),
            vec!["A", "B"],
            vec![Some(0), Some(0)],
            None,
        )?;

        let coverage = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            rel_path,
            &timestamp_index(),
        )
        .await?;

        assert_eq!(coverage.cardinality(), 2);
        assert_eq!(buckets(&coverage, "A"), vec![EPOCH_BUCKET]);
        assert_eq!(buckets(&coverage, "B"), vec![EPOCH_BUCKET]);
        Ok(())
    }

    #[tokio::test]
    async fn identities_change_across_record_batches() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        let mut entities = vec!["A"; INSPECTION_BATCH_SIZE];
        entities.push("B");
        let mut timestamps = vec![Some(0); INSPECTION_BATCH_SIZE];
        timestamps.push(Some(3_600_000));
        write_timestamp_segment(&temp.path().join(rel_path), entities, timestamps, None)?;

        let coverage = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            rel_path,
            &timestamp_index(),
        )
        .await?;

        assert_eq!(buckets(&coverage, "A"), vec![EPOCH_BUCKET]);
        assert_eq!(buckets(&coverage, "B"), vec![EPOCH_BUCKET + 1]);
        Ok(())
    }

    #[tokio::test]
    async fn identities_change_across_row_groups() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        write_timestamp_segment(
            &temp.path().join(rel_path),
            vec!["A", "B"],
            vec![Some(0), Some(3_600_000)],
            Some(1),
        )?;

        let coverage = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            rel_path,
            &timestamp_index(),
        )
        .await?;

        assert_eq!(buckets(&coverage, "A"), vec![EPOCH_BUCKET]);
        assert_eq!(buckets(&coverage, "B"), vec![EPOCH_BUCKET + 1]);
        Ok(())
    }

    #[tokio::test]
    async fn composite_identity_preserves_configured_component_order() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("region", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["A", "A"])),
                Arc::new(TimestampMillisecondArray::from(vec![0, 3_600_000])),
                Arc::new(StringArray::from(vec!["us", "eu"])),
            ],
        )?;
        write_batch(&temp.path().join(rel_path), &batch, None)?;
        let index = IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec!["region".to_string(), "symbol".to_string()],
            kind: IndexKind::Timestamp {
                bucket: TimeBucket::Hours(1),
                timezone: None,
            },
        };

        let coverage =
            compute_segment_entity_coverage(&TableLocation::local(temp.path()), rel_path, &index)
                .await?;

        let us_a = EntityIdentity::try_new(vec!["us".into(), "A".into()])?;
        let eu_a = EntityIdentity::try_new(vec!["eu".into(), "A".into()])?;
        assert_eq!(
            coverage
                .get(&us_a)
                .expect("us/A coverage")
                .present()
                .iter()
                .collect::<Vec<_>>(),
            vec![EPOCH_BUCKET]
        );
        assert_eq!(
            coverage
                .get(&eu_a)
                .expect("eu/A coverage")
                .present()
                .iter()
                .collect::<Vec<_>>(),
            vec![EPOCH_BUCKET + 1]
        );
        Ok(())
    }

    #[tokio::test]
    async fn dotted_top_level_names_are_resolved_literally() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("device.id", DataType::Utf8, false),
            Field::new(
                "event.ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["A", "B"])),
                Arc::new(TimestampMillisecondArray::from(vec![0, 3_600_000])),
            ],
        )?;
        write_batch(&temp.path().join(rel_path), &batch, None)?;
        let index = IndexSpec {
            column: "event.ts".to_string(),
            entity_columns: vec!["device.id".to_string()],
            kind: IndexKind::Timestamp {
                bucket: TimeBucket::Hours(1),
                timezone: None,
            },
        };

        let coverage =
            compute_segment_entity_coverage(&TableLocation::local(temp.path()), rel_path, &index)
                .await?;

        assert_eq!(buckets(&coverage, "A"), vec![EPOCH_BUCKET]);
        assert_eq!(buckets(&coverage, "B"), vec![EPOCH_BUCKET + 1]);
        Ok(())
    }

    #[tokio::test]
    async fn timestamp_indexes_support_every_parquet_unit() -> TestResult {
        let temp = TempDir::new()?;
        let cases: Vec<(&str, DataType, Arc<dyn Array>)> = vec![
            (
                "milliseconds.parquet",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                Arc::new(TimestampMillisecondArray::from(vec![0, 3_600_000])),
            ),
            (
                "microseconds.parquet",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Arc::new(TimestampMicrosecondArray::from(vec![0, 3_600_000_000])),
            ),
            (
                "nanoseconds.parquet",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                Arc::new(TimestampNanosecondArray::from(vec![0, 3_600_000_000_000])),
            ),
        ];

        for (file_name, data_type, timestamps) in cases {
            let rel_path = Path::new(file_name);
            let schema = Arc::new(Schema::new(vec![
                Field::new("entity", DataType::Utf8, false),
                Field::new("ts", data_type, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec!["A", "A"])), timestamps],
            )?;
            write_batch(&temp.path().join(rel_path), &batch, None)?;

            let coverage = compute_segment_entity_coverage(
                &TableLocation::local(temp.path()),
                rel_path,
                &timestamp_index(),
            )
            .await?;

            assert_eq!(
                buckets(&coverage, "A"),
                vec![EPOCH_BUCKET, EPOCH_BUCKET + 1]
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn large_utf8_entity_succeeds() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::LargeUtf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(LargeStringArray::from(vec!["A", "B"])),
                Arc::new(TimestampMillisecondArray::from(vec![0, 3_600_000])),
            ],
        )?;
        write_batch(&temp.path().join(rel_path), &batch, None)?;

        let coverage = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            rel_path,
            &timestamp_index(),
        )
        .await?;

        assert_eq!(buckets(&coverage, "A"), vec![EPOCH_BUCKET]);
        assert_eq!(buckets(&coverage, "B"), vec![EPOCH_BUCKET + 1]);
        Ok(())
    }

    #[tokio::test]
    async fn every_supported_entity_type_is_extracted_without_loss() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("typed-entities.parquet");
        let above_i64_max = i64::MAX as u64 + 1;
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("large_text", DataType::LargeUtf8, false),
            Field::new("signed_32", DataType::Int32, false),
            Field::new("signed_64", DataType::Int64, false),
            Field::new("unsigned_64", DataType::UInt64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["min", "zero", "max"])),
                Arc::new(LargeStringArray::from(vec!["min", "zero", "max"])),
                Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])),
                Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])),
                Arc::new(UInt64Array::from(vec![above_i64_max, 0, u64::MAX])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    0, 3_600_000, 7_200_000,
                ])),
            ],
        )?;
        write_batch(&temp.path().join(rel_path), &batch, None)?;
        let index = IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec![
                "text".to_string(),
                "large_text".to_string(),
                "signed_32".to_string(),
                "signed_64".to_string(),
                "unsigned_64".to_string(),
            ],
            kind: IndexKind::Timestamp {
                bucket: TimeBucket::Hours(1),
                timezone: None,
            },
        };

        let coverage =
            compute_segment_entity_coverage(&TableLocation::local(temp.path()), rel_path, &index)
                .await?;

        let cases = [
            (
                EntityIdentity::try_new(vec![
                    EntityValue::from("min"),
                    EntityValue::from("min"),
                    EntityValue::Int32(i32::MIN),
                    EntityValue::Int64(i64::MIN),
                    EntityValue::UInt64(above_i64_max),
                ])?,
                EPOCH_BUCKET,
            ),
            (
                EntityIdentity::try_new(vec![
                    EntityValue::from("zero"),
                    EntityValue::from("zero"),
                    EntityValue::Int32(0),
                    EntityValue::Int64(0),
                    EntityValue::UInt64(0),
                ])?,
                EPOCH_BUCKET + 1,
            ),
            (
                EntityIdentity::try_new(vec![
                    EntityValue::from("max"),
                    EntityValue::from("max"),
                    EntityValue::Int32(i32::MAX),
                    EntityValue::Int64(i64::MAX),
                    EntityValue::UInt64(u64::MAX),
                ])?,
                EPOCH_BUCKET + 2,
            ),
        ];
        assert_eq!(coverage.identity_count(), cases.len());
        for (identity, bucket) in cases {
            assert_eq!(
                coverage
                    .get(&identity)
                    .expect("typed entity coverage")
                    .present()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![bucket]
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn null_entity_returns_typed_error() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("A"), None])),
                Arc::new(TimestampMillisecondArray::from(vec![Some(0), None])),
            ],
        )?;
        write_batch(&temp.path().join(rel_path), &batch, None)?;

        let error = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            rel_path,
            &timestamp_index(),
        )
        .await
        .expect_err("null entity must fail");

        assert!(matches!(
            error,
            SegmentCoverageError::EntityColumnHasNull { path, column }
                if path == "segment.parquet" && column == "entity"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn each_missing_composite_entity_column_returns_typed_error() -> TestResult {
        let temp = TempDir::new()?;
        let mut index = timestamp_index();
        index.entity_columns = vec!["region".to_string(), "symbol".to_string()];

        for (missing, present) in [("region", "symbol"), ("symbol", "region")] {
            let filename = format!("missing-{missing}.parquet");
            let rel_path = Path::new(&filename);
            let schema = Arc::new(Schema::new(vec![
                Field::new(present, DataType::Utf8, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["value"])),
                    Arc::new(TimestampMillisecondArray::from(vec![0])),
                ],
            )?;
            write_batch(&temp.path().join(rel_path), &batch, None)?;

            let error = compute_segment_entity_coverage(
                &TableLocation::local(temp.path()),
                rel_path,
                &index,
            )
            .await
            .expect_err("missing entity column must fail");

            assert!(matches!(
                error,
                SegmentCoverageError::EntityColumnNotFound { path, column }
                    if path == filename && column == missing
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_entity_type_returns_typed_error() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Boolean, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(TimestampMillisecondArray::from(vec![0])),
            ],
        )?;
        write_batch(&temp.path().join(rel_path), &batch, None)?;

        let error = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            rel_path,
            &timestamp_index(),
        )
        .await
        .expect_err("unsupported entity type must fail");

        assert!(matches!(
            error,
            SegmentCoverageError::EntityColumnUnsupportedType { path, column, datatype }
                if path == "segment.parquet" && column == "entity" && datatype == "Boolean"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn empty_segment_returns_typed_entity_error() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]));
        ArrowWriter::try_new(File::create(temp.path().join(rel_path))?, schema, None)?.close()?;

        let error = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            rel_path,
            &timestamp_index(),
        )
        .await
        .expect_err("empty entity data must fail");

        assert!(matches!(
            error,
            SegmentCoverageError::EntityColumnEmpty { path, column }
                if path == "segment.parquet" && column == "entity"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn integer_indexes_use_registered_bucket_semantics() -> TestResult {
        let temp = TempDir::new()?;
        let signed_path = Path::new("signed.parquet");
        let signed_schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let signed_batch = RecordBatch::try_new(
            signed_schema,
            vec![
                Arc::new(StringArray::from(vec!["A"; 6])),
                Arc::new(Int64Array::from(vec![-11, -10, -1, 0, 9, 10])),
            ],
        )?;
        write_batch(&temp.path().join(signed_path), &signed_batch, None)?;
        let signed_index = IndexSpec {
            column: "value".to_string(),
            entity_columns: vec!["entity".to_string()],
            kind: IndexKind::Int64 {
                bucket_width: NonZeroU64::new(10).expect("nonzero bucket width"),
            },
        };

        let signed = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            signed_path,
            &signed_index,
        )
        .await?;
        assert_eq!(
            buckets(&signed, "A"),
            vec![
                EPOCH_BUCKET - 2,
                EPOCH_BUCKET - 1,
                EPOCH_BUCKET,
                EPOCH_BUCKET + 1
            ]
        );

        let unsigned_path = Path::new("unsigned.parquet");
        let unsigned_schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::UInt64, false),
        ]));
        let unsigned_batch = RecordBatch::try_new(
            unsigned_schema,
            vec![
                Arc::new(StringArray::from(vec!["A"; 4])),
                Arc::new(UInt64Array::from(vec![0, 9, 10, u64::MAX])),
            ],
        )?;
        write_batch(&temp.path().join(unsigned_path), &unsigned_batch, None)?;
        let unsigned_index = IndexSpec {
            column: "value".to_string(),
            entity_columns: vec!["entity".to_string()],
            kind: IndexKind::UInt64 {
                bucket_width: NonZeroU64::new(10).expect("nonzero bucket width"),
            },
        };

        let unsigned = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            unsigned_path,
            &unsigned_index,
        )
        .await?;
        assert_eq!(buckets(&unsigned, "A"), vec![0, 1, u64::MAX / 10]);
        Ok(())
    }

    #[tokio::test]
    async fn duplicates_collapse_and_null_indexes_preserve_empty_identities() -> TestResult {
        let temp = TempDir::new()?;
        let rel_path = Path::new("segment.parquet");
        write_timestamp_segment(
            &temp.path().join(rel_path),
            vec!["A", "A", "B", "C"],
            vec![Some(0), Some(0), None, None],
            None,
        )?;

        let coverage = compute_segment_entity_coverage(
            &TableLocation::local(temp.path()),
            rel_path,
            &timestamp_index(),
        )
        .await?;

        assert_eq!(coverage.identity_count(), 3);
        assert_eq!(coverage.cardinality(), 1);
        assert_eq!(buckets(&coverage, "A"), vec![EPOCH_BUCKET]);
        assert!(coverage.get(&identity("B")).expect("B coverage").is_empty());
        assert!(coverage.get(&identity("C")).expect("C coverage").is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn statistics_presence_does_not_change_coverage() -> TestResult {
        let temp = TempDir::new()?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["A", "B", "A"])),
                Arc::new(TimestampMillisecondArray::from(vec![0, 0, 3_600_000])),
            ],
        )?;
        let with_stats = Path::new("with-stats.parquet");
        let without_stats = Path::new("without-stats.parquet");
        write_batch(
            &temp.path().join(with_stats),
            &batch,
            Some(
                WriterProperties::builder()
                    .set_statistics_enabled(EnabledStatistics::Chunk)
                    .build(),
            ),
        )?;
        write_batch(
            &temp.path().join(without_stats),
            &batch,
            Some(
                WriterProperties::builder()
                    .set_statistics_enabled(EnabledStatistics::None)
                    .build(),
            ),
        )?;

        let location = TableLocation::local(temp.path());
        let with_stats =
            compute_segment_entity_coverage(&location, with_stats, &timestamp_index()).await?;
        let without_stats =
            compute_segment_entity_coverage(&location, without_stats, &timestamp_index()).await?;

        assert_eq!(with_stats, without_stats);
        Ok(())
    }
}
