//! Helpers for reading and computing segment-level ordered-index coverage.
//!
//! This module provides utilities for analyzing Parquet segments to extract
//! coverage metadata: bucket assignments for ordered-index values within each
//! segment. Coverage data is persisted in a RoaringTreemap sidecar
//! file and referenced by the transaction log for efficient time-range queries.
//!
//! The error types in this module cover common failure points:
//! - Storage I/O errors when accessing segment files.
//! - Parquet format violations or missing/malformed metadata.
//! - Unsupported or out-of-range ordered-index values.

use std::path::Path;

use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{
    Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use chrono::{TimeZone, Utc};
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
use snafu::{Backtrace, Snafu};
use tokio::task::JoinSet;

use crate::{
    coverage::bucket::{BucketError, LogicalBucketRange, bucket_id, logical_bucket_range},
    coverage::{Bucket, Coverage, EntityIdentity, EntityIdentityError},
    metadata::{
        segments::ParquetIndexColumnError,
        table_metadata::{IndexKind, IndexSpec, IndexValue},
    },
    storage::{StorageError, TableLocation, open_parquet_reader},
};

use super::schema::validate_parquet_index;
use super::{INSPECTION_BATCH_SIZE, resolve_rg_settings};

/// Errors that can occur when reading or computing segment coverage.
///
/// Coverage computation typically:
/// 1. Reads the Parquet segment file from storage.
/// 2. Inspects the Parquet schema to locate the registered index column.
/// 3. Validates that the column matches the registered index domain.
/// 4. Streams projected index values and maps them to buckets.
/// 5. Stores computed bucket IDs in a RoaringTreemap for efficient serialization.
///
/// Errors at any stage are captured here with context about the segment path,
/// column name, and raw values involved.
#[derive(Debug, Snafu)]
pub enum SegmentCoverageError {
    /// Storage layer failed to read the segment file at the given path.
    ///
    /// This may indicate the file is missing, inaccessible, or suffered an I/O error.
    #[snafu(display("Storage error reading parquet file {path}: {source}"))]
    Storage {
        /// The path to the segment file that could not be read.
        path: String,
        /// The underlying storage error that caused this failure.
        #[snafu(source, backtrace)]
        source: StorageError,
    },

    /// Parquet format violation or metadata read error.
    ///
    /// This may indicate the file is corrupted, truncated, or uses an unsupported
    /// Parquet feature.
    #[snafu(display("Parquet read error for {path}: {source}"))]
    ParquetRead {
        /// The path to the segment file with a Parquet format error.
        path: String,
        /// The underlying Parquet library error.
        #[snafu(source)]
        source: ParquetError,
        /// The backtrace at the time the error occurred.
        backtrace: Backtrace,
    },

    /// The registered ordered-index column is missing or incompatible.
    #[snafu(transparent)]
    OrderedIndexColumn {
        /// Exact registered and observed Parquet column details.
        source: ParquetIndexColumnError,
    },

    /// A projected ordered-index value cannot be represented in its registered domain.
    #[snafu(display(
        "Invalid {expected_domain} value for ordered-index column {column} in segment at {path}: {detail}"
    ))]
    IndexValue {
        /// Path to the segment file.
        path: String,
        /// Registered ordered-index column.
        column: String,
        /// Registered ordered-index domain.
        expected_domain: &'static str,
        /// Value decoding failure.
        detail: String,
    },

    /// Ordered-index bucket mapping failed.
    #[snafu(display("Bucket mapping failed for segment {path}: {source}"))]
    Bucket {
        /// The path to the segment file.
        path: String,
        /// Bucket mapping failure.
        source: BucketError,
    },

    /// Two rows for the same entity occupy one ordered-index interval.
    #[snafu(display(
        "Duplicate ordered-index interval {example_index_interval} in segment {path}"
    ))]
    DuplicateIndexInterval {
        /// Path to the segment file.
        path: String,
        /// Complete entity identity, or `None` for a table without entity columns.
        example_identity: Option<EntityIdentity>,
        /// Logical ordered-index interval occupied by both rows.
        example_index_interval: LogicalBucketRange,
    },

    /// A configured entity column is missing from the segment.
    #[snafu(display("Entity column not found in {path}: {column}"))]
    EntityColumnNotFound {
        /// Path to the segment file.
        path: String,
        /// Missing configured entity column.
        column: String,
    },

    /// A configured entity column has an unsupported Arrow type.
    #[snafu(display("Unsupported entity column type in {path}: {column} has {datatype}"))]
    EntityColumnUnsupportedType {
        /// Path to the segment file.
        path: String,
        /// Configured entity column.
        column: String,
        /// Observed Arrow type.
        datatype: String,
    },

    /// A configured entity column contains a null value.
    #[snafu(display("Entity column contains nulls in {path}: {column}"))]
    EntityColumnHasNull {
        /// Path to the segment file.
        path: String,
        /// Configured entity column.
        column: String,
    },

    /// The segment has no rows from which to construct an entity identity.
    #[snafu(display("Entity column has no values (empty segment) in {path}: {column}"))]
    EntityColumnEmpty {
        /// Path to the segment file.
        path: String,
        /// First configured entity column.
        column: String,
    },

    /// Ordered entity components could not form a complete identity.
    #[snafu(display("Invalid entity identity in segment {path}: {source}"))]
    EntityIdentity {
        /// Path to the segment file.
        path: String,
        /// Identity validation failure.
        source: EntityIdentityError,
    },
}

pub(super) fn arrow_index_error(
    path: &str,
    index: &IndexSpec,
    observed_type: String,
) -> SegmentCoverageError {
    SegmentCoverageError::OrderedIndexColumn {
        source: ParquetIndexColumnError {
            path: path.to_string(),
            column: index.column.clone(),
            expected_domain: index.kind.name(),
            observed_type,
        },
    }
}

pub(super) fn timestamp_value(
    path: &str,
    index: &IndexSpec,
    unit: TimeUnit,
    raw: i64,
) -> Result<IndexValue, SegmentCoverageError> {
    let value = match unit {
        TimeUnit::Second => Utc.timestamp_opt(raw, 0),
        TimeUnit::Millisecond => Utc.timestamp_millis_opt(raw),
        TimeUnit::Microsecond => Utc.timestamp_micros(raw),
        TimeUnit::Nanosecond => {
            let seconds = raw.div_euclid(1_000_000_000);
            let nanos = raw.rem_euclid(1_000_000_000) as u32;
            Utc.timestamp_opt(seconds, nanos)
        }
    };
    value
        .single()
        .map(IndexValue::Timestamp)
        .ok_or_else(|| SegmentCoverageError::IndexValue {
            path: path.to_string(),
            column: index.column.clone(),
            expected_domain: index.kind.name(),
            detail: format!("timestamp value {raw} is out of range for {unit:?}"),
        })
}

pub(super) fn map_and_insert_bucket(
    bitmap: &mut RoaringTreemap,
    path: &str,
    index: &IndexSpec,
    value: IndexValue,
) -> Result<(Bucket, bool), SegmentCoverageError> {
    let bucket = bucket_id(&index.kind, &value).map_err(|source| SegmentCoverageError::Bucket {
        path: path.to_string(),
        source,
    })?;
    Ok((bucket, bitmap.insert(bucket)))
}

pub(super) fn duplicate_index_interval_error(
    path: &str,
    index: &IndexSpec,
    identity: Option<&EntityIdentity>,
    bucket: Bucket,
) -> SegmentCoverageError {
    match logical_bucket_range(&index.kind, bucket) {
        Ok(example_index_interval) => SegmentCoverageError::DuplicateIndexInterval {
            path: path.to_string(),
            example_identity: identity.cloned(),
            example_index_interval,
        },
        Err(source) => SegmentCoverageError::Bucket {
            path: path.to_string(),
            source,
        },
    }
}

fn add_array_buckets<T, F>(
    bitmap: &mut RoaringTreemap,
    path: &str,
    index: &IndexSpec,
    array: &arrow_array::PrimitiveArray<T>,
    mut to_value: F,
) -> Result<(), SegmentCoverageError>
where
    T: arrow_array::types::ArrowPrimitiveType,
    F: FnMut(T::Native) -> Result<IndexValue, SegmentCoverageError>,
{
    if array.null_count() == 0 {
        for &raw in array.values() {
            let (bucket, inserted) = map_and_insert_bucket(bitmap, path, index, to_value(raw)?)?;
            if !inserted {
                return Err(duplicate_index_interval_error(path, index, None, bucket));
            }
        }
    } else {
        for raw in array.iter().flatten() {
            let (bucket, inserted) = map_and_insert_bucket(bitmap, path, index, to_value(raw)?)?;
            if !inserted {
                return Err(duplicate_index_interval_error(path, index, None, bucket));
            }
        }
    }
    Ok(())
}

async fn compute_coverage_bitmap_from_stream(
    mut reader: impl Stream<
        Item = Result<arrow::record_batch::RecordBatch, parquet::errors::ParquetError>,
    > + Unpin,
    path_str: &str,
    index: &IndexSpec,
) -> Result<RoaringTreemap, SegmentCoverageError> {
    let mut bitmap = RoaringTreemap::new();

    while let Some(batch_res) = reader.next().await {
        let batch = batch_res.map_err(|source| SegmentCoverageError::ParquetRead {
            path: path_str.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })?;

        let col = batch.column(0);

        match (&index.kind, col.data_type()) {
            (IndexKind::Timestamp { .. }, DataType::Timestamp(unit, _)) => {
                macro_rules! process_timestamp_array {
                    ($array_type:ty) => {{
                        let array =
                            col.as_any().downcast_ref::<$array_type>().ok_or_else(|| {
                                arrow_index_error(
                                    path_str,
                                    index,
                                    format!("Arrow {}", col.data_type()),
                                )
                            })?;
                        add_array_buckets(&mut bitmap, path_str, index, array, |raw| {
                            timestamp_value(path_str, index, unit.clone(), raw)
                        })?;
                    }};
                }
                match unit {
                    TimeUnit::Second => process_timestamp_array!(TimestampSecondArray),
                    TimeUnit::Millisecond => {
                        process_timestamp_array!(TimestampMillisecondArray)
                    }
                    TimeUnit::Microsecond => {
                        process_timestamp_array!(TimestampMicrosecondArray)
                    }
                    TimeUnit::Nanosecond => process_timestamp_array!(TimestampNanosecondArray),
                }
            }
            (IndexKind::Int64 { .. }, DataType::Int64) => {
                let array = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    arrow_index_error(path_str, index, format!("Arrow {}", col.data_type()))
                })?;
                add_array_buckets(&mut bitmap, path_str, index, array, |raw| {
                    Ok(IndexValue::Int64(raw))
                })?;
            }
            (IndexKind::UInt64 { .. }, DataType::UInt64) => {
                let array = col.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                    arrow_index_error(path_str, index, format!("Arrow {}", col.data_type()))
                })?;
                add_array_buckets(&mut bitmap, path_str, index, array, |raw| {
                    Ok(IndexValue::UInt64(raw))
                })?;
            }
            other => {
                return Err(arrow_index_error(
                    path_str,
                    index,
                    format!("Arrow {other:?}"),
                ));
            }
        }

        tokio::task::yield_now().await;
    }

    Ok(bitmap)
}

/// Computes segment-level ordered-index coverage from a Parquet segment file.
///
/// This function:
/// 1. Reads the Parquet segment file from storage.
/// 2. Validates and projects the registered ordered-index column.
/// 3. Iterates over non-null values and maps each through the shared bucket helper.
/// 4. Returns a Coverage bitmap containing all bucket IDs found in the segment.
///
/// # Arguments
///
/// * `location` - The table location for accessing the storage layer.
/// * `rel_path` - The relative path to the Parquet segment file.
/// * `index` - The registered ordered-index column, domain, and bucket configuration.
///
/// # Returns
///
/// A `Coverage` bitmap containing the bucket IDs of all observed index values in
/// the segment, or a `SegmentCoverageError` if any stage of the process fails.
pub async fn compute_segment_coverage(
    location: &TableLocation,
    rel_path: &Path,
    index: &IndexSpec,
) -> Result<Coverage, SegmentCoverageError> {
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
    validate_parquet_index(&path, metadata.parquet_schema(), index)
        .map_err(|source| SegmentCoverageError::OrderedIndexColumn { source })?;
    drop(file);

    let mask = ProjectionMask::columns(metadata.parquet_schema(), [index.column.as_str()]);
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
            compute_coverage_bitmap_from_stream(reader, &path, &index).await
        });
    }

    let mut merged = RoaringTreemap::new();
    while let Some(result) = tasks.join_next().await {
        let bitmap = result.map_err(|source| SegmentCoverageError::ParquetRead {
            path: path.clone(),
            source: ParquetError::General(format!("row-group scan task failed: {source}")),
            backtrace: Backtrace::capture(),
        })??;
        if !merged.is_disjoint(&bitmap)
            && let Some(duplicate) = (&merged & &bitmap).min()
        {
            return Err(duplicate_index_interval_error(
                &path, index, None, duplicate,
            ));
        }
        merged |= bitmap;
    }

    Ok(Coverage::from_treemap(merged))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::File, io::SeekFrom, num::NonZeroU64, sync::Arc};

    use crate::metadata::table_metadata::TimeIndexGranularity;
    use arrow::{
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use arrow_array::builder::{
        BinaryBuilder, Int32Builder, StringBuilder, TimestampMillisecondBuilder,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::{
        basic::Compression,
        file::{
            properties::WriterProperties,
            reader::{FileReader, SerializedFileReader},
        },
    };
    use tempfile::TempDir;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    type TestResult = Result<(), Box<dyn std::error::Error>>;
    const EPOCH_BUCKET: u64 = 0x8000_0000_0000_0000;

    fn timestamp_index(column: &str, index_granularity: TimeIndexGranularity) -> IndexSpec {
        IndexSpec {
            column: column.to_string(),
            entity_columns: Vec::new(),
            kind: IndexKind::Timestamp {
                index_granularity,
                timezone: None,
            },
        }
    }

    fn int64_index(column: &str, index_granularity: u64) -> IndexSpec {
        IndexSpec {
            column: column.to_string(),
            entity_columns: Vec::new(),
            kind: IndexKind::Int64 {
                index_granularity: NonZeroU64::new(index_granularity).expect("nonzero test bucket"),
            },
        }
    }

    fn uint64_index(column: &str, index_granularity: u64) -> IndexSpec {
        IndexSpec {
            column: column.to_string(),
            entity_columns: Vec::new(),
            kind: IndexKind::UInt64 {
                index_granularity: NonZeroU64::new(index_granularity).expect("nonzero test bucket"),
            },
        }
    }

    fn write_parquet_batch(
        path: &Path,
        schema: Schema,
        columns: Vec<Arc<dyn Array>>,
    ) -> TestResult {
        let schema = Arc::new(schema);
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        write_parquet_batches(
            path,
            schema,
            vec![batch],
            WriterProperties::builder().build(),
        )
    }

    fn write_parquet_batches(
        path: &Path,
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        props: WriterProperties,
    ) -> TestResult {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(props))?;
        for batch in batches {
            writer.write(&batch)?;
            writer.flush()?;
        }
        writer.close()?;
        Ok(())
    }

    fn write_parquet_with_timestamps(path: &Path, ts_values: &[Option<i64>]) -> TestResult {
        let schema = Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("val", DataType::Int32, false),
        ]);

        let mut ts_builder = TimestampMillisecondBuilder::with_capacity(ts_values.len());
        for v in ts_values {
            match v {
                Some(ts) => ts_builder.append_value(*ts),
                None => ts_builder.append_null(),
            }
        }
        let ts_array = Arc::new(ts_builder.finish()) as Arc<dyn Array>;

        let mut val_builder = Int32Builder::with_capacity(ts_values.len());
        for i in 0..ts_values.len() {
            val_builder.append_value(i as i32);
        }
        let val_array = Arc::new(val_builder.finish()) as Arc<dyn Array>;

        write_parquet_batch(path, schema, vec![ts_array, val_array])
    }

    fn timestamp_batch(schema: Arc<Schema>, values: &[Option<i64>]) -> RecordBatch {
        let timestamps = Arc::new(TimestampMillisecondArray::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![timestamps]).expect("timestamp batch")
    }

    fn int64_batch(schema: Arc<Schema>, values: &[Option<i64>]) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
            .expect("int64 batch")
    }

    fn uint64_batch(schema: Arc<Schema>, values: &[Option<u64>]) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values.to_vec()))])
            .expect("uint64 batch")
    }

    fn expected_buckets(
        index: &IndexSpec,
        values: impl IntoIterator<Item = IndexValue>,
    ) -> Vec<u64> {
        let mut buckets = values
            .into_iter()
            .map(|value| bucket_id(&index.kind, &value).expect("valid test index value"))
            .collect::<Vec<_>>();
        buckets.sort_unstable();
        buckets.dedup();
        buckets
    }

    #[tokio::test]
    async fn compute_coverage_supports_nulls_and_multiple_specs() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/seg.parquet");
        let abs_path = tmp.path().join(rel_path);

        let ts_values = vec![Some(1_000), Some(3_600_000), None];
        write_parquet_with_timestamps(&abs_path, &ts_values)?;

        let location = TableLocation::local(tmp.path());

        let cov_min = compute_segment_coverage(
            &location,
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
        )
        .await?;
        let buckets_min: Vec<u64> = cov_min.present().iter().collect();
        assert_eq!(buckets_min, vec![EPOCH_BUCKET, EPOCH_BUCKET + 60]);

        let cov_hr = compute_segment_coverage(
            &location,
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Hours(1)),
        )
        .await?;
        let buckets_hr: Vec<u64> = cov_hr.present().iter().collect();
        assert_eq!(buckets_hr, vec![EPOCH_BUCKET, EPOCH_BUCKET + 1]);

        Ok(())
    }

    fn assert_implicit_duplicate(error: SegmentCoverageError, expected_path: &str) {
        match error {
            SegmentCoverageError::DuplicateIndexInterval {
                path,
                example_identity,
                example_index_interval,
            } => {
                assert_eq!(path, expected_path);
                assert_eq!(example_identity, None);
                assert_eq!(
                    example_index_interval.to_string(),
                    "[1970-01-01T00:00:00Z, 1970-01-01T00:01:00Z)"
                );
            }
            other => panic!("expected duplicate interval error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn compute_coverage_rejects_equal_and_distinct_timestamp_duplicates() -> TestResult {
        let tmp = TempDir::new()?;
        for (name, values) in [
            ("equal", [Some(1_000), Some(1_000)]),
            ("distinct", [Some(1_000), Some(30_000)]),
        ] {
            let rel_path = Path::new("data").join(format!("{name}-timestamp-duplicate.parquet"));
            write_parquet_with_timestamps(&tmp.path().join(&rel_path), &values)?;

            let error = compute_segment_coverage(
                &TableLocation::local(tmp.path()),
                &rel_path,
                &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
            )
            .await
            .expect_err("same-worker duplicate must be rejected");

            assert_implicit_duplicate(error, &rel_path.display().to_string());
        }
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_respects_exact_timestamp_boundary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/timestamp-boundary.parquet");
        write_parquet_with_timestamps(&tmp.path().join(rel_path), &[Some(59_999), Some(60_000)])?;

        let coverage = compute_segment_coverage(
            &TableLocation::local(tmp.path()),
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
        )
        .await?;

        assert_eq!(
            coverage.present().iter().collect::<Vec<_>>(),
            vec![EPOCH_BUCKET, EPOCH_BUCKET + 1]
        );
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_rejects_duplicate_across_parallel_workers() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/cross-worker-duplicate.parquet");
        assert_eq!(resolve_rg_settings(2), (2, 1));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        write_parquet_batches(
            &tmp.path().join(rel_path),
            Arc::clone(&schema),
            vec![
                timestamp_batch(Arc::clone(&schema), &[Some(1_000)]),
                timestamp_batch(Arc::clone(&schema), &[Some(30_000)]),
            ],
            WriterProperties::builder().build(),
        )?;

        let error = compute_segment_coverage(
            &TableLocation::local(tmp.path()),
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
        )
        .await
        .expect_err("cross-worker duplicate must be rejected");

        assert_implicit_duplicate(error, "data/cross-worker-duplicate.parquet");
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_merges_multiple_row_groups() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/row_groups.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let batches = vec![
            timestamp_batch(Arc::clone(&schema), &[Some(1_000), Some(61_000)]),
            timestamp_batch(Arc::clone(&schema), &[Some(121_000), None]),
            timestamp_batch(Arc::clone(&schema), &[Some(181_000), Some(241_000)]),
        ];
        write_parquet_batches(
            &tmp.path().join(rel_path),
            schema,
            batches,
            WriterProperties::builder().build(),
        )?;

        let coverage = compute_segment_coverage(
            &TableLocation::local(tmp.path()),
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
        )
        .await?;
        assert_eq!(
            coverage.present().iter().collect::<Vec<_>>(),
            vec![
                EPOCH_BUCKET,
                EPOCH_BUCKET + 1,
                EPOCH_BUCKET + 2,
                EPOCH_BUCKET + 3,
                EPOCH_BUCKET + 4
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_supports_integer_indexes_across_row_groups() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let signed_path = Path::new("data/int64-row-groups.parquet");
        let signed_schema = Arc::new(Schema::new(vec![Field::new(
            "index",
            DataType::Int64,
            true,
        )]));
        let signed_values = [i64::MIN, -11, -1, 0, 10, i64::MAX];
        write_parquet_batches(
            &tmp.path().join(signed_path),
            Arc::clone(&signed_schema),
            vec![
                int64_batch(Arc::clone(&signed_schema), &[Some(i64::MIN), Some(-11)]),
                int64_batch(Arc::clone(&signed_schema), &[None, Some(-1), Some(0)]),
                int64_batch(Arc::clone(&signed_schema), &[Some(10), Some(i64::MAX)]),
            ],
            WriterProperties::builder().build(),
        )?;
        let signed_index = int64_index("index", 10);
        let signed = compute_segment_coverage(&location, signed_path, &signed_index).await?;
        assert_eq!(
            signed.present().iter().collect::<Vec<_>>(),
            expected_buckets(
                &signed_index,
                signed_values.into_iter().map(IndexValue::Int64)
            )
        );

        let unsigned_path = Path::new("data/uint64-row-groups.parquet");
        let unsigned_schema = Arc::new(Schema::new(vec![Field::new(
            "index",
            DataType::UInt64,
            true,
        )]));
        let unsigned_values = [0, 10, i64::MAX as u64 + 1, u64::MAX];
        write_parquet_batches(
            &tmp.path().join(unsigned_path),
            Arc::clone(&unsigned_schema),
            vec![
                uint64_batch(Arc::clone(&unsigned_schema), &[Some(0)]),
                uint64_batch(Arc::clone(&unsigned_schema), &[None, Some(10)]),
                uint64_batch(
                    Arc::clone(&unsigned_schema),
                    &[Some(i64::MAX as u64 + 1), Some(u64::MAX)],
                ),
            ],
            WriterProperties::builder().build(),
        )?;
        let unsigned_index = uint64_index("index", 10);
        let unsigned = compute_segment_coverage(&location, unsigned_path, &unsigned_index).await?;
        assert_eq!(
            unsigned.present().iter().collect::<Vec<_>>(),
            expected_buckets(
                &unsigned_index,
                unsigned_values.into_iter().map(IndexValue::UInt64)
            )
        );
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_rejects_integer_duplicates_at_domain_boundaries() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        for (name, values, expected_range) in [
            ("negative", [-10, -1], "[-10, 0)"),
            ("zero", [0, 9], "[0, 10)"),
            (
                "maximum",
                [i64::MAX - 7, i64::MAX],
                "[9223372036854775800, 9223372036854775807]",
            ),
        ] {
            let rel_path = Path::new("data").join(format!("int64-{name}-duplicate.parquet"));
            write_parquet_batch(
                &tmp.path().join(&rel_path),
                Schema::new(vec![Field::new("index", DataType::Int64, false)]),
                vec![Arc::new(Int64Array::from(values.to_vec()))],
            )?;

            let error = compute_segment_coverage(&location, &rel_path, &int64_index("index", 10))
                .await
                .expect_err("signed duplicate must be rejected");
            assert!(matches!(
                error,
                SegmentCoverageError::DuplicateIndexInterval {
                    example_identity: None,
                    example_index_interval,
                    ..
                } if example_index_interval.to_string() == expected_range
            ));
        }

        for (name, values, expected_range) in [
            ("boundary", [10, 11], "[10, 20)"),
            (
                "maximum",
                [u64::MAX - 5, u64::MAX],
                "[18446744073709551610, 18446744073709551615]",
            ),
        ] {
            let rel_path = Path::new("data").join(format!("uint64-{name}-duplicate.parquet"));
            write_parquet_batch(
                &tmp.path().join(&rel_path),
                Schema::new(vec![Field::new("index", DataType::UInt64, false)]),
                vec![Arc::new(UInt64Array::from(values.to_vec()))],
            )?;

            let error = compute_segment_coverage(&location, &rel_path, &uint64_index("index", 10))
                .await
                .expect_err("unsigned duplicate must be rejected");
            assert!(matches!(
                error,
                SegmentCoverageError::DuplicateIndexInterval {
                    example_identity: None,
                    example_index_interval,
                    ..
                } if example_index_interval.to_string() == expected_range
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_scans_multiple_bounded_batches() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/batches.parquet");
        let row_count = INSPECTION_BATCH_SIZE * 2 + 17;
        let values = (0..row_count)
            .map(|value| Some(value as i64 * 1_000))
            .collect::<Vec<_>>();
        write_parquet_with_timestamps(&tmp.path().join(rel_path), &values)?;

        let coverage = compute_segment_coverage(
            &TableLocation::local(tmp.path()),
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Seconds(1)),
        )
        .await?;
        assert_eq!(coverage.cardinality(), row_count as u64);
        assert_eq!(coverage.present().min(), Some(EPOCH_BUCKET));
        assert_eq!(
            coverage.present().max(),
            Some(EPOCH_BUCKET + row_count as u64 - 1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_rejects_duplicate_across_decoder_batches() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/decoder-batch-duplicate.parquet");
        let mut values = (0..INSPECTION_BATCH_SIZE)
            .map(|value| Some(value as i64 * 60_000))
            .collect::<Vec<_>>();
        values[INSPECTION_BATCH_SIZE / 2] = None;
        values.push(Some(30_000));
        write_parquet_with_timestamps(&tmp.path().join(rel_path), &values)?;

        let error = compute_segment_coverage(
            &TableLocation::local(tmp.path()),
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
        )
        .await
        .expect_err("duplicate split across decoder batches must be rejected");

        assert_implicit_duplicate(error, "data/decoder-batch-duplicate.parquet");
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_supports_every_parquet_timestamp_unit() -> TestResult {
        let tmp = TempDir::new()?;
        let cases: Vec<(&str, DataType, Arc<dyn Array>)> = vec![
            (
                "milliseconds.parquet",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1_000),
                    Some(60_000),
                ])),
            ),
            (
                "microseconds.parquet",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_000_000),
                    Some(60_000_000),
                ])),
            ),
            (
                "nanoseconds.parquet",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(1_000_000_000),
                    Some(60_000_000_000),
                ])),
            ),
        ];

        for (file_name, data_type, array) in cases {
            let rel_path = Path::new("data").join(file_name);
            write_parquet_batch(
                &tmp.path().join(&rel_path),
                Schema::new(vec![Field::new("ts", data_type, true)]),
                vec![array],
            )?;
            let coverage = compute_segment_coverage(
                &TableLocation::local(tmp.path()),
                &rel_path,
                &timestamp_index("ts", TimeIndexGranularity::Seconds(1)),
            )
            .await?;
            assert_eq!(
                coverage.present().iter().collect::<Vec<_>>(),
                vec![EPOCH_BUCKET + 1, EPOCH_BUCKET + 60]
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_returns_empty_for_empty_and_all_null_files() -> TestResult {
        let tmp = TempDir::new()?;
        for (file_name, values) in [
            ("empty.parquet", Vec::new()),
            ("all_null.parquet", vec![None, None, None]),
        ] {
            let rel_path = Path::new("data").join(file_name);
            write_parquet_with_timestamps(&tmp.path().join(&rel_path), &values)?;
            let coverage = compute_segment_coverage(
                &TableLocation::local(tmp.path()),
                &rel_path,
                &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
            )
            .await?;
            assert!(coverage.present().is_empty());
        }
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_ignores_large_unprojected_payload() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/payload.parquet");
        let abs_path = tmp.path().join(rel_path);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("payload", DataType::Binary, false),
        ]));
        let timestamps = Arc::new(TimestampMillisecondArray::from(vec![
            1_000, 61_000, 121_000, 181_000,
        ]));
        let payload = vec![0xA5; 1024 * 1024];
        let mut payloads = BinaryBuilder::with_capacity(4, 4 * payload.len());
        for _ in 0..4 {
            payloads.append_value(&payload);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![timestamps, Arc::new(payloads.finish())],
        )?;
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        write_parquet_batches(&abs_path, schema, vec![batch], props)?;

        let reader = SerializedFileReader::new(File::open(&abs_path)?)?;
        let payload_page = reader.metadata().row_group(0).column(1).data_page_offset() as u64;
        drop(reader);
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&abs_path)
            .await?;
        file.seek(SeekFrom::Start(payload_page)).await?;
        file.write_all(&[0xFF; 32]).await?;
        file.flush().await?;
        drop(file);

        assert!(tokio::fs::metadata(&abs_path).await?.len() > 4 * 1024 * 1024);
        let coverage = compute_segment_coverage(
            &TableLocation::local(tmp.path()),
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
        )
        .await?;
        assert_eq!(
            coverage.present().iter().collect::<Vec<_>>(),
            vec![
                EPOCH_BUCKET,
                EPOCH_BUCKET + 1,
                EPOCH_BUCKET + 2,
                EPOCH_BUCKET + 3
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_errors_on_missing_time_column() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/seg.parquet");
        let abs_path = tmp.path().join(rel_path);
        write_parquet_with_timestamps(&abs_path, &[Some(1_000)])?;

        let location = TableLocation::local(tmp.path());
        let err = compute_segment_coverage(
            &location,
            rel_path,
            &timestamp_index("missing_ts", TimeIndexGranularity::Minutes(1)),
        )
        .await
        .expect_err("expected missing column error");

        assert!(matches!(
            err,
            SegmentCoverageError::OrderedIndexColumn {
                source: ParquetIndexColumnError {
                    ref column,
                    expected_domain: "timestamp",
                    ref observed_type,
                    ..
                }
            } if column == "missing_ts" && observed_type == "missing"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_rejects_unsupported_time_type() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/string_ts.parquet");
        let abs_path = tmp.path().join(rel_path);

        let schema = Schema::new(vec![
            Field::new("ts", DataType::Utf8, false),
            Field::new("val", DataType::Int32, false),
        ]);
        let mut ts_builder = StringBuilder::with_capacity(2, 8);
        ts_builder.append_value("a");
        ts_builder.append_value("b");
        let ts_array = Arc::new(ts_builder.finish()) as Arc<dyn Array>;

        let mut val_builder = Int32Builder::with_capacity(2);
        val_builder.append_value(1);
        val_builder.append_value(2);
        let val_array = Arc::new(val_builder.finish()) as Arc<dyn Array>;

        write_parquet_batch(&abs_path, schema, vec![ts_array, val_array])?;

        let location = TableLocation::local(tmp.path());
        let err = compute_segment_coverage(
            &location,
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
        )
        .await
        .expect_err("expected unsupported arrow type");

        assert!(matches!(
            err,
            SegmentCoverageError::OrderedIndexColumn {
                source: ParquetIndexColumnError {
                    expected_domain: "timestamp",
                    ref observed_type,
                    ..
                }
            } if observed_type.contains("BYTE_ARRAY")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_rejects_signed_unsigned_mismatch() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/signed.parquet");
        write_parquet_batch(
            &tmp.path().join(rel_path),
            Schema::new(vec![Field::new("index", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )?;

        let error = compute_segment_coverage(
            &TableLocation::local(tmp.path()),
            rel_path,
            &uint64_index("index", 1),
        )
        .await
        .expect_err("signed column must not be read as uint64");

        assert!(matches!(
            error,
            SegmentCoverageError::OrderedIndexColumn {
                source: ParquetIndexColumnError {
                    expected_domain: "uint64",
                    observed_type,
                    ..
                }
            } if observed_type.contains("logical=None")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_supports_buckets_above_u32() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/overflow.parquet");
        let abs_path = tmp.path().join(rel_path);
        let overflow_ms = ((u32::MAX as i64) + 1) * 1_000;
        write_parquet_with_timestamps(&abs_path, &[Some(overflow_ms)])?;

        let location = TableLocation::local(tmp.path());
        let coverage = compute_segment_coverage(
            &location,
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Seconds(1)),
        )
        .await?;

        assert!(
            coverage
                .present()
                .contains(0x8000_0000_0000_0000 + u64::from(u32::MAX) + 1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_bubbles_up_storage_errors() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("missing/seg.parquet");
        let location = TableLocation::local(tmp.path());

        let err = compute_segment_coverage(
            &location,
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
        )
        .await
        .expect_err("expected storage error");

        assert!(matches!(
            err,
            SegmentCoverageError::Storage {
                source: StorageError::NotFound { .. },
                ..
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_surfaces_parquet_read_errors() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/corrupt.parquet");
        let abs_path = tmp.path().join(rel_path);
        if let Some(parent) = abs_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&abs_path, b"not a parquet file")?;

        let location = TableLocation::local(tmp.path());
        let err = compute_segment_coverage(
            &location,
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
        )
        .await
        .expect_err("expected parquet read error");

        assert!(matches!(err, SegmentCoverageError::ParquetRead { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_surfaces_projected_column_corruption() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/corrupt_timestamp.parquet");
        let abs_path = tmp.path().join(rel_path);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(TimestampMillisecondArray::from(vec![
                1_000, 2_000,
            ]))],
        )?;
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        write_parquet_batches(&abs_path, schema, vec![batch], props)?;

        let reader = SerializedFileReader::new(File::open(&abs_path)?)?;
        let timestamp_page = reader.metadata().row_group(0).column(0).data_page_offset() as u64;
        drop(reader);
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&abs_path)
            .await?;
        file.seek(SeekFrom::Start(timestamp_page)).await?;
        file.write_all(&[0xFF; 16]).await?;
        file.flush().await?;
        drop(file);

        let err = compute_segment_coverage(
            &TableLocation::local(tmp.path()),
            rel_path,
            &timestamp_index("ts", TimeIndexGranularity::Minutes(1)),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SegmentCoverageError::ParquetRead { .. }));
        Ok(())
    }
}
