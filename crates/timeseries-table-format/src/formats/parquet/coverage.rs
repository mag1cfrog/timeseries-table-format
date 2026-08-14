//! Helpers for reading and computing segment-level time-series coverage.
//!
//! This module provides utilities for analyzing Parquet segments to extract
//! time-series coverage metadata: bucket assignments for timestamps within each
//! segment. Coverage data is typically persisted in a RoaringBitmap sidecar
//! file and referenced by the transaction log for efficient time-range queries.
//!
//! The error types in this module cover common failure points:
//! - Storage I/O errors when accessing segment files.
//! - Parquet format violations or missing/malformed metadata.
//! - Unsupported or out-of-range timestamp values.
//! - Bucket ID overflow (when a bucket index exceeds u32 range).

use std::path::Path;

use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
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
use roaring::RoaringBitmap;
use snafu::{Backtrace, Snafu};
use tokio::task::JoinSet;

use crate::{
    coverage::Coverage,
    coverage::bucket::bucket_id_from_epoch_secs,
    metadata::table_metadata::TimeBucket,
    metadata::time_column::TimeColumnError,
    storage::{StorageError, TableLocation, open_parquet_reader},
};

use super::{INSPECTION_BATCH_SIZE, resolve_rg_settings};

/// Errors that can occur when reading or computing segment coverage.
///
/// Coverage computation typically:
/// 1. Reads the Parquet segment file from storage.
/// 2. Inspects the Parquet schema to locate the timestamp column.
/// 3. Validates that the timestamp column uses a supported type.
/// 4. Streams projected timestamp values and maps them to buckets.
/// 5. Stores computed bucket IDs in a RoaringBitmap for efficient serialization.
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

    /// Time column validation or metadata error.
    ///
    /// This may occur when the timestamp column is missing, has an unsupported type,
    /// or fails validation during coverage computation.
    #[snafu(display("Time column error in segment at {path}: {source}"))]
    TimeColumn {
        /// The path to the segment file with a time column error.
        path: String,
        /// The underlying time column error.
        source: TimeColumnError,
    },

    /// A computed bucket ID exceeds u32 range and cannot be stored in the coverage bitmap.
    ///
    /// RoaringBitmap uses u32 bucket IDs; bucket computations that yield larger
    /// values indicate a mismatch between segment data and the configured time bucket
    /// specification.
    #[snafu(display("Bucket id {bucket_id} does not fit into u32 bucket domain in {path}"))]
    BucketOverflow {
        /// The path to the segment file.
        path: String,
        /// The computed bucket ID that exceeded u32::MAX.
        bucket_id: u64,
    },
}

fn secs_from_raw(unit: TimeUnit, raw: i64) -> i64 {
    match unit {
        TimeUnit::Second => raw,
        TimeUnit::Millisecond => raw.div_euclid(1_000),
        TimeUnit::Microsecond => raw.div_euclid(1_000_000),
        TimeUnit::Nanosecond => raw.div_euclid(1_000_000_000),
    }
}

fn insert_bucket(
    bitmap: &mut RoaringBitmap,
    path: &str,
    bucket: u64,
) -> Result<(), SegmentCoverageError> {
    if bucket > u32::MAX as u64 {
        return Err(SegmentCoverageError::BucketOverflow {
            path: path.to_string(),
            bucket_id: bucket,
        });
    }
    bitmap.insert(bucket as u32);
    Ok(())
}

fn add_buckets_from_iter(
    bitmap: &mut RoaringBitmap,
    path: &str,
    spec: &TimeBucket,
    unit: TimeUnit,
    iter: impl Iterator<Item = Option<i64>>,
) -> Result<(), SegmentCoverageError> {
    for raw in iter.flatten() {
        let secs = secs_from_raw(unit, raw);
        let bucket = bucket_id_from_epoch_secs(spec, secs);

        insert_bucket(bitmap, path, bucket)?;
    }
    Ok(())
}

fn add_buckets_from_values(
    bitmap: &mut RoaringBitmap,
    path: &str,
    spec: &TimeBucket,
    unit: TimeUnit,
    values: &[i64],
) -> Result<(), SegmentCoverageError> {
    for &raw in values {
        let secs = secs_from_raw(unit, raw);
        let bucket = bucket_id_from_epoch_secs(spec, secs);
        insert_bucket(bitmap, path, bucket)?;
    }
    Ok(())
}

async fn compute_bitmap_from_stream(
    mut reader: impl Stream<
        Item = Result<arrow::record_batch::RecordBatch, parquet::errors::ParquetError>,
    > + Unpin,
    path_str: &str,
    time_column: &str,
    bucket_spec: &TimeBucket,
) -> Result<RoaringBitmap, SegmentCoverageError> {
    let mut bitmap = RoaringBitmap::new();

    macro_rules! process_timestamp_array {
        ($array_type: ty, $col: expr, $unit: expr) => {{
            let arr = $col.as_any().downcast_ref::<$array_type>().ok_or_else(|| {
                SegmentCoverageError::TimeColumn {
                    path: path_str.to_string(),
                    source: TimeColumnError::UnsupportedArrowType {
                        column: time_column.to_string(),
                        datatype: $col.data_type().to_string(),
                    },
                }
            })?;

            if arr.null_count() == 0 {
                add_buckets_from_values(&mut bitmap, path_str, bucket_spec, $unit, arr.values())
            } else {
                add_buckets_from_iter(&mut bitmap, path_str, bucket_spec, $unit, arr.iter())
            }
        }};
    }

    while let Some(batch_res) = reader.next().await {
        let batch = batch_res.map_err(|source| SegmentCoverageError::ParquetRead {
            path: path_str.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })?;

        let col = batch.column(0);

        match col.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => process_timestamp_array!(TimestampSecondArray, col, *unit)?,

                TimeUnit::Millisecond => {
                    process_timestamp_array!(TimestampMillisecondArray, col, *unit)?
                }

                TimeUnit::Microsecond => {
                    process_timestamp_array!(TimestampMicrosecondArray, col, *unit)?
                }

                TimeUnit::Nanosecond => {
                    process_timestamp_array!(TimestampNanosecondArray, col, *unit)?
                }
            },

            other => {
                return Err(SegmentCoverageError::TimeColumn {
                    path: path_str.to_string(),
                    source: TimeColumnError::UnsupportedArrowType {
                        column: time_column.to_string(),
                        datatype: other.to_string(),
                    },
                });
            }
        }

        tokio::task::yield_now().await;
    }

    Ok(bitmap)
}

/// Computes segment-level time-series coverage by reading a Parquet segment file
/// and mapping timestamps to bucket IDs based on the provided time bucket specification.
///
/// This function:
/// 1. Reads the Parquet segment file from storage.
/// 2. Extracts the specified timestamp column.
/// 3. Validates that the timestamp column uses a supported time unit.
/// 4. Iterates over timestamp values and maps each to a bucket ID.
/// 5. Returns a Coverage bitmap containing all bucket IDs found in the segment.
///
/// # Arguments
///
/// * `location` - The table location for accessing the storage layer.
/// * `rel_path` - The relative path to the Parquet segment file.
/// * `time_column` - The name of the timestamp column to analyze.
/// * `bucket_spec` - The time bucket specification for mapping timestamps to bucket IDs.
///
/// # Returns
///
/// A `Coverage` bitmap containing the bucket IDs of all timestamps in the segment,
/// or a `SegmentCoverageError` if any stage of the process fails.
pub async fn compute_segment_coverage(
    location: &TableLocation,
    rel_path: &Path,
    time_column: &str,
    bucket_spec: &TimeBucket,
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
    let field = metadata
        .schema()
        .field_with_name(time_column)
        .map_err(|_| SegmentCoverageError::TimeColumn {
            path: path.clone(),
            source: TimeColumnError::Missing {
                column: time_column.to_string(),
            },
        })?;
    if !matches!(field.data_type(), DataType::Timestamp(_, _)) {
        return Err(SegmentCoverageError::TimeColumn {
            path,
            source: TimeColumnError::UnsupportedArrowType {
                column: time_column.to_string(),
                datatype: field.data_type().to_string(),
            },
        });
    }
    drop(file);

    let mask = ProjectionMask::columns(metadata.parquet_schema(), [time_column]);
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
        let time_column = time_column.to_string();
        let bucket_spec = bucket_spec.clone();
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
            compute_bitmap_from_stream(reader, &path, &time_column, &bucket_spec).await
        });
    }

    let mut merged = RoaringBitmap::new();
    while let Some(result) = tasks.join_next().await {
        let bitmap = result.map_err(|source| SegmentCoverageError::ParquetRead {
            path: path.clone(),
            source: ParquetError::General(format!("row-group scan task failed: {source}")),
            backtrace: Backtrace::capture(),
        })??;
        merged |= bitmap;
    }

    Ok(Coverage::from_bitmap(merged))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::File, io::SeekFrom, sync::Arc};

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

    #[tokio::test]
    async fn compute_coverage_supports_nulls_and_dedup_and_multiple_specs() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/seg.parquet");
        let abs_path = tmp.path().join(rel_path);

        // Two points in bucket 0, one point in bucket 60 (1 hour), and one null.
        let ts_values = vec![Some(1_000), Some(30_000), Some(3_600_000), None];
        write_parquet_with_timestamps(&abs_path, &ts_values)?;

        let location = TableLocation::local(tmp.path());

        // Minutes bucket: 1 second and 30 seconds map to bucket 0; 3600s -> bucket 60.
        let cov_min =
            compute_segment_coverage(&location, rel_path, "ts", &TimeBucket::Minutes(1)).await?;
        let buckets_min: Vec<u32> = cov_min.present().iter().collect();
        assert_eq!(buckets_min, vec![0, 60]);

        // Hours bucket: 1 second -> bucket 0; 3600s -> bucket 1.
        let cov_hr =
            compute_segment_coverage(&location, rel_path, "ts", &TimeBucket::Hours(1)).await?;
        let buckets_hr: Vec<u32> = cov_hr.present().iter().collect();
        assert_eq!(buckets_hr, vec![0, 1]);

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
            timestamp_batch(Arc::clone(&schema), &[Some(181_000), Some(1_000)]),
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
            "ts",
            &TimeBucket::Minutes(1),
        )
        .await?;
        assert_eq!(
            coverage.present().iter().collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
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
            "ts",
            &TimeBucket::Seconds(1),
        )
        .await?;
        assert_eq!(coverage.cardinality(), row_count as u64);
        assert_eq!(coverage.present().min(), Some(0));
        assert_eq!(coverage.present().max(), Some(row_count as u32 - 1));
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_supports_every_timestamp_unit() -> TestResult {
        let tmp = TempDir::new()?;
        let cases: Vec<(&str, DataType, Arc<dyn Array>)> = vec![
            (
                "seconds.parquet",
                DataType::Timestamp(TimeUnit::Second, None),
                Arc::new(TimestampSecondArray::from(vec![Some(1), Some(60)])),
            ),
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
                "ts",
                &TimeBucket::Seconds(1),
            )
            .await?;
            assert_eq!(coverage.present().iter().collect::<Vec<_>>(), vec![1, 60]);
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
                "ts",
                &TimeBucket::Minutes(1),
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
            "ts",
            &TimeBucket::Minutes(1),
        )
        .await?;
        assert_eq!(
            coverage.present().iter().collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
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
        let err =
            compute_segment_coverage(&location, rel_path, "missing_ts", &TimeBucket::Minutes(1))
                .await
                .expect_err("expected missing column error");

        assert!(matches!(
            err,
            SegmentCoverageError::TimeColumn {
                source: TimeColumnError::Missing { ref column },
                ..
            } if column == "missing_ts"
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
        let err = compute_segment_coverage(&location, rel_path, "ts", &TimeBucket::Minutes(1))
            .await
            .expect_err("expected unsupported arrow type");

        assert!(matches!(
            err,
            SegmentCoverageError::TimeColumn {
                source: TimeColumnError::UnsupportedArrowType { ref datatype, .. },
                ..
            } if datatype == "Utf8"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_errors_on_bucket_overflow() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/overflow.parquet");
        let abs_path = tmp.path().join(rel_path);
        let overflow_ms = ((u32::MAX as i64) + 1) * 1_000;
        write_parquet_with_timestamps(&abs_path, &[Some(overflow_ms)])?;

        let location = TableLocation::local(tmp.path());
        let err = compute_segment_coverage(&location, rel_path, "ts", &TimeBucket::Seconds(1))
            .await
            .expect_err("expected bucket overflow error");

        assert!(matches!(
            err,
            SegmentCoverageError::BucketOverflow { bucket_id, .. }
            if bucket_id == (u32::MAX as u64 + 1)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn compute_coverage_bubbles_up_storage_errors() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("missing/seg.parquet");
        let location = TableLocation::local(tmp.path());

        let err = compute_segment_coverage(&location, rel_path, "ts", &TimeBucket::Minutes(1))
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
        let err = compute_segment_coverage(&location, rel_path, "ts", &TimeBucket::Minutes(1))
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
            "ts",
            &TimeBucket::Minutes(1),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SegmentCoverageError::ParquetRead { .. }));
        Ok(())
    }
}
