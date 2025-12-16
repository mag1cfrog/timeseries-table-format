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

use arrow::{
    datatypes::{DataType, TimeUnit},
    error::ArrowError,
};
use arrow_array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use bytes::Bytes;
use parquet::{
    arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder},
    errors::ParquetError,
};
use roaring::RoaringBitmap;
use snafu::Snafu;

use crate::{
    common::time_column::TimeColumnError,
    coverage::Coverage,
    helpers::time_bucket::bucket_id_from_epoch_secs,
    storage::{self, StorageError, TableLocation},
    transaction_log::TimeBucket,
};

/// Errors that can occur when reading or computing segment coverage.
///
/// Coverage computation typically:
/// 1. Reads the Parquet segment file from storage.
/// 2. Inspects the Parquet schema to locate the timestamp column.
/// 3. Validates that the timestamp column uses a supported type.
/// 4. Iterates over row group statistics or raw values to map timestamps to buckets.
/// 5. Stores computed bucket IDs in a RoaringBitmap for efficient serialization.
///
/// Errors at any stage are captured here with context about the segment path,
/// column name, and raw values involved.
#[derive(Debug, Snafu)]
pub enum SegmentCoverageError {
    /// Storage layer failed to read the segment file at the given path.
    ///
    /// This may indicate the file is missing, inaccessible, or suffered an I/O error.
    #[snafu(display("Storage error reading parquet bytes for {path}: {source}"))]
    Storage {
        /// The path to the segment file that could not be read.
        path: String,
        /// The underlying storage error that caused this failure.
        #[snafu(source)]
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
    },

    /// Arrow read error.
    #[snafu(display("Arrow read error for {path}: {source}"))]
    ArrowRead {
        /// The path to the segment file with a Parquet format error.
        path: String,
        /// The underlying Parquet library error.
        #[snafu(source)]
        source: ArrowError,
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
    let path_str = rel_path.display().to_string();

    // 1) Read parquet bytes.
    let bytes = storage::read_all_bytes(location, rel_path)
        .await
        .map_err(|source| SegmentCoverageError::Storage {
            path: path_str.clone(),
            source,
        })?;
    let data = Bytes::from(bytes);

    // 2) Build parquet -> arrow batch reader.
    let builder = ParquetRecordBatchReaderBuilder::try_new(data).map_err(|source| {
        SegmentCoverageError::ParquetRead {
            path: path_str.clone(),
            source,
        }
    })?;

    // 3) Find the time column index, and ideally project only that one.
    let schema = builder.schema();
    // Validate the column exists in the Arrow schema (good error message)
    let _arrow_idx =
        schema
            .index_of(time_column)
            .map_err(|_| SegmentCoverageError::TimeColumn {
                path: path_str.clone(),
                source: TimeColumnError::Missing {
                    column: time_column.to_string(),
                },
            })?;

    let mask = ProjectionMask::columns(builder.parquet_schema(), [time_column]);
    let builder = builder.with_projection(mask);

    let reader = builder
        .build()
        .map_err(|source| SegmentCoverageError::ParquetRead {
            path: path_str.clone(),
            source,
        })?;

    // 4) Compute coverage.
    let mut bitmap = RoaringBitmap::new();

    for batch_res in reader {
        let batch = batch_res.map_err(|source| SegmentCoverageError::ArrowRead {
            path: path_str.clone(),
            source,
        })?;

        // After projection, the timestamp column is at 0;
        let col = batch.column(0);

        match col.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| SegmentCoverageError::TimeColumn {
                            path: path_str.clone(),
                            source: TimeColumnError::UnsupportedArrowType {
                                column: time_column.to_string(),
                                datatype: col.data_type().to_string(),
                            },
                        })?;
                    if arr.null_count() == 0 {
                        add_buckets_from_values(
                            &mut bitmap,
                            &path_str,
                            bucket_spec,
                            *unit,
                            arr.values(),
                        )?;
                    } else {
                        add_buckets_from_iter(
                            &mut bitmap,
                            &path_str,
                            bucket_spec,
                            *unit,
                            arr.iter(),
                        )?;
                    }
                }

                TimeUnit::Millisecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| SegmentCoverageError::TimeColumn {
                            path: path_str.clone(),
                            source: TimeColumnError::UnsupportedArrowType {
                                column: time_column.to_string(),
                                datatype: col.data_type().to_string(),
                            },
                        })?;

                    if arr.null_count() == 0 {
                        add_buckets_from_values(
                            &mut bitmap,
                            &path_str,
                            bucket_spec,
                            *unit,
                            arr.values(),
                        )?;
                    } else {
                        add_buckets_from_iter(
                            &mut bitmap,
                            &path_str,
                            bucket_spec,
                            *unit,
                            arr.iter(),
                        )?;
                    }
                }

                TimeUnit::Microsecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| SegmentCoverageError::TimeColumn {
                            path: path_str.clone(),
                            source: TimeColumnError::UnsupportedArrowType {
                                column: time_column.to_string(),
                                datatype: col.data_type().to_string(),
                            },
                        })?;

                    if arr.null_count() == 0 {
                        add_buckets_from_values(
                            &mut bitmap,
                            &path_str,
                            bucket_spec,
                            *unit,
                            arr.values(),
                        )?;
                    } else {
                        add_buckets_from_iter(
                            &mut bitmap,
                            &path_str,
                            bucket_spec,
                            *unit,
                            arr.iter(),
                        )?;
                    }
                }

                TimeUnit::Nanosecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| SegmentCoverageError::TimeColumn {
                            path: path_str.clone(),
                            source: TimeColumnError::UnsupportedArrowType {
                                column: time_column.to_string(),
                                datatype: col.data_type().to_string(),
                            },
                        })?;

                    if arr.null_count() == 0 {
                        add_buckets_from_values(
                            &mut bitmap,
                            &path_str,
                            bucket_spec,
                            *unit,
                            arr.values(),
                        )?;
                    } else {
                        add_buckets_from_iter(
                            &mut bitmap,
                            &path_str,
                            bucket_spec,
                            *unit,
                            arr.iter(),
                        )?;
                    }
                }
            },

            other => {
                return Err(SegmentCoverageError::TimeColumn {
                    path: path_str.clone(),
                    source: TimeColumnError::UnsupportedArrowType {
                        column: time_column.to_string(),
                        datatype: other.to_string(),
                    },
                });
            }
        }
    }

    Ok(Coverage::from_bitmap(bitmap))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::{
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use arrow_array::builder::{Int32Builder, StringBuilder, TimestampMillisecondBuilder};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn write_parquet_batch(
        path: &Path,
        schema: Schema,
        columns: Vec<Arc<dyn Array>>,
    ) -> TestResult {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;

        let file = std::fs::File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(&batch)?;
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
    async fn compute_coverage_errors_on_missing_time_column() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/seg.parquet");
        let abs_path = tmp.path().join(rel_path);
        write_parquet_with_timestamps(&abs_path, &[Some(1_000)])?;

        let location = TableLocation::local(tmp.path());
        let err = compute_segment_coverage(&location, rel_path, "missing_ts", &TimeBucket::Minutes(1))
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
        let err =
            compute_segment_coverage(&location, rel_path, "ts", &TimeBucket::Minutes(1))
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
        let err =
            compute_segment_coverage(&location, rel_path, "ts", &TimeBucket::Seconds(1))
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
        let err =
            compute_segment_coverage(&location, rel_path, "ts", &TimeBucket::Minutes(1))
                .await
                .expect_err("expected parquet read error");

        assert!(matches!(err, SegmentCoverageError::ParquetRead { .. }));
        Ok(())
    }
}
