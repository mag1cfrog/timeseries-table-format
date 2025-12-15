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
