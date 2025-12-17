//! Helpers for extracting metadata from Parquet files.
//!
//! This module provides [`segment_meta_from_parquet_location`], an async function
//! that reads a Parquet file and constructs a [`SegmentMeta`] with:
//! - Minimum and maximum timestamps from a designated time column
//! - Row count from file metadata
//! - Segment ID and path supplied by the caller
//!
//! The timestamp extraction uses a two-stage approach:
//! 1. **Fast path**: Extract min/max from row-group statistics (if present)
//! 2. **Fallback**: Full row scan if statistics are missing or incomplete
//!
//! Timestamp units (millis, micros, nanos) are automatically detected from the
//! Parquet column's logical type. Both little-endian i64 byte encoding and
//! chrono's timestamp range are validated during conversion.
//!
//! Errors are reported with detailed context (file path, column name, specific
//! mismatch details) to aid debugging of Parquet schema mismatches or corruption.

use std::path::Path;

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::file::metadata::FileMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnDescPtr;
use snafu::Backtrace;

use crate::common::time_column::TimeColumnError;
use crate::storage::{self, TableLocation, read_all_bytes};
use crate::transaction_log::segments::{SegmentMetaError, SegmentResult, map_storage_error};
use crate::transaction_log::table_metadata::{LogicalDataType, LogicalTimestampUnit};
use crate::transaction_log::{
    FileFormat, LogicalColumn, LogicalSchema, LogicalSchemaError, SegmentId, SegmentMeta,
};

/// Convert little-endian i64 bytes into i64 with proper error handling.
fn le_bytes_to_i64(
    path: &str,
    column: &str,
    what: &str,
    bytes: &[u8],
) -> Result<i64, SegmentMetaError> {
    if bytes.len() != 8 {
        return Err(SegmentMetaError::ParquetStatsShape {
            path: path.to_string(),
            column: column.to_string(),
            detail: format!("{what} stats not 8 bytes (len={})", bytes.len()),
        });
    }

    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes); // this will panic only if lengths differ, which we just checked
    Ok(i64::from_le_bytes(buf))
}

fn ts_from_i64(unit: TimestampUnit, value: i64) -> Result<DateTime<Utc>, SegmentMetaError> {
    let dt_opt = match unit {
        TimestampUnit::Millis => Utc.timestamp_millis_opt(value),
        TimestampUnit::Micros => Utc.timestamp_micros(value),
        TimestampUnit::Nanos => {
            let secs = value.div_euclid(1_000_000_000);
            let nanos = value.rem_euclid(1_000_000_000) as u32;
            Utc.timestamp_opt(secs, nanos)
        }
    };

    dt_opt
        .single()
        .ok_or_else(|| SegmentMetaError::ParquetStatsShape {
            path: "<unknown>".to_string(),
            column: "<ts>".to_string(),
            detail: format!("timestamp value {value} out of chrono range"),
        })
}

/// Internal enum to capture which Parquet timestamp unit we selected.
#[derive(Debug, Clone, Copy)]
enum TimestampUnit {
    Millis,
    Micros,
    Nanos,
}

fn choose_timestamp_unit_from_logical(
    column: &str,
    physical: PhysicalType,
    logical: Option<&LogicalType>,
) -> Result<TimestampUnit, TimeColumnError> {
    if physical != PhysicalType::INT64 {
        return Err(TimeColumnError::UnsupportedParquetType {
            column: column.to_string(),
            physical: format!("{physical:?}"),
            logical: format!("{logical:?}"),
        });
    }

    match logical {
        Some(LogicalType::Timestamp { unit, .. }) => match unit {
            TimeUnit::MILLIS => Ok(TimestampUnit::Millis),
            TimeUnit::MICROS => Ok(TimestampUnit::Micros),
            TimeUnit::NANOS => Ok(TimestampUnit::Nanos),
        },
        other => Err(TimeColumnError::UnsupportedParquetType {
            column: column.to_string(),
            physical: format!("{physical:?}"),
            logical: format!("{other:?}"),
        }),
    }
}

/// Try to compute min/max timestamps from row-group statistics.
///
/// Returns Ok(Some((ts_min_raw, ts_max_raw))) if we can get stats,
/// Ok(None) if stats are missing or incomplete (caller should fall back
/// to row scan), or Err on hard failures.
fn min_max_from_stats(
    path: &str,
    column: &str,
    time_idx: usize,
    reader: &SerializedFileReader<Bytes>,
) -> Result<Option<(i64, i64)>, SegmentMetaError> {
    let meta = reader.metadata();

    let mut global_min: Option<i64> = None;
    let mut global_max: Option<i64> = None;

    for rg in meta.row_groups() {
        let col_meta = rg.column(time_idx);

        let stats = match col_meta.statistics() {
            Some(s) => s,
            None => continue,
        };

        let min_bytes_opt = stats.min_bytes_opt();
        let max_bytes_opt = stats.max_bytes_opt();

        let (min_bytes, max_bytes) = match (min_bytes_opt, max_bytes_opt) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                // Incomplete stats. bail out to row scan.
                return Ok(None);
            }
        };

        let group_min = le_bytes_to_i64(path, column, "min", min_bytes)?;
        let group_max = le_bytes_to_i64(path, column, "max", max_bytes)?;

        global_min = Some(match global_min {
            Some(prev) => prev.min(group_min),
            None => group_min,
        });

        global_max = Some(match global_max {
            Some(prev) => prev.max(group_max),
            None => group_max,
        });
    }

    match (global_min, global_max) {
        (Some(lo), Some(hi)) => Ok(Some((lo, hi))),
        _ => Ok(None),
    }
}

fn min_max_from_scan(
    path: &str,
    column: &str,
    time_idx: usize,
    reader: &SerializedFileReader<Bytes>,
) -> Result<(i64, i64), SegmentMetaError> {
    use parquet::record::Field;

    let iter = reader
        .get_row_iter(None)
        .map_err(|source| SegmentMetaError::ParquetRead {
            path: path.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })?;

    let mut min_val: Option<i64> = None;
    let mut max_val: Option<i64> = None;

    for row_res in iter {
        let row = row_res.map_err(|source| SegmentMetaError::ParquetRead {
            path: path.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })?;

        // Get raw field and extract i64 value from any timestamp type
        let field = row.get_column_iter().nth(time_idx).map(|(_, f)| f);
        let v = match field {
            Some(Field::Long(val)) => *val,
            Some(Field::TimestampMillis(val)) => *val,
            Some(Field::TimestampMicros(val)) => *val,
            _ => {
                return Err(SegmentMetaError::ParquetRead {
                    path: path.to_string(),
                    source: parquet::errors::ParquetError::General(format!(
                        "Cannot read timestamp from field at index {time_idx}"
                    )),
                    backtrace: Backtrace::capture(),
                });
            }
        };

        min_val = Some(match min_val {
            Some(prev) => prev.min(v),
            None => v,
        });
        max_val = Some(match max_val {
            Some(prev) => prev.max(v),
            None => v,
        });
    }

    match (min_val, max_val) {
        (Some(lo), Some(hi)) => Ok((lo, hi)),
        _ => Err(SegmentMetaError::ParquetStatsMissing {
            path: path.to_string(),
            column: column.to_string(),
        }),
    }
}

/// Build a `SegmentMeta` from in-memory Parquet bytes.
///
/// This mirrors `segment_meta_from_parquet_location` but operates on a provided
/// `Bytes` buffer instead of reading from storage. The caller supplies:
/// - `rel_path`: relative path of the segment within the table (for metadata/logging).
/// - `segment_id`: stable identifier to store in the resulting `SegmentMeta`.
/// - `time_column`: name of the timestamp column used for min/max extraction.
/// - `data`: Parquet file contents (complete file).
///
/// Behavior:
/// - Validates a minimal length before parsing (defensive guard).
/// - Reads Parquet metadata to get row count and locate the time column.
/// - Determines the timestamp unit from the column’s logical type.
/// - Tries to derive min/max timestamps from row-group stats; falls back to a
///   row scan if stats are missing or incomplete.
/// - Converts raw i64 timestamps to `DateTime<Utc>` using the chosen unit.
/// - Returns a `SegmentMeta` with `coverage_path` left as `None`.
pub fn segment_meta_from_parquet_bytes(
    rel_path: &Path,
    segment_id: SegmentId,
    time_column: &str,
    data: Bytes,
) -> SegmentResult<SegmentMeta> {
    let path_str = rel_path.display().to_string();

    if data.len() < 8 {
        return Err(SegmentMetaError::TooShort { path: path_str });
    }

    // Parquet reader works on any Read + Seek.
    let reader =
        SerializedFileReader::new(data).map_err(|source| SegmentMetaError::ParquetRead {
            path: path_str.clone(),
            source,
            backtrace: Backtrace::capture(),
        })?;

    let meta = reader.metadata();
    let file_meta = meta.file_metadata();
    let row_count = file_meta.num_rows() as u64;

    // Locate the time column in the schema descriptor.
    let schema = file_meta.schema_descr();

    let time_idx = schema
        .columns()
        .iter()
        .position(|c| c.path().string() == time_column)
        .ok_or_else(|| SegmentMetaError::TimeColumn {
            path: path_str.clone(),
            source: TimeColumnError::Missing {
                column: time_column.to_string(),
            },
        })?;

    // Optionally sanity-check the physical type and logical annotation.
    let col_descr = &schema.column(time_idx);
    let physical = col_descr.physical_type();
    let logical = col_descr.logical_type_ref();

    // Decide which timestamp unit we support for this column.
    let unit =
        choose_timestamp_unit_from_logical(time_column, physical, logical).map_err(|source| {
            SegmentMetaError::TimeColumn {
                path: path_str.clone(),
                source,
            }
        })?;

    // Try fast path: min/max from row-group stats.
    let stats_min_max = min_max_from_stats(&path_str, time_column, time_idx, &reader)?;

    let (ts_min_raw, ts_max_raw) = match stats_min_max {
        Some(pair) => pair,
        None => {
            // Fallback: row scan if stats are missing/incomplete.
            min_max_from_scan(&path_str, time_column, time_idx, &reader)?
        }
    };

    // Convert raw i64 timestamps to DateTime<Utc> using the chosen unit.
    let ts_min = ts_from_i64(unit, ts_min_raw)?;
    let ts_max = ts_from_i64(unit, ts_max_raw)?;

    // Build SegmentMeta: caller supplies segment_id; we fill in the ts_* and row_count.
    Ok(SegmentMeta {
        segment_id,
        path: rel_path.to_string_lossy().into_owned(),
        format: FileFormat::Parquet,
        ts_min,
        ts_max,
        row_count,
        coverage_path: None,
    })
}

/// Read a Parquet file at `rel_path` from `location` and produce a SegmentMeta
/// containing the given `segment_id`, the min/max timestamps for `time_column`,
/// and the row count; returns a SegmentMeta on success or a SegmentMetaError on failure.
pub async fn segment_meta_from_parquet_location(
    location: &TableLocation,
    rel_path: &Path,
    segment_id: SegmentId,
    time_column: &str,
) -> SegmentResult<SegmentMeta> {
    // 1) Read whole file via storage abstraction.
    let bytes = storage::read_all_bytes(location, rel_path)
        .await
        .map_err(map_storage_error)?;

    segment_meta_from_parquet_bytes(rel_path, segment_id, time_column, Bytes::from(bytes))
}

fn map_parquet_col_to_logical_type(
    physical: PhysicalType,
    logical: Option<&LogicalType>,
) -> LogicalDataType {
    // First: look at logical annotation when present
    if let Some(logical) = logical {
        match logical {
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c: _,
                unit,
            } => {
                let unit = match unit {
                    TimeUnit::MILLIS => LogicalTimestampUnit::Millis,
                    TimeUnit::MICROS => LogicalTimestampUnit::Micros,
                    TimeUnit::NANOS => LogicalTimestampUnit::Nanos,
                };

                // No capture of timezone for now.
                return LogicalDataType::Timestamp {
                    unit,
                    timezone: None,
                };
            }
            LogicalType::String => {
                // Semantically a UTF-8 string, even though it's BYTE_ARRAY underneath
                return LogicalDataType::Utf8;
            }
            LogicalType::Map
            | LogicalType::List
            | LogicalType::Enum
            | LogicalType::Decimal {
                scale: _,
                precision: _,
            } => {
                // For now, treat “complex” logical types as Other – v0.1 doesn’t need to fully support them.
                return LogicalDataType::Other(format!("parquet::{logical:?}"));
            }

            _ => {}
        }
    }
    // Second: fall back to physical type when no (or unsupported) logical annotation
    match physical {
        PhysicalType::BOOLEAN => LogicalDataType::Bool,
        PhysicalType::INT32 => LogicalDataType::Int32,
        PhysicalType::INT64 => LogicalDataType::Int64,
        PhysicalType::FLOAT => LogicalDataType::Float32,
        PhysicalType::DOUBLE => LogicalDataType::Float64,
        PhysicalType::BYTE_ARRAY => LogicalDataType::Binary,
        PhysicalType::FIXED_LEN_BYTE_ARRAY => LogicalDataType::FixedBinary,
        PhysicalType::INT96 => LogicalDataType::Int96,
    }
}

fn column_nullable(desc: &ColumnDescPtr) -> bool {
    match desc.self_type().get_basic_info().repetition() {
        Repetition::REQUIRED => false,
        Repetition::OPTIONAL | Repetition::REPEATED => true,
    }
}

fn logical_schema_from_parquet(meta: &FileMetaData) -> Result<LogicalSchema, LogicalSchemaError> {
    let descr = meta.schema_descr();
    let mut cols = Vec::with_capacity(descr.num_columns());

    for col in descr.columns() {
        let name = col.path().string();

        let physical = col.physical_type();
        let logical = col.logical_type_ref();

        let data_type = map_parquet_col_to_logical_type(physical, logical);

        let nullable = column_nullable(col);
        cols.push(LogicalColumn {
            name,
            data_type,
            nullable,
        });
    }

    LogicalSchema::new(cols)
}

/// Reads the Parquet file at `rel_path` from `location` and returns the inferred logical schema.
pub async fn logical_schema_from_parquet_location(
    location: &TableLocation,
    rel_path: &Path,
) -> SegmentResult<LogicalSchema> {
    let path_str = rel_path.display().to_string();

    let bytes = read_all_bytes(location, rel_path)
        .await
        .map_err(map_storage_error)?;

    let data = Bytes::from(bytes);

    let reader =
        SerializedFileReader::new(data).map_err(|source| SegmentMetaError::ParquetRead {
            path: path_str.clone(),
            source,
            backtrace: Backtrace::capture(),
        })?;

    let file_meta = reader.metadata().file_metadata();

    logical_schema_from_parquet(file_meta).map_err(|source| {
        SegmentMetaError::LogicalSchemaInvalid {
            path: path_str,
            source,
        }
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::{LogicalType, Repetition, TimeUnit};
    use parquet::column::writer::ColumnWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn write_parquet_file(
        path: &Path,
        time_column: &str,
        logical: Option<&str>,
        physical: PhysicalType,
        values: &[i64],
        stats_enabled: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut builder = Type::primitive_type_builder(time_column, physical)
            .with_repetition(Repetition::REQUIRED);

        if let Some(l) = logical {
            let lt = match l {
                "TIMESTAMP_MILLIS" => LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: TimeUnit::MILLIS,
                },
                "TIMESTAMP_MICROS" => LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: TimeUnit::MICROS,
                },
                "TIMESTAMP_NANOS" => LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: TimeUnit::NANOS,
                },
                other => return Err(format!("unsupported logical for test: {other}").into()),
            };
            builder = builder.with_logical_type(Some(lt));
        }

        let col = Arc::new(builder.build()?);
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );

        let props = if stats_enabled {
            WriterProperties::builder().build()
        } else {
            WriterProperties::builder()
                .set_statistics_enabled(EnabledStatistics::None)
                .build()
        };

        let file = File::create(path)?;
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let mut row_group_writer = writer.next_row_group()?;
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            match col_writer.untyped() {
                ColumnWriter::Int64ColumnWriter(typed) => {
                    typed.write_batch(values, None, None)?;
                }
                ColumnWriter::Int32ColumnWriter(typed) => {
                    let downcast: Vec<i32> = values.iter().map(|v| *v as i32).collect();
                    typed.write_batch(&downcast, None, None)?;
                }
                _ => return Err("unexpected column writer type".into()),
            }
            col_writer.close()?;
        }
        row_group_writer.close()?;
        writer.close()?;
        Ok(())
    }

    #[test]
    fn le_bytes_to_i64_rejects_wrong_length() {
        let err = le_bytes_to_i64("path", "ts", "min", &[1, 2, 3]).unwrap_err();
        assert!(matches!(err, SegmentMetaError::ParquetStatsShape { .. }));
    }

    #[test]
    fn ts_from_i64_out_of_range_is_error() {
        let err = ts_from_i64(TimestampUnit::Millis, i64::MAX).unwrap_err();
        assert!(matches!(err, SegmentMetaError::ParquetStatsShape { .. }));
    }

    #[test]
    fn choose_timestamp_unit_rejects_wrong_logical() {
        // No logical type (None) should fail
        let err = choose_timestamp_unit_from_logical("ts", PhysicalType::INT64, None).unwrap_err();
        assert!(matches!(
            err,
            TimeColumnError::UnsupportedParquetType { .. }
        ));
    }

    #[test]
    fn choose_timestamp_unit_rejects_wrong_physical() {
        let lt = LogicalType::Timestamp {
            is_adjusted_to_u_t_c: true,
            unit: TimeUnit::MILLIS,
        };
        let err =
            choose_timestamp_unit_from_logical("ts", PhysicalType::INT32, Some(&lt)).unwrap_err();
        assert!(matches!(
            err,
            TimeColumnError::UnsupportedParquetType { .. }
        ));
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_timestamp_units() {
        let cases = vec![
            (TimeUnit::MILLIS, LogicalTimestampUnit::Millis),
            (TimeUnit::MICROS, LogicalTimestampUnit::Micros),
            (TimeUnit::NANOS, LogicalTimestampUnit::Nanos),
        ];

        for (unit, expected_unit) in cases {
            let logical = LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit,
            };

            let mapped = map_parquet_col_to_logical_type(PhysicalType::INT64, Some(&logical));
            assert_eq!(
                mapped,
                LogicalDataType::Timestamp {
                    unit: expected_unit,
                    timezone: None,
                }
            );
        }
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_string_logical() {
        let mapped =
            map_parquet_col_to_logical_type(PhysicalType::BYTE_ARRAY, Some(&LogicalType::String));
        assert_eq!(mapped, LogicalDataType::Utf8);
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_complex_logical_to_other() {
        let map_type =
            map_parquet_col_to_logical_type(PhysicalType::BYTE_ARRAY, Some(&LogicalType::Map));
        assert_eq!(map_type, LogicalDataType::Other("parquet::Map".to_string()));

        let list_type =
            map_parquet_col_to_logical_type(PhysicalType::BYTE_ARRAY, Some(&LogicalType::List));
        assert_eq!(
            list_type,
            LogicalDataType::Other("parquet::List".to_string())
        );

        let enum_type =
            map_parquet_col_to_logical_type(PhysicalType::BYTE_ARRAY, Some(&LogicalType::Enum));
        assert_eq!(
            enum_type,
            LogicalDataType::Other("parquet::Enum".to_string())
        );

        let decimal = LogicalType::Decimal {
            scale: 2,
            precision: 10,
        };
        let decimal_type = map_parquet_col_to_logical_type(PhysicalType::INT64, Some(&decimal));
        assert_eq!(
            decimal_type,
            LogicalDataType::Other("parquet::Decimal { scale: 2, precision: 10 }".to_string())
        );
    }

    #[test]
    fn map_parquet_col_to_logical_type_prefers_physical_for_unknown_logical() {
        let mapped = map_parquet_col_to_logical_type(PhysicalType::INT64, Some(&LogicalType::Json));
        assert_eq!(mapped, LogicalDataType::Int64);
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_physical_types_without_logical() {
        let cases = vec![
            (PhysicalType::BOOLEAN, LogicalDataType::Bool),
            (PhysicalType::INT32, LogicalDataType::Int32),
            (PhysicalType::INT64, LogicalDataType::Int64),
            (PhysicalType::FLOAT, LogicalDataType::Float32),
            (PhysicalType::DOUBLE, LogicalDataType::Float64),
            (PhysicalType::BYTE_ARRAY, LogicalDataType::Binary),
            (
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
                LogicalDataType::FixedBinary,
            ),
            (PhysicalType::INT96, LogicalDataType::Int96),
        ];

        for (physical, expected) in cases {
            let mapped = map_parquet_col_to_logical_type(physical, None);
            assert_eq!(mapped, expected);
        }
    }

    #[tokio::test]
    async fn segment_meta_happy_path_uses_stats() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/ts.parquet");
        let abs = tmp.path().join(rel_path);

        write_parquet_file(
            &abs,
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &[10, 20, 30],
            true,
        )?;

        let meta = segment_meta_from_parquet_location(
            &TableLocation::local(tmp.path()),
            rel_path,
            SegmentId("seg-1".to_string()),
            "ts",
        )
        .await?;

        assert_eq!(meta.ts_min.timestamp_millis(), 10);
        assert_eq!(meta.ts_max.timestamp_millis(), 30);
        assert_eq!(meta.row_count, 3);
        Ok(())
    }

    #[tokio::test]
    async fn segment_meta_falls_back_to_scan_when_stats_missing() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/no_stats.parquet");
        let abs = tmp.path().join(rel_path);

        write_parquet_file(
            &abs,
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &[5, 7],
            false,
        )?;

        let meta = segment_meta_from_parquet_location(
            &TableLocation::local(tmp.path()),
            rel_path,
            SegmentId("seg-scan".to_string()),
            "ts",
        )
        .await?;

        assert_eq!(meta.ts_min.timestamp_millis(), 5);
        assert_eq!(meta.ts_max.timestamp_millis(), 7);
        assert_eq!(meta.row_count, 2);
        Ok(())
    }

    #[tokio::test]
    async fn segment_meta_errors_when_no_rows_and_no_stats() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/empty.parquet");
        let abs = tmp.path().join(rel_path);

        write_parquet_file(
            &abs,
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &[],
            false,
        )?;

        let result = segment_meta_from_parquet_location(
            &TableLocation::local(tmp.path()),
            rel_path,
            SegmentId("seg-empty".to_string()),
            "ts",
        )
        .await;

        assert!(matches!(
            result,
            Err(SegmentMetaError::ParquetStatsMissing { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn segment_meta_supports_micro_and_nano_units() -> TestResult {
        let tmp = TempDir::new()?;

        // Micros
        let rel_micro = Path::new("data/micro.parquet");
        let abs_micro = tmp.path().join(rel_micro);
        write_parquet_file(
            &abs_micro,
            "ts",
            Some("TIMESTAMP_MICROS"),
            PhysicalType::INT64,
            &[1_000, 2_000],
            true,
        )?;

        let meta_micro = segment_meta_from_parquet_location(
            &TableLocation::local(tmp.path()),
            rel_micro,
            SegmentId("seg-micro".to_string()),
            "ts",
        )
        .await?;
        assert_eq!(
            meta_micro.ts_min.timestamp_nanos_opt().map(|n| n / 1_000),
            Some(1_000)
        );
        assert_eq!(
            meta_micro.ts_max.timestamp_nanos_opt().map(|n| n / 1_000),
            Some(2_000)
        );

        // Nanos
        let rel_nano = Path::new("data/nano.parquet");
        let abs_nano = tmp.path().join(rel_nano);
        write_parquet_file(
            &abs_nano,
            "ts",
            Some("TIMESTAMP_NANOS"),
            PhysicalType::INT64,
            &[3_000, 9_000],
            true,
        )?;

        let meta_nano = segment_meta_from_parquet_location(
            &TableLocation::local(tmp.path()),
            rel_nano,
            SegmentId("seg-nano".to_string()),
            "ts",
        )
        .await?;
        assert_eq!(meta_nano.ts_min.timestamp_nanos_opt(), Some(3_000));
        assert_eq!(meta_nano.ts_max.timestamp_nanos_opt(), Some(9_000));

        Ok(())
    }

    #[tokio::test]
    async fn missing_time_column_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/no_time.parquet");
        let abs = tmp.path().join(rel_path);

        // Write a parquet file with a different column name.
        write_parquet_file(
            &abs,
            "other",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &[1, 2],
            true,
        )?;

        let result = segment_meta_from_parquet_location(
            &TableLocation::local(tmp.path()),
            rel_path,
            SegmentId("seg-missing".to_string()),
            "ts",
        )
        .await;

        assert!(matches!(
            result,
            Err(SegmentMetaError::TimeColumn {
                source: TimeColumnError::Missing { .. },
                ..
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_time_type_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/unsupported_time.parquet");
        let abs = tmp.path().join(rel_path);

        // INT32 with timestamp logical is unsupported.
        write_parquet_file(&abs, "ts", None, PhysicalType::INT32, &[1, 2], true)?;

        let result = segment_meta_from_parquet_location(
            &TableLocation::local(tmp.path()),
            rel_path,
            SegmentId("seg-bad".to_string()),
            "ts",
        )
        .await;

        assert!(matches!(
            result,
            Err(SegmentMetaError::TimeColumn {
                source: TimeColumnError::UnsupportedParquetType { .. },
                ..
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn bad_parquet_file_returns_parquet_read_error() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/corrupt.parquet");
        let abs = tmp.path().join(rel_path);

        // Valid magic bytes but invalid body so the parquet reader fails.
        tokio::fs::create_dir_all(abs.parent().unwrap()).await?;
        tokio::fs::write(&abs, b"PAR1PAR1garbage").await?;

        let result = segment_meta_from_parquet_location(
            &TableLocation::local(tmp.path()),
            rel_path,
            SegmentId("seg-corrupt".to_string()),
            "ts",
        )
        .await;

        assert!(matches!(result, Err(SegmentMetaError::ParquetRead { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn missing_file_returns_missing_error() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/missing.parquet");

        let result = segment_meta_from_parquet_location(
            &TableLocation::local(tmp.path()),
            rel_path,
            SegmentId("seg-missing".to_string()),
            "ts",
        )
        .await;

        assert!(matches!(result, Err(SegmentMetaError::MissingFile { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn too_short_file_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/short.parquet");
        let abs = tmp.path().join(rel_path);
        tokio::fs::create_dir_all(abs.parent().unwrap()).await?;
        tokio::fs::write(&abs, b"short").await?;

        let result = segment_meta_from_parquet_location(
            &TableLocation::local(tmp.path()),
            rel_path,
            SegmentId("seg-short".to_string()),
            "ts",
        )
        .await;

        assert!(matches!(result, Err(SegmentMetaError::TooShort { .. })));
        Ok(())
    }
}
