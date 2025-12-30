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
    column: &str,
    physical: PhysicalType,
    logical: Option<&LogicalType>,
    fixed_len_byte_array_len: Option<i32>,
) -> Result<LogicalDataType, LogicalSchemaError> {
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
                return Ok(LogicalDataType::Timestamp {
                    unit,
                    timezone: None,
                });
            }
            LogicalType::String => {
                // Semantically a UTF-8 string, even though it's BYTE_ARRAY underneath
                return Ok(LogicalDataType::Utf8);
            }
            LogicalType::Map | LogicalType::List | LogicalType::Enum => {
                // For now, treat “complex” logical types as Other – v0.1 doesn’t need to fully support them.
                return Ok(LogicalDataType::Other(format!("parquet::{logical:?}")));
            }
            LogicalType::Decimal { scale, precision } => {
                return Ok(LogicalDataType::Decimal {
                    precision: *precision,
                    scale: *scale,
                });
            }

            _ => {}
        }
    }
    // Second: fall back to physical type when no (or unsupported) logical annotation
    Ok(match physical {
        PhysicalType::BOOLEAN => LogicalDataType::Bool,
        PhysicalType::INT32 => LogicalDataType::Int32,
        PhysicalType::INT64 => LogicalDataType::Int64,
        PhysicalType::FLOAT => LogicalDataType::Float32,
        PhysicalType::DOUBLE => LogicalDataType::Float64,
        PhysicalType::BYTE_ARRAY => LogicalDataType::Binary,
        PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            let byte_width = fixed_len_byte_array_len.ok_or_else(|| {
                LogicalSchemaError::FixedBinaryMissingLength {
                    column: column.to_string(),
                }
            })?;
            if byte_width <= 0 {
                return Err(LogicalSchemaError::FixedBinaryInvalidWidthInSchema {
                    column: column.to_string(),
                    byte_width,
                });
            }
            LogicalDataType::FixedBinary { byte_width }
        }
        PhysicalType::INT96 => LogicalDataType::Int96,
    })
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

        let fixed_len_byte_array_len = if physical == PhysicalType::FIXED_LEN_BYTE_ARRAY {
            Some(col.type_length())
        } else {
            None
        };
        let data_type =
            map_parquet_col_to_logical_type(&name, physical, logical, fixed_len_byte_array_len)?;

        let nullable = column_nullable(col);
        cols.push(LogicalColumn {
            name,
            data_type,
            nullable,
        });
    }

    LogicalSchema::new(cols)
}

/// Build a `LogicalSchema` from Parquet bytes.
///
/// This mirrors `logical_schema_from_parquet_location` but consumes a provided
/// `Bytes` buffer. The caller supplies the relative path (for error context)
/// and the full Parquet file contents. On success it returns a `LogicalSchema`
/// derived from the Parquet physical/logical types; otherwise it returns a
/// `SegmentMetaError` with contextual path information.
pub fn logical_schema_from_parquet_bytes(
    rel_path: &Path,
    data: Bytes,
) -> SegmentResult<LogicalSchema> {
    let path_str = rel_path.display().to_string();

    // Parse Parquet metadata from the in-memory buffer.
    let reader =
        SerializedFileReader::new(data).map_err(|source| SegmentMetaError::ParquetRead {
            path: path_str.clone(),
            source,
            backtrace: Backtrace::capture(),
        })?;

    let file_meta = reader.metadata().file_metadata();

    // Map Parquet physical/logical types into our LogicalSchema representation.
    logical_schema_from_parquet(file_meta).map_err(|source| {
        SegmentMetaError::LogicalSchemaInvalid {
            path: path_str,
            source,
        }
    })
}

/// Reads the Parquet file at `rel_path` from `location` and returns the inferred logical schema.
pub async fn logical_schema_from_parquet_location(
    location: &TableLocation,
    rel_path: &Path,
) -> SegmentResult<LogicalSchema> {
    let bytes = read_all_bytes(location, rel_path)
        .await
        .map_err(map_storage_error)?;

    logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))
}
#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::{LogicalType, Repetition, TimeUnit};
    use parquet::column::writer::ColumnWriter;
    use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
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

    enum TestColumnValues {
        Bool(Vec<bool>),
        Int32(Vec<i32>),
        Int64(Vec<i64>),
        Float32(Vec<f32>),
        Float64(Vec<f64>),
        Int96(Vec<Int96>),
    }

    fn write_single_column_parquet(
        path: &Path,
        column_name: &str,
        physical: PhysicalType,
        values: TestColumnValues,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let col = Arc::new(
            Type::primitive_type_builder(column_name, physical)
                .with_repetition(Repetition::REQUIRED)
                .build()?,
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let mut row_group_writer = writer.next_row_group()?;
        let mut values = Some(values);
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            let values = values.take().ok_or("unexpected extra column")?;
            match (col_writer.untyped(), values) {
                (ColumnWriter::BoolColumnWriter(typed), TestColumnValues::Bool(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                (ColumnWriter::Int32ColumnWriter(typed), TestColumnValues::Int32(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                (ColumnWriter::Int64ColumnWriter(typed), TestColumnValues::Int64(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                (ColumnWriter::FloatColumnWriter(typed), TestColumnValues::Float32(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                (ColumnWriter::DoubleColumnWriter(typed), TestColumnValues::Float64(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                (ColumnWriter::Int96ColumnWriter(typed), TestColumnValues::Int96(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                _ => return Err("unexpected column writer type".into()),
            }
            col_writer.close()?;
        }
        row_group_writer.close()?;
        writer.close()?;
        Ok(())
    }

    fn assert_logical_schema_for_physical(
        file_name: &str,
        physical: PhysicalType,
        expected: LogicalDataType,
        values: TestColumnValues,
    ) -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new(file_name);
        let abs = tmp.path().join(rel_path);

        write_single_column_parquet(&abs, "col", physical, values)?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "col");
        assert_eq!(cols[0].data_type, expected);
        assert!(!cols[0].nullable);
        Ok(())
    }

    fn write_fixed_len_byte_array_file(
        path: &Path,
        column_name: &str,
        byte_width: i32,
        values: &[Vec<u8>],
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_fixed_len_byte_array_file_with_logical(path, column_name, byte_width, None, values)
    }

    fn write_fixed_len_byte_array_file_with_logical(
        path: &Path,
        column_name: &str,
        byte_width: i32,
        logical: Option<LogicalType>,
        values: &[Vec<u8>],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        for value in values {
            if value.len() != byte_width as usize {
                return Err(format!(
                    "fixed-len value size {} != expected {}",
                    value.len(),
                    byte_width
                )
                .into());
            }
        }

        let mut builder =
            Type::primitive_type_builder(column_name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .with_length(byte_width);
        if let Some(logical) = logical {
            builder = builder.with_logical_type(Some(logical));
        }
        let col = Arc::new(builder.build()?);
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let fixed_values: Vec<FixedLenByteArray> = values
            .iter()
            .cloned()
            .map(FixedLenByteArray::from)
            .collect();

        let mut row_group_writer = writer.next_row_group()?;
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            match col_writer.untyped() {
                ColumnWriter::FixedLenByteArrayColumnWriter(typed) => {
                    typed.write_batch(&fixed_values, None, None)?;
                }
                _ => return Err("unexpected column writer type".into()),
            }
            col_writer.close()?;
        }
        row_group_writer.close()?;
        writer.close()?;
        Ok(())
    }

    fn write_byte_array_file(
        path: &Path,
        column_name: &str,
        values: &[&[u8]],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        write_byte_array_file_with_logical(path, column_name, None, values)
    }

    fn write_byte_array_file_with_logical(
        path: &Path,
        column_name: &str,
        logical: Option<LogicalType>,
        values: &[&[u8]],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut builder = Type::primitive_type_builder(column_name, PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED);
        if let Some(logical) = logical {
            builder = builder.with_logical_type(Some(logical));
        }
        let col = Arc::new(builder.build()?);
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let byte_values: Vec<ByteArray> = values.iter().map(|v| ByteArray::from(*v)).collect();

        let mut row_group_writer = writer.next_row_group()?;
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            match col_writer.untyped() {
                ColumnWriter::ByteArrayColumnWriter(typed) => {
                    typed.write_batch(&byte_values, None, None)?;
                }
                _ => return Err("unexpected column writer type".into()),
            }
            col_writer.close()?;
        }
        row_group_writer.close()?;
        writer.close()?;
        Ok(())
    }

    fn write_decimal_int64_file(
        path: &Path,
        column_name: &str,
        precision: i32,
        scale: i32,
        values: &[i64],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let col = Arc::new(
            Type::primitive_type_builder(column_name, PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::Decimal { scale, precision }))
                .with_precision(precision)
                .with_scale(scale)
                .build()?,
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let mut row_group_writer = writer.next_row_group()?;
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            match col_writer.untyped() {
                ColumnWriter::Int64ColumnWriter(typed) => {
                    typed.write_batch(values, None, None)?;
                }
                _ => return Err("unexpected column writer type".into()),
            }
            col_writer.close()?;
        }
        row_group_writer.close()?;
        writer.close()?;
        Ok(())
    }

    fn write_schema_only_parquet(
        path: &Path,
        schema: Arc<Type>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;
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

            let mapped =
                map_parquet_col_to_logical_type("ts", PhysicalType::INT64, Some(&logical), None)
                    .unwrap();
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
        let mapped = map_parquet_col_to_logical_type(
            "text",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::String),
            None,
        )
        .unwrap();
        assert_eq!(mapped, LogicalDataType::Utf8);
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_complex_logical_to_other() {
        let map_type = map_parquet_col_to_logical_type(
            "map",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::Map),
            None,
        )
        .unwrap();
        assert_eq!(map_type, LogicalDataType::Other("parquet::Map".to_string()));

        let list_type = map_parquet_col_to_logical_type(
            "list",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::List),
            None,
        )
        .unwrap();
        assert_eq!(
            list_type,
            LogicalDataType::Other("parquet::List".to_string())
        );

        let enum_type = map_parquet_col_to_logical_type(
            "enum",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::Enum),
            None,
        )
        .unwrap();
        assert_eq!(
            enum_type,
            LogicalDataType::Other("parquet::Enum".to_string())
        );
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_decimal() {
        let decimal = LogicalType::Decimal {
            scale: 2,
            precision: 10,
        };
        let decimal_type =
            map_parquet_col_to_logical_type("dec", PhysicalType::INT64, Some(&decimal), None)
                .unwrap();
        assert_eq!(
            decimal_type,
            LogicalDataType::Decimal {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn map_parquet_col_to_logical_type_prefers_physical_for_unknown_logical() {
        let mapped = map_parquet_col_to_logical_type(
            "json",
            PhysicalType::INT64,
            Some(&LogicalType::Json),
            None,
        )
        .unwrap();
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
                LogicalDataType::FixedBinary { byte_width: 16 },
            ),
            (PhysicalType::INT96, LogicalDataType::Int96),
        ];

        for (physical, expected) in cases {
            let fixed_len_byte_array_len = if physical == PhysicalType::FIXED_LEN_BYTE_ARRAY {
                Some(16)
            } else {
                None
            };
            let mapped =
                map_parquet_col_to_logical_type("col", physical, None, fixed_len_byte_array_len)
                    .unwrap();
            assert_eq!(mapped, expected);
        }
    }

    #[test]
    fn map_parquet_col_to_logical_type_requires_fixed_len_byte_array_length() {
        let err =
            map_parquet_col_to_logical_type("bin", PhysicalType::FIXED_LEN_BYTE_ARRAY, None, None)
                .unwrap_err();
        assert!(
            matches!(
                &err,
                LogicalSchemaError::FixedBinaryMissingLength { column } if column == "bin"
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn logical_schema_maps_fixed_len_byte_array_with_width() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/fixed-len.parquet");
        let abs = tmp.path().join(rel_path);

        write_fixed_len_byte_array_file(&abs, "bin", 4, &[vec![0, 1, 2, 3], vec![4, 5, 6, 7]])?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "bin");
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::FixedBinary { byte_width: 4 }
        );
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_byte_array_to_binary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/byte-array.parquet");
        let abs = tmp.path().join(rel_path);

        write_byte_array_file(&abs, "bin", &[b"a", b"bc"])?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "bin");
        assert_eq!(cols[0].data_type, LogicalDataType::Binary);
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_string_logical_to_utf8() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/byte-array-string.parquet");
        let abs = tmp.path().join(rel_path);

        write_byte_array_file_with_logical(
            &abs,
            "text",
            Some(LogicalType::String),
            &[b"alpha", b"beta"],
        )?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "text");
        assert_eq!(cols[0].data_type, LogicalDataType::Utf8);
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_enum_logical_to_other() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/byte-array-enum.parquet");
        let abs = tmp.path().join(rel_path);

        write_byte_array_file_with_logical(&abs, "kind", Some(LogicalType::Enum), &[b"A", b"B"])?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "kind");
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::Other("parquet::Enum".to_string())
        );
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_decimal_logical() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/decimal-int64.parquet");
        let abs = tmp.path().join(rel_path);

        write_decimal_int64_file(&abs, "dec", 10, 2, &[1234, 5678])?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "dec");
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_uuid_logical_to_fixed_binary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/uuid.parquet");
        let abs = tmp.path().join(rel_path);

        write_fixed_len_byte_array_file_with_logical(
            &abs,
            "uuid",
            16,
            Some(LogicalType::Uuid),
            &[vec![0; 16], vec![1; 16]],
        )?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "uuid");
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::FixedBinary { byte_width: 16 }
        );
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_float16_logical_to_fixed_binary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/float16.parquet");
        let abs = tmp.path().join(rel_path);

        write_fixed_len_byte_array_file_with_logical(
            &abs,
            "f16",
            2,
            Some(LogicalType::Float16),
            &[vec![0, 0], vec![0x3c, 0x00]],
        )?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "f16");
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::FixedBinary { byte_width: 2 }
        );
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_json_logical_to_binary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/json.parquet");
        let abs = tmp.path().join(rel_path);

        write_byte_array_file_with_logical(
            &abs,
            "json",
            Some(LogicalType::Json),
            &[br#"{"a":1}"#, br#"{"b":2}"#],
        )?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "json");
        assert_eq!(cols[0].data_type, LogicalDataType::Binary);
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_bson_logical_to_binary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/bson.parquet");
        let abs = tmp.path().join(rel_path);

        write_byte_array_file_with_logical(
            &abs,
            "bson",
            Some(LogicalType::Bson),
            &[b"\x05\x00\x00\x00\x00", b"\x05\x00\x00\x00\x01"],
        )?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "bson");
        assert_eq!(cols[0].data_type, LogicalDataType::Binary);
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_map_logical_to_other() {
        let mapped = map_parquet_col_to_logical_type(
            "map",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::Map),
            None,
        )
        .unwrap();
        assert_eq!(mapped, LogicalDataType::Other("parquet::Map".to_string()));
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_list_logical_to_other() {
        let mapped = map_parquet_col_to_logical_type(
            "list",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::List),
            None,
        )
        .unwrap();
        assert_eq!(mapped, LogicalDataType::Other("parquet::List".to_string()));
    }

    #[test]
    fn logical_schema_ignores_list_group_annotation() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/list-group.parquet");
        let abs = tmp.path().join(rel_path);

        let element = Type::primitive_type_builder("element", PhysicalType::INT32)
            .with_repetition(Repetition::REPEATED)
            .build()?;
        let list_group = Type::group_type_builder("list_field")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(element)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(list_group)])
                .build()?,
        );

        write_schema_only_parquet(&abs, schema)?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].data_type, LogicalDataType::Int32);
        Ok(())
    }

    #[test]
    fn logical_schema_ignores_map_group_annotation() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/map-group.parquet");
        let abs = tmp.path().join(rel_path);

        let key = Type::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let value = Type::primitive_type_builder("value", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()?;
        let key_value = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(key), Arc::new(value)])
            .build()?;
        let map_group = Type::group_type_builder("map_field")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(key_value)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(map_group)])
                .build()?,
        );

        write_schema_only_parquet(&abs, schema)?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].data_type, LogicalDataType::Binary);
        assert_eq!(cols[1].data_type, LogicalDataType::Int64);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_bool() -> TestResult {
        assert_logical_schema_for_physical(
            "data/bool.parquet",
            PhysicalType::BOOLEAN,
            LogicalDataType::Bool,
            TestColumnValues::Bool(vec![true, false]),
        )
    }

    #[test]
    fn logical_schema_maps_int32() -> TestResult {
        assert_logical_schema_for_physical(
            "data/int32.parquet",
            PhysicalType::INT32,
            LogicalDataType::Int32,
            TestColumnValues::Int32(vec![1, 2, 3]),
        )
    }

    #[test]
    fn logical_schema_maps_int64() -> TestResult {
        assert_logical_schema_for_physical(
            "data/int64.parquet",
            PhysicalType::INT64,
            LogicalDataType::Int64,
            TestColumnValues::Int64(vec![1, 2, 3]),
        )
    }

    #[test]
    fn logical_schema_maps_float32() -> TestResult {
        assert_logical_schema_for_physical(
            "data/float32.parquet",
            PhysicalType::FLOAT,
            LogicalDataType::Float32,
            TestColumnValues::Float32(vec![1.25, 2.5]),
        )
    }

    #[test]
    fn logical_schema_maps_float64() -> TestResult {
        assert_logical_schema_for_physical(
            "data/float64.parquet",
            PhysicalType::DOUBLE,
            LogicalDataType::Float64,
            TestColumnValues::Float64(vec![1.25, 2.5]),
        )
    }

    #[test]
    fn logical_schema_maps_int96() -> TestResult {
        let v1 = Int96::from(vec![1, 2, 3]);
        let v2 = Int96::from(vec![4, 5, 6]);
        assert_logical_schema_for_physical(
            "data/int96.parquet",
            PhysicalType::INT96,
            LogicalDataType::Int96,
            TestColumnValues::Int96(vec![v1, v2]),
        )
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
