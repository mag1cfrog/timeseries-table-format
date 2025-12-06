use std::path::Path;

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use parquet::basic::Type as PhysicalType;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use snafu::Backtrace;

use crate::storage::{self, TableLocation};
use crate::transaction_log::segments::{SegmentMetaError, SegmentResult, map_storage_error};
use crate::transaction_log::{FileFormat, SegmentId, SegmentMeta};

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
            let secs = value / 1_000_000_000;
            let nanos = (value % 1_000_000_000) as u32;
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

fn choose_timestamp_unit(
    path: &str,
    column: &str,
    physical: PhysicalType,
    logical_desc: &str,
) -> Result<TimestampUnit, SegmentMetaError> {
    match physical {
        PhysicalType::INT64 => match logical_desc {
            "timestamp_millis" | "TIMESTAMP_MILLIS" => Ok(TimestampUnit::Millis),
            "timestamp_micros" | "TIMESTAMP_MICROS" => Ok(TimestampUnit::Micros),
            "timestamp_nanos" | "TIMESTAMP_NANOS" => Ok(TimestampUnit::Nanos),
            other => Err(SegmentMetaError::UnsupportedTimeType {
                path: path.to_string(),
                column: column.to_string(),
                physical: format!("{physical:?}",),
                logical: other.to_string(),
            }),
        },
        other => Err(SegmentMetaError::UnsupportedTimeType {
            path: path.to_string(),
            column: column.to_string(),
            physical: format!("{physical:?}",),
            logical: other.to_string(),
        }),
    }
}

/// Best-effort description of the logical type for debugging / error messages.
fn logical_type_description(col_descr: &parquet::schema::types::ColumnDescriptor) -> String {
    if let Some(logical) = col_descr.logical_type_ref() {
        format!("{logical:?}")
    } else {
        format!("{:?}", col_descr.converted_type())
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

        let v = row
            .get_long(time_idx)
            .map_err(|e| SegmentMetaError::ParquetRead {
                path: path.to_string(),
                source: e,
                backtrace: Backtrace::capture(),
            })?;

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

/// Read a Parquet file at `rel_path` from `location` and produce a SegmentMeta
/// containing the given `segment_id`, the min/max timestamps for `time_column`,
/// and the row count; returns a SegmentMeta on success or a SegmentMetaError on failure.
pub async fn segment_meta_from_parquet_location(
    location: &TableLocation,
    rel_path: &Path,
    segment_id: SegmentId,
    time_column: &str,
) -> SegmentResult<SegmentMeta> {
    let path_str = rel_path.display().to_string();

    // 1) Read whole file via storage abstraction.
    let bytes = storage::read_all_bytes(location, rel_path)
        .await
        .map_err(map_storage_error)?;

    if bytes.len() < 8 {
        return Err(SegmentMetaError::TooShort { path: path_str });
    }

    // 2) Wrap in Bytes so we satisfy ChunkReader.
    let data = Bytes::from(bytes);

    // 3) Parquet reader works on any Read + Seek.
    let reader =
        SerializedFileReader::new(data).map_err(|source| SegmentMetaError::ParquetRead {
            path: path_str.clone(),
            source,
            backtrace: Backtrace::capture(),
        })?;

    let meta = reader.metadata();
    let file_meta = meta.file_metadata();
    let row_count = file_meta.num_rows() as u64;

    // 4) Locate the time column in the schema descriptor.
    let schema = file_meta.schema_descr();

    let time_idx = schema
        .columns()
        .iter()
        .position(|c| c.path().string() == time_column)
        .ok_or_else(|| SegmentMetaError::MissingTimeColumn {
            path: path_str.clone(),
            column: time_column.to_string(),
        })?;

    // Optionally sanity-check the physical type.
    let col_descr = &schema.column(time_idx);
    let physical = col_descr.physical_type();
    let logical_desc = logical_type_description(col_descr);

    // 5) Decide which timestamp unit we support for this column.
    let unit = choose_timestamp_unit(&path_str, time_column, physical, &logical_desc)?;

    // 6) Try fast path: min/max from row-group stats.
    let stats_min_max = min_max_from_stats(&path_str, time_column, time_idx, &reader)?;

    let (ts_min_raw, ts_max_raw) = match stats_min_max {
        Some(pair) => pair,
        None => {
            // 7) Fallback: row scan if stats are missing/incomplete.
            min_max_from_scan(&path_str, time_column, time_idx, &reader)?
        }
    };

    // 8) Convert raw i64 timestamps to DateTime<Utc> using the chosen unit.
    let ts_min = ts_from_i64(unit, ts_min_raw)?;
    let ts_max = ts_from_i64(unit, ts_max_raw)?;

    // 9) Build SegmentMeta: caller supplies segment_id; we fill in the ts_* and row_count.
    Ok(SegmentMeta {
        segment_id,
        path: rel_path.to_string_lossy().into_owned(),
        format: FileFormat::Parquet,
        ts_min,
        ts_max,
        row_count,
    })
}
