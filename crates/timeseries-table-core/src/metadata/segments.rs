//! Segment identifiers, formats, and per-file metadata recorded in table metadata.
//!
//! This module contains **pure** data types + non-IO validation/decoding errors.
//! Any functions that touch storage backends (filesystem, object store, etc.)
//! must live outside `metadata/` (for example under `transaction_log` or
//! format-specific helpers).

use std::fmt;

use arrow::error::ArrowError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parquet::errors::ParquetError;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, prelude::*};

use crate::metadata::{logical_schema::LogicalSchemaError, time_column::TimeColumnError};

/// Identifier for a physical segment (e.g. a Parquet file or group).
///
/// This is a logical ID used by the metadata; the actual file path is stored
/// separately in [`SegmentMeta`]. Using a newtype makes it harder to mix
/// up segment IDs with other stringly-typed fields.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct SegmentId(pub String);

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Supported on-disk file formats for segments.
///
/// In v0.1, only `Parquet` is implemented, but the enum keeps the metadata model
/// open to other formats in future versions.
///
/// JSON layout example: `"format": "parquet"`
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    /// Apache Parquet columnar format.
    #[default]
    Parquet,
    // Future:
    // Orc,
    // Avro,
    // Csv,
}

/// Metadata about a single physical segment.
///
/// In v0.1, a "segment" corresponds to a single data file on disk.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentMeta {
    /// Logical identifier for this segment.
    pub segment_id: SegmentId,

    /// File path relative to the table root (for example, `"data/nvda_1h_0001.parquet"`).
    pub path: String,

    /// File format for this segment.
    pub format: FileFormat,

    /// Minimum timestamp contained in this segment (inclusive), in RFC3339 UTC.
    pub ts_min: DateTime<Utc>,

    /// Maximum timestamp contained in this segment (inclusive), in RFC3339 UTC.
    pub ts_max: DateTime<Utc>,

    /// Number of rows in this segment.
    pub row_count: u64,

    /// Optional file size in bytes at the time metadata was captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,

    /// Coverage sidecar pointer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coverage_path: Option<String>,
}

impl SegmentMeta {
    /// Set the coverage sidecar path for this segment metadata.
    pub fn with_coverage_path(mut self, path: impl Into<String>) -> Self {
        self.coverage_path = Some(path.into());
        self
    }
}

/// Errors that can occur while validating or decoding segment metadata.
///
/// This enum intentionally contains **no storage backend errors**. IO-related
/// errors should be wrapped at the IO boundary (for example, in
/// `transaction_log::segments::SegmentError`).
#[derive(Debug, Snafu)]
pub enum SegmentMetaError {
    /// File format is not supported for v0.1.
    #[snafu(display("Unsupported file format: {format:?}"))]
    UnsupportedFormat {
        /// The offending file format.
        format: FileFormat,
    },

    /// The file is too short to be a valid Parquet file.
    #[snafu(display("Segment file too short to be valid Parquet: {path}"))]
    TooShort {
        /// The path to the file that was too short.
        path: String,
    },

    /// Magic bytes at the start / end of file don't match the Parquet spec.
    #[snafu(display("Invalid Parquet magic bytes in segment file: {path}"))]
    InvalidMagic {
        /// The path to the file with invalid magic bytes.
        path: String,
    },

    /// Parquet reader / metadata failure.
    #[snafu(display("Error reading Parquet metadata for segment at {path}: {source}"))]
    ParquetRead {
        /// The path to the file that caused the Parquet read failure.
        path: String,
        /// Underlying parquet error that caused this failure.
        source: ParquetError,
        /// Diagnostic backtrace for this error.
        backtrace: Backtrace,
    },

    /// Arrow decode failure while reading Parquet data.
    #[snafu(display("Arrow read error for segment at {path}: {source}"))]
    ArrowRead {
        /// The path to the file that caused the Arrow read failure.
        path: String,
        /// Underlying Arrow error that caused this failure.
        source: ArrowError,
        /// Diagnostic backtrace for this error.
        backtrace: Backtrace,
    },

    /// Time column validation or metadata error.
    #[snafu(display("Time column error in segment at {path}: {source}"))]
    TimeColumn {
        /// The path to the segment file with a time column error.
        path: String,
        /// The underlying time column error.
        source: TimeColumnError,
    },

    /// Statistics exist but are not well-shaped (wrong length / unexpected type).
    #[snafu(display(
        "Parquet statistics shape invalid for {column} in segment at {path}: {detail}"
    ))]
    ParquetStatsShape {
        /// The path to the file with malformed Parquet statistics.
        path: String,
        /// The column whose statistics are malformed.
        column: String,
        /// Details about how the statistics are malformed.
        detail: String,
    },

    /// No usable statistics for the time column; v0.1 may fall back to a scan.
    #[snafu(display("Parquet statistics missing for {column} in segment at {path}"))]
    ParquetStatsMissing {
        /// The path to the file missing statistics for the column.
        path: String,
        /// The column missing statistics.
        column: String,
    },

    /// Failed to derive a valid LogicalSchema from the Parquet file.
    #[snafu(display("Invalid logical schema derived from Parquet at {path}: {source}"))]
    LogicalSchemaInvalid {
        /// The path to the file without a valid LogicalSchema.
        path: String,
        /// Underlying logical schema error that triggered this failure.
        #[snafu(source)]
        source: LogicalSchemaError,
    },
}

/// Derive a deterministic segment id for an append entry.
///
/// This is content-addressable: it hashes both the relative path and the bytes
/// so retries with the same input stay stable while same bytes at different
/// paths diverge. The returned id uses the `seg-` prefix followed by 32 hex
/// chars of the BLAKE3 digest, keeping ids bounded and safe for idempotent
/// appends.
pub fn segment_id_v1(relative_path: &str, data: &Bytes) -> SegmentId {
    let mut h = blake3::Hasher::new();
    h.update(b"segment-id-v1");
    h.update(b"\0");
    h.update(relative_path.as_bytes());
    h.update(b"\0");
    h.update(data.as_ref());
    let hex = h.finalize().to_hex();
    SegmentId(format!("seg-{}", &hex[..32]))
}

/// Result type for pure (non-IO) segment metadata operations.
#[allow(clippy::result_large_err)]
pub type SegmentMetaResult<T> = Result<T, SegmentMetaError>;

/// Deterministic ordering for segments by time.
///
/// Ordering is by `ts_min`, then `ts_max`, and finally `segment_id` as a stable
/// tie-breaker.
pub(crate) fn cmp_segment_meta_by_time(a: &SegmentMeta, b: &SegmentMeta) -> std::cmp::Ordering {
    a.ts_min
        .cmp(&b.ts_min)
        .then_with(|| a.ts_max.cmp(&b.ts_max))
        .then_with(|| a.segment_id.0.cmp(&b.segment_id.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn seg(id: &str, ts_min: i64, ts_max: i64) -> SegmentMeta {
        SegmentMeta {
            segment_id: SegmentId(id.to_string()),
            path: format!("data/{id}.parquet"),
            format: FileFormat::Parquet,
            ts_min: Utc.timestamp_opt(ts_min, 0).single().unwrap(),
            ts_max: Utc.timestamp_opt(ts_max, 0).single().unwrap(),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        }
    }

    #[test]
    fn ordering_is_deterministic_with_tie_breakers() {
        let mut v = vec![
            seg("c", 10, 20),
            seg("b", 10, 20),
            seg("a", 10, 30),
            seg("d", 5, 7),
        ];

        v.sort_unstable_by(cmp_segment_meta_by_time);

        let ids: Vec<String> = v.into_iter().map(|s| s.segment_id.0).collect();
        assert_eq!(ids, vec!["d", "b", "c", "a"]);
    }

    #[test]
    fn ordering_is_equal_for_identical_segments() {
        let a = seg("same", 10, 20);
        let b = seg("same", 10, 20);
        assert_eq!(cmp_segment_meta_by_time(&a, &b), std::cmp::Ordering::Equal);
        assert_eq!(cmp_segment_meta_by_time(&b, &a), std::cmp::Ordering::Equal);
    }

    #[test]
    fn ordering_primary_key_ts_min_dominates() {
        let mut v = vec![seg("z", 20, 30), seg("a", 10, 50), seg("m", 15, 10)];

        v.sort_unstable_by(cmp_segment_meta_by_time);

        let ids: Vec<String> = v.into_iter().map(|s| s.segment_id.0).collect();
        assert_eq!(ids, vec!["a", "m", "z"]);
    }

    #[test]
    fn ordering_uses_segment_id_as_final_tie_breaker() {
        let mut v = vec![seg("b", 10, 20), seg("a", 10, 20), seg("c", 10, 20)];

        v.sort_unstable_by(cmp_segment_meta_by_time);

        let ids: Vec<String> = v.into_iter().map(|s| s.segment_id.0).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }
}
