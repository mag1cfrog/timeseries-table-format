//! Segment identifiers, formats, and per-file metadata recorded in the log.
//!
//! These types describe each physical data slice a commit can add or remove:
//! [`SegmentId`] is a strong string newtype, [`SegmentMeta`] captures relative
//! paths, timestamp bounds, and row counts, and [`FileFormat`] tracks the
//! on-disk encoding. They are used by `LogAction::AddSegment` and related
//! reader logic to rebuild the live segment map.

use std::fmt;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use parquet::errors::ParquetError;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, prelude::*};

use crate::{
    common::time_column::TimeColumnError,
    storage::{self, StorageError, TableLocation},
    transaction_log::logical_schema::LogicalSchemaError,
};

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
/// In v0.1, only `Parquet` will be implemented, but the enum keeps the
/// metadata model open to other formats in future versions.
///
/// JSON layout example:
/// `"format": "parquet"`
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

    /// Coverage sidecar pointer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coverage_path: Option<String>,
}

/// Errors that can occur while validating or handling segment metadata.
#[derive(Debug, Snafu)]
pub enum SegmentMetaError {
    /// File format is not supported for v0.1.
    #[snafu(display("Unsupported file format: {format:?}"))]
    UnsupportedFormat {
        /// The offending file format.
        format: FileFormat,
    },

    /// The file is missing or not a regular file.
    #[snafu(display("Segment file missing or not a regular file: {path}"))]
    MissingFile {
        /// The path to the missing or invalid file.
        path: String,
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

    /// Generic I/O error while validating the segment.
    #[snafu(display("I/O error while validating segment at {path}: {source}"))]
    Io {
        /// The path to the file that caused the I/O error.
        path: String,
        /// Underlying storage error that caused this I/O failure.
        #[snafu(source, backtrace)]
        source: StorageError,
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
        /// The path to the file without a valid LogicalSchema
        path: String,
        #[snafu(source)]
        /// Underlying logical schema error that triggered this failure.
        source: LogicalSchemaError,
    },
}

impl SegmentMetaError {
    /// Construct a `SegmentMetaError::Io` from a lower-level `StorageError` and
    /// the associated path so the storage error can be preserved as the source.
    pub fn from_storage(err: StorageError, path: String) -> Self {
        SegmentMetaError::Io { path, source: err }
    }
}

/// Derive a deterministic segment id for an append entry.
///
/// This is content-addressable: it hashes both the relative path and the bytes so
/// retries with the same input stay stable while same bytes at different paths
/// diverge. The returned id uses the `seg-` prefix followed by 32 hex chars of
/// the BLAKE3 digest, keeping ids bounded and safe for idempotent appends.
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

/// Convenience alias for results returned by segment metadata operations.
#[allow(clippy::result_large_err)]
pub type SegmentResult<T> = Result<T, SegmentMetaError>;

/// Convert a lower-level `StorageError` into the corresponding `SegmentMetaError`.
///
/// - `StorageError::NotFound` is mapped to `SegmentMetaError::MissingFile` with a fresh
///   backtrace.
/// - All other storage errors are wrapped in `SegmentMetaError::Io`, preserving the original
///   `StorageError` as the source for diagnostics.
pub fn map_storage_error(err: StorageError) -> SegmentMetaError {
    match &err {
        StorageError::NotFound { path, .. } => SegmentMetaError::MissingFile { path: path.clone() },

        // For everything else, preserve the full StorageError as the source.
        StorageError::AlreadyExists { path, .. } | StorageError::OtherIo { path, .. } => {
            SegmentMetaError::Io {
                path: path.clone(),
                source: err, // move the full StorageError in
            }
        }
    }
}

impl SegmentMeta {
    /// Construct a validated Parquet SegmentMeta for a file.
    ///
    /// - `location` describes where the table lives (e.g. local root).
    /// - `path` is the logical path stored in the log (e.g. "data/seg1.parquet"
    ///   or an absolute path).
    ///
    /// This is a v0.1 local-filesystem helper: it relies on `storage::read_head_tail_4`
    /// which currently only supports `TableLocation::Local`.
    pub async fn for_parquet(
        location: &TableLocation,
        segment_id: SegmentId,
        path: &str,
        ts_min: DateTime<Utc>,
        ts_max: DateTime<Utc>,
        row_count: u64,
    ) -> SegmentResult<Self> {
        // Use storage layer to get len + first/last 4 bytes.
        let probe = storage::read_head_tail_4(location, std::path::Path::new(path))
            .await
            .map_err(map_storage_error)?;

        if probe.len < 8 {
            return TooShortSnafu {
                path: path.to_string(),
            }
            .fail();
        }

        const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

        if &probe.head != PARQUET_MAGIC || &probe.tail != PARQUET_MAGIC {
            return InvalidMagicSnafu {
                path: path.to_string(),
            }
            .fail();
        }

        Ok(SegmentMeta {
            segment_id,
            path: path.to_string(),
            format: FileFormat::Parquet,
            ts_min,
            ts_max,
            row_count,
            coverage_path: None,
        })
    }

    /// Set the coverage sidecar path for this segment metadata.
    pub fn with_coverage_path(mut self, path: impl Into<String>) -> Self {
        self.coverage_path = Some(path.into());
        self
    }

    /// Format-dispatching constructor that can grow in future versions.
    ///
    /// v0.1: only `FileFormat::Parquet` is supported and validated via
    /// `for_parquet`.
    pub async fn new_validated(
        location: &TableLocation,
        segment_id: SegmentId,
        path: &str,
        format: FileFormat,
        ts_min: DateTime<Utc>,
        ts_max: DateTime<Utc>,
        row_count: u64,
    ) -> SegmentResult<Self> {
        match format {
            FileFormat::Parquet => {
                SegmentMeta::for_parquet(location, segment_id, path, ts_min, ts_max, row_count)
                    .await
            } // other => UnsupportedFormatSnafu { format: other }.fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone};
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn utc_datetime(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .expect("valid UTC timestamp")
    }

    fn sample_segment_meta() -> SegmentMeta {
        SegmentMeta {
            segment_id: SegmentId("seg-001".to_string()),
            path: "data/seg-001.parquet".to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2025, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2025, 1, 1, 1, 0, 0),
            row_count: 123,
            coverage_path: None,
        }
    }

    #[test]
    fn segment_meta_json_roundtrip_with_and_without_coverage_path() {
        // Without coverage_path
        let seg = sample_segment_meta();
        let json = serde_json::to_string(&seg).unwrap();
        let back: SegmentMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(back.coverage_path, None);

        // With coverage_path
        let seg2 = sample_segment_meta().with_coverage_path("_coverage/segments/a.roar");
        let json2 = serde_json::to_string(&seg2).unwrap();
        let back2: SegmentMeta = serde_json::from_str(&json2).unwrap();
        assert_eq!(
            back2.coverage_path.as_deref(),
            Some("_coverage/segments/a.roar")
        );
    }

    async fn write_bytes(path: &std::path::Path, bytes: &[u8]) -> Result<(), std::io::Error> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, bytes).await
    }

    fn into_boxed(err: SegmentMetaError) -> Box<dyn std::error::Error> {
        Box::new(err)
    }

    #[tokio::test]
    async fn parquet_segment_validation_succeeds() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = "data/valid.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_bytes(&abs_path, b"PAR1PAR1").await?;

        let ts_min = utc_datetime(2025, 1, 1, 0, 0, 0);
        let ts_max = utc_datetime(2025, 1, 1, 1, 0, 0);

        let meta = SegmentMeta::for_parquet(
            &location,
            SegmentId("seg-001".to_string()),
            rel_path,
            ts_min,
            ts_max,
            1_234,
        )
        .await
        .map_err(into_boxed)?;

        assert_eq!(meta.path, rel_path);
        assert_eq!(meta.segment_id, SegmentId("seg-001".to_string()));
        assert_eq!(meta.format, FileFormat::Parquet);
        assert_eq!(meta.ts_min, ts_min);
        assert_eq!(meta.ts_max, ts_max);
        assert_eq!(meta.row_count, 1_234);

        Ok(())
    }

    #[tokio::test]
    async fn parquet_segment_missing_file_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let result = SegmentMeta::for_parquet(
            &location,
            SegmentId("missing".to_string()),
            "data/missing.parquet",
            utc_datetime(2025, 1, 1, 0, 0, 0),
            utc_datetime(2025, 1, 1, 1, 0, 0),
            42,
        )
        .await;

        assert!(matches!(result, Err(SegmentMetaError::MissingFile { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn parquet_segment_too_short_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = "data/short.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_bytes(&abs_path, b"PAR1").await?;

        let result = SegmentMeta::for_parquet(
            &location,
            SegmentId("short".to_string()),
            rel_path,
            utc_datetime(2025, 1, 1, 0, 0, 0),
            utc_datetime(2025, 1, 1, 1, 0, 0),
            10,
        )
        .await;

        assert!(matches!(result, Err(SegmentMetaError::TooShort { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn parquet_segment_invalid_magic_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = "data/invalid_magic.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_bytes(&abs_path, b"PAR1NOPE").await?;

        let result = SegmentMeta::for_parquet(
            &location,
            SegmentId("bad".to_string()),
            rel_path,
            utc_datetime(2025, 1, 1, 0, 0, 0),
            utc_datetime(2025, 1, 1, 1, 0, 0),
            10,
        )
        .await;

        assert!(matches!(result, Err(SegmentMetaError::InvalidMagic { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn new_validated_delegates_to_parquet_constructor() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = "data/delegate.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_bytes(&abs_path, b"PAR1PAR1").await?;

        let ts_min = utc_datetime(2025, 1, 1, 0, 0, 0);
        let ts_max = utc_datetime(2025, 1, 1, 1, 0, 0);

        let meta = SegmentMeta::new_validated(
            &location,
            SegmentId("delegate".to_string()),
            rel_path,
            FileFormat::Parquet,
            ts_min,
            ts_max,
            5,
        )
        .await
        .map_err(into_boxed)?;

        assert_eq!(meta.path, rel_path);
        assert_eq!(meta.format, FileFormat::Parquet);
        assert_eq!(meta.row_count, 5);
        Ok(())
    }
}
