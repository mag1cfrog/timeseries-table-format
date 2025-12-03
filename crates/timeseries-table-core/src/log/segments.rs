//! Segment identifiers, formats, and per-file metadata recorded in the log.
//!
//! These types describe each physical data slice a commit can add or remove:
//! [`SegmentId`] is a strong string newtype, [`SegmentMeta`] captures relative
//! paths, timestamp bounds, and row counts, and [`FileFormat`] tracks the
//! on-disk encoding. They are used by `LogAction::AddSegment` and related
//! reader logic to rebuild the live segment map.
use std::io;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, prelude::*};

use crate::storage::{self, StorageError, TableLocation};

/// Identifier for a physical segment (e.g. a Parquet file or group).
///
/// This is a logical ID used by the metadata; the actual file path is stored
/// separately in [`SegmentMeta`]. Using a newtype makes it harder to mix
/// up segment IDs with other stringly-typed fields.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct SegmentId(pub String);

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
}

/// Errors that can occur while validating or handling segment metadata.
#[derive(Debug, Snafu)]
pub enum SegmentMetaError {
    /// File format is not supported for v0.1.
    #[snafu(display("Unsupported file format: {format:?}"))]
    UnsupportedFormat {
        /// The offending file format.
        format: FileFormat,
        /// Diagnostic backtrace for this error.
        backtrace: Backtrace,
    },

    /// The file is missing or not a regular file.
    #[snafu(display("Segment file missing or not a regular file: {path}"))]
    MissingFile {
        /// The path to the missing or invalid file.
        path: String,
        /// Diagnostic backtrace for this error.
        backtrace: Backtrace,
    },

    /// The file is too short to be a valid Parquet file.
    #[snafu(display("Segment file too short to be valid Parquet: {path}"))]
    TooShort {
        /// The path to the file that was too short.
        path: String,
        /// Diagnostic backtrace for this error.
        backtrace: Backtrace,
    },

    /// Magic bytes at the start / end of file don't match the Parquet spec.
    #[snafu(display("Invalid Parquet magic bytes in segment file: {path}"))]
    InvalidMagic {
        /// The path to the file with invalid magic bytes.
        path: String,
        /// Diagnostic backtrace for this error.
        backtrace: Backtrace,
    },

    /// Generic I/O error while validating the segment.
    #[snafu(display("I/O error while validating segment at {path}: {source}"))]
    Io {
        /// The path to the file that caused the I/O error.
        path: String,
        /// The underlying I/O error.
        source: io::Error,
        /// Diagnostic backtrace for this error.
        backtrace: Backtrace,
    },
}

type SegmentResult<T> = Result<T, SegmentMetaError>;

fn map_storage_error(err: StorageError) -> SegmentMetaError {
    match err {
        StorageError::NotFound { path, .. } => SegmentMetaError::MissingFile {
            path,
            backtrace: Backtrace::capture(),
        },
        StorageError::LocalIo { path, source, .. } => SegmentMetaError::Io {
            path,
            source,
            backtrace: Backtrace::capture(),
        },
        StorageError::AlreadyExists { path, .. } => SegmentMetaError::Io {
            path,
            // Shouldn't really happen on reads, but we map it generically.
            source: io::Error::other("unexpected AlreadyExists while validating segment"),
            backtrace: Backtrace::capture(),
        },
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
        })
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
