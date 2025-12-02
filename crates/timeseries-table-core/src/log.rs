//! Append-only metadata log and table state.
//!
//! This module implements the Delta-inspired metadata layer for
//! `timeseries-table-format` and defines the logical metadata model
//! written to and read from the `_timeseries_log/` directory.
//!
//! - A simple append-only commit log stored as JSON files under a
//!   `_timeseries_log/` directory (for example, `_timeseries_log/0000000000.json`).
//! - A `CURRENT` pointer that tracks the latest committed table version.
//! - Strongly-typed metadata structures such as `TableMeta`,
//!   `TableKind`, `TimeIndexSpec`, `SegmentMeta`, and `LogAction`.
//! - An optimistic concurrency model based on version guards, so that
//!   commits fail cleanly with a conflict error when the expected
//!   version does not match the current version.
//! - A `TableState` representation materialized from the log, which
//!   describes the current table version, metadata, and active segments.
//!
//! The log is designed to be:
//!
//! - **Append-only**: commits never mutate existing files.
//! - **Monotonically versioned**: versions are `u64` values that only
//!   increase, enforced by the commit API.
//! - **Human-inspectable**: JSON commits and a small set of actions
//!   make it easy to debug with basic tools.
//!
//! ## On-disk layout (high level)
//!
//! ```text
//! table_root/
//!   _timeseries_log/
//!     CURRENT                  # latest committed version (e.g. "3\n")
//!     0000000001.json          # Commit version 1
//!     0000000002.json          # Commit version 2
//!     0000000003.json          # Commit version 3
//!   data/                      # Parquet segments live here (convention for now)
//! ```
//!
//! Each `*.json` file contains a single [`Commit`] value, encoded as JSON. For
//! example:
//!
//! ```json
//! {
//!   "version": 1,
//!   "base_version": 0,
//!   "timestamp": "2025-01-01T00:00:00Z",
//!   "actions": [
//!     {
//!       "AddSegment": {
//!         "segment_id": "seg-0001",
//!         "path": "data/nvda_1h_0001.parquet",
//!         "ts_min": "2020-01-01T00:00:00Z",
//!         "ts_max": "2020-01-02T00:00:00Z",
//!         "row_count": 1024,
//!         "format": "parquet"
//!       }
//!     }
//!   ]
//! }
//! ```
//!
//! In v0.1 the log is strictly append-only, and table state is reconstructed by
//! replaying every commit up to the version referenced by `CURRENT`. This module
//! does not know about query engines; it only provides the persisted metadata
//! and an API for committing changes safely.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, prelude::*};

use crate::storage::{self, StorageError, TableLocation};

/// Errors that can occur while reading or writing the commit log.
#[derive(Debug, Snafu)]
pub enum CommitError {
    /// The caller's expected_version does not match the CURRENT pointer.
    #[snafu(display("Commit conflict: expected version {expected}, but CURRENT is {found}"))]
    Conflict {
        /// The version the caller expected to be current.
        expected: u64,
        /// The actual current version found.
        found: u64,
        /// Backtrace for debugging.
        backtrace: Backtrace,
    },

    /// Underlying storage error while working with the log or CURRENT file.
    ///
    /// Backtraces are delegated to the inner StorageError.
    #[snafu(display("Storage error while accessing commit log: {source}"))]
    Storage {
        /// Underlying storage error returned by the storage backend.
        #[snafu(backtrace)]
        source: StorageError,
    },

    /// The log or CURRENT file is in an unexpected / malformed state.
    #[snafu(display("Corrupt log state: {msg}"))]
    CorruptState {
        /// A description of the corrupt state.
        msg: String,
        /// Backtrace for debugging.
        backtrace: Backtrace,
    },
}

/// Helper for reading and writing the commit log under a table root.
///
/// Layout:
///   <root>/_timeseries_log/0000000001.json
///   <root>/_timeseries_log/0000000002.json
///   <root>/_timeseries_log/CURRENT
#[derive(Debug, Clone)]
pub struct LogStore {
    location: TableLocation,
}

impl LogStore {
    /// Name of the subdirectory containing the commit log.
    pub const LOG_DIR_NAME: &str = "_timeseries_log";
    /// Name of the file that stores the current version pointer.
    pub const CURRENT_FILE_NAME: &str = "CURRENT";
    /// Number of digits used in zero-padded commit file names.
    pub const COMMIT_FILENAME_DIGITS: usize = 10;

    /// Create a new LogStore rooted at a table directory.
    pub fn new(location: TableLocation) -> Self {
        Self { location }
    }

    fn log_rel_dir() -> PathBuf {
        PathBuf::from(Self::LOG_DIR_NAME)
    }

    fn current_rel_path() -> PathBuf {
        Self::log_rel_dir().join(Self::CURRENT_FILE_NAME)
    }

    fn commit_rel_path(version: u64) -> PathBuf {
        let file_name = format!(
            "{:0width$}.json",
            version,
            width = Self::COMMIT_FILENAME_DIGITS
        );
        Self::log_rel_dir().join(file_name)
    }

    async fn write_atomic_rel(&self, rel: &Path, contents: &[u8]) -> Result<(), CommitError> {
        storage::write_atomic(&self.location, rel, contents)
            .await
            .context(StorageSnafu)?;
        Ok(())
    }

    /// Load the CURRENT version pointer.
    ///
    /// Behavior:
    /// - If CURRENT does not exist, treat as a fresh table and return 0.
    /// - If CURRENT contains invalid or empty content, return CorruptState.
    pub async fn load_current_version(&self) -> Result<u64, CommitError> {
        let rel = Self::current_rel_path();

        let contents = match storage::read_to_string(&self.location, &rel).await {
            Ok(s) => s,
            Err(StorageError::NotFound { .. }) => return Ok(0),
            Err(source) => return Err(CommitError::Storage { source }),
        };

        let trimmed = contents.trim();
        if trimmed.is_empty() {
            return CorruptStateSnafu {
                msg: format!("CURRENT has empty content at {rel:?}",),
            }
            .fail();
        }
        let version = trimmed
            .parse::<u64>()
            .map_err(|e| CommitError::CorruptState {
                msg: format!("CURRENT has invalid content {trimmed:?}: {e}"),
                backtrace: Backtrace::capture(),
            })?;

        Ok(version)
    }

    /// Commit a new version with an optimistic concurrency guard.
    ///
    /// Steps:
    /// - Load CURRENT.
    /// - If CURRENT != expected, return CommitError::Conflict.
    /// - Compute version = expected + 1 (with overflow check).
    /// - Build a Commit.
    /// - Serialize to JSON.
    /// - Create commit file `_timeseries_log/<zero-padded>.json` using
    ///   "create only if not exists" semantics.
    /// - Update `_timeseries_log/CURRENT` with the new version (e.g. "1 ").
    pub async fn commit_with_expected_version(
        &self,
        expected: u64,
        actions: Vec<LogAction>,
    ) -> Result<u64, CommitError> {
        // 1) Guard on CURRENT
        let current = self.load_current_version().await?;
        if current != expected {
            return ConflictSnafu {
                expected,
                found: current,
            }
            .fail();
        }

        // 2) Compute next version with overflow guard
        let version = expected.checked_add(1).context(CorruptStateSnafu {
            msg: "version counter overflow".to_string(),
        })?;

        // 3) Build commit payload
        let commit = Commit {
            version,
            base_version: expected,
            timestamp: Utc::now(),
            actions,
        };

        let json = serde_json::to_vec(&commit).map_err(|e| CommitError::CorruptState {
            msg: format!("failed to serialize commit {version}: {e}"),
            backtrace: Backtrace::capture(),
        })?;

        // 4) Attempt to create the commit file *only if it does not already exist*.
        let commit_rel = Self::commit_rel_path(version);
        if let Err(source) = storage::write_new(&self.location, &commit_rel, &json).await {
            match &source {
                // If the commit file for this version already exists, someone else
                // committed first. Map to a conflict, using CURRENT to report `found`.
                StorageError::AlreadyExists { .. } => {
                    let found = self.load_current_version().await?;
                    return ConflictSnafu { expected, found }.fail();
                }
                _ => {
                    return Err(CommitError::Storage { source });
                }
            }
        }

        // 5) Update CURRENT via atomic write (temp + rename).
        let current_rel = Self::current_rel_path();
        let current_contents = format!("{version}\n");
        self.write_atomic_rel(&current_rel, current_contents.as_bytes())
            .await?;

        Ok(version)
    }
}

/// Granularity for time buckets used by coverage/bitmap logic.
///
/// This does not affect physical storage directly, but describes how the time
/// axis is discretized when building coverage bitmaps and computing gaps.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeBucket {
    /// A bucket spanning a fixed number of seconds.
    Seconds(u32),
    /// A bucket spanning a fixed number of minutes.
    Minutes(u32),
    /// A bucket spanning a fixed number of hours.
    Hours(u32),
    /// A bucket spanning a fixed number of days.
    Days(u32),
}

/// Configuration for the time index of a time-series table.
///
/// In v0.1 this is assumed to exist for all "time-series" tables; a future
/// `TableKind::Generic` may omit it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeIndexSpec {
    /// Name of the timestamp column (for example, `"ts"` or `"timestamp"`).
    pub timestamp_column: String,

    /// Optional entity/symbol columns that help partition the time axis
    /// (for example, `["symbol"]` or `["symbol", "venue"]`).
    ///
    /// This is metadata only; enforcement and partitioning are handled by
    /// higher layers.
    #[serde(default)]
    pub entity_columns: Vec<String>,

    /// Logical bucket size used by coverage bitmaps (for example, 1 minute, 1 hour).
    pub bucket: TimeBucket,

    /// Optional IANA timezone identifier (for example, `"America/New_York"`).
    ///
    /// For v0.1 this is primarily reserved for future use; timestamps are
    /// generally expected to be stored in UTC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

/// The high-level "kind" of table.
///
/// v0.1 supports only `TimeSeries`, but a `Generic` kind is reserved so that
/// the log format can represent non-timeseries tables later without breaking
/// existing JSON.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TableKind {
    /// A time-series table with an explicit time index specification.
    TimeSeries(TimeIndexSpec),

    /// Placeholder for future basic tables that do not have a time index.
    /// Not used in v0.1.
    Generic,
}

/// A minimal logical schema representation.
///
/// This is intentionally simple in v0.1: it records column names, types as
/// strings, and nullability. A future version may align this more closely
/// with Arrow or another schema model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalColumn {
    /// Column name as it appears in the data.
    pub name: String,
    /// Logical data type as a free-form string (e.g. `"int64"`, `"timestamp[us]"`).
    pub data_type: String,
    /// Whether the column may contain NULLs.
    #[serde(default)]
    pub nullable: bool,
}

/// Logical schema metadata describing the ordered collection of logical columns.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalSchema {
    /// All logical columns that compose the schema in their defined order.
    pub columns: Vec<LogicalColumn>,
}

/// High-level table metadata stored in the log.
///
/// This describes the table kind, a logical schema (optional in v0.1), and
/// basic bookkeeping fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableMeta {
    /// Table kind: TimeSeries or Generic.
    pub kind: TableKind,

    /// Optional logical schema description.
    ///
    /// v0.1 can treat this as informational; enforcement is handled by
    /// higher layers.
    pub logical_schema: Option<LogicalSchema>,

    /// Creation timestamp of the table, stored as RFC3339 UTC.
    pub created_at: DateTime<Utc>,

    /// Format version for future evolution of the log/table format.
    ///
    /// v0.1 can hard-code this to 1.
    pub format_version: u32,
}

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

/// For v0.1, a `TableMetaDelta` is just a full replacement of [`TableMeta`].
///
/// This alias keeps the wire format simple (the JSON is the same as `TableMeta`)
/// while leaving room to evolve to more granular metadata updates in future
/// versions (for example, partial updates or additive fields).
pub type TableMetaDelta = TableMeta;

/// An action recorded in a commit.
///
/// Each commit contains a sequence of actions that are applied in order to
/// evolve table state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogAction {
    /// Add or replace a segment.
    AddSegment(SegmentMeta),

    /// Remove a segment by its logical ID.
    RemoveSegment {
        /// Logical identifier of the segment to remove.
        segment_id: SegmentId,
    },

    /// Update table-level metadata (v0.1 uses full replacement).
    UpdateTableMeta(TableMetaDelta),
}

/// A single, immutable commit in the metadata log.
///
/// Commits are written to files such as `_timeseries_log/0000000001.json`.
/// The version field must match the file name; `base_version` records what
/// the writer believed was the current version when the commit was prepared.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Commit {
    /// The version number of this commit (monotonic, starting from 1).
    pub version: u64,

    /// The version that the writer believed was current when preparing this
    /// commit. Used by the OCC layer as a guard.
    pub base_version: u64,

    /// Commit creation timestamp, stored as RFC3339 UTC.
    pub timestamp: DateTime<Utc>,

    /// Ordered list of actions that describe how table state changes in this commit.
    pub actions: Vec<LogAction>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json;

    #[test]
    fn commit_json_roundtrip() {
        let ts0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let ts1 = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();

        let time_index = TimeIndexSpec {
            timestamp_column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string()],
            bucket: TimeBucket::Minutes(60),
            timezone: Some("UTC".to_string()),
        };

        let table_meta = TableMeta {
            kind: TableKind::TimeSeries(time_index),
            logical_schema: Some(LogicalSchema {
                columns: vec![
                    LogicalColumn {
                        name: "ts".to_string(),
                        data_type: "timestamp[us]".to_string(),
                        nullable: false,
                    },
                    LogicalColumn {
                        name: "symbol".to_string(),
                        data_type: "utf8".to_string(),
                        nullable: false,
                    },
                ],
            }),
            created_at: ts0,
            format_version: 1,
        };

        let seg_meta = SegmentMeta {
            segment_id: SegmentId("seg-0001".to_string()),
            path: "data/nvda_1h_0001.parquet".to_string(),
            format: FileFormat::Parquet,
            ts_min: ts0,
            ts_max: ts1,
            row_count: 1024,
        };

        let commit = Commit {
            version: 1,
            base_version: 0,
            timestamp: ts1,
            actions: vec![
                LogAction::UpdateTableMeta(table_meta),
                LogAction::AddSegment(seg_meta),
            ],
        };

        // Serialize to JSON.
        let json = serde_json::to_string_pretty(&commit).expect("serialize commit");
        // println!("{json}");

        // Deserialize back.
        let decoded: Commit = serde_json::from_str(&json).expect("deserialize commit");

        // Round-trip equality.
        assert_eq!(commit, decoded);
    }

    #[test]
    fn time_index_spec_defaults() {
        // JSON with optional fields omitted.
        let json = r#"{
            "timestamp_column": "ts",
            "bucket": { "Hours": 1 }
        }"#;

        let spec: TimeIndexSpec = serde_json::from_str(json).expect("deserialize");

        assert_eq!(spec.timestamp_column, "ts");
        assert_eq!(spec.entity_columns, Vec::<String>::new()); // default
        assert_eq!(spec.bucket, TimeBucket::Hours(1));
        assert_eq!(spec.timezone, None); // default
    }

    #[test]
    fn time_index_spec_skips_none_timezone_on_serialize() {
        let spec = TimeIndexSpec {
            timestamp_column: "ts".to_string(),
            entity_columns: vec![],
            bucket: TimeBucket::Seconds(30),
            timezone: None,
        };

        let json = serde_json::to_string(&spec).expect("serialize");

        // "timezone" key should be absent.
        assert!(!json.contains("timezone"));
    }

    #[test]
    fn logical_column_nullable_defaults_to_false() {
        let json = r#"{ "name": "price", "data_type": "f64" }"#;

        let col: LogicalColumn = serde_json::from_str(json).expect("deserialize");

        assert_eq!(col.name, "price");
        assert_eq!(col.data_type, "f64");
        assert!(!col.nullable); // default is false
    }

    #[test]
    fn table_kind_generic_roundtrip() {
        let kind = TableKind::Generic;
        let json = serde_json::to_string(&kind).expect("serialize");
        let decoded: TableKind = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(kind, decoded);
        assert_eq!(json, r#""Generic""#);
    }

    #[test]
    fn all_time_bucket_variants_roundtrip() {
        let buckets = vec![
            TimeBucket::Seconds(15),
            TimeBucket::Minutes(5),
            TimeBucket::Hours(24),
            TimeBucket::Days(7),
        ];

        for bucket in buckets {
            let json = serde_json::to_string(&bucket).expect("serialize");
            let decoded: TimeBucket = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(bucket, decoded);
        }
    }

    #[test]
    fn file_format_serializes_lowercase() {
        let format = FileFormat::Parquet;
        let json = serde_json::to_string(&format).expect("serialize");

        assert_eq!(json, r#""parquet""#);

        // Also verify round-trip.
        let decoded: FileFormat = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(format, decoded);
    }

    #[test]
    fn file_format_default_is_parquet() {
        assert_eq!(FileFormat::default(), FileFormat::Parquet);
    }

    #[test]
    fn remove_segment_action_roundtrip() {
        let action = LogAction::RemoveSegment {
            segment_id: SegmentId("seg-to-remove".to_string()),
        };

        let json = serde_json::to_string(&action).expect("serialize");
        let decoded: LogAction = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(action, decoded);
    }

    #[test]
    fn commit_with_empty_actions() {
        let ts = Utc.with_ymd_and_hms(2025, 6, 15, 12, 0, 0).unwrap();

        let commit = Commit {
            version: 1,
            base_version: 0,
            timestamp: ts,
            actions: vec![],
        };

        let json = serde_json::to_string(&commit).expect("serialize");
        let decoded: Commit = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(commit, decoded);
        assert!(decoded.actions.is_empty());
    }

    #[test]
    fn segment_id_transparent_serialization() {
        let id = SegmentId("my-segment".to_string());
        let json = serde_json::to_string(&id).expect("serialize");

        // Should be a plain string, not {"0": "my-segment"}.
        assert_eq!(json, r#""my-segment""#);

        let decoded: SegmentId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(id, decoded);
    }
}
