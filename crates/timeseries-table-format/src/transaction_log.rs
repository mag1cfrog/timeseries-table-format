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
//!   `TableKind`, `IndexSpec`, `SegmentMeta`, and `LogAction`.
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
//!         "path": "data/nvda_1h_0001.parquet",
//!         "format": "parquet",
//!         "entity_layout": {"Single": ["NVDA"]},
//!         "index_min": {"type": "timestamp", "value": "2020-01-01T00:00:00Z"},
//!         "index_max": {"type": "timestamp", "value": "2020-01-02T00:00:00Z"},
//!         "row_count": 1024
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
pub mod actions;
pub mod log_store;
pub(crate) mod segments;
pub mod table_state;

#[cfg(test)]
mod log_integration_tests;

pub use crate::metadata::{
    index::{IndexKind, IndexSpec, IndexValue, TimeIndexGranularity},
    protocol::TableProtocolError,
    table::{TableKind, TableMeta, TableMetaDelta},
};
pub use actions::{Commit, LogAction};
pub use log_store::TransactionLogStore;
pub use segments::{FileFormat, SegmentEntityLayout, SegmentError, SegmentMeta};
pub use table_state::TableState;

use snafu::{Backtrace, prelude::*};

use crate::{
    metadata::{
        index::IndexSpecError, schema_compat::SchemaCompatibilityError, segments::SegmentMetaError,
    },
    storage::StorageError,
};

/// Errors that can occur while reading or writing the commit log.
#[derive(Debug, Snafu)]
#[non_exhaustive]
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

    /// The table protocol is incompatible with this operation.
    #[snafu(context(false), display("Table protocol error: {source}"))]
    Protocol {
        /// Complete table protocol failure.
        #[snafu(source)]
        source: crate::metadata::protocol::TableProtocolError,
        /// Backtrace captured at the transaction-log boundary.
        backtrace: Backtrace,
    },

    /// A commit payload could not be decoded from JSON.
    #[snafu(display("Failed to deserialize commit {version}: {source}"))]
    CommitDeserialization {
        /// Commit version being decoded.
        version: u64,
        /// JSON decoding failure.
        source: serde_json::Error,
        /// Backtrace captured at the transaction-log boundary.
        backtrace: Backtrace,
    },

    /// A commit payload could not be encoded as JSON.
    #[snafu(display("Failed to serialize commit {version}: {source}"))]
    CommitSerialization {
        /// Commit version being encoded.
        version: u64,
        /// JSON encoding failure.
        source: serde_json::Error,
        /// Backtrace captured at the transaction-log boundary.
        backtrace: Backtrace,
    },

    /// The CURRENT pointer is not an unsigned transaction-log version.
    #[snafu(display("CURRENT has invalid content {contents:?}: {source}"))]
    CurrentVersionParse {
        /// Invalid trimmed CURRENT contents.
        contents: String,
        /// Integer parsing failure.
        source: std::num::ParseIntError,
        /// Backtrace captured at the transaction-log boundary.
        backtrace: Backtrace,
    },

    /// A persisted table-relative path is invalid.
    #[snafu(display("Invalid persisted {description} {path:?}: {source}"))]
    InvalidPersistedPath {
        /// Kind of persisted path being validated.
        description: String,
        /// Rejected persisted path.
        path: String,
        /// Structured path validation failure.
        #[snafu(source(from(StorageError, Box::new)), backtrace)]
        source: Box<StorageError>,
    },

    /// A persisted ordered-index specification is invalid.
    #[snafu(display("Invalid persisted ordered-index specification: {source}"))]
    InvalidIndexSpec {
        /// Ordered-index validation failure.
        source: IndexSpecError,
        /// Backtrace captured while rebuilding table state.
        backtrace: Backtrace,
    },

    /// Persisted table schema and ordered-index metadata are incompatible.
    #[snafu(display("Persisted table schema is incompatible with its ordered index: {source}"))]
    TableSchemaCompatibility {
        /// Complete schema compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)), backtrace)]
        source: Box<SchemaCompatibilityError>,
    },

    /// A persisted single-entity segment identity is incompatible with the table schema.
    #[snafu(display("Invalid single-entity identity in segment at {path}: {source}"))]
    SegmentEntityIdentitySchema {
        /// Persisted segment path.
        path: String,
        /// Complete entity identity compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)), backtrace)]
        source: Box<SchemaCompatibilityError>,
    },

    /// Persisted segment metadata violates its registered ordered-index domain.
    #[snafu(display("Invalid persisted segment metadata: {source}"))]
    SegmentMetadata {
        /// Complete segment metadata validation failure.
        #[snafu(source(from(SegmentMetaError, Box::new)), backtrace)]
        source: Box<SegmentMetaError>,
    },

    /// Rebuilding table state was requested before the first commit.
    #[snafu(display("Cannot rebuild table state because CURRENT is 0"))]
    UninitializedTableState {
        /// Backtrace captured at the state rebuild boundary.
        backtrace: Backtrace,
    },

    /// A commit file name and its payload disagree on the version.
    #[snafu(display("Commit version mismatch: expected {expected}, found {found} in the payload"))]
    CommitVersionMismatch {
        /// Version selected by the commit file name.
        expected: u64,
        /// Version stored in the payload.
        found: u64,
        /// Backtrace captured while rebuilding table state.
        backtrace: Backtrace,
    },

    /// More than one live AddSegment action uses the same path.
    #[snafu(display("Duplicate live segment path: {path}"))]
    DuplicateLiveSegmentPath {
        /// Repeated live segment path.
        path: String,
        /// Backtrace captured while rebuilding table state.
        backtrace: Backtrace,
    },

    /// No table metadata was found while replaying the selected commits.
    #[snafu(display("No table metadata found in commits up to version {current_version}"))]
    MissingTableMetadata {
        /// Latest commit version included in the replay.
        current_version: u64,
        /// Backtrace captured while rebuilding table state.
        backtrace: Backtrace,
    },

    /// A persisted coverage pointer describes a different ordered index.
    #[snafu(display(
        "Table coverage index kind does not match the table index: expected {expected:?}, found {actual:?} in pointer from version {pointer_version}"
    ))]
    CoverageIndexKindMismatch {
        /// Ordered-index kind from table metadata.
        expected: IndexKind,
        /// Ordered-index kind stored in the coverage pointer.
        actual: IndexKind,
        /// Commit version that supplied the pointer.
        pointer_version: u64,
        /// Backtrace captured while rebuilding table state.
        backtrace: Box<Backtrace>,
    },

    /// Persisted segments exist without the logical schema needed to validate them.
    #[snafu(display("Persisted segments require a logical schema"))]
    MissingLogicalSchemaForSegments {
        /// Backtrace captured while rebuilding table state.
        backtrace: Backtrace,
    },

    /// A persisted segment entity layout is incompatible with the table metadata.
    #[snafu(display(
        "Invalid entity layout in segment at {path}: table has {entity_column_count} entity columns, layout is {layout:?}"
    ))]
    InvalidSegmentEntityLayout {
        /// Persisted segment path.
        path: String,
        /// Number of entity columns configured by the table.
        entity_column_count: usize,
        /// Rejected persisted layout.
        layout: SegmentEntityLayout,
        /// Backtrace captured while rebuilding table state.
        backtrace: Backtrace,
    },

    /// Incrementing the transaction-log version would overflow `u64`.
    #[snafu(display("Transaction-log version overflow at {current_version}"))]
    VersionOverflow {
        /// Current version that cannot be incremented.
        current_version: u64,
        /// Backtrace captured at the version calculation boundary.
        backtrace: Backtrace,
    },

    /// The CURRENT pointer contains no version.
    #[snafu(display("CURRENT has empty content at {path}"))]
    EmptyCurrentPointer {
        /// Table-relative CURRENT path.
        path: String,
        /// Backtrace captured at the transaction-log boundary.
        backtrace: Backtrace,
    },

    /// A commit operation failed and its newly-created commit file may remain.
    #[snafu(display(
        "Commit outcome is ambiguous at {commit_path}: {operation_error}; failed to remove the commit file: {cleanup_error}"
    ))]
    AmbiguousOutcome {
        /// Path of the commit file that may remain unpublished.
        commit_path: String,
        /// Write, sync, or publish failure that triggered cleanup.
        #[snafu(source, backtrace)]
        operation_error: Box<StorageError>,
        /// Failure encountered while removing the unpublished commit file.
        cleanup_error: Box<StorageError>,
    },
}

pub(crate) fn checked_next_version(expected: u64) -> Result<u64, CommitError> {
    expected
        .checked_add(1)
        .ok_or_else(|| CommitError::VersionOverflow {
            current_version: expected,
            backtrace: Backtrace::capture(),
        })
}

#[cfg(test)]
mod tests {
    use crate::coverage::EntityIdentity;
    use crate::metadata::logical_schema::{
        LogicalDataType, LogicalField, LogicalSchema, LogicalSchemaValidationError,
        LogicalTimestampUnit,
    };
    use crate::metadata::protocol::TABLE_PROTOCOL_VERSION;
    use crate::transaction_log::*;

    use chrono::{DateTime, TimeZone, Utc};
    use serde_json;

    // ==================== Serialization tests ====================

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

    #[test]
    fn commit_json_roundtrip() {
        let ts0 = utc_datetime(2025, 1, 1, 0, 0, 0);
        let ts1 = utc_datetime(2025, 1, 1, 1, 0, 0);

        let time_index = IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string()],
            kind: IndexKind::Timestamp {
                index_granularity: TimeIndexGranularity::Minutes(60),
                timezone: Some("UTC".to_string()),
            },
        };

        let table_meta = TableMeta {
            kind: TableKind::TimeSeries(time_index),
            logical_schema: Some(
                LogicalSchema::new(vec![
                    LogicalField {
                        name: "ts".to_string(),
                        data_type: LogicalDataType::Timestamp {
                            unit: LogicalTimestampUnit::Micros,
                            timezone: None,
                        },
                        nullable: false,
                    },
                    LogicalField {
                        name: "symbol".to_string(),
                        data_type: LogicalDataType::Utf8,
                        nullable: false,
                    },
                ])
                .expect("valid logical schema"),
            ),
            created_at: ts0,
            protocol_version: TABLE_PROTOCOL_VERSION,
            required_reader_features: Default::default(),
            required_writer_features: Default::default(),
        };

        let seg_meta = SegmentMeta {
            path: "data/nvda_1h_0001.parquet".to_string(),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::Single(
                EntityIdentity::try_new(vec!["NVDA".into()]).expect("valid identity"),
            ),
            index_min: (ts0).into(),
            index_max: (ts1).into(),
            row_count: 1024,
            file_size: None,
            coverage_path: None,
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
        assert!(json.contains(&format!("\"protocol_version\": {TABLE_PROTOCOL_VERSION}")));
        assert!(json.contains("\"required_reader_features\": []"));
        assert!(json.contains("\"required_writer_features\": []"));
        // println!("{json}");

        // Deserialize back.
        let decoded: Commit = serde_json::from_str(&json).expect("deserialize commit");

        // Round-trip equality.
        assert_eq!(commit, decoded);
    }

    #[test]
    fn logical_schema_rejects_duplicate_columns() {
        let dup = LogicalSchema::new(vec![
            LogicalField {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Micros,
                    timezone: None,
                },
                nullable: false,
            },
            LogicalField {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Micros,
                    timezone: None,
                },
                nullable: false,
            },
        ]);

        let err = dup.expect_err("duplicate columns should be rejected");
        assert!(
            matches!(err, LogicalSchemaValidationError::DuplicateColumn { column } if column == "ts")
        );
    }

    #[test]
    fn time_index_spec_defaults() {
        // JSON with optional fields omitted.
        let json = r#"{
            "column": "ts",
            "kind": {
                "type": "timestamp",
                "index_granularity": { "Hours": 1 }
            }
        }"#;

        let spec: IndexSpec = serde_json::from_str(json).expect("deserialize");

        assert_eq!(spec.column, "ts");
        assert_eq!(spec.entity_columns, Vec::<String>::new()); // default
        assert_eq!(
            spec.kind,
            IndexKind::Timestamp {
                index_granularity: TimeIndexGranularity::Hours(1),
                timezone: None
            }
        );
    }

    #[test]
    fn time_index_spec_skips_none_timezone_on_serialize() {
        let spec = IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec![],
            kind: IndexKind::Timestamp {
                index_granularity: TimeIndexGranularity::Seconds(30),
                timezone: None,
            },
        };

        let json = serde_json::to_string(&spec).expect("serialize");

        // "timezone" key should be absent.
        assert!(!json.contains("timezone"));
    }

    #[test]
    fn logical_column_nullable_requires_explicit_value() {
        let json = r#"{ "name": "price", "data_type": "Float64" }"#;

        let err = serde_json::from_str::<LogicalField>(json).unwrap_err();
        assert!(
            err.to_string().contains("missing field `nullable`"),
            "unexpected error: {err}"
        );
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
    fn all_time_index_granularity_variants_roundtrip() {
        let granularities = vec![
            TimeIndexGranularity::Seconds(15),
            TimeIndexGranularity::Minutes(5),
            TimeIndexGranularity::Hours(24),
            TimeIndexGranularity::Days(7),
        ];

        for index_granularity in granularities {
            let json = serde_json::to_string(&index_granularity).expect("serialize");
            let decoded: TimeIndexGranularity = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(index_granularity, decoded);
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
            path: "data/seg-to-remove.parquet".to_string(),
        };

        let json = serde_json::to_string(&action).expect("serialize");
        let decoded: LogAction = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(action, decoded);
    }

    #[test]
    fn commit_with_empty_actions() {
        let ts = utc_datetime(2025, 6, 15, 12, 0, 0);

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
}
