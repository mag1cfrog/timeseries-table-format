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
pub mod actions;
pub mod log_store;
pub mod segments;
pub mod table_metadata;
pub mod table_state;

pub use actions::{Commit, LogAction};
pub use log_store::TransactionLogStore;
pub use segments::{FileFormat, SegmentId, SegmentMeta};
pub use table_metadata::{
    LogicalColumn, LogicalSchema, LogicalSchemaError, TableKind, TableMeta, TableMetaDelta,
    TimeBucket, TimeIndexSpec,
};
pub use table_state::TableState;

use snafu::{Backtrace, prelude::*};

use crate::storage::StorageError;

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

#[cfg(test)]
mod tests {
    use crate::transaction_log::{
        table_metadata::{LogicalDataType, LogicalTimestampUnit},
        *,
    };

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

        let time_index = TimeIndexSpec {
            timestamp_column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string()],
            bucket: TimeBucket::Minutes(60),
            timezone: Some("UTC".to_string()),
        };

        let table_meta = TableMeta {
            kind: TableKind::TimeSeries(time_index),
            logical_schema: Some(
                LogicalSchema::new(vec![
                    LogicalColumn {
                        name: "ts".to_string(),
                        data_type: LogicalDataType::Timestamp {
                            unit: LogicalTimestampUnit::Micros,
                            timezone: None,
                        },
                        nullable: false,
                    },
                    LogicalColumn {
                        name: "symbol".to_string(),
                        data_type: LogicalDataType::Utf8,
                        nullable: false,
                    },
                ])
                .expect("valid logical schema"),
            ),
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
        // println!("{json}");

        // Deserialize back.
        let decoded: Commit = serde_json::from_str(&json).expect("deserialize commit");

        // Round-trip equality.
        assert_eq!(commit, decoded);
    }

    #[test]
    fn logical_schema_rejects_duplicate_columns() {
        let dup = LogicalSchema::new(vec![
            LogicalColumn {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Micros,
                    timezone: None,
                },
                nullable: false,
            },
            LogicalColumn {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Micros,
                    timezone: None,
                },
                nullable: false,
            },
        ]);

        let err = dup.expect_err("duplicate columns should be rejected");
        assert!(matches!(err, LogicalSchemaError::DuplicateColumn { column } if column == "ts"));
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
        let json = r#"{ "name": "price", "data_type": "Float64" }"#;

        let col: LogicalColumn = serde_json::from_str(json).expect("deserialize");

        assert_eq!(col.name, "price");
        assert_eq!(col.data_type, LogicalDataType::Float64);
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
