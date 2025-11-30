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
