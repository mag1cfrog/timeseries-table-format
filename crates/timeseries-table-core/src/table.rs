//! Time-series table abstraction on top of the metadata log.
//!
//! This module defines the high-level `TimeSeriesTable` API that sits
//! on top of the metadata log (`log` module) and segment metadata.
//!
//! Responsibilities:
//!
//! - Interpret `TableMeta` and `TableKind` for tables that are declared
//!   as `TableKind::TimeSeries(TimeIndexSpec)`.
//! - Provide create/open operations for time-series tables rooted at a
//!   given filesystem path.
//! - Use the underlying `TableState` and `SegmentMeta` to expose
//!   logical operations such as scanning a time range.
//! - Maintain the invariant that `TimeSeriesTable` only wraps tables
//!   with a defined time index; attempts to open a generic table through
//!   this API should fail with a clear error.
//!
//! For v0.1, the focus is on a minimal but solid read path:
//!
//! - Append-only segments (for example, Parquet files) that are sorted
//!   by the designated time column.
//! - A `scan_range` API that selects relevant segments based on
//!   `ts_min` / `ts_max` and returns a globally-sorted stream of Arrow
//!   `RecordBatch` values.
//!
//! More advanced features (for example, symbol-level pruning, parallel
//! reads, or DataFusion integration) are expected to be built on top
//! of this module in companion crates.
