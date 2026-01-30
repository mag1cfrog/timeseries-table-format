//! Core engine for a log-structured time-series table format.
//!
//! This crate provides the foundational pieces for `timeseries-table-format`.
//! Over time it is being refactored into clearer "layers", but the crate keeps
//! compatibility re-exports so downstream crates do not need to change
//! immediately.
//!
//! Responsibilities (high level):
//! - A Delta-inspired, append-only metadata log with version-guard optimistic
//!   concurrency control ([`transaction_log`]).
//! - Strongly-typed table metadata that distinguishes time-series tables (with a
//!   [`TimeIndexSpec`]) from generic tables ([`metadata::model`] /
//!   [`transaction_log::table_metadata`]).
//! - A [`TimeSeriesTable`] abstraction that exposes a logical view over the log
//!   and segment metadata ([`table`] / [`time_series_table`]).
//! - RoaringBitmap-based coverage utilities for reasoning about which time
//!   buckets are present or missing ([`coverage`]).
//! - Filesystem utilities for managing on-disk layout (log directory, CURRENT
//!   pointer, segment paths, etc.) ([`storage`] and [`layout`]).
//!
//! Main layers (new paths):
//! - [`metadata`]: metadata model + schema/segment validation (no IO).
//! - [`table`]: the [`table::TimeSeriesTable`] user-facing abstraction.
//! - [`storage`]: storage backend + table-root IO utilities.
//! - [`coverage`]: bitmap-based coverage math and utilities.
//! - [`formats`]: format-specific helpers (currently Parquet).
//!
//! Compatibility shims:
//! - [`transaction_log`] implements the append-only metadata log (OCC).
//! - [`time_series_table`] mirrors [`table`]
//! - [`log`] and [`fs`] exist as thin re-exports for older docs/paths.
//!
//! Higher-level integration crates (for example, DataFusion, backtesting tools,
//! or a CLI) are expected to depend on this core crate rather than re-
//! implementing the storage and metadata logic.
pub mod common;
pub mod coverage;
pub mod formats;
pub mod helpers;
pub mod layout;
pub mod metadata;
pub mod storage;
pub mod table;
pub mod time_series_table;
pub mod transaction_log;

pub use time_series_table::error::TableError;
pub use transaction_log::table_metadata::ParseTimeBucketError;

/// Compatibility shim for older `timeseries_table_core::log::*` paths.
pub mod log {
    pub use crate::transaction_log::*;
}

/// Compatibility shim for older `timeseries_table_core::fs::*` paths.
///
/// This is intentionally small; prefer [`storage`] and [`layout`] directly.
pub mod fs {
    pub use crate::layout::*;
    pub use crate::storage::*;
}
