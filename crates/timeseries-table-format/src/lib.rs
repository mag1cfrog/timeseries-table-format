//! # timeseries-table-format
//!
//! Append-only time-series table format with gap/overlap tracking.
//!
//! This crate re-exports the core and (optionally) DataFusion components for convenience.
//!
//! ## Features
//!
//! - `datafusion` (default): Enables DataFusion integration for SQL queries
//! - `cli`: Includes the CLI tool
//!
//! ## Example
//!
//! ```rust,ignore
//! use timeseries_table_format::TimeSeriesTable;
//! ```

/// Re-export everything from the core crate.
pub use timeseries_table_core::*;

/// DataFusion integration (enabled by default).
#[cfg(feature = "datafusion")]
pub mod datafusion {
    pub use timeseries_table_datafusion::*;
}

/// Convenience re-export of the main table type.
pub use timeseries_table_core::time_series_table::TimeSeriesTable;

/// DataFusion table provider (enabled by default).
#[cfg(feature = "datafusion")]
pub use timeseries_table_datafusion::TsTableProvider;
