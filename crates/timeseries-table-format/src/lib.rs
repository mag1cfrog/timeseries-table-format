//! # timeseries-table-format
//!
//! Append-only time-series table format with gap/overlap tracking.
//!
//! This crate is the canonical library for the table engine and optional integrations.
//!
//! ## Features
//!
//! - `datafusion` (default): Enables DataFusion integration for SQL queries
//!
//! ## Quick start
//!
//! Open an existing table (async; returns a `Future`):
//!
//! ```rust
//! use timeseries_table_format::{TableLocation, TimeSeriesTable};
//!
//! let location = TableLocation::local("./my_table");
//! let _open = TimeSeriesTable::open(location);
//! ```
//!
//! Or import the stable, supported surface via the prelude:
//!
//! ```rust
//! use timeseries_table_format::prelude::*;
//! ```

pub mod coverage;
#[cfg(feature = "datafusion")]
pub mod datafusion;
pub mod formats;
pub mod metadata;
/// Convenience prelude with the stable, supported surface.
pub mod prelude;
pub mod storage;
pub mod table;
pub mod transaction_log;

pub use metadata::logical_schema::{LogicalDataType, LogicalField, LogicalSchema};
pub use metadata::table_metadata::{
    IndexKind, IndexSpec, IndexSpecError, IndexValue, IndexValueError, ParseTimeBucketError,
    TableMeta, TimeBucket, validate_index_range,
};
pub use storage::TableLocation;
pub use table::{TableError, TimeSeriesTable};

/// DataFusion table provider (enabled by default).
#[cfg(feature = "datafusion")]
pub use datafusion::TsTableProvider;
