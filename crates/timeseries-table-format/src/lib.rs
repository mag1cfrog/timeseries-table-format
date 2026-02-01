//! # timeseries-table-format
//!
//! Append-only time-series table format with gap/overlap tracking.
//!
//! This crate is the supported public entry point and provides a small, stable surface.
//!
//! ## Features
//!
//! - `datafusion` (default): Enables DataFusion integration for SQL queries
//!
//! ## Example
//!
//! ```rust,ignore
//! use timeseries_table_format::prelude::*;
//! ```

/// Convenience prelude with the stable, supported surface.
pub mod prelude;

/// Coverage namespace (wrapper-only).
pub mod coverage {
    pub use timeseries_table_core::coverage::Bucket;
}

/// DataFusion integration (enabled by default).
#[cfg(feature = "datafusion")]
pub mod datafusion {
    pub use timeseries_table_datafusion::*;
}

pub use timeseries_table_core::metadata::logical_schema::{
    LogicalDataType, LogicalField, LogicalSchema,
};
pub use timeseries_table_core::metadata::table_metadata::{
    ParseTimeBucketError, TableMeta, TimeBucket, TimeIndexSpec,
};
pub use timeseries_table_core::storage::TableLocation;
pub use timeseries_table_core::table::{TableError, TimeSeriesTable};

/// DataFusion table provider (enabled by default).
#[cfg(feature = "datafusion")]
pub use timeseries_table_datafusion::TsTableProvider;
