//! Compatibility facade for the time-series table API.
//!
//! The canonical implementation lives in [`crate::table`]. This module exists
//! to preserve older public paths (`timeseries_table_core::time_series_table::*`)
//! while the crate is being refactored.

pub mod append;
/// Append profiling report types used by CLI benchmarks.
pub mod append_report;
pub mod coverage_queries;
pub mod coverage_state;
pub mod error;
pub mod scan;

#[cfg(test)]
pub(crate) mod test_util;

pub use crate::table::{TimeSeriesScan, TimeSeriesTable};
