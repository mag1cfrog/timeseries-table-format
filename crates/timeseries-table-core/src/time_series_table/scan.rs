//! Compatibility shim for older `timeseries_table_core::time_series_table::scan`.
//!
//! The canonical implementation lives in [`crate::table::scan`].

#[allow(unused_imports)]
pub use crate::table::scan::*;
