//! Compatibility shim for older `timeseries_table_core::time_series_table::append`.
//!
//! The canonical implementation lives in [`crate::table::append`].

#[allow(unused_imports)]
pub use crate::table::append::*;
