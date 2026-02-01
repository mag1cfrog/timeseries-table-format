//! Table layer.
//!
//! This is the user-facing API surface (create/open/append/scan). For now it is
//! a thin re-export over the existing `time_series_table` module.

pub mod coverage;

pub use crate::time_series_table::*;
