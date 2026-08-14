//! Convenience prelude.
//!
//! The `timeseries-table-format` crate is the supported public entry point.
//! Downstream code can import its common types from this module.

pub use crate::coverage;
pub use crate::{
    LogicalDataType, LogicalField, LogicalSchema, ParseTimeBucketError, TableError, TableLocation,
    TableMeta, TimeBucket, TimeIndexSpec, TimeSeriesTable,
};
