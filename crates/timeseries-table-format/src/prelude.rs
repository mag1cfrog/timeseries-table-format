//! Wrapper prelude.
//!
//! The `timeseries-table-format` crate is the supported public entry point.
//! Downstream code should prefer importing from this prelude instead of
//! depending on internal core module paths.

pub use crate::coverage;
pub use crate::{
    LogicalDataType, LogicalField, LogicalSchema, ParseTimeBucketError, TableError, TableLocation,
    TableMeta, TimeBucket, TimeIndexSpec, TimeSeriesTable,
};
