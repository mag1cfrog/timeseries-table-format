//! Convenience prelude.
//!
//! The `timeseries-table-format` crate is the supported public entry point.
//! Downstream code can import its common types from this module.

pub use crate::coverage;
pub use crate::{
    AppendRequest, IndexKind, IndexSpec, IndexSpecError, IndexValue, IndexValueError,
    IntoBatchStream, LogicalDataType, LogicalField, LogicalSchema, OptimizeReport,
    ParseTimeBucketError, TableError, TableLocation, TableMeta, TimeBucket, TimeSeriesTable,
    validate_index_range,
};
