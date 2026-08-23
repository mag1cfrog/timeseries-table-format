//! Convenience prelude.
//!
//! The `timeseries-table-format` crate is the supported public entry point.
//! Downstream code can import its common types from this module.

pub use crate::coverage;
pub use crate::{
    AppendRequest, IndexKind, IndexSpec, IndexSpecError, IndexValue, IndexValueError,
    IntoRecordBatchReader, LogicalDataType, LogicalField, LogicalSchema, OptimizeReport,
    ParseTimeIndexGranularityError, TableError, TableLocation, TableMeta, TimeIndexGranularity,
    TimeSeriesTable, validate_index_range,
};
