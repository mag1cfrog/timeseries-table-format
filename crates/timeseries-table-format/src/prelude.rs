//! Convenience prelude.
//!
//! The `timeseries-table-format` crate is the supported public entry point.
//! Downstream code can import its common types from this module.

pub use crate::coverage;
pub use crate::{
    AppendError, AppendRequest, CoverageQueryError, CreateTableError, IndexKind, IndexSpec,
    IndexSpecError, IndexValue, IndexValueError, IntoRecordBatchReader, LogicalDataType,
    LogicalField, LogicalSchema, OpenTableError, OptimizeError, OptimizeReport, ParquetCompression,
    ParseTimeIndexGranularityError, ScanError, TableError, TableLocation, TableMeta,
    TableProtocolError, TableStateAccessError, TimeIndexGranularity, TimeSeriesTable,
    VacuumArtifact, VacuumArtifactDisposition, VacuumArtifactReason, VacuumError, VacuumMode,
    VacuumReport, validate_index_range,
};
