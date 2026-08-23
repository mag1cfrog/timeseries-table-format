//! Metadata model types.
//!
//! Convenience re-exports over [`crate::metadata`] submodules.

pub use crate::metadata::{
    logical_schema::{
        LogicalDataType, LogicalField, LogicalSchema, LogicalSchemaValidationError,
        LogicalTimestampUnit, LogicalToArrowSchemaError,
    },
    segments::{FileFormat, SegmentEntityLayout, SegmentMeta, SegmentMetaError},
    table_metadata::{
        IndexKind, IndexSpec, IndexSpecError, IndexValue, IndexValueError,
        ParseTimeIndexGranularityError, TABLE_PROTOCOL_VERSION, TableArrowSchemaError, TableKind,
        TableMeta, TableMetaDelta, TimeIndexGranularity, validate_index_range,
    },
};
