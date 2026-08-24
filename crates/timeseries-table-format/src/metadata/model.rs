//! Metadata model types.
//!
//! Convenience re-exports over [`crate::metadata`] submodules.

pub use crate::metadata::{
    index::{
        IndexKind, IndexSpec, IndexSpecError, IndexValue, IndexValueError,
        ParseTimeIndexGranularityError, TimeIndexGranularity, validate_index_range,
    },
    logical_schema::{
        LogicalDataType, LogicalField, LogicalSchema, LogicalSchemaValidationError,
        LogicalTimestampUnit, LogicalToArrowSchemaError,
    },
    protocol::{TABLE_PROTOCOL_VERSION, TableProtocolError},
    segments::{FileFormat, SegmentEntityLayout, SegmentMeta, SegmentMetaError},
    table::{TableArrowSchemaError, TableKind, TableMeta, TableMetaDelta},
};
