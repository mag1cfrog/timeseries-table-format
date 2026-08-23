//! Metadata model types.
//!
//! Convenience re-exports over [`crate::metadata`] submodules.

pub use crate::metadata::{
    logical_schema::{
        ArrowSchemaConversionError, LogicalDataType, LogicalField, LogicalSchema,
        LogicalSchemaValidationError, LogicalTimestampUnit,
    },
    segments::{FileFormat, SegmentEntityLayout, SegmentMeta, SegmentMetaError},
    table_metadata::{
        IndexKind, IndexSpec, IndexSpecError, IndexValue, IndexValueError,
        ParseTimeIndexGranularityError, TABLE_FORMAT_VERSION, TableArrowSchemaError, TableKind,
        TableMeta, TableMetaDelta, TimeIndexGranularity, validate_index_range,
    },
};
