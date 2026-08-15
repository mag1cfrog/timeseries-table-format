//! Metadata model types.
//!
//! Convenience re-exports over [`crate::metadata`] submodules.

pub use crate::metadata::{
    logical_schema::{
        LogicalDataType, LogicalField, LogicalSchema, LogicalSchemaError, LogicalTimestampUnit,
        SchemaConvertError,
    },
    segments::{FileFormat, SegmentMeta, SegmentMetaError},
    table_metadata::{
        IndexKind, IndexSpec, IndexSpecError, IndexValue, IndexValueError, ParseTimeBucketError,
        TABLE_FORMAT_VERSION, TableKind, TableMeta, TableMetaDelta, TimeBucket, TimeIndexSpec,
        validate_index_range,
    },
    time_column::TimeColumnError,
};
