//! Metadata model types.
//!
//! Convenience re-exports over [`crate::metadata`] submodules.

pub use crate::metadata::{
    logical_schema::{
        LogicalDataType, LogicalField, LogicalSchema, LogicalSchemaError, LogicalTimestampUnit,
        SchemaConvertError,
    },
    segments::{FileFormat, SegmentId, SegmentMeta, SegmentMetaError},
    table_metadata::{
        ParseTimeBucketError, TABLE_FORMAT_VERSION, TableKind, TableMeta, TableMetaDelta,
        TimeBucket, TimeIndexSpec,
    },
    time_column::TimeColumnError,
};
