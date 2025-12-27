//! Helpers for reading and validating entity identity metadata from a segment.

use std::collections::BTreeMap;

use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use snafu::prelude::*;

use crate::storage::StorageError; 

/// Mapping of entity attribute names to their normalized string values.
pub type EntityIdentity = BTreeMap<String, String>;

#[derive(Debug, Snafu)]
/// Errors returned while extracting an entity identity from a segment.
pub enum SegmentEntityIdentityError {
    /// Failed to read from the underlying storage layer.
    #[snafu(display("Storage error while reading {path}: {source}"))]
    Storage { 
        /// Path of the segment or file being read.
        path: String, 
        /// Storage error produced by the backend.
        #[snafu(backtrace)]
        source: StorageError
    },

    /// Failed to decode a Parquet file.
    #[snafu(display("Parquet read error for {path}: {source}"))]
    ParquetRead {
        /// Path of the Parquet file being read.
        path: String,
        /// Parquet error emitted by the reader.
        source: ParquetError
    },

    /// Failed to decode Arrow data from the Parquet reader.
    #[snafu(display("Arrow read error for {path}: {source}"))]
    ArrowRead {
        /// Path of the Parquet file being read.
        path: String,
        /// Arrow error emitted while decoding batches.
        source: ArrowError
    },

    /// Requested entity column is missing from the segment schema.
    #[snafu(display("Entity column not found in {path}: {column}"))]
    EntityColumnNotFound {
        /// Path of the segment that was inspected.
        path: String,
        /// Column name that was expected.
        column: String,
    },

    /// Entity column type is not supported for identity extraction.
    #[snafu(display("Unsupported entity column type in {path}: {column} has {datatype}"))]
    EntityColumnUnsupportedType {
        /// Path of the segment that was inspected.
        path: String,
        /// Column name that had the unsupported type.
        column: String,
        /// Human-readable representation of the column's data type.
        datatype: String,
    },

    /// Entity column contains null values that are not allowed.
    #[snafu(display("Entity column contains nulls in {path}: {column}"))]
    EntityColumnHasNull {
        /// Path of the segment that was inspected.
        path: String,
        /// Column name that contained nulls.
        column: String,
    },

    /// Entity column contains more than one distinct value.
    #[snafu(display("Entity column has multiple values in {path}: {column} (first={first}, other={other})"))]
    EntityColumnMultipleValues {
        /// Path of the segment that was inspected.
        path: String,
        /// Column name that contained multiple values.
        column: String,
        /// First value observed in the column.
        first: String,
        /// Additional, conflicting value observed in the column.
        other: String,
    },

    /// Entity column has no values, which indicates an empty segment.
    #[snafu(display("Entity column has no values (empty segment) in {path}: {column}"))]
    EntityColumnEmpty {
        /// Path of the segment that was inspected.
        path: String,
        /// Column name that had no values.
        column: String,
    }

}

