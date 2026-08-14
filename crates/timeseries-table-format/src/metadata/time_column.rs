//! Errors related to discovering and validating timestamp columns in schemas.
//!
//! When processing Parquet or Arrow data, the crate needs to locate and validate
//! the timestamp column specified in the table's index spec. This module provides
//! error types for the common failure modes:
//! - Column not found in schema.
//! - Column uses an unsupported Parquet physical/logical type pair.
//! - Column uses an unsupported Arrow data type.
//!
//! These errors are used by segment metadata validation, coverage computation,
//! and schema introspection logic.

use snafu::Snafu;

/// Errors that can occur when locating or validating a timestamp column.
///
/// A timestamp column is required by the table's time index specification to
/// enable time-series range queries and coverage computation. This enum captures
/// the various ways that validation can fail:
/// - The column may not exist in the schema.
/// - The column may exist but use a Parquet type that is not supported.
/// - The column may exist but use an Arrow type that is not supported.
#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
pub enum TimeColumnError {
    /// The specified time column was not found in the schema.
    ///
    /// This typically indicates either a configuration mismatch (the table's
    /// index spec references a column that doesn't exist in the data) or
    /// a schema evolution issue (the column was removed in a later segment).
    #[snafu(display("Time column {column} not found in schema"))]
    Missing {
        /// The name of the time column that was expected but not found.
        column: String,
    },

    /// The time column uses a Parquet physical/logical type pair that is not supported.
    ///
    /// For example, the column might use an integer or string type instead of
    /// a timestamp type, or it might use a timestamp variant (e.g., nanoseconds)
    /// that the crate does not currently support.
    #[snafu(display(
        "Unsupported physical type for time column {column}: physical={physical} logical={logical}"
    ))]
    UnsupportedParquetType {
        /// The name of the time column.
        column: String,
        /// The Parquet physical type (e.g., INT64, BYTE_ARRAY).
        physical: String,
        /// The Parquet logical type annotation, if present (e.g., TIMESTAMP, STRING).
        logical: String,
    },

    /// The time column uses an Arrow data type that is not supported.
    ///
    /// This can occur when converting from Parquet to Arrow or when working
    /// directly with Arrow data. Supported types typically include timestamp
    /// variants with microsecond or millisecond precision.
    #[snafu(display("Unsupported arrow type for time column {column}: {datatype}"))]
    UnsupportedArrowType {
        /// The name of the time column.
        column: String,
        /// The Arrow data type that is not supported (e.g., "String", "Int32").
        datatype: String,
    },
}
