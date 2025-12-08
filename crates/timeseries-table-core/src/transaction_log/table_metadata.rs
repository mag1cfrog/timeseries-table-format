//! Table-level metadata structures recorded in the log.
//!
//! This module models the schema and configuration captured by
//! `LogAction::UpdateTableMeta`, including table kind, logical schema, and the
//! time index specification. Future evolutions can extend these types without
//! touching the storage/reader code paths.
use std::{collections::HashSet, fmt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

/// The high-level "kind" of table.
///
/// v0.1 supports only `TimeSeries`, but a `Generic` kind is reserved so that
/// the log format can represent non-timeseries tables later without breaking
/// existing JSON.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TableKind {
    /// A time-series table with an explicit time index specification.
    TimeSeries(TimeIndexSpec),

    /// Placeholder for future basic tables that do not have a time index.
    /// Not used in v0.1.
    Generic,
}

/// High-level table metadata stored in the log.
///
/// This describes the table kind, a logical schema (optional in v0.1), and
/// basic bookkeeping fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableMeta {
    /// Table kind: TimeSeries or Generic.
    pub kind: TableKind,

    /// Optional logical schema description.
    ///
    /// v0.1 can treat this as informational; enforcement is handled by
    /// higher layers.
    pub logical_schema: Option<LogicalSchema>,

    /// Creation timestamp of the table, stored as RFC3339 UTC.
    pub created_at: DateTime<Utc>,

    /// Format version for future evolution of the log/table format.
    ///
    /// v0.1 can hard-code this to 1.
    pub format_version: u32,
}

/// For v0.1, a `TableMetaDelta` is just a full replacement of [`TableMeta`].
///
/// This alias keeps the wire format simple (the JSON is the same as `TableMeta`)
/// while leaving room to evolve to more granular metadata updates in future
/// versions (for example, partial updates or additive fields).
pub type TableMetaDelta = TableMeta;

/// Units for logical timestamps recorded in the table metadata.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogicalTimestampUnit {
    /// Millisecond precision timestamps.
    Millis,
    /// Microsecond precision timestamps.
    Micros,
    /// Nanosecond precision timestamps.
    Nanos,
}

/// Logical data types that can be stored in the table schema metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogicalDataType {
    /// Boolean value.
    Bool,
    /// 32-bit signed integer.
    Int32,
    /// 64-bit signed integer.
    Int64,
    /// 32-bit floating point.
    Float32,
    /// 64-bit floating point.
    Float64,
    /// Variable-length binary data.
    Binary,
    /// Fixed-length binary data.
    FixedBinary,
    /// UTF-8 encoded string.
    Utf8,
    /// Legacy 96-bit integer (primarily for Parquet compatibility).
    Int96,

    /// Timestamp value with a precision unit and optional timezone.
    Timestamp {
        /// Timestamp precision unit (millis, micros, nanos).
        unit: LogicalTimestampUnit,
        /// Optional IANA timezone identifier.
        timezone: Option<String>, // keep Option for future TZ support
    },

    /// Catch-all logical data type referenced by name.
    Other(String),
}

impl fmt::Display for LogicalTimestampUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalTimestampUnit::Millis => write!(f, "ms"),
            LogicalTimestampUnit::Micros => write!(f, "us"),
            LogicalTimestampUnit::Nanos => write!(f, "ns"),
        }
    }
}

impl fmt::Display for LogicalDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalDataType::Bool => write!(f, "bool"),
            LogicalDataType::Int32 => write!(f, "int32"),
            LogicalDataType::Int64 => write!(f, "int64"),
            LogicalDataType::Float32 => write!(f, "float32"),
            LogicalDataType::Float64 => write!(f, "float64"),
            LogicalDataType::Binary => write!(f, "binary"),
            LogicalDataType::FixedBinary => write!(f, "fixed_binary"),
            LogicalDataType::Utf8 => write!(f, "utf8"),
            LogicalDataType::Int96 => write!(f, "int96"),

            LogicalDataType::Timestamp { unit, timezone } => match timezone {
                Some(tz) => write!(f, "timestamp[{}]({})", unit, tz),
                None => write!(f, "timestamp[{}]", unit),
            },

            LogicalDataType::Other(s) => write!(f, "{s}"),
        }
    }
}
/// A minimal logical schema representation.
///
/// This is intentionally simple in v0.1: it records column names, types as
/// strings, and nullability. A future version may align this more closely
/// with Arrow or another schema model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalColumn {
    /// Column name as it appears in the data.
    pub name: String,
    /// Logical data type as a free-form string (e.g. `"int64"`, `"timestamp[us]"`).
    pub data_type: LogicalDataType,
    /// Whether the column may contain NULLs.
    #[serde(default)]
    pub nullable: bool,
}

/// Logical schema metadata describing the ordered collection of logical columns.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalSchema {
    /// All logical columns that compose the schema in their defined order.
    columns: Vec<LogicalColumn>,
}

/// Errors that can occur while constructing or validating a logical schema.
#[derive(Debug, Clone, Snafu, PartialEq, Eq)]
pub enum LogicalSchemaError {
    /// Duplicate column names are not allowed.
    #[snafu(display("Duplicate column name: {column}"))]
    DuplicateColumn {
        /// The duplicate column name.
        column: String,
    },
}

impl LogicalSchema {
    /// Construct a validated logical schema (rejects duplicate column names).
    pub fn new(columns: Vec<LogicalColumn>) -> Result<Self, LogicalSchemaError> {
        let mut seen = HashSet::new();
        for col in &columns {
            if !seen.insert(col.name.clone()) {
                return DuplicateColumnSnafu {
                    column: col.name.clone(),
                }
                .fail();
            }
        }

        Ok(Self { columns })
    }

    /// Borrow the logical columns.
    pub fn columns(&self) -> &[LogicalColumn] {
        &self.columns
    }
}

/// Granularity for time buckets used by coverage/bitmap logic.
///
/// This does not affect physical storage directly, but describes how the time
/// axis is discretized when building coverage bitmaps and computing gaps.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeBucket {
    /// A bucket spanning a fixed number of seconds.
    Seconds(u32),
    /// A bucket spanning a fixed number of minutes.
    Minutes(u32),
    /// A bucket spanning a fixed number of hours.
    Hours(u32),
    /// A bucket spanning a fixed number of days.
    Days(u32),
}

/// Configuration for the time index of a time-series table.
///
/// In v0.1 this is assumed to exist for all "time-series" tables; a future
/// `TableKind::Generic` may omit it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeIndexSpec {
    /// Name of the timestamp column (for example, `"ts"` or `"timestamp"`).
    pub timestamp_column: String,

    /// Optional entity/symbol columns that help partition the time axis
    /// (for example, `["symbol"]` or `["symbol", "venue"]`).
    ///
    /// This is metadata only; enforcement and partitioning are handled by
    /// higher layers.
    #[serde(default)]
    pub entity_columns: Vec<String>,

    /// Logical bucket size used by coverage bitmaps (for example, 1 minute, 1 hour).
    pub bucket: TimeBucket,

    /// Optional IANA timezone identifier (for example, `"America/New_York"`).
    ///
    /// For v0.1 this is primarily reserved for future use; timestamps are
    /// generally expected to be stored in UTC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}
