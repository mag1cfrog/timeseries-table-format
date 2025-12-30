//! Table-level metadata structures recorded in the log.
//!
//! This module models the schema and configuration captured by
//! `LogAction::UpdateTableMeta`, including table kind, logical schema, and the
//! time index specification. Future evolutions can extend these types without
//! touching the storage/reader code paths.
use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    sync::Arc,
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

/// Current table metadata / log format version.
///
/// Bumped only when we make a breaking change to the on-disk JSON format.
pub const TABLE_FORMAT_VERSION: u32 = 1;

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
    pub(crate) kind: TableKind,

    /// Optional logical schema description.
    ///
    /// v0.1 can treat this as informational; enforcement is handled by
    /// higher layers.
    pub(crate) logical_schema: Option<LogicalSchema>,

    /// Creation timestamp of the table, stored as RFC3339 UTC.
    pub(crate) created_at: DateTime<Utc>,

    /// Format version for future evolution of the log/table format.
    ///
    /// v0.1 can hard-code this to 1.
    pub(crate) format_version: u32,

    /// v0.1: If TimeIndexSpec.entity_columns is non-empty, we pin a single entity identity
    /// per table (map keyed by column name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_identity: Option<BTreeMap<String, String>>,
}

impl TableMeta {
    /// Returns the table kind (e.g. time series or generic).
    pub fn kind(&self) -> &TableKind {
        &self.kind
    }

    /// Returns the optional logical schema if it has been set.
    pub fn logical_schema(&self) -> Option<&LogicalSchema> {
        self.logical_schema.as_ref()
    }

    /// Returns the UTC timestamp when the table was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns the on-disk table metadata format version.
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Convenience constructor for a time-series table.
    ///
    /// - Fills `created_at` with `Utc::now()`.
    /// - Fills `format_version` with `TABLE_FORMAT_VERSION`.
    /// - Leaves `logical_schema` as `None`; it will be adopted from the
    ///   first appended segment in v0.1.
    pub fn new_time_series(index: TimeIndexSpec) -> Self {
        TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: None,
            created_at: Utc::now(),
            format_version: TABLE_FORMAT_VERSION,
            entity_identity: None,
        }
    }

    /// Variant that lets you explicitly pass a logical schema up front.
    pub fn new_time_series_with_schema(
        index: TimeIndexSpec,
        logical_schema: LogicalSchema,
    ) -> Self {
        TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: Some(logical_schema),
            created_at: Utc::now(),
            format_version: TABLE_FORMAT_VERSION,
            entity_identity: None,
        }
    }
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

impl LogicalTimestampUnit {
    fn to_arrow_time_unit(self) -> TimeUnit {
        match self {
            LogicalTimestampUnit::Millis => TimeUnit::Millisecond,
            LogicalTimestampUnit::Micros => TimeUnit::Microsecond,
            LogicalTimestampUnit::Nanos => TimeUnit::Nanosecond,
        }
    }
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
    FixedBinary {
        /// Fixed byte width for each value (in bytes).
        byte_width: i32,
    },
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

    /// Fixed-precision decimal value with declared precision and scale.
    Decimal {
        /// Total number of decimal digits (both sides of the decimal point).
        precision: i32,
        /// Number of digits to the right of the decimal point.
        scale: i32,
    },

    /// Catch-all logical data type referenced by name.
    Other(String),
}

impl LogicalDataType {
    fn to_arrow_datatype(&self, column: &str) -> Result<DataType, SchemaConvertError> {
        Ok(match self {
            LogicalDataType::Bool => DataType::Boolean,
            LogicalDataType::Int32 => DataType::Int32,
            LogicalDataType::Int64 => DataType::Int64,
            LogicalDataType::Float32 => DataType::Float32,
            LogicalDataType::Float64 => DataType::Float64,
            LogicalDataType::Binary => DataType::Binary,
            LogicalDataType::Utf8 => DataType::Utf8,

            LogicalDataType::FixedBinary { byte_width } => {
                if *byte_width <= 0 {
                    return Err(SchemaConvertError::FixedBinaryInvalidWidth {
                        column: column.to_string(),
                        byte_width: *byte_width,
                    });
                }
                DataType::FixedSizeBinary(*byte_width)
            }

            LogicalDataType::Timestamp { unit, timezone } => {
                let tz: Option<Arc<str>> = timezone.as_ref().map(|s| Arc::<str>::from(s.as_str()));
                DataType::Timestamp(unit.to_arrow_time_unit(), tz)
            }

            LogicalDataType::Int96 => {
                return Err(SchemaConvertError::Int96Unsupported {
                    column: column.to_string(),
                });
            }

            LogicalDataType::Decimal { precision, scale } => {
                let precision = *precision;
                let scale = *scale;
                if precision <= 0 {
                    return Err(SchemaConvertError::DecimalInvalid {
                        column: column.to_string(),
                        precision,
                        scale,
                        details: "precision must be > 0".to_string(),
                    });
                }
                if scale < 0 {
                    return Err(SchemaConvertError::DecimalInvalid {
                        column: column.to_string(),
                        precision,
                        scale,
                        details: "scale must be >= 0".to_string(),
                    });
                }
                if scale > precision {
                    return Err(SchemaConvertError::DecimalInvalid {
                        column: column.to_string(),
                        precision,
                        scale,
                        details: "scale must be <= precision".to_string(),
                    });
                }

                if precision <= 38 {
                    DataType::Decimal128(precision as u8, scale as i8)
                } else if precision <= 76 {
                    DataType::Decimal256(precision as u8, scale as i8)
                } else {
                    return Err(SchemaConvertError::DecimalInvalid {
                        column: column.to_string(),
                        precision,
                        scale,
                        details: "precision exceeds Arrow maximum (76 digits)".to_string(),
                    });
                }
            }

            LogicalDataType::Other(name) => {
                return Err(SchemaConvertError::OtherTypeUnsupported {
                    column: column.to_string(),
                    name: name.clone(),
                });
            }
        })
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
            LogicalDataType::FixedBinary { byte_width } => write!(f, "fixed_binary[{byte_width}]"),
            LogicalDataType::Utf8 => write!(f, "utf8"),
            LogicalDataType::Int96 => write!(f, "int96"),

            LogicalDataType::Timestamp { unit, timezone } => match timezone {
                Some(tz) => write!(f, "timestamp[{}]({})", unit, tz),
                None => write!(f, "timestamp[{}]", unit),
            },

            LogicalDataType::Decimal { precision, scale } => {
                write!(f, "decimal(precision={precision}, scale={scale})")
            }

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
    /// Logical data type as a strongly-typed enum (e.g. `LogicalDataType::Int64`, `LogicalDataType::Timestamp { ... }`).
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

impl LogicalSchema {
    /// Convert this logical schema to an owned Arrow [`Schema`].
    ///
    /// Fails if any column uses a logical type that cannot be represented in
    /// Arrow (see [`SchemaConvertError`]).
    pub fn to_arrow_schema(&self) -> Result<Schema, SchemaConvertError> {
        let mut fields = Vec::with_capacity(self.columns.len());
        for c in &self.columns {
            let dt = c.data_type.to_arrow_datatype(&c.name)?;
            fields.push(Field::new(c.name.clone(), dt, c.nullable));
        }

        Ok(Schema::new(fields))
    }

    /// Convert this logical schema to a shared Arrow [`SchemaRef`].
    ///
    /// This is a convenience wrapper around [`Self::to_arrow_schema`].
    pub fn to_arrow_schema_ref(&self) -> Result<SchemaRef, SchemaConvertError> {
        Ok(Arc::new(self.to_arrow_schema()?))
    }
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

/// Errors encountered while converting between logical schema representations.
#[derive(Debug, Snafu)]
pub enum SchemaConvertError {
    /// The logical type is not supported by the target representation.
    #[snafu(display("unsupported logical type for column '{column}': {type_name} ({details})"))]
    UnsupportedLogicalType {
        /// Column name that failed conversion.
        column: String,
        /// High-level type name (for diagnostics).
        type_name: String,
        /// Additional details describing why it is unsupported.
        details: String,
    },

    /// FixedBinary fields must declare a positive byte width.
    #[snafu(display(
        "invalid FixedBinary byte_width for column '{column}': {byte_width} (must be > 0)"
    ))]
    FixedBinaryInvalidWidth {
        /// Column name that failed validation.
        column: String,
        /// Declared byte width.
        byte_width: i32,
    },

    /// Int96 is rejected for now to avoid legacy timestamp ambiguity.
    #[snafu(display("Int96 is not supported in v0.1 for column '{column}'"))]
    Int96Unsupported {
        /// Column name that failed conversion.
        column: String,
    },

    /// Catch-all "Other" types are not accepted in v0.1.
    #[snafu(display("Other type '{name}' is not supported in v0.1 for column '{column}'"))]
    OtherTypeUnsupported {
        /// Column name that failed conversion.
        column: String,
        /// Type name reported by the source.
        name: String,
    },

    /// Decimal precision/scale is out of supported bounds for Arrow conversion.
    #[snafu(display(
        "invalid decimal definition for column '{column}': precision={precision}, scale={scale} ({details})"
    ))]
    DecimalInvalid {
        /// Column name that failed conversion.
        column: String,
        /// Declared total precision.
        precision: i32,
        /// Declared scale (digits to the right of the decimal point).
        scale: i32,
        /// Human-readable details describing the constraint violation.
        details: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::Value;

    fn utc_datetime(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .expect("valid UTC timestamp")
    }

    fn sample_time_index_spec() -> TimeIndexSpec {
        TimeIndexSpec {
            timestamp_column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string()],
            bucket: TimeBucket::Minutes(1),
            timezone: None,
        }
    }

    #[test]
    fn table_meta_json_roundtrip_with_entity_identity_none() {
        let meta = TableMeta {
            kind: TableKind::TimeSeries(sample_time_index_spec()),
            logical_schema: None,
            created_at: utc_datetime(2025, 1, 1, 0, 0, 0),
            format_version: TABLE_FORMAT_VERSION,
            entity_identity: None,
        };

        let json = serde_json::to_string(&meta).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert!(value.get("entity_identity").is_none());

        let back: TableMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(back.entity_identity, None);
        assert_eq!(back, meta);
    }

    #[test]
    fn table_meta_json_roundtrip_with_entity_identity_some() {
        let entity_identity = BTreeMap::from([
            ("symbol".to_string(), "AAPL".to_string()),
            ("venue".to_string(), "NASDAQ".to_string()),
        ]);
        let meta = TableMeta {
            kind: TableKind::TimeSeries(sample_time_index_spec()),
            logical_schema: None,
            created_at: utc_datetime(2025, 1, 1, 0, 0, 0),
            format_version: TABLE_FORMAT_VERSION,
            entity_identity: Some(entity_identity.clone()),
        };

        let json = serde_json::to_string(&meta).unwrap();
        let value: Value = serde_json::from_str(&json).unwrap();
        assert!(value.get("entity_identity").is_some());

        let back: TableMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(back.entity_identity, Some(entity_identity));
        assert_eq!(back, meta);
    }
}
