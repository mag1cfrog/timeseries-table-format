//! Table-level metadata structures recorded in the log.
//!
//! This module models the schema and configuration captured by
//! `LogAction::UpdateTableMeta`, including table kind, logical schema, and the
//! time index specification. Future evolutions can extend these types without
//! touching the storage/reader code paths.
use std::{collections::BTreeMap, str::FromStr};

use arrow::datatypes::SchemaRef;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::transaction_log::logical_schema::{LogicalSchema, SchemaConvertError};

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

/// Errors encountered while retrieving or converting a table's logical schema.
#[derive(Debug, Snafu)]
pub enum TableMetaSchemaError {
    /// The table metadata has not yet recorded a canonical logical schema.
    #[snafu(display("table has no canonical logical schema yet (logical_schema is None)"))]
    MissingCanonicalSchema,

    /// Failed to convert the logical schema to Arrow types.
    #[snafu(transparent)]
    Convert {
        /// Underlying conversion error.
        source: SchemaConvertError,
    },
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

    /// Convert the table's logical schema to a shared Arrow [`SchemaRef`].
    ///
    /// Returns [`TableMetaSchemaError::MissingCanonicalSchema`] if the schema has
    /// not yet been established for the table.
    pub fn arrow_schema_ref(&self) -> Result<SchemaRef, TableMetaSchemaError> {
        let logical = self
            .logical_schema
            .as_ref()
            .ok_or(TableMetaSchemaError::MissingCanonicalSchema)?;

        logical
            .to_arrow_schema_ref()
            .map_err(|source| TableMetaSchemaError::Convert { source })
    }
}

/// For v0.1, a `TableMetaDelta` is just a full replacement of [`TableMeta`].
///
/// This alias keeps the wire format simple (the JSON is the same as `TableMeta`)
/// while leaving room to evolve to more granular metadata updates in future
/// versions (for example, partial updates or additive fields).
pub type TableMetaDelta = TableMeta;

/// Errors produced when parsing a human-friendly time bucket spec (e.g. `1h`).
#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum ParseTimeBucketError {
    /// The spec string was empty or only whitespace.
    #[snafu(display("time bucket spec is empty"))]
    Empty,

    /// The spec did not include a numeric value.
    #[snafu(display("time bucket spec '{spec}' is missing a numeric value"))]
    MissingNumber {
        /// The original spec string.
        spec: String,
    },

    /// The spec did not include a required unit suffix.
    #[snafu(display("time bucket spec '{spec}' is missing a unit suffix (expected s|m|h|d)"))]
    MissingUnit {
        /// The original spec string.
        spec: String,
    },

    /// The numeric portion of the spec failed to parse.
    #[snafu(display("invalid bucket value in '{spec}': {source}"))]
    InvalidNumber {
        /// The original spec string.
        spec: String,
        /// The parse error returned by `u64::from_str`.
        source: std::num::ParseIntError,
    },

    /// The parsed numeric value was zero.
    #[snafu(display("bucket value must be > 0 (got {value}) in '{spec}'"))]
    NonPositive {
        /// The original spec string.
        spec: String,
        /// The parsed numeric value.
        value: u64,
    },

    /// The parsed numeric value did not fit in a `u32`.
    #[snafu(display("bucket value too large for u32 (got {value}) in '{spec}'"))]
    TooLarge {
        /// The original spec string.
        spec: String,
        /// The parsed numeric value.
        value: u64,
    },

    /// The spec used an unsupported unit suffix.
    #[snafu(display("unknown time bucket unit '{unit}' in '{spec}' (expected s|m|h|d)"))]
    UnknownUnit {
        /// The original spec string.
        spec: String,
        /// The unrecognized unit suffix.
        unit: String,
    },
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

impl FromStr for TimeBucket {
    type Err = ParseTimeBucketError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let spec = input.trim();
        if spec.is_empty() {
            return Err(ParseTimeBucketError::Empty);
        }

        // Split into leading digits + trailing unit
        let digit_len = spec
            .char_indices()
            .take_while(|(_, c)| c.is_ascii_digit())
            .last()
            .map(|(i, c)| i + c.len_utf8())
            .unwrap_or(0);

        if digit_len == 0 {
            // No leading digits (e.g. "h")
            return Err(ParseTimeBucketError::MissingNumber {
                spec: spec.to_string(),
            });
        }

        let (num_str, unit_str) = spec.split_at(digit_len);
        let unit_str = unit_str.trim();

        if unit_str.is_empty() {
            return Err(ParseTimeBucketError::MissingUnit {
                spec: spec.to_string(),
            });
        }

        let value: u64 = num_str
            .parse()
            .map_err(|source| ParseTimeBucketError::InvalidNumber {
                spec: spec.to_string(),
                source,
            })?;

        if value == 0 {
            return Err(ParseTimeBucketError::NonPositive {
                spec: spec.to_string(),
                value,
            });
        }

        if value > u32::MAX as u64 {
            return Err(ParseTimeBucketError::TooLarge {
                spec: spec.to_string(),
                value,
            });
        }

        let v = value as u32;
        let unit = unit_str.to_ascii_lowercase();

        match unit.as_str() {
            "s" | "sec" | "secs" | "second" | "seconds" => Ok(TimeBucket::Seconds(v)),
            "m" | "min" | "mins" | "minute" | "minutes" => Ok(TimeBucket::Minutes(v)),
            "h" | "hr" | "hrs" | "hour" | "hours" => Ok(TimeBucket::Hours(v)),
            "d" | "day" | "days" => Ok(TimeBucket::Days(v)),
            _ => Err(ParseTimeBucketError::UnknownUnit {
                spec: spec.to_string(),
                unit: unit_str.to_string(),
            }),
        }
    }
}

impl TimeBucket {
    /// Parse a human-friendly time bucket spec (e.g. `1h`, `15m`, `30s`, `2d`).
    ///
    /// This is a convenience wrapper around `str::parse` for `TimeBucket`, and
    /// accepts common unit aliases (e.g. `sec`, `min`, `hr`, `day`).
    ///
    /// # Errors
    /// Returns [`ParseTimeBucketError`] if the spec is empty, missing a unit,
    /// has an invalid or non-positive number, overflows `u32`, or uses an
    /// unsupported unit.
    pub fn parse(spec: &str) -> Result<Self, ParseTimeBucketError> {
        spec.parse()
    }
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

#[cfg(test)]
mod tests {
    use crate::transaction_log::logical_schema::{LogicalDataType, LogicalField};

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

    #[test]
    fn table_meta_arrow_schema_ref_requires_logical_schema() {
        let meta = TableMeta::new_time_series(sample_time_index_spec());
        let err = meta.arrow_schema_ref().unwrap_err();
        assert!(matches!(err, TableMetaSchemaError::MissingCanonicalSchema));
    }

    #[test]
    fn table_meta_arrow_schema_ref_propagates_convert_error() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "legacy_ts".to_string(),
            data_type: LogicalDataType::Int96,
            nullable: false,
        }])
        .expect("valid schema structure");
        let meta = TableMeta::new_time_series_with_schema(sample_time_index_spec(), logical);

        let err = meta.arrow_schema_ref().unwrap_err();
        assert!(
            matches!(
                &err,
                TableMetaSchemaError::Convert {
                    source: SchemaConvertError::Int96Unsupported { column }
                } if column == "legacy_ts"
            ),
            "unexpected error: {err:?}"
        );
    }
}
