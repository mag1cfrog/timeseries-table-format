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

use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::Value;
    use std::sync::Arc;

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

    fn sample_logical_schema_all_supported() -> LogicalSchema {
        LogicalSchema::new(vec![
            LogicalField {
                name: "flag".to_string(),
                data_type: LogicalDataType::Bool,
                nullable: false,
            },
            LogicalField {
                name: "i32".to_string(),
                data_type: LogicalDataType::Int32,
                nullable: false,
            },
            LogicalField {
                name: "i64".to_string(),
                data_type: LogicalDataType::Int64,
                nullable: true,
            },
            LogicalField {
                name: "f32".to_string(),
                data_type: LogicalDataType::Float32,
                nullable: false,
            },
            LogicalField {
                name: "f64".to_string(),
                data_type: LogicalDataType::Float64,
                nullable: true,
            },
            LogicalField {
                name: "text".to_string(),
                data_type: LogicalDataType::Utf8,
                nullable: true,
            },
            LogicalField {
                name: "bytes".to_string(),
                data_type: LogicalDataType::Binary,
                nullable: true,
            },
            LogicalField {
                name: "fixed".to_string(),
                data_type: LogicalDataType::FixedBinary { byte_width: 16 },
                nullable: false,
            },
            LogicalField {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Micros,
                    timezone: Some("UTC".to_string()),
                },
                nullable: false,
            },
        ])
        .expect("valid logical schema")
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
    fn logical_schema_to_arrow_schema_happy_path() {
        let logical = sample_logical_schema_all_supported();
        let schema = logical.to_arrow_schema().expect("arrow schema conversion");

        let expected = Schema::new(vec![
            Field::new("flag", DataType::Boolean, false),
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, true),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("fixed", DataType::FixedSizeBinary(16), false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC"))),
                false,
            ),
        ]);

        assert_eq!(schema, expected);
    }

    #[test]
    fn logical_schema_rejects_fixed_binary_invalid_width() {
        for width in [0, -1] {
            let logical = LogicalSchema::new(vec![LogicalField {
                name: "bad_fixed".to_string(),
                data_type: LogicalDataType::FixedBinary { byte_width: width },
                nullable: false,
            }])
            .expect("valid schema structure");

            let err = logical.to_arrow_schema().unwrap_err();
            assert!(
                matches!(
                    &err,
                    SchemaConvertError::FixedBinaryInvalidWidth {
                        column,
                        byte_width
                    } if column == "bad_fixed" && *byte_width == width
                ),
                "unexpected error: {err:?}"
            );
        }
    }

    #[test]
    fn logical_schema_rejects_int96() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "legacy_ts".to_string(),
            data_type: LogicalDataType::Int96,
            nullable: false,
        }])
        .expect("valid schema structure");

        let err = logical.to_arrow_schema().unwrap_err();
        assert!(
            matches!(
                &err,
                SchemaConvertError::Int96Unsupported { column } if column == "legacy_ts"
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn logical_schema_rejects_other_type() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "opaque".to_string(),
            data_type: LogicalDataType::Other("parquet::Map".to_string()),
            nullable: true,
        }])
        .expect("valid schema structure");

        let err = logical.to_arrow_schema().unwrap_err();
        assert!(
            matches!(
                &err,
                SchemaConvertError::OtherTypeUnsupported { column, name }
                    if column == "opaque" && name == "parquet::Map"
            ),
            "unexpected error: {err:?}"
        );
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

    #[test]
    fn logical_schema_timestamp_without_timezone() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "ts".to_string(),
            data_type: LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Millis,
                timezone: None,
            },
            nullable: false,
        }])
        .expect("valid schema structure");

        let schema = logical.to_arrow_schema().expect("arrow schema conversion");
        let expected = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]);
        assert_eq!(schema, expected);
    }

    #[test]
    fn logical_schema_decimal_conversion_bounds() {
        let valid_128 = LogicalSchema::new(vec![LogicalField {
            name: "dec128".to_string(),
            data_type: LogicalDataType::Decimal {
                precision: 38,
                scale: 10,
            },
            nullable: false,
        }])
        .expect("valid schema structure");
        let schema = valid_128
            .to_arrow_schema()
            .expect("arrow schema conversion");
        assert_eq!(
            schema,
            Schema::new(vec![Field::new(
                "dec128",
                DataType::Decimal128(38, 10),
                false
            )])
        );

        let valid_256 = LogicalSchema::new(vec![LogicalField {
            name: "dec256".to_string(),
            data_type: LogicalDataType::Decimal {
                precision: 76,
                scale: 5,
            },
            nullable: false,
        }])
        .expect("valid schema structure");
        let schema = valid_256
            .to_arrow_schema()
            .expect("arrow schema conversion");
        assert_eq!(
            schema,
            Schema::new(vec![Field::new(
                "dec256",
                DataType::Decimal256(76, 5),
                false
            )])
        );

        let invalid = LogicalSchema::new(vec![LogicalField {
            name: "dec_too_large".to_string(),
            data_type: LogicalDataType::Decimal {
                precision: 77,
                scale: 0,
            },
            nullable: false,
        }])
        .expect("valid schema structure");
        let err = invalid.to_arrow_schema().unwrap_err();
        assert!(
            matches!(
                &err,
                SchemaConvertError::DecimalInvalid { column, precision, scale, .. }
                    if column == "dec_too_large" && *precision == 77 && *scale == 0
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn logical_schema_decimal_validation_errors() {
        let cases = vec![
            ("dec_precision_zero", 0, 0, "precision must be > 0"),
            ("dec_scale_negative", 10, -1, "scale must be >= 0"),
            ("dec_scale_gt_precision", 4, 5, "scale must be <= precision"),
        ];

        for (name, precision, scale, details_substr) in cases {
            let logical = LogicalSchema::new(vec![LogicalField {
                name: name.to_string(),
                data_type: LogicalDataType::Decimal { precision, scale },
                nullable: false,
            }])
            .expect("valid schema structure");

            let err = logical.to_arrow_schema().unwrap_err();
            assert!(
                matches!(
                    &err,
                    SchemaConvertError::DecimalInvalid { column, precision: p, scale: s, details }
                        if column == name && *p == precision && *s == scale && details.contains(details_substr)
                ),
                "unexpected error: {err:?}"
            );
        }
    }

    #[test]
    fn logical_schema_fixed_binary_json_roundtrip() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "fixed".to_string(),
            data_type: LogicalDataType::FixedBinary { byte_width: 8 },
            nullable: false,
        }])
        .expect("valid schema structure");

        let json = serde_json::to_string(&logical).unwrap();
        let back: LogicalSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(back, logical);
    }

    #[test]
    fn logical_schema_decimal_json_roundtrip() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "amount".to_string(),
            data_type: LogicalDataType::Decimal {
                precision: 18,
                scale: 4,
            },
            nullable: true,
        }])
        .expect("valid schema structure");

        let json = serde_json::to_string(&logical).unwrap();
        let back: LogicalSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(back, logical);
    }
}
