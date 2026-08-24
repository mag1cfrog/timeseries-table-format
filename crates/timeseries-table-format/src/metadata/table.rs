//! Table-level metadata structures recorded in the log.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::SchemaRef;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, prelude::*};

use crate::metadata::{
    index::IndexSpec,
    logical_schema::{LogicalSchema, LogicalToArrowSchemaError},
    protocol::{TABLE_PROTOCOL_VERSION, deserialize_required_features},
};

/// The high-level "kind" of table.
///
/// v0.1 supports only `TimeSeries`, but a `Generic` kind is reserved so that
/// the log format can represent non-timeseries tables later without breaking
/// existing JSON.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TableKind {
    /// A time-series table with an explicit ordered index specification.
    TimeSeries(IndexSpec),

    /// Placeholder for future basic tables that do not have a time index.
    /// Not used in v0.1.
    Generic,
}

/// High-level table metadata stored in the log.
///
/// This describes the table kind, a logical schema (optional in v0.1), and
/// basic bookkeeping fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(try_from = "RawTableMeta")]
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

    /// Version of the core metadata and commit-log protocol.
    ///
    /// Writers set this to [`TABLE_PROTOCOL_VERSION`].
    pub(crate) protocol_version: u32,

    /// Features a client must support to read this table.
    pub(crate) required_reader_features: BTreeSet<String>,

    /// Features a client must support to write this table.
    pub(crate) required_writer_features: BTreeSet<String>,
}

#[derive(Deserialize)]
struct RawTableMeta {
    kind: TableKind,
    logical_schema: Option<LogicalSchema>,
    created_at: DateTime<Utc>,
    protocol_version: u32,
    #[serde(deserialize_with = "deserialize_required_features")]
    required_reader_features: BTreeSet<String>,
    #[serde(deserialize_with = "deserialize_required_features")]
    required_writer_features: BTreeSet<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

impl TryFrom<RawTableMeta> for TableMeta {
    type Error = String;

    fn try_from(raw: RawTableMeta) -> Result<Self, Self::Error> {
        if raw.extra.contains_key("format_version") {
            return Err("legacy field 'format_version' is not valid protocol-v7 metadata".into());
        }

        Ok(Self {
            kind: raw.kind,
            logical_schema: raw.logical_schema,
            created_at: raw.created_at,
            protocol_version: raw.protocol_version,
            required_reader_features: raw.required_reader_features,
            required_writer_features: raw.required_writer_features,
        })
    }
}

/// Errors encountered while retrieving or converting a table's logical schema.
#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum TableArrowSchemaError {
    /// The table metadata has not yet recorded a canonical logical schema.
    #[snafu(display("table has no canonical logical schema yet (logical_schema is None)"))]
    MissingCanonicalSchema {
        /// Backtrace captured at the table schema boundary.
        backtrace: Backtrace,
    },

    /// Failed to convert the table's logical schema to Arrow types.
    #[snafu(display("Failed to convert the table logical schema to Arrow: {source}"))]
    Conversion {
        /// Underlying conversion error.
        #[snafu(source, backtrace)]
        source: LogicalToArrowSchemaError,
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

    /// Convenience constructor for a time-series table.
    ///
    /// - Fills `created_at` with `Utc::now()`.
    /// - Fills `protocol_version` with `TABLE_PROTOCOL_VERSION`.
    /// - Starts with no required reader or writer features.
    /// - Leaves `logical_schema` as `None`; it will be adopted from the
    ///   first appended segment in v0.1.
    pub fn new_time_series(index: IndexSpec) -> Self {
        TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: None,
            created_at: Utc::now(),
            protocol_version: TABLE_PROTOCOL_VERSION,
            required_reader_features: BTreeSet::new(),
            required_writer_features: BTreeSet::new(),
        }
    }

    /// Variant that lets you explicitly pass a logical schema up front.
    pub fn new_time_series_with_schema(index: IndexSpec, logical_schema: LogicalSchema) -> Self {
        TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: Some(logical_schema),
            created_at: Utc::now(),
            protocol_version: TABLE_PROTOCOL_VERSION,
            required_reader_features: BTreeSet::new(),
            required_writer_features: BTreeSet::new(),
        }
    }

    /// Convert the table's logical schema to a shared Arrow [`SchemaRef`].
    ///
    /// Returns [`TableArrowSchemaError::MissingCanonicalSchema`] if the schema has
    /// not yet been established for the table.
    pub fn arrow_schema_ref(&self) -> Result<SchemaRef, TableArrowSchemaError> {
        let logical = self
            .logical_schema
            .as_ref()
            .ok_or_else(|| MissingCanonicalSchemaSnafu.build())?;

        logical.to_arrow_schema_ref().context(ConversionSnafu)
    }
}

/// For v0.1, a `TableMetaDelta` is just a full replacement of [`TableMeta`].
///
/// This alias keeps the wire format simple (the JSON is the same as `TableMeta`)
/// while leaving room to evolve to more granular metadata updates in future
/// versions (for example, partial updates or additive fields).
pub type TableMetaDelta = TableMeta;

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use chrono::TimeZone;
    use snafu::ErrorCompat;

    use crate::metadata::{
        index::{IndexKind, IndexSpec, TimeIndexGranularity},
        logical_schema::{LogicalDataType, LogicalField},
        protocol::TABLE_PROTOCOL_VERSION,
    };

    use super::*;

    fn sample_time_index_spec() -> IndexSpec {
        IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string()],
            kind: IndexKind::Timestamp {
                index_granularity: TimeIndexGranularity::Minutes(1),
                timezone: None,
            },
        }
    }

    #[test]
    fn table_meta_baseline_protocol_json_is_stable() {
        let mut meta = TableMeta::new_time_series(sample_time_index_spec());
        meta.created_at = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).single().unwrap();

        assert_eq!(
            serde_json::to_value(meta).unwrap(),
            serde_json::json!({
                "kind": {
                    "TimeSeries": {
                        "column": "ts",
                        "entity_columns": ["symbol"],
                        "kind": {
                            "type": "timestamp",
                            "index_granularity": {"Minutes": 1}
                        }
                    }
                },
                "logical_schema": null,
                "created_at": "2025-01-01T00:00:00Z",
                "protocol_version": 7,
                "required_reader_features": [],
                "required_writer_features": []
            })
        );
    }

    #[test]
    fn table_meta_protocol_fields_roundtrip_canonically() {
        let mut meta = TableMeta::new_time_series(sample_time_index_spec());
        meta.required_reader_features.insert("z_reader".to_string());
        meta.required_reader_features.insert("a_reader".to_string());
        meta.required_writer_features.insert("writer_2".to_string());

        let mut json = serde_json::to_value(&meta).unwrap();
        assert_eq!(json["protocol_version"], TABLE_PROTOCOL_VERSION);
        assert_eq!(
            json["required_reader_features"],
            serde_json::json!(["a_reader", "z_reader"])
        );
        assert_eq!(
            json["required_writer_features"],
            serde_json::json!(["writer_2"])
        );
        assert!(json.get("format_version").is_none());

        json["future_field"] = serde_json::json!({"ignored": true});
        let decoded: TableMeta = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn table_meta_rejects_legacy_format_version_field() {
        let mut json =
            serde_json::to_value(TableMeta::new_time_series(sample_time_index_spec())).unwrap();
        json["format_version"] = serde_json::json!(6);

        let error = serde_json::from_value::<TableMeta>(json).unwrap_err();
        assert!(error.to_string().contains("legacy field 'format_version'"));
    }

    #[test]
    fn table_meta_arrow_schema_ref_requires_logical_schema() {
        let meta = TableMeta::new_time_series(sample_time_index_spec());
        let err = meta.arrow_schema_ref().unwrap_err();
        assert!(matches!(
            &err,
            TableArrowSchemaError::MissingCanonicalSchema { .. }
        ));
        assert!(ErrorCompat::backtrace(&err).is_some());
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
        let table_backtrace = ErrorCompat::backtrace(&err).expect("table schema backtrace");
        let conversion = err
            .source()
            .and_then(|source| source.downcast_ref::<LogicalToArrowSchemaError>())
            .expect("Arrow conversion source");
        let conversion_backtrace =
            ErrorCompat::backtrace(conversion).expect("conversion backtrace");

        assert!(matches!(
            conversion,
            LogicalToArrowSchemaError::Int96Unsupported { column, .. }
                if column == "legacy_ts"
        ));
        assert!(std::ptr::eq(table_backtrace, conversion_backtrace));
    }
}
