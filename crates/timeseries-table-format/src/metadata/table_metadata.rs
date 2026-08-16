//! Table-level metadata structures recorded in the log.
//!
//! This module models the schema and configuration captured by
//! `LogAction::UpdateTableMeta`, including table kind, logical schema, and the
//! time index specification. Future evolutions can extend these types without
//! touching the storage/reader code paths.
use std::{cmp::Ordering, collections::HashSet, fmt, num::NonZeroU64, str::FromStr};

use arrow::datatypes::SchemaRef;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::metadata::logical_schema::{LogicalSchema, SchemaConvertError};

/// Current table metadata / log format version written by new tables.
///
/// Bumped when persisted table semantics require version-aware decoding.
pub const TABLE_FORMAT_VERSION: u32 = 4;

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
    /// Writers set this to [`TABLE_FORMAT_VERSION`].
    pub(crate) format_version: u32,
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
    pub fn new_time_series(index: IndexSpec) -> Self {
        TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: None,
            created_at: Utc::now(),
            format_version: TABLE_FORMAT_VERSION,
        }
    }

    /// Variant that lets you explicitly pass a logical schema up front.
    pub fn new_time_series_with_schema(index: IndexSpec, logical_schema: LogicalSchema) -> Self {
        TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: Some(logical_schema),
            created_at: Utc::now(),
            format_version: TABLE_FORMAT_VERSION,
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

        // Split into numeric prefix + unit suffix (unit starts at first alphabetic char).
        let unit_start = spec
            .char_indices()
            .find(|(_, c)| c.is_ascii_alphabetic())
            .map(|(i, _)| i);

        let Some(unit_start) = unit_start else {
            return Err(ParseTimeBucketError::MissingUnit {
                spec: spec.to_string(),
            });
        };

        if unit_start == 0 {
            // No leading digits (e.g. "h")
            return Err(ParseTimeBucketError::MissingNumber {
                spec: spec.to_string(),
            });
        }

        let (num_str, unit_str) = spec.split_at(unit_start);
        let num_str = num_str.trim();
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

/// Canonical ordered-index configuration for a time-series table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IndexSpec {
    /// Name of the single ordered index column.
    pub column: String,

    /// Optional ordered entity columns used for entity-scoped coverage.
    #[serde(default)]
    pub entity_columns: Vec<String>,

    /// Ordered value domain and coverage bucket configuration.
    pub kind: IndexKind,
}

impl IndexSpec {
    /// Validate structural invariants that do not require a logical schema.
    ///
    /// # Errors
    /// Returns [`IndexSpecError`] for an empty index column, an empty entity
    /// column, or a duplicate entity column.
    pub fn validate(&self) -> Result<(), IndexSpecError> {
        if self.column.is_empty() {
            return Err(IndexSpecError::EmptyColumn);
        }

        let mut seen = HashSet::with_capacity(self.entity_columns.len());
        for (position, column) in self.entity_columns.iter().enumerate() {
            if column.is_empty() {
                return Err(IndexSpecError::EmptyEntityColumn { position });
            }
            if !seen.insert(column) {
                return Err(IndexSpecError::DuplicateEntityColumn {
                    column: column.clone(),
                });
            }
        }

        self.kind.validate()
    }
}

/// Ordered value domain and its coverage bucket configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum IndexKind {
    /// Timestamp index with fixed time buckets and optional timezone metadata.
    Timestamp {
        /// Logical coverage bucket size.
        bucket: TimeBucket,
        /// Optional IANA timezone identifier.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timezone: Option<String>,
    },
    /// Signed 64-bit integer index.
    Int64 {
        /// Positive bucket width in index-value units.
        bucket_width: NonZeroU64,
    },
    /// Unsigned 64-bit integer index.
    UInt64 {
        /// Positive bucket width in index-value units.
        bucket_width: NonZeroU64,
    },
}

impl IndexKind {
    /// Stable user-facing domain name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Timestamp { .. } => "timestamp",
            Self::Int64 { .. } => "int64",
            Self::UInt64 { .. } => "uint64",
        }
    }

    /// Validate bucket configuration not enforced by the Rust type system.
    pub fn validate(&self) -> Result<(), IndexSpecError> {
        if let Self::Timestamp { bucket, .. } = self {
            let width = match bucket {
                TimeBucket::Seconds(width)
                | TimeBucket::Minutes(width)
                | TimeBucket::Hours(width)
                | TimeBucket::Days(width) => *width,
            };
            if width == 0 {
                return Err(IndexSpecError::ZeroTimeBucket);
            }
        }
        Ok(())
    }
}

/// A value in one of the supported ordered-index domains.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum IndexValue {
    /// UTC timestamp value.
    Timestamp(DateTime<Utc>),
    /// Signed 64-bit integer value.
    Int64(i64),
    /// Unsigned 64-bit integer value.
    UInt64(u64),
}

impl IndexValue {
    /// Stable user-facing domain name.
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Timestamp(_) => "timestamp",
            Self::Int64(_) => "int64",
            Self::UInt64(_) => "uint64",
        }
    }

    /// Compare two values in the same ordered domain.
    ///
    /// # Errors
    /// Returns [`IndexValueError::DomainMismatch`] for cross-domain values.
    pub fn compare(&self, other: &Self) -> Result<Ordering, IndexValueError> {
        match (self, other) {
            (Self::Timestamp(left), Self::Timestamp(right)) => Ok(left.cmp(right)),
            (Self::Int64(left), Self::Int64(right)) => Ok(left.cmp(right)),
            (Self::UInt64(left), Self::UInt64(right)) => Ok(left.cmp(right)),
            _ => Err(IndexValueError::DomainMismatch {
                left: self.kind_name(),
                right: other.kind_name(),
            }),
        }
    }

    /// Validate that this value belongs to `kind`.
    ///
    /// # Errors
    /// Returns [`IndexValueError::KindMismatch`] when the domains differ.
    pub fn validate_kind(&self, kind: &IndexKind) -> Result<(), IndexValueError> {
        if self.kind_name() == kind.name() {
            Ok(())
        } else {
            Err(IndexValueError::KindMismatch {
                expected: kind.name(),
                actual: self.kind_name(),
            })
        }
    }
}

impl fmt::Display for IndexValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timestamp(value) => write!(f, "timestamp({value})"),
            Self::Int64(value) => write!(f, "int64({value})"),
            Self::UInt64(value) => write!(f, "uint64({value})"),
        }
    }
}

impl From<DateTime<Utc>> for IndexValue {
    fn from(value: DateTime<Utc>) -> Self {
        Self::Timestamp(value)
    }
}

impl From<i64> for IndexValue {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<u64> for IndexValue {
    fn from(value: u64) -> Self {
        Self::UInt64(value)
    }
}

/// Validate a public half-open ordered-index range.
///
/// # Errors
/// Returns [`IndexValueError`] when a bound has the wrong domain, the bounds
/// use different domains, or `start >= end`.
pub fn validate_index_range(
    kind: &IndexKind,
    start: &IndexValue,
    end: &IndexValue,
) -> Result<(), IndexValueError> {
    start.validate_kind(kind)?;
    end.validate_kind(kind)?;
    if start.compare(end)? != Ordering::Less {
        return Err(IndexValueError::InvalidRange {
            start: start.clone(),
            end: end.clone(),
        });
    }
    Ok(())
}

/// Structural errors in an [`IndexSpec`].
#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum IndexSpecError {
    /// The registered index column is empty.
    #[snafu(display("ordered index column is empty"))]
    EmptyColumn,
    /// An entity column is empty.
    #[snafu(display("entity column at position {position} is empty"))]
    EmptyEntityColumn {
        /// Zero-based position of the empty entity column.
        position: usize,
    },
    /// An entity column is repeated.
    #[snafu(display("duplicate entity column: {column}"))]
    DuplicateEntityColumn {
        /// Repeated entity column name.
        column: String,
    },
    /// A timestamp bucket was constructed directly with a zero width.
    #[snafu(display("timestamp bucket width must be nonzero"))]
    ZeroTimeBucket,
}

/// Domain and range errors for [`IndexValue`].
#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum IndexValueError {
    /// Two values use different ordered domains.
    #[snafu(display("ordered index domain mismatch: left={left}, right={right}"))]
    DomainMismatch {
        /// Left value domain.
        left: &'static str,
        /// Right value domain.
        right: &'static str,
    },
    /// A value does not match the table's registered domain.
    #[snafu(display("ordered index kind mismatch: expected {expected}, found {actual}"))]
    KindMismatch {
        /// Registered domain.
        expected: &'static str,
        /// Supplied value domain.
        actual: &'static str,
    },
    /// A half-open range is empty or reversed.
    #[snafu(display(
        "invalid ordered index range: start={start}, end={end} (expected start < end)"
    ))]
    InvalidRange {
        /// Inclusive lower bound.
        start: IndexValue,
        /// Exclusive upper bound.
        end: IndexValue,
    },
    /// Inclusive segment bounds are reversed.
    #[snafu(display("invalid ordered index bounds: min={min}, max={max} (expected min <= max)"))]
    InvalidBounds {
        /// Inclusive observed minimum.
        min: IndexValue,
        /// Inclusive observed maximum.
        max: IndexValue,
    },
}

#[cfg(test)]
mod tests {
    use crate::metadata::logical_schema::{LogicalDataType, LogicalField};

    use super::*;
    use chrono::TimeZone;

    fn sample_time_index_spec() -> IndexSpec {
        IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string()],
            kind: IndexKind::Timestamp {
                bucket: TimeBucket::Minutes(1),
                timezone: None,
            },
        }
    }

    #[test]
    fn index_spec_json_roundtrips_all_domains() {
        let specs = [
            sample_time_index_spec(),
            IndexSpec {
                column: "sequence".to_string(),
                entity_columns: Vec::new(),
                kind: IndexKind::Int64 {
                    bucket_width: NonZeroU64::new(u64::MAX).unwrap(),
                },
            },
            IndexSpec {
                column: "offset".to_string(),
                entity_columns: vec!["source".to_string()],
                kind: IndexKind::UInt64 {
                    bucket_width: NonZeroU64::new(7).unwrap(),
                },
            },
        ];

        for spec in specs {
            let json = serde_json::to_string(&spec).unwrap();
            let restored: IndexSpec = serde_json::from_str(&json).unwrap();
            assert_eq!(restored, spec);
        }
    }

    #[test]
    fn index_spec_json_rejects_impossible_field_combinations() {
        let timestamp_with_integer_width = r#"{
            "column":"ts",
            "kind":{"type":"timestamp","bucket":{"Seconds":1},"bucket_width":1}
        }"#;
        let integer_with_timestamp_bucket = r#"{
            "column":"id",
            "kind":{"type":"int64","bucket_width":1,"bucket":{"Seconds":1}}
        }"#;
        let zero_integer_width = r#"{"column":"id","kind":{"type":"uint64","bucket_width":0}}"#;

        assert!(serde_json::from_str::<IndexSpec>(timestamp_with_integer_width).is_err());
        assert!(serde_json::from_str::<IndexSpec>(integer_with_timestamp_bucket).is_err());
        assert!(serde_json::from_str::<IndexSpec>(zero_integer_width).is_err());
    }

    #[test]
    fn index_spec_validation_rejects_invalid_structure_and_time_bucket() {
        let mut spec = sample_time_index_spec();
        spec.column.clear();
        assert_eq!(spec.validate(), Err(IndexSpecError::EmptyColumn));

        let mut spec = sample_time_index_spec();
        spec.entity_columns.push("symbol".to_string());
        assert!(matches!(
            spec.validate(),
            Err(IndexSpecError::DuplicateEntityColumn { .. })
        ));

        let mut spec = sample_time_index_spec();
        spec.kind = IndexKind::Timestamp {
            bucket: TimeBucket::Seconds(0),
            timezone: None,
        };
        assert_eq!(spec.validate(), Err(IndexSpecError::ZeroTimeBucket));
    }

    #[test]
    fn index_value_roundtrips_and_compares_integer_extremes() {
        let timestamp = Utc.timestamp_opt(1, 987_654_321).single().unwrap();
        let values = [
            IndexValue::Timestamp(timestamp),
            IndexValue::Int64(i64::MIN),
            IndexValue::Int64(i64::MAX),
            IndexValue::UInt64(0),
            IndexValue::UInt64(u64::MAX),
        ];

        for value in values {
            let json = serde_json::to_string(&value).unwrap();
            assert_eq!(serde_json::from_str::<IndexValue>(&json).unwrap(), value);
            assert_eq!(value.compare(&value).unwrap(), Ordering::Equal);
        }
        assert_eq!(
            IndexValue::Int64(i64::MIN)
                .compare(&IndexValue::Int64(i64::MAX))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            IndexValue::UInt64(u64::MAX)
                .compare(&IndexValue::UInt64(0))
                .unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn index_value_cross_domain_comparison_and_ranges_are_typed_errors() {
        assert_eq!(
            IndexValue::Int64(0).compare(&IndexValue::UInt64(0)),
            Err(IndexValueError::DomainMismatch {
                left: "int64",
                right: "uint64"
            })
        );

        let kind = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        };
        assert!(matches!(
            validate_index_range(&kind, &IndexValue::Int64(0), &IndexValue::Int64(1)),
            Err(IndexValueError::KindMismatch { .. })
        ));
        assert!(matches!(
            validate_index_range(&kind, &IndexValue::UInt64(1), &IndexValue::UInt64(1)),
            Err(IndexValueError::InvalidRange { .. })
        ));
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
    fn time_bucket_parse_accepts_basic_units() {
        let cases = [
            ("1s", TimeBucket::Seconds(1)),
            ("2m", TimeBucket::Minutes(2)),
            ("3h", TimeBucket::Hours(3)),
            ("4d", TimeBucket::Days(4)),
        ];

        for (input, expected) in cases {
            assert_eq!(input.parse::<TimeBucket>().unwrap(), expected);
        }
    }

    #[test]
    fn time_bucket_parse_accepts_aliases_case_and_whitespace() {
        let cases = [
            ("1sec", TimeBucket::Seconds(1)),
            ("1secs", TimeBucket::Seconds(1)),
            ("1second", TimeBucket::Seconds(1)),
            ("1seconds", TimeBucket::Seconds(1)),
            ("1min", TimeBucket::Minutes(1)),
            ("1mins", TimeBucket::Minutes(1)),
            ("1minute", TimeBucket::Minutes(1)),
            ("1minutes", TimeBucket::Minutes(1)),
            ("1hr", TimeBucket::Hours(1)),
            ("1hrs", TimeBucket::Hours(1)),
            ("1hour", TimeBucket::Hours(1)),
            ("1hours", TimeBucket::Hours(1)),
            ("1day", TimeBucket::Days(1)),
            ("1days", TimeBucket::Days(1)),
            ("1H", TimeBucket::Hours(1)),
            ("1MiN", TimeBucket::Minutes(1)),
            ("  2h", TimeBucket::Hours(2)),
            ("3d  ", TimeBucket::Days(3)),
            ("  4m  ", TimeBucket::Minutes(4)),
            ("1 h", TimeBucket::Hours(1)),
        ];

        for (input, expected) in cases {
            assert_eq!(input.parse::<TimeBucket>().unwrap(), expected);
        }
    }

    #[test]
    fn time_bucket_parse_rejects_empty_or_whitespace() {
        let cases = ["", "   ", "\n\t"];
        for input in cases {
            let err = input.parse::<TimeBucket>().unwrap_err();
            assert!(matches!(err, ParseTimeBucketError::Empty));
        }
    }

    #[test]
    fn time_bucket_parse_rejects_missing_number() {
        let cases = ["h", " hr", "day", "abcmin"];
        for input in cases {
            let err = input.parse::<TimeBucket>().unwrap_err();
            assert!(
                matches!(err, ParseTimeBucketError::MissingNumber { .. }),
                "expected MissingNumber for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn time_bucket_parse_rejects_missing_unit() {
        let cases = ["1", "  42  "];
        for input in cases {
            let err = input.parse::<TimeBucket>().unwrap_err();
            assert!(
                matches!(err, ParseTimeBucketError::MissingUnit { .. }),
                "expected MissingUnit for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn time_bucket_parse_rejects_invalid_number() {
        let cases = ["1.5h", "1_000s"];
        for input in cases {
            let err = input.parse::<TimeBucket>().unwrap_err();
            assert!(
                matches!(err, ParseTimeBucketError::InvalidNumber { .. }),
                "expected InvalidNumber for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn time_bucket_parse_rejects_non_positive() {
        let cases = ["0s", "0m"];
        for input in cases {
            let err = input.parse::<TimeBucket>().unwrap_err();
            assert!(
                matches!(err, ParseTimeBucketError::NonPositive { value: 0, .. }),
                "expected NonPositive for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn time_bucket_parse_rejects_too_large() {
        let too_large = (u32::MAX as u64 + 1).to_string();
        let input = format!("{too_large}h");
        let err = input.parse::<TimeBucket>().unwrap_err();
        assert!(
            matches!(err, ParseTimeBucketError::TooLarge { value, .. } if value == u32::MAX as u64 + 1),
            "expected TooLarge for {input:?}, got {err:?}"
        );
    }

    #[test]
    fn time_bucket_parse_rejects_unknown_units() {
        let cases = ["1w", "1ms", "1mo", "10msec"];
        for input in cases {
            let err = input.parse::<TimeBucket>().unwrap_err();
            assert!(
                matches!(err, ParseTimeBucketError::UnknownUnit { .. }),
                "expected UnknownUnit for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn time_bucket_parse_matches_from_str() {
        let via_method = TimeBucket::parse("5m").unwrap();
        let via_trait: TimeBucket = "5m".parse().unwrap();
        assert_eq!(via_method, via_trait);
    }
}
