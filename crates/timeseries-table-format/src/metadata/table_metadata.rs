//! Table-level metadata structures recorded in the log.
//!
//! This module models the schema and configuration captured by
//! `LogAction::UpdateTableMeta`, including table kind, logical schema, and the
//! time index specification. Future evolutions can extend these types without
//! touching the storage/reader code paths.
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt,
    num::NonZeroU64,
    str::FromStr,
};

use arrow::datatypes::SchemaRef;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, prelude::*};

use crate::metadata::logical_schema::{LogicalSchema, LogicalToArrowSchemaError};

/// Current table protocol version written by new tables.
///
/// This changes only when the core metadata or commit-log envelope can no
/// longer be decoded under the current protocol. Optional capabilities are
/// declared separately in the reader and writer feature sets.
pub const TABLE_PROTOCOL_VERSION: u32 = 7;

const SUPPORTED_READER_FEATURES: &[&str] = &[];
const SUPPORTED_WRITER_FEATURES: &[&str] = &[];

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

fn deserialize_required_features<'de, D>(deserializer: D) -> Result<BTreeSet<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;

    let features = Vec::<String>::deserialize(deserializer)?;
    let mut unique = BTreeSet::new();
    for feature in features {
        if !is_valid_feature_name(&feature) {
            return Err(D::Error::custom(format!(
                "invalid table feature {feature:?}; expected [a-z][a-z0-9_]*"
            )));
        }
        if !unique.insert(feature.clone()) {
            return Err(D::Error::custom(format!(
                "duplicate table feature {feature:?}"
            )));
        }
    }
    Ok(unique)
}

fn is_valid_feature_name(feature: &str) -> bool {
    let mut bytes = feature.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// A table protocol requirement or transition is not supported.
#[derive(Debug, Snafu, PartialEq, Eq)]
#[non_exhaustive]
pub enum TableProtocolError {
    /// The table uses a core protocol version this client cannot decode.
    #[snafu(display("unsupported table protocol version: expected {expected}, found {found}"))]
    UnsupportedVersion {
        /// Core protocol version supported by this client.
        expected: u32,
        /// Core protocol version required by the table.
        found: u64,
    },

    /// The table requires reader features this client does not support.
    #[snafu(display("unsupported table reader features: {features:?}"))]
    UnsupportedReaderFeatures {
        /// Unsupported feature identifiers in canonical order.
        features: Vec<String>,
    },

    /// The table requires writer features this client does not support.
    #[snafu(display("unsupported table writer features: {features:?}"))]
    UnsupportedWriterFeatures {
        /// Unsupported feature identifiers in canonical order.
        features: Vec<String>,
    },

    /// A metadata update removed previously required reader features.
    #[snafu(display("table metadata removed required reader features: {features:?}"))]
    ReaderFeaturesRemoved {
        /// Removed feature identifiers in canonical order.
        features: Vec<String>,
    },

    /// A metadata update removed previously required writer features.
    #[snafu(display("table metadata removed required writer features: {features:?}"))]
    WriterFeaturesRemoved {
        /// Removed feature identifiers in canonical order.
        features: Vec<String>,
    },

    /// A metadata update decreased the core protocol version.
    #[snafu(display("table metadata decreased protocol version from {previous} to {next}"))]
    ProtocolVersionDecreased {
        /// Protocol version before the metadata update.
        previous: u32,
        /// Protocol version after the metadata update.
        next: u32,
    },
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

    /// Failed to convert the logical schema to Arrow types.
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

    /// Returns the on-disk table protocol version.
    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// Returns the features a client must support to read the table.
    pub fn required_reader_features(&self) -> &BTreeSet<String> {
        &self.required_reader_features
    }

    /// Returns the features a client must support to write the table.
    pub fn required_writer_features(&self) -> &BTreeSet<String> {
        &self.required_writer_features
    }

    pub(crate) fn ensure_read_compatible(&self) -> Result<(), TableProtocolError> {
        self.ensure_read_compatible_with(SUPPORTED_READER_FEATURES)
    }

    pub(crate) fn ensure_write_compatible(&self) -> Result<(), TableProtocolError> {
        self.ensure_write_compatible_with(SUPPORTED_READER_FEATURES, SUPPORTED_WRITER_FEATURES)
    }

    fn ensure_read_compatible_with(
        &self,
        supported_reader_features: &[&str],
    ) -> Result<(), TableProtocolError> {
        if self.protocol_version != TABLE_PROTOCOL_VERSION {
            return Err(TableProtocolError::UnsupportedVersion {
                expected: TABLE_PROTOCOL_VERSION,
                found: u64::from(self.protocol_version),
            });
        }

        let unsupported =
            unsupported_features(&self.required_reader_features, supported_reader_features);
        if !unsupported.is_empty() {
            return Err(TableProtocolError::UnsupportedReaderFeatures {
                features: unsupported,
            });
        }
        Ok(())
    }

    fn ensure_write_compatible_with(
        &self,
        supported_reader_features: &[&str],
        supported_writer_features: &[&str],
    ) -> Result<(), TableProtocolError> {
        self.ensure_read_compatible_with(supported_reader_features)?;

        let unsupported =
            unsupported_features(&self.required_writer_features, supported_writer_features);
        if !unsupported.is_empty() {
            return Err(TableProtocolError::UnsupportedWriterFeatures {
                features: unsupported,
            });
        }
        Ok(())
    }

    pub(crate) fn ensure_valid_transition_to(&self, next: &Self) -> Result<(), TableProtocolError> {
        if next.protocol_version < self.protocol_version {
            return Err(TableProtocolError::ProtocolVersionDecreased {
                previous: self.protocol_version,
                next: next.protocol_version,
            });
        }

        let removed_reader_features = self
            .required_reader_features
            .difference(&next.required_reader_features)
            .cloned()
            .collect::<Vec<_>>();
        if !removed_reader_features.is_empty() {
            return Err(TableProtocolError::ReaderFeaturesRemoved {
                features: removed_reader_features,
            });
        }

        let removed_writer_features = self
            .required_writer_features
            .difference(&next.required_writer_features)
            .cloned()
            .collect::<Vec<_>>();
        if !removed_writer_features.is_empty() {
            return Err(TableProtocolError::WriterFeaturesRemoved {
                features: removed_writer_features,
            });
        }
        Ok(())
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

fn unsupported_features(required: &BTreeSet<String>, supported: &[&str]) -> Vec<String> {
    required
        .iter()
        .filter(|feature| !supported.contains(&feature.as_str()))
        .cloned()
        .collect()
}

/// For v0.1, a `TableMetaDelta` is just a full replacement of [`TableMeta`].
///
/// This alias keeps the wire format simple (the JSON is the same as `TableMeta`)
/// while leaving room to evolve to more granular metadata updates in future
/// versions (for example, partial updates or additive fields).
pub type TableMetaDelta = TableMeta;

/// Errors produced when parsing a human-friendly time index granularity (e.g. `1h`).
#[derive(Debug, Snafu, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseTimeIndexGranularityError {
    /// The spec string was empty or only whitespace.
    #[snafu(display("time index granularity is empty"))]
    Empty,

    /// The spec did not include a numeric value.
    #[snafu(display("time index granularity '{spec}' is missing a numeric value"))]
    MissingNumber {
        /// The original spec string.
        spec: String,
    },

    /// The spec did not include a required unit suffix.
    #[snafu(display(
        "time index granularity '{spec}' is missing a unit suffix (expected s|m|h|d)"
    ))]
    MissingUnit {
        /// The original spec string.
        spec: String,
    },

    /// The numeric portion of the spec failed to parse.
    #[snafu(display("invalid index granularity value in '{spec}': {source}"))]
    InvalidNumber {
        /// The original spec string.
        spec: String,
        /// The parse error returned by `u64::from_str`.
        source: std::num::ParseIntError,
    },

    /// The parsed numeric value was zero.
    #[snafu(display("index granularity value must be > 0 (got {value}) in '{spec}'"))]
    NonPositive {
        /// The original spec string.
        spec: String,
        /// The parsed numeric value.
        value: u64,
    },

    /// The parsed numeric value did not fit in a `u32`.
    #[snafu(display("index granularity value too large for u32 (got {value}) in '{spec}'"))]
    TooLarge {
        /// The original spec string.
        spec: String,
        /// The parsed numeric value.
        value: u64,
    },

    /// The spec used an unsupported unit suffix.
    #[snafu(display(
        "unknown time index granularity unit '{unit}' in '{spec}' (expected s|m|h|d)"
    ))]
    UnknownUnit {
        /// The original spec string.
        spec: String,
        /// The unrecognized unit suffix.
        unit: String,
    },
}

/// Time interval size used by coverage bitmap logic.
///
/// This does not affect physical storage directly, but describes how the time
/// axis is discretized when building coverage bitmaps and computing gaps.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeIndexGranularity {
    /// A fixed number of seconds.
    Seconds(u32),
    /// A fixed number of minutes.
    Minutes(u32),
    /// A fixed number of hours.
    Hours(u32),
    /// A fixed number of days.
    Days(u32),
}

impl FromStr for TimeIndexGranularity {
    type Err = ParseTimeIndexGranularityError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let spec = input.trim();
        if spec.is_empty() {
            return Err(ParseTimeIndexGranularityError::Empty);
        }

        // Split into numeric prefix + unit suffix (unit starts at first alphabetic char).
        let unit_start = spec
            .char_indices()
            .find(|(_, c)| c.is_ascii_alphabetic())
            .map(|(i, _)| i);

        let Some(unit_start) = unit_start else {
            return Err(ParseTimeIndexGranularityError::MissingUnit {
                spec: spec.to_string(),
            });
        };

        if unit_start == 0 {
            // No leading digits (e.g. "h")
            return Err(ParseTimeIndexGranularityError::MissingNumber {
                spec: spec.to_string(),
            });
        }

        let (num_str, unit_str) = spec.split_at(unit_start);
        let num_str = num_str.trim();
        let unit_str = unit_str.trim();

        if unit_str.is_empty() {
            return Err(ParseTimeIndexGranularityError::MissingUnit {
                spec: spec.to_string(),
            });
        }

        let value: u64 =
            num_str
                .parse()
                .map_err(|source| ParseTimeIndexGranularityError::InvalidNumber {
                    spec: spec.to_string(),
                    source,
                })?;

        if value == 0 {
            return Err(ParseTimeIndexGranularityError::NonPositive {
                spec: spec.to_string(),
                value,
            });
        }

        if value > u32::MAX as u64 {
            return Err(ParseTimeIndexGranularityError::TooLarge {
                spec: spec.to_string(),
                value,
            });
        }

        let v = value as u32;
        let unit = unit_str.to_ascii_lowercase();

        match unit.as_str() {
            "s" | "sec" | "secs" | "second" | "seconds" => Ok(TimeIndexGranularity::Seconds(v)),
            "m" | "min" | "mins" | "minute" | "minutes" => Ok(TimeIndexGranularity::Minutes(v)),
            "h" | "hr" | "hrs" | "hour" | "hours" => Ok(TimeIndexGranularity::Hours(v)),
            "d" | "day" | "days" => Ok(TimeIndexGranularity::Days(v)),
            _ => Err(ParseTimeIndexGranularityError::UnknownUnit {
                spec: spec.to_string(),
                unit: unit_str.to_string(),
            }),
        }
    }
}

impl TimeIndexGranularity {
    /// Parse a human-friendly time index granularity (e.g. `1h`, `15m`, `30s`, `2d`).
    ///
    /// This is a convenience wrapper around `str::parse` for [`TimeIndexGranularity`], and
    /// accepts common unit aliases (e.g. `sec`, `min`, `hr`, `day`).
    ///
    /// # Errors
    /// Returns [`ParseTimeIndexGranularityError`] if the spec is empty, missing a unit,
    /// has an invalid or non-positive number, overflows `u32`, or uses an
    /// unsupported unit.
    pub fn parse(spec: &str) -> Result<Self, ParseTimeIndexGranularityError> {
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

    /// Ordered value domain and index granularity configuration.
    pub kind: IndexKind,
}

impl IndexSpec {
    /// Validate structural invariants that do not require a logical schema.
    ///
    /// # Errors
    /// Returns [`IndexSpecError`] for an empty index column, an empty entity
    /// column, a duplicate entity column, or an entity column that is also the
    /// ordered index.
    pub fn validate(&self) -> Result<(), IndexSpecError> {
        if self.column.is_empty() {
            return Err(IndexSpecError::EmptyColumn);
        }

        let mut seen = HashSet::with_capacity(self.entity_columns.len());
        for (position, column) in self.entity_columns.iter().enumerate() {
            if column.is_empty() {
                return Err(IndexSpecError::EmptyEntityColumn { position });
            }
            if column == &self.column {
                return Err(IndexSpecError::EntityColumnMatchesIndex {
                    column: column.clone(),
                });
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

/// Ordered value domain and its index granularity configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum IndexKind {
    /// Timestamp index with a fixed granularity and optional timezone metadata.
    Timestamp {
        /// Logical index interval size.
        index_granularity: TimeIndexGranularity,
        /// Optional IANA timezone identifier.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timezone: Option<String>,
    },
    /// Signed 64-bit integer index.
    Int64 {
        /// Positive index granularity in index-value units.
        index_granularity: NonZeroU64,
    },
    /// Unsigned 64-bit integer index.
    #[serde(rename = "uint64")]
    UInt64 {
        /// Positive index granularity in index-value units.
        index_granularity: NonZeroU64,
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

    /// Validate index granularity not enforced by the Rust type system.
    pub fn validate(&self) -> Result<(), IndexSpecError> {
        if let Self::Timestamp {
            index_granularity, ..
        } = self
        {
            let width = match index_granularity {
                TimeIndexGranularity::Seconds(width)
                | TimeIndexGranularity::Minutes(width)
                | TimeIndexGranularity::Hours(width)
                | TimeIndexGranularity::Days(width) => *width,
            };
            if width == 0 {
                return Err(IndexSpecError::ZeroTimeIndexGranularity);
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
#[non_exhaustive]
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
    /// An entity column is also the ordered index column.
    #[snafu(display("entity column cannot also be the ordered index column: {column}"))]
    EntityColumnMatchesIndex {
        /// Conflicting column name.
        column: String,
    },
    /// A timestamp index granularity was constructed directly with a zero width.
    #[snafu(display("timestamp index granularity must be nonzero"))]
    ZeroTimeIndexGranularity,
}

/// Domain and range errors for [`IndexValue`].
#[derive(Debug, Snafu, PartialEq, Eq)]
#[non_exhaustive]
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
    use std::error::Error as _;

    use crate::metadata::logical_schema::{LogicalDataType, LogicalField};

    use super::*;
    use chrono::TimeZone;
    use snafu::ErrorCompat;

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
    fn table_meta_requires_valid_unique_feature_lists() {
        let valid =
            serde_json::to_value(TableMeta::new_time_series(sample_time_index_spec())).unwrap();
        let invalid_lists = [
            serde_json::Value::Null,
            serde_json::json!(["duplicate", "duplicate"]),
            serde_json::json!(["Uppercase"]),
            serde_json::json!([""]),
        ];

        for invalid in invalid_lists {
            let mut json = valid.clone();
            json["required_reader_features"] = invalid;
            assert!(serde_json::from_value::<TableMeta>(json).is_err());
        }

        for field in ["required_reader_features", "required_writer_features"] {
            let mut json = valid.clone();
            json.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<TableMeta>(json).is_err());
        }
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
    fn table_protocol_compatibility_is_operation_specific() {
        let mut meta = TableMeta::new_time_series(sample_time_index_spec());
        meta.required_reader_features
            .extend(["reader_a".to_string(), "reader_b".to_string()]);
        meta.required_writer_features.insert("writer_a".to_string());

        assert!(
            meta.ensure_read_compatible_with(&["reader_a", "reader_b"])
                .is_ok()
        );
        assert_eq!(
            meta.ensure_read_compatible_with(&["reader_b"]),
            Err(TableProtocolError::UnsupportedReaderFeatures {
                features: vec!["reader_a".to_string()],
            })
        );
        assert_eq!(
            meta.ensure_write_compatible_with(&["reader_a", "reader_b"], &[]),
            Err(TableProtocolError::UnsupportedWriterFeatures {
                features: vec!["writer_a".to_string()],
            })
        );
        assert!(
            meta.ensure_write_compatible_with(&["reader_a", "reader_b"], &["writer_a"])
                .is_ok()
        );
        assert_eq!(
            meta.ensure_write_compatible(),
            Err(TableProtocolError::UnsupportedReaderFeatures {
                features: vec!["reader_a".to_string(), "reader_b".to_string()],
            })
        );
    }

    #[test]
    fn table_protocol_transition_requirements_are_monotonic() {
        let mut previous = TableMeta::new_time_series(sample_time_index_spec());
        previous
            .required_reader_features
            .extend(["reader_a".to_string(), "reader_b".to_string()]);
        previous
            .required_writer_features
            .insert("writer_a".to_string());

        let mut additive = previous.clone();
        additive
            .required_writer_features
            .insert("writer_b".to_string());
        assert!(previous.ensure_valid_transition_to(&additive).is_ok());

        let mut removed = previous.clone();
        removed.required_reader_features.clear();
        assert_eq!(
            previous.ensure_valid_transition_to(&removed),
            Err(TableProtocolError::ReaderFeaturesRemoved {
                features: vec!["reader_a".to_string(), "reader_b".to_string()],
            })
        );

        let mut removed = previous.clone();
        removed.required_writer_features.clear();
        assert_eq!(
            previous.ensure_valid_transition_to(&removed),
            Err(TableProtocolError::WriterFeaturesRemoved {
                features: vec!["writer_a".to_string()],
            })
        );

        let mut decreased = previous.clone();
        decreased.protocol_version -= 1;
        assert_eq!(
            previous.ensure_valid_transition_to(&decreased),
            Err(TableProtocolError::ProtocolVersionDecreased {
                previous: TABLE_PROTOCOL_VERSION,
                next: TABLE_PROTOCOL_VERSION - 1,
            })
        );
    }

    #[test]
    fn index_spec_json_roundtrips_all_domains() {
        let cases = [
            (
                sample_time_index_spec(),
                serde_json::json!({
                    "column": "ts",
                    "entity_columns": ["symbol"],
                    "kind": {
                        "type": "timestamp",
                        "index_granularity": {"Minutes": 1}
                    }
                }),
            ),
            (
                IndexSpec {
                    column: "sequence".to_string(),
                    entity_columns: Vec::new(),
                    kind: IndexKind::Int64 {
                        index_granularity: NonZeroU64::new(u64::MAX).unwrap(),
                    },
                },
                serde_json::json!({
                    "column": "sequence",
                    "entity_columns": [],
                    "kind": {
                        "type": "int64",
                        "index_granularity": u64::MAX
                    }
                }),
            ),
            (
                IndexSpec {
                    column: "offset".to_string(),
                    entity_columns: vec!["source".to_string()],
                    kind: IndexKind::UInt64 {
                        index_granularity: NonZeroU64::new(7).unwrap(),
                    },
                },
                serde_json::json!({
                    "column": "offset",
                    "entity_columns": ["source"],
                    "kind": {
                        "type": "uint64",
                        "index_granularity": 7
                    }
                }),
            ),
        ];

        for (spec, expected_json) in cases {
            let json = serde_json::to_value(&spec).unwrap();
            assert_eq!(json, expected_json);
            let restored: IndexSpec = serde_json::from_value(json).unwrap();
            assert_eq!(restored, spec);
        }
    }

    #[test]
    fn index_spec_json_rejects_impossible_field_combinations() {
        let timestamp_with_integer_granularity = r#"{
            "column":"ts","kind":{"type":"timestamp","index_granularity":1}
        }"#;
        let integer_with_time_granularity_object = r#"{
            "column":"id","kind":{"type":"int64","index_granularity":{"Seconds":1}}
        }"#;
        let zero_integer_granularity =
            r#"{"column":"id","kind":{"type":"uint64","index_granularity":0}}"#;

        assert!(serde_json::from_str::<IndexSpec>(timestamp_with_integer_granularity).is_err());
        assert!(serde_json::from_str::<IndexSpec>(integer_with_time_granularity_object).is_err());
        assert!(serde_json::from_str::<IndexSpec>(zero_integer_granularity).is_err());
    }

    #[test]
    fn index_spec_validation_rejects_invalid_structure_and_time_granularity() {
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
        spec.entity_columns = vec![spec.column.clone()];
        assert_eq!(
            spec.validate(),
            Err(IndexSpecError::EntityColumnMatchesIndex {
                column: "ts".to_string(),
            })
        );

        let mut spec = sample_time_index_spec();
        spec.kind = IndexKind::Timestamp {
            index_granularity: TimeIndexGranularity::Seconds(0),
            timezone: None,
        };
        assert_eq!(
            spec.validate(),
            Err(IndexSpecError::ZeroTimeIndexGranularity)
        );
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
            index_granularity: NonZeroU64::new(1).unwrap(),
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

    #[test]
    fn time_index_granularity_parse_accepts_basic_units() {
        let cases = [
            ("1s", TimeIndexGranularity::Seconds(1)),
            ("2m", TimeIndexGranularity::Minutes(2)),
            ("3h", TimeIndexGranularity::Hours(3)),
            ("4d", TimeIndexGranularity::Days(4)),
        ];

        for (input, expected) in cases {
            assert_eq!(input.parse::<TimeIndexGranularity>().unwrap(), expected);
        }
    }

    #[test]
    fn time_index_granularity_parse_accepts_aliases_case_and_whitespace() {
        let cases = [
            ("1sec", TimeIndexGranularity::Seconds(1)),
            ("1secs", TimeIndexGranularity::Seconds(1)),
            ("1second", TimeIndexGranularity::Seconds(1)),
            ("1seconds", TimeIndexGranularity::Seconds(1)),
            ("1min", TimeIndexGranularity::Minutes(1)),
            ("1mins", TimeIndexGranularity::Minutes(1)),
            ("1minute", TimeIndexGranularity::Minutes(1)),
            ("1minutes", TimeIndexGranularity::Minutes(1)),
            ("1hr", TimeIndexGranularity::Hours(1)),
            ("1hrs", TimeIndexGranularity::Hours(1)),
            ("1hour", TimeIndexGranularity::Hours(1)),
            ("1hours", TimeIndexGranularity::Hours(1)),
            ("1day", TimeIndexGranularity::Days(1)),
            ("1days", TimeIndexGranularity::Days(1)),
            ("1H", TimeIndexGranularity::Hours(1)),
            ("1MiN", TimeIndexGranularity::Minutes(1)),
            ("  2h", TimeIndexGranularity::Hours(2)),
            ("3d  ", TimeIndexGranularity::Days(3)),
            ("  4m  ", TimeIndexGranularity::Minutes(4)),
            ("1 h", TimeIndexGranularity::Hours(1)),
        ];

        for (input, expected) in cases {
            assert_eq!(input.parse::<TimeIndexGranularity>().unwrap(), expected);
        }
    }

    #[test]
    fn time_index_granularity_parse_rejects_empty_or_whitespace() {
        let cases = ["", "   ", "\n\t"];
        for input in cases {
            let err = input.parse::<TimeIndexGranularity>().unwrap_err();
            assert!(matches!(err, ParseTimeIndexGranularityError::Empty));
        }
    }

    #[test]
    fn time_index_granularity_parse_rejects_missing_number() {
        let cases = ["h", " hr", "day", "abcmin"];
        for input in cases {
            let err = input.parse::<TimeIndexGranularity>().unwrap_err();
            assert!(
                matches!(err, ParseTimeIndexGranularityError::MissingNumber { .. }),
                "expected MissingNumber for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn time_index_granularity_parse_rejects_missing_unit() {
        let cases = ["1", "  42  "];
        for input in cases {
            let err = input.parse::<TimeIndexGranularity>().unwrap_err();
            assert!(
                matches!(err, ParseTimeIndexGranularityError::MissingUnit { .. }),
                "expected MissingUnit for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn time_index_granularity_parse_rejects_invalid_number() {
        let cases = ["1.5h", "1_000s"];
        for input in cases {
            let err = input.parse::<TimeIndexGranularity>().unwrap_err();
            assert!(
                matches!(err, ParseTimeIndexGranularityError::InvalidNumber { .. }),
                "expected InvalidNumber for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn time_index_granularity_parse_rejects_non_positive() {
        let cases = ["0s", "0m"];
        for input in cases {
            let err = input.parse::<TimeIndexGranularity>().unwrap_err();
            assert!(
                matches!(
                    err,
                    ParseTimeIndexGranularityError::NonPositive { value: 0, .. }
                ),
                "expected NonPositive for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn time_index_granularity_parse_rejects_too_large() {
        let too_large = (u32::MAX as u64 + 1).to_string();
        let input = format!("{too_large}h");
        let err = input.parse::<TimeIndexGranularity>().unwrap_err();
        assert!(
            matches!(err, ParseTimeIndexGranularityError::TooLarge { value, .. } if value == u32::MAX as u64 + 1),
            "expected TooLarge for {input:?}, got {err:?}"
        );
    }

    #[test]
    fn time_index_granularity_parse_rejects_unknown_units() {
        let cases = ["1w", "1ms", "1mo", "10msec"];
        for input in cases {
            let err = input.parse::<TimeIndexGranularity>().unwrap_err();
            assert!(
                matches!(err, ParseTimeIndexGranularityError::UnknownUnit { .. }),
                "expected UnknownUnit for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn time_index_granularity_parse_matches_from_str() {
        let via_method = TimeIndexGranularity::parse("5m").unwrap();
        let via_trait: TimeIndexGranularity = "5m".parse().unwrap();
        assert_eq!(via_method, via_trait);
    }
}
