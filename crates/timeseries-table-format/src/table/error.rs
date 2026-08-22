//! Error types and SNAFU context selectors for the table layer.
//!
//! This module centralizes the `TableError` enum used by the public API and
//! exposes context selectors (via `#[snafu(visibility(pub(crate)))]`) so
//! implementation details in sibling modules can attach error context without
//! re-exporting everything at the crate root. Keep new variants here to ensure
//! consistent user-facing messages and to avoid scattering selectors.

use arrow::{datatypes::DataType, error::ArrowError};
use chrono::{DateTime, Utc};
use parquet::errors::ParquetError;
use snafu::prelude::*;

use crate::{
    coverage::{
        EntityIdentity,
        bucket::{BucketError, LogicalBucketRange},
        io::CoverageError,
    },
    formats::parquet::{EntityRewriteError, SegmentCoverageError},
    metadata::{
        schema_compat::SchemaCompatibilityError,
        table_metadata::{IndexKind, IndexSpecError, IndexValueError},
    },
    storage::StorageError,
    transaction_log::{CommitError, TableKind, segments::SegmentError},
};

/// Errors from high-level time-series table operations.
///
/// Each variant carries enough context for callers to surface actionable
/// messages to users or implement retries where appropriate (for example,
/// conflicts on optimistic concurrency control).
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum TableError {
    /// Any error coming from the transaction log / commit machinery
    /// (for example, OCC conflicts, storage failures, or corrupt commits).
    #[snafu(display("Transaction log error: {source}"))]
    TransactionLog {
        /// Underlying transaction log / commit error.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// Entity-layout optimization does not apply to a table without entities.
    #[snafu(display(
        "Entity-layout optimization is not applicable to table {table_root}: no entity columns are configured"
    ))]
    OptimizeNotApplicable {
        /// User-facing table root.
        table_root: String,
    },

    /// Rewriting one mixed source into staged replacements failed.
    #[snafu(display("Entity-layout optimization rewrite failed: {source}"))]
    OptimizeRewrite {
        /// Storage-level mixed segment rewrite failure.
        #[snafu(source)]
        source: EntityRewriteError,
    },

    /// A staged optimization plan violated an atomic publication invariant.
    #[snafu(display("Invalid entity-layout optimization plan: {reason}"))]
    OptimizeInvariant {
        /// Failed plan invariant.
        reason: String,
    },

    /// An optimization count could not be represented without wrapping.
    #[snafu(display("Entity-layout optimization count overflow: {field}"))]
    OptimizeCountOverflow {
        /// Report or version field that overflowed.
        field: &'static str,
    },

    /// Optimization failed and one or more owned staged objects could not be removed.
    #[snafu(display(
        "Entity-layout optimization failed: {source}; staged-object cleanup also failed: {cleanup_errors:?}"
    ))]
    OptimizeRollback {
        /// Original optimization failure.
        #[snafu(source)]
        source: Box<TableError>,
        /// Every private path whose cleanup failed.
        cleanup_errors: Vec<String>,
    },

    /// Append failed and one or more attempt-owned artifacts could not be removed.
    #[snafu(display("Append failed: {source}; artifact rollback also failed: {cleanup_errors:?}"))]
    AppendRollback {
        /// Original append failure that triggered rollback.
        #[snafu(source)]
        source: Box<TableError>,
        /// Cleanup failures, including each affected attempt-owned path.
        cleanup_errors: Vec<String>,
    },

    /// A batch source failed while yielding rows for a streaming append.
    #[snafu(display("Arrow batch source error while appending: {source}"))]
    AppendSource {
        /// Arrow error returned by the source reader.
        source: ArrowError,
    },

    /// Parquet schema conversion or streaming output failed during append.
    #[snafu(display("Parquet write error while appending: {source}"))]
    AppendParquet {
        /// Parquet writer error.
        source: ParquetError,
    },

    /// A streaming append may have committed, so its generated data path must
    /// be preserved until the caller resolves the transaction outcome.
    #[snafu(display(
        "Append commit outcome is ambiguous; generated Parquet path {segment_path} was preserved: {source}"
    ))]
    AppendCommitAmbiguous {
        /// Generated table-relative Parquet path that was preserved.
        segment_path: String,
        /// Ambiguous transaction-log failure.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// An append source contained no rows.
    #[snafu(display("Cannot append an empty Arrow batch source"))]
    EmptyAppendSource,

    /// A configured Parquet row group cannot contain zero rows.
    #[snafu(display(
        "Invalid maximum rows per Parquet row group: {max_rows_per_row_group}; expected a positive value"
    ))]
    InvalidMaxRowsPerRowGroup {
        /// Rejected per-append row-group limit.
        max_rows_per_row_group: usize,
    },

    /// Attempting to open a table that has no commits at all (CURRENT == 0).
    #[snafu(display("Cannot open table with no commits (CURRENT version is 0)"))]
    EmptyTable,

    /// The underlying table is not a time-series table (TableKind mismatch).
    #[snafu(display("Table kind is {kind:?}, expected TableKind::TimeSeries"))]
    NotTimeSeries {
        /// The actual kind of the underlying table that was discovered.
        kind: TableKind,
    },

    /// Attempting to create a table with an unsupported metadata format version.
    #[snafu(display("Unsupported table format version: expected {expected}, found {found}"))]
    UnsupportedFormatVersion {
        /// Format version supported by this writer.
        expected: u32,
        /// Format version supplied by the caller.
        found: u32,
    },

    /// Attempt to create a table where commits already exist (idempotency guard for create).
    #[snafu(display("Table already exists; current transaction log version is {current_version}"))]
    AlreadyExists {
        /// Current transaction log version that indicates the table already exists.
        current_version: u64,
    },

    /// The ordered-index specification is structurally invalid.
    #[snafu(display("Invalid ordered index specification: {source}"))]
    IndexSpec {
        /// Structural or bucket configuration failure.
        source: IndexSpecError,
    },

    /// An ordered value could not be mapped to its coverage bucket.
    #[snafu(display("Coverage bucket mapping failed: {source}"))]
    CoverageBucket {
        /// Domain, range, or bucket configuration failure.
        source: BucketError,
    },

    /// Segment bounds cannot be ordered in one native index domain.
    #[snafu(display("Invalid segment ordered-index bounds: {source}"))]
    InvalidSegmentBounds {
        /// Domain or bound-order failure.
        source: IndexValueError,
    },

    /// Segment-level metadata / Parquet error during append (for example, missing time column, unsupported type, corrupt stats).
    #[snafu(display("Segment metadata error while appending: {source}"))]
    SegmentMeta {
        /// Underlying segment metadata error.
        #[snafu(source, backtrace)]
        source: SegmentError,
    },

    /// Schema compatibility error when appending a segment with incompatible schema (no evolution allowed in v0.1).
    #[snafu(display("Schema compatibility error: {source}"))]
    SchemaCompatibility {
        /// Underlying schema compatibility error.
        #[snafu(source)]
        source: SchemaCompatibilityError,
    },

    /// A segment's schema is incompatible with the table or index specification.
    #[snafu(display("Schema compatibility error for segment {path}: {source}"))]
    SegmentSchemaCompatibility {
        /// Table-relative segment path.
        path: String,
        /// Underlying schema compatibility error.
        #[snafu(source)]
        source: SchemaCompatibilityError,
    },

    /// Table has progressed past the initial metadata commit but still lacks
    /// a canonical logical schema (invariant violation for v0.1).
    #[snafu(display("Table has no logical_schema at version {version}; cannot append in v0.1"))]
    MissingCanonicalSchema {
        /// The transaction log version missing a canonical logical schema.
        version: u64,
    },

    /// Storage error while accessing table data (read/write failure at the storage layer).
    #[snafu(display("Storage error while accessing table data: {source}"))]
    Storage {
        /// Underlying storage error while reading or writing table data.
        source: StorageError,
    },

    /// Ordered-index range validation failed.
    #[snafu(display("Ordered-index range validation failed: {source}"))]
    InvalidRange {
        /// Domain, kind, or bound-order failure.
        source: IndexValueError,
    },

    /// An identity-free coverage query was used on an entity-aware table.
    #[snafu(display(
        "Entity identity is required for coverage queries; configured entity columns: {entity_columns:?}"
    ))]
    EntityIdentityRequired {
        /// Entity columns that require values from the caller.
        entity_columns: Vec<String>,
    },

    /// An entity-aware coverage query was used on a table with global coverage.
    #[snafu(display("Table has no configured entity columns"))]
    EntityIdentityNotConfigured,

    /// A required entity column has no caller-provided value.
    #[snafu(display("Missing entity identity component for column {column}"))]
    MissingEntityIdentityColumn {
        /// Configured entity column missing from the caller input.
        column: String,
    },

    /// Caller input repeats one entity column.
    #[snafu(display("Duplicate entity identity component for column {column}"))]
    DuplicateEntityIdentityColumn {
        /// Repeated entity column name.
        column: String,
    },

    /// Caller input contains a column that is not part of the entity identity.
    #[snafu(display("Unexpected entity identity component for column {column}"))]
    UnexpectedEntityIdentityColumn {
        /// Unknown entity column name.
        column: String,
    },

    /// Parquet read/IO error during scanning or schema extraction.
    #[snafu(display("Parquet read error for segment {path}: {source}"))]
    ParquetRead {
        /// Normalized table-relative path of the segment being scanned.
        path: String,
        /// Underlying Parquet error raised during read or schema extraction.
        source: ParquetError,
    },

    /// Arrow compute or conversion error while materializing or filtering batches.
    #[snafu(display("Arrow error while filtering column {column} in segment {path}: {source}"))]
    Arrow {
        /// Normalized table-relative path of the segment being scanned.
        path: String,
        /// Configured time column being filtered.
        column: String,
        /// Underlying Arrow error raised during batch conversion or filtering.
        source: ArrowError,
    },

    /// Segment is missing the configured ordered-index column required for scans.
    #[snafu(display("Missing ordered-index column {column} in segment {path}"))]
    MissingIndexColumn {
        /// Normalized table-relative path of the segment being scanned.
        path: String,
        /// Name of the expected ordered-index column that was not found.
        column: String,
    },

    /// Ordered-index column has an Arrow type that disagrees with the table index.
    #[snafu(display(
        "Ordered-index column {column} in segment {path} has Arrow type {datatype:?}, expected {expected}"
    ))]
    IndexColumnTypeMismatch {
        /// Normalized table-relative path of the segment being scanned.
        path: String,
        /// Name of the ordered-index column with the mismatched type.
        column: String,
        /// Registered ordered-index domain.
        expected: &'static str,
        /// Arrow data type encountered for the ordered-index column.
        datatype: DataType,
    },

    /// Converting a timestamp to the requested unit would overflow `i64`.
    #[snafu(display(
        "Timestamp conversion overflow for column {column} in segment {path} (value: {timestamp})"
    ))]
    TimeConversionOverflow {
        /// Normalized table-relative path of the segment being scanned.
        path: String,
        /// Name of the time column being converted.
        column: String,
        /// The timestamp value that could not be represented as i64 nanos.
        timestamp: DateTime<Utc>,
    },

    /// Segment Coverage error.
    #[snafu(display("Segment coverage error: {source}"))]
    SegmentCoverage {
        /// Underlying coverage error.
        #[snafu(source, backtrace)]
        source: SegmentCoverageError,
    },

    /// Table coverage pointer uses a different ordered-index descriptor.
    #[snafu(display(
        "Table coverage index kind mismatch: expected {expected:?}, found {actual:?} (from coverage version {pointer_version})"
    ))]
    TableCoverageIndexKindMismatch {
        /// Index descriptor defined by table metadata.
        expected: IndexKind,
        /// Index descriptor recorded in the table coverage pointer.
        actual: IndexKind,
        /// Log version where the mismatching coverage pointer was recorded.
        pointer_version: u64,
    },

    /// Coverage sidecar read/write or computation error.
    #[snafu(display("Coverage sidecar error: {source}"))]
    CoverageSidecar {
        /// Underlying Coverage error.
        #[snafu(source, backtrace)]
        source: CoverageError,
    },

    /// Appending would overlap existing table coverage.
    #[snafu(display(
        "Coverage overlap while appending {segment_path}: {overlap_count} overlapping buckets (example_bucket_range={example_bucket_range})"
    ))]
    CoverageOverlap {
        /// Relative path of the segment being appended.
        segment_path: String,
        /// Number of overlapping buckets detected.
        overlap_count: u64,
        /// Internal example bucket retained for programmatic compatibility.
        example_bucket: Option<u64>,
        /// Logical ordered-index range covered by the example bucket.
        example_bucket_range: LogicalBucketRange,
    },

    /// Appending would overlap entity-scoped table coverage.
    #[snafu(display(
        "Entity coverage overlap while appending {segment_path}: {overlap_count} overlapping identity/bucket pairs (example_identity={example_identity:?}, example_bucket_range={example_bucket_range})"
    ))]
    EntityCoverageOverlap {
        /// Relative path of the segment being appended.
        segment_path: String,
        /// Number of overlapping `(entity identity, bucket)` pairs.
        overlap_count: u128,
        /// First overlapping identity in canonical order.
        example_identity: EntityIdentity,
        /// Smallest overlapping bucket for `example_identity`.
        example_bucket: u64,
        /// Logical ordered-index range covered by the example bucket.
        example_bucket_range: LogicalBucketRange,
    },

    /// Entity-aware append produced no entity coverage.
    #[snafu(display("No entity coverage derived while appending segment {segment_path}"))]
    EmptySegmentEntityCoverage {
        /// Relative path of the segment being appended.
        segment_path: String,
    },

    /// One entity has rows but no usable ordered-index coverage.
    #[snafu(display(
        "Entity {identity:?} in segment {segment_path} has no non-null ordered-index values"
    ))]
    EntityWithoutIndexCoverage {
        /// Relative path of the segment being appended.
        segment_path: String,
        /// Complete identity whose rows all have null ordered-index values.
        identity: EntityIdentity,
    },

    /// Existing segment lacks a coverage_path when coverage is required.
    #[snafu(display(
        "Cannot append because existing segment {path} is missing coverage_path (required for coverage tracking)"
    ))]
    ExistingSegmentMissingCoverage {
        /// Canonical segment path missing a coverage_path entry.
        path: String,
    },

    /// Reading the per-segment coverage sidecar failed while rebuilding coverage.
    #[snafu(display(
        "Cannot recover table coverage: failed to read segment coverage sidecar for {path} at {coverage_path}: {source}"
    ))]
    SegmentCoverageSidecarRead {
        /// Canonical path of the segment whose coverage sidecar could not be read.
        path: String,
        /// Path to the coverage sidecar file that failed to read.
        coverage_path: String,
        /// Underlying coverage error (boxed to keep the variant size small).
        #[snafu(source(from(CoverageError, Box::new)), backtrace)]
        source: Box<CoverageError>,
    },

    /// Table state is missing a coverage snapshot pointer when required.
    #[snafu(display(
        "Cannot append because table has segments but no table coverage snapshot pointer in state"
    ))]
    MissingTableCoveragePointer,
}
