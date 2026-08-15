//! Error types and SNAFU context selectors for the table layer.
//!
//! This module centralizes the `TableError` enum used by the public API and
//! exposes context selectors (via `#[snafu(visibility(pub(crate)))]`) so
//! implementation details in sibling modules can attach error context without
//! re-exporting everything at the crate root. Keep new variants here to ensure
//! consistent user-facing messages and to avoid scattering selectors.

use std::collections::BTreeMap;

use arrow::{datatypes::DataType, error::ArrowError};
use chrono::{DateTime, Utc};
use parquet::errors::ParquetError;
use snafu::prelude::*;

use crate::{
    coverage::{bucket::BucketError, io::CoverageError},
    formats::parquet::{SegmentCoverageError, SegmentEntityIdentityError},
    metadata::{
        schema_compat::SchemaCompatibilityError,
        table_metadata::{IndexKind, IndexSpecError, IndexValueError, TimeBucket},
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

    /// Append failed and one or more owned coverage sidecars could not be removed.
    #[snafu(display(
        "Append failed: {source}; coverage sidecar rollback also failed: {cleanup_errors:?}"
    ))]
    AppendRollback {
        /// Original append failure that triggered rollback.
        #[snafu(source)]
        source: Box<TableError>,
        /// Cleanup failures, including each affected sidecar path.
        cleanup_errors: Vec<String>,
    },

    /// Append failed and its provisional external Parquet copy could not be removed.
    #[snafu(display(
        "Append failed: {source}; failed to remove provisional Parquet copy at {path}: {cleanup_error}"
    ))]
    ExternalParquetRollback {
        /// Table-relative path of the provisional copy that may remain.
        path: String,
        /// Original append failure that triggered rollback.
        #[snafu(source)]
        source: Box<TableError>,
        /// Storage failure encountered while removing the provisional copy.
        cleanup_error: StorageError,
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

    /// A timestamp-only operation was requested for another ordered domain.
    #[snafu(display("Operation {operation} requires a timestamp index, found {actual}"))]
    UnsupportedIndexKind {
        /// Timestamp-only operation name.
        operation: &'static str,
        /// Actual registered ordered domain.
        actual: &'static str,
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

    /// Start and end timestamps must satisfy start < end when scanning.
    #[snafu(display("Invalid scan range: start={start}, end={end} (expect start < end)"))]
    InvalidRange {
        /// Inclusive/lower timestamp bound supplied by caller.
        start: DateTime<Utc>,
        /// Exclusive/upper timestamp bound supplied by caller.
        end: DateTime<Utc>,
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

    /// Segment is missing the configured time column required for scans.
    #[snafu(display("Missing time column {column} in segment {path}"))]
    MissingTimeColumn {
        /// Normalized table-relative path of the segment being scanned.
        path: String,
        /// Name of the expected time column that was not found in the segment.
        column: String,
    },

    /// Time column exists but has an unsupported Arrow type for scanning.
    #[snafu(display("Unsupported time column {column} with type {datatype:?} in segment {path}"))]
    UnsupportedTimeType {
        /// Normalized table-relative path of the segment being scanned.
        path: String,
        /// Name of the time column with an unsupported type.
        column: String,
        /// Arrow data type encountered for the time column.
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

    /// Compatibility error for timestamp-only coverage pointers.
    #[snafu(display(
        "Table coverage bucket spec mismatch: expected {expected:?}, found {actual:?} (from coverage version {pointer_version})"
    ))]
    TableCoverageBucketMismatch {
        /// Bucket spec defined by table metadata.
        expected: TimeBucket,
        /// Bucket spec recorded in the table coverage pointer.
        actual: TimeBucket,
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
        "Coverage overlap while appending {segment_path}: {overlap_count} overlapping buckets (example={example_bucket:?})"
    ))]
    CoverageOverlap {
        /// Relative path of the segment being appended.
        segment_path: String,
        /// Number of overlapping buckets detected.
        overlap_count: u64,
        /// Example overlapping bucket (if available) to aid debugging.
        example_bucket: Option<u64>,
    },

    /// A live segment already uses the normalized path supplied for append.
    #[snafu(display("Segment path is already live: {path}"))]
    DuplicateSegmentPath {
        /// Canonical table-relative path that is already registered.
        path: String,
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

    /// Failed to read or validate the entity identity stored in a segment.
    #[snafu(display("Segment entity identity error: {source}"))]
    SegmentEntityIdentity {
        /// Underlying entity identity extraction error.
        #[snafu(source, backtrace)]
        source: SegmentEntityIdentityError,
    },

    /// Segment entity identity does not match the expected table identity.
    #[snafu(display(
        "Entity mismatch while appending {segment_path}: expected={expected:?}, found={found:?}"
    ))]
    EntityMismatch {
        /// Relative path of the segment being appended.
        segment_path: String,
        /// Expected entity identity derived from table metadata or state.
        expected: BTreeMap<String, String>,
        /// Entity identity observed in the segment.
        found: BTreeMap<String, String>,
    },
}
