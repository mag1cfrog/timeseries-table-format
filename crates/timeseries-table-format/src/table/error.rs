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
use snafu::{Backtrace, prelude::*};

use crate::{
    coverage::{
        EntityIdentity, IndexIntervalId,
        index_interval::{IndexInterval, IndexIntervalMappingError},
        io::CoverageSidecarError,
    },
    formats::parquet::{EntityRewriteError, SegmentCoverageError},
    metadata::{
        logical_schema::ArrowToLogicalSchemaError,
        schema_compat::SchemaCompatibilityError,
        table_metadata::{IndexKind, IndexSpecError, IndexValueError},
    },
    storage::StorageError,
    transaction_log::{CommitError, TableKind, segments::SegmentError},
};

/// Errors owned by an append operation.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum AppendError {
    /// An Arrow input reader or batch normalization failed.
    #[snafu(display("Arrow input error: {source}"))]
    ArrowInput {
        /// Arrow error returned while reading or normalizing a batch.
        #[snafu(source)]
        source: ArrowError,
        /// Backtrace captured at the append input boundary.
        backtrace: Backtrace,
    },

    /// An Arrow schema could not be represented by the table logical schema.
    #[snafu(display("Invalid append input schema: {source}"))]
    ArrowToLogicalSchema {
        /// Arrow-to-logical schema conversion failure.
        #[snafu(source)]
        source: ArrowToLogicalSchemaError,
        /// Backtrace captured at the append schema boundary.
        backtrace: Backtrace,
    },

    /// An append input contained no rows.
    #[snafu(display("Cannot append an empty Arrow input"))]
    EmptyInput,

    /// A configured Parquet row group cannot contain zero rows.
    #[snafu(display(
        "Invalid maximum rows per Parquet row group: {max_rows_per_row_group}; expected a positive value"
    ))]
    InvalidMaxRowsPerRowGroup {
        /// Rejected per-append row-group limit.
        max_rows_per_row_group: usize,
    },

    /// Append input is incompatible with the table schema or index specification.
    #[snafu(
        context(false),
        display("Append input schema is incompatible: {source}")
    )]
    InputSchemaCompatibility {
        /// Complete schema compatibility failure.
        #[snafu(source, backtrace)]
        source: SchemaCompatibilityError,
    },

    /// A generated segment is incompatible with the table schema.
    #[snafu(display(
        "Generated segment {segment_path} is incompatible with the table schema: {source}"
    ))]
    GeneratedSegmentSchemaCompatibility {
        /// Generated table-relative segment path.
        segment_path: String,
        /// Complete schema compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)), backtrace)]
        source: Box<SchemaCompatibilityError>,
    },

    /// Table state lacks the canonical schema required by append.
    #[snafu(display(
        "Table has no logical_schema at version {version}; cannot append without a canonical schema"
    ))]
    MissingCanonicalTableSchema {
        /// Transaction log version missing a canonical logical schema.
        version: u64,
    },

    /// Reading metadata from the generated segment failed.
    #[snafu(context(false), display("Generated segment metadata error: {source}"))]
    SegmentMetadata {
        /// Complete segment metadata failure.
        #[snafu(source(from(SegmentError, Box::new)), backtrace)]
        source: Box<SegmentError>,
    },

    /// Streaming the append input into Parquet failed.
    #[snafu(display("Parquet write error: {source}"))]
    ParquetWrite {
        /// Parquet writer failure.
        #[snafu(source)]
        source: ParquetError,
        /// Backtrace captured at the append writer boundary.
        backtrace: Backtrace,
    },

    /// Deriving ordered-index coverage from the generated segment failed.
    #[snafu(context(false), display("Segment coverage error: {source}"))]
    GeneratedSegmentCoverage {
        /// Complete segment coverage derivation failure.
        #[snafu(source(from(SegmentCoverageError, Box::new)), backtrace)]
        source: Box<SegmentCoverageError>,
    },

    /// An ordered-index interval could not be reconstructed for a diagnostic.
    #[snafu(context(false), display("Index interval mapping failed: {source}"))]
    IndexIntervalMapping {
        /// Complete interval mapping failure.
        #[snafu(source)]
        source: IndexIntervalMappingError,
        /// Backtrace captured at the append boundary.
        backtrace: Backtrace,
    },

    /// A coverage sidecar could not be prepared or written.
    #[snafu(context(false), display("Coverage sidecar error: {source}"))]
    CoverageSidecar {
        /// Complete coverage sidecar failure.
        #[snafu(source(from(CoverageSidecarError, Box::new)), backtrace)]
        source: Box<CoverageSidecarError>,
    },

    /// The generated segment overlaps coverage already persisted by the table.
    #[snafu(display(
        "Ordered-index interval overlap while appending {segment_path}: {overlap_count} overlapping identity/index interval pairs (example_identity={example_identity:?}, example_index_interval={example_index_interval})"
    ))]
    PersistedIndexIntervalOverlap {
        /// Relative path of the generated segment.
        segment_path: String,
        /// Number of overlapping identity/index interval pairs.
        overlap_count: u128,
        /// First overlapping identity, or `None` for a table-wide index.
        example_identity: Option<EntityIdentity>,
        /// Internal ID of the example interval.
        example_index_interval_id: IndexIntervalId,
        /// Logical ordered-index interval represented by the example ID.
        example_index_interval: Box<IndexInterval>,
    },

    /// An entity-aware generated segment produced no entity coverage.
    #[snafu(display("No entity coverage derived while appending segment {segment_path}"))]
    EmptySegmentEntityCoverage {
        /// Relative path of the generated segment.
        segment_path: String,
    },

    /// One entity has rows but no usable ordered-index coverage.
    #[snafu(display(
        "Entity {identity:?} in segment {segment_path} has no non-null ordered-index values"
    ))]
    EntityWithoutIndexCoverage {
        /// Relative path of the generated segment.
        segment_path: String,
        /// Complete identity whose rows all have null ordered-index values.
        identity: EntityIdentity,
    },

    /// An existing segment lacks coverage metadata required by append.
    #[snafu(display(
        "Cannot append because existing segment {segment_path} is missing coverage_path"
    ))]
    ExistingSegmentMissingCoverageMetadata {
        /// Canonical segment path missing coverage metadata.
        segment_path: String,
    },

    /// A table coverage snapshot describes a different ordered index.
    #[snafu(display(
        "Table coverage index kind mismatch: expected {expected:?}, found {actual:?} (from coverage version {pointer_version})"
    ))]
    CoverageSnapshotIndexKindMismatch {
        /// Index descriptor defined by table metadata.
        expected: IndexKind,
        /// Index descriptor recorded by the snapshot pointer.
        actual: IndexKind,
        /// Log version where the mismatching pointer was recorded.
        pointer_version: u64,
    },

    /// Reading one existing segment's coverage sidecar during recovery failed.
    #[snafu(display(
        "Cannot recover append coverage: failed to read segment {segment_path} coverage at {coverage_path}: {source}"
    ))]
    ExistingSegmentCoverageSidecarRead {
        /// Canonical path of the segment whose sidecar could not be read.
        segment_path: String,
        /// Path of the failed coverage sidecar.
        coverage_path: String,
        /// Complete coverage sidecar failure.
        #[snafu(source(from(CoverageSidecarError, Box::new)), backtrace)]
        source: Box<CoverageSidecarError>,
    },

    /// Direct storage access for an append artifact failed.
    #[snafu(context(false), display("Append storage error: {source}"))]
    Storage {
        /// Complete storage failure.
        #[snafu(source, backtrace)]
        source: StorageError,
    },

    /// Publishing the append transaction failed with a definite outcome.
    #[snafu(context(false), display("Append commit error: {source}"))]
    Commit {
        /// Complete transaction-log failure.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// The append transaction may have committed, so its artifacts were preserved.
    #[snafu(display(
        "Append commit outcome is ambiguous; generated Parquet path {segment_path} was preserved: {source}"
    ))]
    CommitAmbiguous {
        /// Generated table-relative Parquet path that was preserved.
        segment_path: String,
        /// Ambiguous transaction-log failure.
        #[snafu(source(from(CommitError, Box::new)), backtrace)]
        source: Box<CommitError>,
    },

    /// Append failed and one or more attempt-owned artifacts could not be removed.
    #[snafu(display("Append failed: {source}; artifact rollback also failed: {cleanup_errors:?}"))]
    Rollback {
        /// Original append failure that triggered rollback.
        #[snafu(source, backtrace)]
        source: Box<AppendError>,
        /// Typed cleanup failure for every artifact that could not be removed.
        cleanup_errors: Vec<StorageError>,
    },
}

/// Errors from high-level time-series table operations.
///
/// Each variant carries enough context for callers to surface actionable
/// messages to users or implement retries where appropriate (for example,
/// conflicts on optimistic concurrency control).
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum TableError {
    /// An append operation failed.
    #[snafu(context(false), display("Append failed: {source}"))]
    Append {
        /// Complete append-owned failure.
        #[snafu(source, backtrace)]
        source: AppendError,
    },

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
        /// Structural or index granularity failure.
        source: IndexSpecError,
    },

    /// An ordered value could not be mapped to its index interval ID.
    #[snafu(display("Index interval mapping failed: {source}"))]
    IndexIntervalMapping {
        /// Domain, range, or index granularity failure.
        source: IndexIntervalMappingError,
    },

    /// Segment bounds cannot be ordered in one native index domain.
    #[snafu(display("Invalid segment ordered-index bounds: {source}"))]
    InvalidSegmentBounds {
        /// Domain or bound-order failure.
        source: IndexValueError,
    },

    /// Schema compatibility validation failed.
    #[snafu(display("Schema compatibility error: {source}"))]
    SchemaCompatibility {
        /// Underlying schema compatibility error.
        #[snafu(source)]
        source: SchemaCompatibilityError,
    },

    /// Table state lacks a canonical logical schema.
    #[snafu(display("Table has no logical_schema at version {version}"))]
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
        /// Underlying coverage sidecar error.
        #[snafu(source, backtrace)]
        source: CoverageSidecarError,
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
        /// Underlying coverage sidecar error (boxed to keep the variant size small).
        #[snafu(source(from(CoverageSidecarError, Box::new)), backtrace)]
        source: Box<CoverageSidecarError>,
    },

    /// Table state is missing a coverage snapshot pointer when required.
    #[snafu(display(
        "Cannot append because table has segments but no table coverage snapshot pointer in state"
    ))]
    MissingTableCoveragePointer,
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use snafu::ErrorCompat;

    use super::*;

    #[test]
    fn append_facade_preserves_arrow_source_and_backtrace() {
        let append_error = AppendError::ArrowInput {
            source: ArrowError::ComputeError("input failed".to_string()),
            backtrace: Backtrace::capture(),
        };
        let error = TableError::from(append_error);

        let append_source = error
            .source()
            .and_then(|source| source.downcast_ref::<AppendError>())
            .expect("append source");
        let arrow_source = append_source
            .source()
            .and_then(|source| source.downcast_ref::<ArrowError>())
            .expect("Arrow source");
        let append_backtrace = ErrorCompat::backtrace(append_source).expect("append backtrace");
        let table_backtrace = ErrorCompat::backtrace(&error).expect("table backtrace");

        assert!(
            matches!(arrow_source, ArrowError::ComputeError(message) if message == "input failed")
        );
        assert!(std::ptr::eq(table_backtrace, append_backtrace));
    }

    #[test]
    fn append_facade_preserves_parquet_source_and_backtrace() {
        let error = TableError::from(AppendError::ParquetWrite {
            source: ParquetError::General("write failed".to_string()),
            backtrace: Backtrace::capture(),
        });

        let append_source = error
            .source()
            .and_then(|source| source.downcast_ref::<AppendError>())
            .expect("append source");
        let parquet_source = append_source
            .source()
            .and_then(|source| source.downcast_ref::<ParquetError>())
            .expect("Parquet source");
        let append_backtrace = ErrorCompat::backtrace(append_source).expect("append backtrace");
        let table_backtrace = ErrorCompat::backtrace(&error).expect("table backtrace");

        assert!(
            matches!(parquet_source, ParquetError::General(message) if message == "write failed")
        );
        assert!(std::ptr::eq(table_backtrace, append_backtrace));
    }
}
