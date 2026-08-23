//! Error types and SNAFU context selectors for the table layer.
//!
//! This module centralizes the `TableError` enum used by the public API and
//! exposes context selectors (via `#[snafu(visibility(pub(crate)))]`) so
//! implementation details in sibling modules can attach error context without
//! re-exporting everything at the crate root. Keep new variants here to ensure
//! consistent user-facing messages and to avoid scattering selectors.

use arrow::error::ArrowError;
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

use super::{coverage::CoverageQueryError, scan::ScanError};

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

    /// Validating the incoming and registered table schemas failed.
    #[snafu(context(false), display("Schema validation failed: {source}"))]
    SchemaValidation {
        /// Complete schema compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)))]
        source: Box<SchemaCompatibilityError>,
        /// Backtrace captured at the append schema boundary.
        backtrace: Backtrace,
    },

    /// A generated segment is incompatible with the table schema.
    #[snafu(display(
        "Generated segment {segment_path} is incompatible with the table schema: {source}"
    ))]
    GeneratedSegmentSchemaCompatibility {
        /// Generated table-relative segment path.
        segment_path: String,
        /// Complete schema compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)))]
        source: Box<SchemaCompatibilityError>,
        /// Backtrace captured at the generated-segment schema boundary.
        backtrace: Backtrace,
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
        #[snafu(source(from(SegmentError, Box::new)))]
        source: Box<SegmentError>,
        /// Backtrace captured because not every segment error variant owns one.
        backtrace: Backtrace,
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
        #[snafu(source(from(SegmentCoverageError, Box::new)))]
        source: Box<SegmentCoverageError>,
        /// Backtrace captured because not every segment coverage error variant owns one.
        backtrace: Backtrace,
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
    #[snafu(context(false), display("Storage error: {source}"))]
    Storage {
        /// Complete storage failure.
        #[snafu(source, backtrace)]
        source: StorageError,
    },

    /// Publishing the append transaction failed with a definite outcome.
    #[snafu(context(false), display("Commit error: {source}"))]
    Commit {
        /// Complete transaction-log failure.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// The append transaction may have committed, so its artifacts were preserved.
    #[snafu(display(
        "Commit outcome is ambiguous; generated Parquet path {segment_path} was preserved: {source}"
    ))]
    CommitAmbiguous {
        /// Generated table-relative Parquet path that was preserved.
        segment_path: String,
        /// Ambiguous transaction-log failure.
        #[snafu(source(from(CommitError, Box::new)), backtrace)]
        source: Box<CommitError>,
    },

    /// Append failed and one or more attempt-owned artifacts could not be removed.
    #[snafu(display(
        "{source}; artifact rollback also failed: [{}]",
        cleanup_errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    ))]
    Rollback {
        /// Original append failure that triggered rollback.
        #[snafu(source, backtrace)]
        source: Box<AppendError>,
        /// Typed cleanup failure for every artifact that could not be removed.
        cleanup_errors: Vec<StorageError>,
    },
}

/// Errors owned by an entity-layout optimization operation.
#[derive(Debug, Snafu)]
#[snafu(module, visibility(pub(crate)))]
pub enum OptimizeError {
    /// Entity-layout optimization requires at least one entity column.
    #[snafu(display(
        "Entity-layout optimization is not applicable to table {table_root}: no entity columns are configured"
    ))]
    NotApplicable {
        /// User-facing table root.
        table_root: String,
    },

    /// Staging replacements for one mixed segment failed.
    #[snafu(context(false), display("Mixed-segment rewrite failed: {source}"))]
    MixedSegmentRewrite {
        /// Complete mixed-segment rewrite failure.
        #[snafu(source(from(EntityRewriteError, Box::new)), backtrace)]
        source: Box<EntityRewriteError>,
    },

    /// Live segment bounds cannot be ordered in one native index domain.
    #[snafu(
        context(false),
        display("Invalid segment ordered-index bounds: {source}")
    )]
    InvalidSegmentBounds {
        /// Complete bounds validation failure.
        #[snafu(source)]
        source: IndexValueError,
        /// Backtrace captured because index value validation does not own one.
        backtrace: Backtrace,
    },

    /// Optimization cannot use the table's canonical schema.
    #[snafu(
        context(false),
        display("Optimization schema validation failed: {source}")
    )]
    SchemaValidation {
        /// Complete schema compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)))]
        source: Box<SchemaCompatibilityError>,
        /// Backtrace captured because schema compatibility errors do not own one.
        backtrace: Backtrace,
    },

    /// A coverage sidecar required to validate a staged plan could not be read.
    #[snafu(
        context(false),
        display("Optimization coverage sidecar error: {source}")
    )]
    CoverageSidecar {
        /// Complete coverage sidecar failure.
        #[snafu(source(from(CoverageSidecarError, Box::new)), backtrace)]
        source: Box<CoverageSidecarError>,
    },

    /// A staged optimization plan violated an atomic publication invariant.
    #[snafu(display("Invalid staged entity-layout optimization plan: {reason}"))]
    InvalidStagedPlan {
        /// Failed plan invariant.
        reason: String,
        /// Backtrace captured at the failed internal invariant.
        backtrace: Backtrace,
    },

    /// An optimization count could not be represented without wrapping.
    #[snafu(display("Entity-layout optimization count overflow: {field}"))]
    CountOverflow {
        /// Report or version field that overflowed.
        field: &'static str,
        /// Backtrace captured at the failed internal arithmetic boundary.
        backtrace: Backtrace,
    },

    /// Publishing the optimization transaction failed.
    #[snafu(context(false), display("Optimization commit failed: {source}"))]
    Commit {
        /// Complete transaction-log failure.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// Optimization failed and one or more owned staged objects could not be removed.
    #[snafu(display(
        "{source}; staged-object rollback also failed: [{}]",
        cleanup_errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    ))]
    Rollback {
        /// Original optimization failure that triggered rollback.
        #[snafu(source, backtrace)]
        source: Box<OptimizeError>,
        /// Typed cleanup failure for every staged object that could not be removed.
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

    /// A table scan failed during planning or lazy execution.
    #[snafu(display("Table scan failed: {source}"))]
    Scan {
        /// Complete scan operation error.
        #[snafu(source, backtrace)]
        source: ScanError,
    },

    /// A table coverage query failed.
    #[snafu(display("Table coverage query failed: {source}"))]
    CoverageQuery {
        /// Complete coverage query operation error.
        #[snafu(source, backtrace)]
        source: CoverageQueryError,
    },

    /// Any error coming from the transaction log / commit machinery
    /// (for example, OCC conflicts, storage failures, or corrupt commits).
    #[snafu(display("Transaction log error: {source}"))]
    TransactionLog {
        /// Underlying transaction log / commit error.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// An entity-layout optimization operation failed.
    #[snafu(context(false), display("Entity-layout optimization failed: {source}"))]
    Optimize {
        /// Complete optimization-owned failure.
        #[snafu(source, backtrace)]
        source: OptimizeError,
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

    /// Coverage sidecar read/write or computation error.
    #[snafu(display("Coverage sidecar error: {source}"))]
    CoverageSidecar {
        /// Underlying coverage sidecar error.
        #[snafu(source, backtrace)]
        source: CoverageSidecarError,
    },
}

#[cfg(test)]
mod tests {
    use std::{error::Error as _, io};

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

    #[test]
    fn append_schema_wrapper_is_neutral_and_captures_a_backtrace() {
        let error = AppendError::from(SchemaCompatibilityError::MissingTableSchema);

        assert!(error.to_string().starts_with("Schema validation failed:"));
        assert!(ErrorCompat::backtrace(&error).is_some());
    }

    #[test]
    fn append_rollback_preserves_primary_chain_cleanup_errors_and_backtrace() {
        let commit_error = CommitError::Conflict {
            expected: 1,
            found: 2,
            backtrace: Backtrace::capture(),
        };
        let cleanup_error = StorageError::OtherIo {
            path: "data/segment.parquet".to_string(),
            source: io::Error::other("cleanup failed").into(),
            backtrace: Backtrace::capture(),
        };
        let error = TableError::from(AppendError::Rollback {
            source: Box::new(AppendError::Commit {
                source: commit_error,
            }),
            cleanup_errors: vec![cleanup_error],
        });

        let rollback = error
            .source()
            .and_then(|source| source.downcast_ref::<AppendError>())
            .expect("rollback source");
        let cleanup_errors = match rollback {
            AppendError::Rollback { cleanup_errors, .. } => cleanup_errors,
            other => panic!("unexpected append error: {other:?}"),
        };
        let primary = rollback
            .source()
            .and_then(|source| source.downcast_ref::<Box<AppendError>>())
            .map(Box::as_ref)
            .expect("primary append source");
        let commit = primary
            .source()
            .and_then(|source| source.downcast_ref::<CommitError>())
            .expect("commit source");
        let commit_backtrace = ErrorCompat::backtrace(commit).expect("commit backtrace");

        assert!(matches!(
            commit,
            CommitError::Conflict {
                expected: 1,
                found: 2,
                ..
            }
        ));
        assert!(matches!(
            cleanup_errors.as_slice(),
            [StorageError::OtherIo { path, .. }] if path == "data/segment.parquet"
        ));
        let message = error.to_string();
        assert!(message.contains("cleanup failed"));
        assert!(!message.contains("Backtrace"));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            commit_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(rollback).expect("rollback backtrace"),
            commit_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(primary).expect("primary backtrace"),
            commit_backtrace
        ));
    }

    #[test]
    fn optimize_facade_preserves_commit_source_and_backtrace() {
        let error = TableError::from(OptimizeError::Commit {
            source: CommitError::Conflict {
                expected: 3,
                found: 4,
                backtrace: Backtrace::capture(),
            },
        });

        let optimize = error
            .source()
            .and_then(|source| source.downcast_ref::<OptimizeError>())
            .expect("optimize source");
        let commit = optimize
            .source()
            .and_then(|source| source.downcast_ref::<CommitError>())
            .expect("commit source");
        let commit_backtrace = ErrorCompat::backtrace(commit).expect("commit backtrace");

        assert!(matches!(
            commit,
            CommitError::Conflict {
                expected: 3,
                found: 4,
                ..
            }
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(optimize).expect("optimize backtrace"),
            commit_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            commit_backtrace
        ));
    }

    #[test]
    fn optimize_facade_preserves_rewrite_storage_source_and_backtrace() {
        let error = TableError::from(OptimizeError::from(EntityRewriteError::Storage {
            source: StorageError::OtherIo {
                path: "data/mixed.parquet".to_string(),
                source: io::Error::other("read failed").into(),
                backtrace: Backtrace::capture(),
            },
        }));

        let optimize = error
            .source()
            .and_then(|source| source.downcast_ref::<OptimizeError>())
            .expect("optimize source");
        let rewrite = optimize
            .source()
            .and_then(|source| source.downcast_ref::<Box<EntityRewriteError>>())
            .map(Box::as_ref)
            .expect("rewrite source");
        let storage = rewrite
            .source()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("storage source");
        let storage_backtrace = ErrorCompat::backtrace(storage).expect("storage backtrace");

        assert!(matches!(
            storage,
            StorageError::OtherIo { path, .. } if path == "data/mixed.parquet"
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(rewrite).expect("rewrite backtrace"),
            storage_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(optimize).expect("optimize backtrace"),
            storage_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            storage_backtrace
        ));
    }

    #[test]
    fn optimize_rollback_preserves_primary_chain_and_typed_cleanup_errors() {
        let error = TableError::from(OptimizeError::Rollback {
            source: Box::new(OptimizeError::Commit {
                source: CommitError::Conflict {
                    expected: 5,
                    found: 6,
                    backtrace: Backtrace::capture(),
                },
            }),
            cleanup_errors: vec![StorageError::OtherIo {
                path: "data/_staged/segment.parquet".to_string(),
                source: io::Error::other("cleanup failed").into(),
                backtrace: Backtrace::capture(),
            }],
        });

        let rollback = error
            .source()
            .and_then(|source| source.downcast_ref::<OptimizeError>())
            .expect("optimize source");
        let cleanup_errors = match rollback {
            OptimizeError::Rollback { cleanup_errors, .. } => cleanup_errors,
            other => panic!("unexpected optimize error: {other:?}"),
        };
        let primary = rollback
            .source()
            .and_then(|source| source.downcast_ref::<Box<OptimizeError>>())
            .map(Box::as_ref)
            .expect("primary optimize source");
        let commit = primary
            .source()
            .and_then(|source| source.downcast_ref::<CommitError>())
            .expect("commit source");
        let commit_backtrace = ErrorCompat::backtrace(commit).expect("commit backtrace");

        assert!(matches!(
            cleanup_errors.as_slice(),
            [StorageError::OtherIo { path, .. }] if path == "data/_staged/segment.parquet"
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(rollback).expect("rollback backtrace"),
            commit_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            commit_backtrace
        ));
    }
}
