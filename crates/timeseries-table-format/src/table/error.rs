//! Public error facade for high-level table operations.

use snafu::prelude::*;

use crate::{
    coverage::io::CoverageSidecarError,
    metadata::{
        schema_compat::SchemaCompatibilityError,
        table_metadata::{IndexSpecError, IndexValueError},
    },
    storage::StorageError,
    transaction_log::{CommitError, TableKind},
};

use super::operations::{
    AppendError, CoverageQueryError, CreateTableError, OpenTableError, OptimizeError, ScanError,
    TableStateAccessError,
};

/// Errors from high-level time-series table operations.
///
/// Each variant carries enough context for callers to surface actionable
/// messages to users or implement retries where appropriate (for example,
/// conflicts on optimistic concurrency control).
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum TableError {
    /// A table creation operation failed.
    #[snafu(context(false), display("Table creation failed: {source}"))]
    Create {
        /// Complete table creation failure.
        #[snafu(source, backtrace)]
        source: CreateTableError,
    },

    /// A table open operation failed.
    #[snafu(context(false), display("Table open failed: {source}"))]
    Open {
        /// Complete table open failure.
        #[snafu(source, backtrace)]
        source: OpenTableError,
    },

    /// A table state access or refresh operation failed.
    #[snafu(context(false), display("Table state access failed: {source}"))]
    StateAccess {
        /// Complete state-access failure.
        #[snafu(source, backtrace)]
        source: TableStateAccessError,
    },

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

    use arrow::error::ArrowError;
    use parquet::errors::ParquetError;
    use snafu::{Backtrace, ErrorCompat};

    use crate::formats::parquet::EntityRewriteError;

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
    fn create_facade_preserves_index_source_and_operation_backtrace() {
        let error = TableError::from(CreateTableError::IndexSpecValidation {
            source: IndexSpecError::EmptyColumn,
            backtrace: Backtrace::capture(),
        });

        let create = error
            .source()
            .and_then(|source| source.downcast_ref::<CreateTableError>())
            .expect("create source");
        let index = create
            .source()
            .and_then(|source| source.downcast_ref::<IndexSpecError>())
            .expect("index specification source");

        assert!(matches!(index, IndexSpecError::EmptyColumn));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            ErrorCompat::backtrace(create).expect("create backtrace")
        ));
    }

    #[test]
    fn open_facade_preserves_commit_source_and_backtrace() {
        let error = TableError::from(OpenTableError::Commit {
            source: CommitError::Conflict {
                expected: 8,
                found: 9,
                backtrace: Backtrace::capture(),
            },
        });

        let open = error
            .source()
            .and_then(|source| source.downcast_ref::<OpenTableError>())
            .expect("open source");
        let commit = open
            .source()
            .and_then(|source| source.downcast_ref::<CommitError>())
            .expect("commit source");
        let commit_backtrace = ErrorCompat::backtrace(commit).expect("commit backtrace");

        assert!(matches!(commit, CommitError::Conflict { .. }));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(open).expect("open backtrace"),
            commit_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            commit_backtrace
        ));
    }

    #[test]
    fn state_access_facade_preserves_commit_source_and_backtrace() {
        let error = TableError::from(TableStateAccessError::Commit {
            source: CommitError::CorruptState {
                msg: "corrupt state".to_string(),
                backtrace: Backtrace::capture(),
            },
        });

        let state_access = error
            .source()
            .and_then(|source| source.downcast_ref::<TableStateAccessError>())
            .expect("state access source");
        let commit = state_access
            .source()
            .and_then(|source| source.downcast_ref::<CommitError>())
            .expect("commit source");
        let commit_backtrace = ErrorCompat::backtrace(commit).expect("commit backtrace");

        assert!(matches!(commit, CommitError::CorruptState { .. }));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(state_access).expect("state access backtrace"),
            commit_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
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
