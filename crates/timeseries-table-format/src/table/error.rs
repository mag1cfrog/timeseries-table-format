//! Top-level error facade for table operations.
//!
//! [`TableError`] adds only operation context. Each variant wraps the complete
//! error owned by that operation and delegates its source and backtrace. Add
//! detailed failures to the owning operation error rather than copying
//! subsystem-specific variants into this facade.

use snafu::prelude::*;

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
#[non_exhaustive]
pub enum TableError {
    /// A table creation operation failed.
    #[snafu(display("Table creation failed: {source}"))]
    Create {
        /// Complete table creation failure.
        #[snafu(source, backtrace)]
        source: CreateTableError,
    },

    /// A table open operation failed.
    #[snafu(display("Table open failed: {source}"))]
    Open {
        /// Complete table open failure.
        #[snafu(source, backtrace)]
        source: OpenTableError,
    },

    /// A table state access or refresh operation failed.
    #[snafu(display("Table state access failed: {source}"))]
    StateAccess {
        /// Complete state-access failure.
        #[snafu(source, backtrace)]
        source: TableStateAccessError,
    },

    /// An append operation failed.
    #[snafu(display("Append failed: {source}"))]
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

    /// An entity-layout optimization operation failed.
    #[snafu(display("Entity-layout optimization failed: {source}"))]
    Optimize {
        /// Complete optimization-owned failure.
        #[snafu(source, backtrace)]
        source: OptimizeError,
    },
}

#[cfg(test)]
mod tests {
    use std::{error::Error as _, io};

    use arrow::error::ArrowError;
    use parquet::errors::ParquetError;
    use snafu::{Backtrace, ErrorCompat, IntoError};

    use crate::coverage::{CoverageCodecError, CoverageSidecarError};
    use crate::formats::parquet::EntityRewriteError;
    use crate::metadata::{
        index::IndexSpecError, logical_schema::LogicalToArrowSchemaError,
        schema_compat::SchemaCompatibilityError,
    };
    use crate::storage::StorageError;
    use crate::transaction_log::CommitError;

    use super::*;

    #[test]
    fn append_facade_preserves_arrow_source_and_backtrace() {
        let append_error = AppendError::ArrowInput {
            source: ArrowError::ComputeError("input failed".to_string()),
            backtrace: Backtrace::capture(),
        };
        let error = AppendSnafu.into_error(append_error);

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
        let error = AppendSnafu.into_error(AppendError::ParquetWrite {
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
    fn append_schema_validation_leaf_does_not_manufacture_a_backtrace() {
        let error = AppendError::from(SchemaCompatibilityError::MissingTableSchema);

        assert!(error.to_string().starts_with("Schema validation failed:"));
        assert!(ErrorCompat::backtrace(&error).is_none());
    }

    #[test]
    fn schema_wrappers_delegate_the_originating_backtrace() {
        let schema = SchemaCompatibilityError::RegisteredSchemaConversion {
            source: Box::new(LogicalToArrowSchemaError::Int96Unsupported {
                column: "time".to_string(),
                backtrace: Backtrace::capture(),
            }),
        };
        let error = AppendSnafu.into_error(AppendError::from(schema));

        let append = error
            .source()
            .and_then(|source| source.downcast_ref::<AppendError>())
            .expect("append source");
        let schema = append
            .source()
            .and_then(|source| source.downcast_ref::<Box<SchemaCompatibilityError>>())
            .map(Box::as_ref)
            .expect("schema compatibility source");
        let conversion = schema
            .source()
            .and_then(|source| source.downcast_ref::<Box<LogicalToArrowSchemaError>>())
            .map(Box::as_ref)
            .expect("logical-to-Arrow source");
        let originating_backtrace = ErrorCompat::backtrace(conversion).expect("source backtrace");

        assert!(std::ptr::eq(
            ErrorCompat::backtrace(schema).expect("schema backtrace"),
            originating_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(append).expect("append backtrace"),
            originating_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            originating_backtrace
        ));
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
        let error = AppendSnafu.into_error(AppendError::Rollback {
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
        let error = CreateSnafu.into_error(CreateTableError::IndexSpecValidation {
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
        let error = OpenSnafu.into_error(OpenTableError::Commit {
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
        let error = StateAccessSnafu.into_error(TableStateAccessError::Commit {
            source: CommitError::MissingTableMetadata {
                current_version: 7,
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

        assert!(matches!(commit, CommitError::MissingTableMetadata { .. }));
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
    fn scan_facade_preserves_parquet_source_and_backtrace() {
        let error = ScanSnafu.into_error(ScanError::Parquet {
            path: "data/segment.parquet".to_string(),
            operation: "reading metadata",
            source: Box::new(ParquetError::General("corrupt footer".to_string())),
            backtrace: Box::new(Backtrace::capture()),
        });

        let scan = error
            .source()
            .and_then(|source| source.downcast_ref::<ScanError>())
            .expect("scan source");
        let parquet = scan
            .source()
            .and_then(|source| source.downcast_ref::<Box<ParquetError>>())
            .map(Box::as_ref)
            .expect("Parquet source");
        let scan_backtrace = ErrorCompat::backtrace(scan).expect("scan backtrace");

        assert!(matches!(parquet, ParquetError::General(message) if message == "corrupt footer"));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            scan_backtrace
        ));
    }

    #[test]
    fn coverage_facade_preserves_codec_source_and_backtrace() {
        let error = CoverageQuerySnafu.into_error(CoverageQueryError::CoverageSnapshotRead {
            coverage_path: "coverage/table.coverage".to_string(),
            source: Box::new(CoverageSidecarError::Codec {
                source: CoverageCodecError::InvalidEntityCoverageMagic {
                    backtrace: Backtrace::capture(),
                },
            }),
        });

        let coverage_query = error
            .source()
            .and_then(|source| source.downcast_ref::<CoverageQueryError>())
            .expect("coverage query source");
        let sidecar = coverage_query
            .source()
            .and_then(|source| source.downcast_ref::<Box<CoverageSidecarError>>())
            .map(Box::as_ref)
            .expect("coverage sidecar source");
        let codec = sidecar
            .source()
            .and_then(|source| source.downcast_ref::<CoverageCodecError>())
            .expect("coverage codec source");
        let codec_backtrace = ErrorCompat::backtrace(codec).expect("codec backtrace");

        assert!(matches!(
            codec,
            CoverageCodecError::InvalidEntityCoverageMagic { .. }
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(coverage_query).expect("coverage query backtrace"),
            codec_backtrace
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            codec_backtrace
        ));
    }

    #[test]
    fn optimize_facade_preserves_commit_source_and_backtrace() {
        let error = OptimizeSnafu.into_error(OptimizeError::Commit {
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
        let error = OptimizeSnafu.into_error(OptimizeError::from(EntityRewriteError::Storage {
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
        let error = OptimizeSnafu.into_error(OptimizeError::Rollback {
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
