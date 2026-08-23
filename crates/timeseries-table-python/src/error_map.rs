use pyo3::{
    Bound, PyErr, PyResult, Python,
    types::{PyAny, PyAnyMethods, PyDict, PyDictMethods},
};

use crate::exceptions::{
    ConflictError, DataFusionError, DuplicateIndexIntervalError, IndexIntervalOverlapError,
    SchemaMismatchError, StorageError, TimeseriesTableError,
};
use timeseries_table_format::{
    coverage::{
        EntityIdentity, EntityValue, index_interval::IndexInterval, io::CoverageSidecarError,
    },
    formats::parquet::{EntityRewriteError, SegmentCoverageError},
    storage::StorageError as CoreStorageError,
    table::{AppendError, CoverageQueryError, OptimizeError, ScanError, TableError},
    transaction_log::{CommitError, segments::SegmentError},
};

#[allow(dead_code)]
pub(crate) fn datafusion_error_to_py(
    _py: Python<'_>,
    err: datafusion::error::DataFusionError,
) -> PyErr {
    DataFusionError::new_err(err.to_string())
}

#[allow(dead_code)]
pub(crate) fn storage_error_to_py(py: Python<'_>, err: CoreStorageError) -> PyErr {
    let msg = err.to_string();

    let path_attr: Option<String> = match &err {
        CoreStorageError::NotFound { path, .. } => Some(path.clone()),
        CoreStorageError::AlreadyExists { path, .. } => Some(path.clone()),
        CoreStorageError::OtherIo { path, .. } => Some(path.clone()),
        CoreStorageError::CleanupFailed { path, .. } => Some(path.clone()),
    };

    let py_err = StorageError::new_err(msg);
    let exc = py_err.value(py);

    if let Some(path) = path_attr
        && let Err(e) = exc.setattr("path", path)
    {
        return e;
    }

    py_err
}

#[allow(dead_code)]
fn commit_error_to_py(py: Python<'_>, err: CommitError) -> PyErr {
    let msg = err.to_string();

    match err {
        CommitError::Conflict {
            expected, found, ..
        } => {
            let py_err = ConflictError::new_err(msg);
            let exc = py_err.value(py);

            if let Err(e) = exc.setattr("expected", expected) {
                return e;
            }
            if let Err(e) = exc.setattr("found", found) {
                return e;
            }

            py_err
        }

        CommitError::Storage { source } => storage_error_to_py(py, source),

        CommitError::AmbiguousOutcome { .. }
        | CommitError::UnsupportedFormatVersion { .. }
        | CommitError::CorruptState { .. } => TimeseriesTableError::new_err(msg),
    }
}

fn set_index_interval_error_attributes(
    py: Python<'_>,
    exc: &Bound<'_, PyAny>,
    segment_path: String,
    example_index_interval: IndexInterval,
    entity_columns: &[String],
    example_identity: Option<&EntityIdentity>,
) -> PyResult<()> {
    exc.setattr("segment_path", segment_path)?;
    exc.setattr("example_index_interval", example_index_interval.to_string())?;

    match example_identity {
        Some(example_identity) => {
            let components = example_identity.components();
            if entity_columns.len() != components.len() {
                return Err(TimeseriesTableError::new_err(
                    "example identity does not match configured entity columns",
                ));
            }
            let identity = PyDict::new(py);
            for (column, component) in entity_columns.iter().zip(components) {
                match component {
                    EntityValue::Utf8(value) => identity.set_item(column, value)?,
                    EntityValue::Int32(value) => identity.set_item(column, value)?,
                    EntityValue::Int64(value) => identity.set_item(column, value)?,
                    EntityValue::UInt64(value) => identity.set_item(column, value)?,
                }
            }
            exc.setattr("example_identity", identity)?;
        }
        None => exc.setattr("example_identity", py.None())?,
    }

    Ok(())
}

fn index_interval_overlap_error_to_py(
    py: Python<'_>,
    msg: String,
    segment_path: String,
    conflict_count: u128,
    example_index_interval: IndexInterval,
    entity_columns: &[String],
    example_identity: Option<&EntityIdentity>,
) -> PyErr {
    let py_err = IndexIntervalOverlapError::new_err(msg);
    let exc = py_err.value(py);

    if let Err(error) = set_index_interval_error_attributes(
        py,
        exc,
        segment_path,
        example_index_interval,
        entity_columns,
        example_identity,
    ) {
        return error;
    }
    if let Err(error) = exc.setattr("conflict_count", conflict_count) {
        return error;
    }

    py_err
}

fn duplicate_index_interval_error_to_py(
    py: Python<'_>,
    msg: String,
    segment_path: String,
    example_index_interval: IndexInterval,
    entity_columns: &[String],
    example_identity: Option<&EntityIdentity>,
) -> PyErr {
    let py_err = DuplicateIndexIntervalError::new_err(msg);
    let exc = py_err.value(py);

    if let Err(error) = set_index_interval_error_attributes(
        py,
        exc,
        segment_path,
        example_index_interval,
        entity_columns,
        example_identity,
    ) {
        return error;
    }

    py_err
}

fn append_error_to_py(
    py: Python<'_>,
    err: AppendError,
    entity_columns: &[String],
    msg: String,
) -> PyErr {
    match err {
        AppendError::Rollback { source, .. } => {
            append_error_to_py(py, *source, entity_columns, msg)
        }
        AppendError::Storage { source } => storage_error_to_py(py, source),
        AppendError::Commit { source } => commit_error_to_py(py, source),
        AppendError::CommitAmbiguous { source, .. } => commit_error_to_py(py, *source),
        AppendError::GeneratedSegmentCoverage { source, .. } => match *source {
            SegmentCoverageError::DuplicateIndexInterval {
                path,
                example_identity,
                example_index_interval,
            } => duplicate_index_interval_error_to_py(
                py,
                msg,
                path,
                example_index_interval,
                entity_columns,
                example_identity.as_ref(),
            ),
            _ => TimeseriesTableError::new_err(msg),
        },
        AppendError::PersistedIndexIntervalOverlap {
            segment_path,
            overlap_count,
            example_identity,
            example_index_interval_id: _,
            example_index_interval,
        } => index_interval_overlap_error_to_py(
            py,
            msg,
            segment_path,
            overlap_count,
            *example_index_interval,
            entity_columns,
            example_identity.as_ref(),
        ),
        AppendError::SchemaValidation { .. }
        | AppendError::GeneratedSegmentSchemaCompatibility { .. } => {
            SchemaMismatchError::new_err(msg)
        }
        _ => TimeseriesTableError::new_err(msg),
    }
}

fn coverage_sidecar_error_to_py(py: Python<'_>, err: CoverageSidecarError, msg: String) -> PyErr {
    match err {
        CoverageSidecarError::Storage { source, .. } => storage_error_to_py(py, source),
        CoverageSidecarError::EntityIdentitySchema { .. } => SchemaMismatchError::new_err(msg),
        _ => TimeseriesTableError::new_err(msg),
    }
}

fn entity_rewrite_error_to_py(py: Python<'_>, err: EntityRewriteError, msg: String) -> PyErr {
    match err {
        EntityRewriteError::Cleanup { source, .. } => entity_rewrite_error_to_py(py, *source, msg),
        EntityRewriteError::SegmentInspection {
            source: SegmentError::MissingFile { source, .. } | SegmentError::Storage { source, .. },
        }
        | EntityRewriteError::CoverageInspection {
            source: SegmentCoverageError::Storage { source, .. },
        }
        | EntityRewriteError::Storage { source } => storage_error_to_py(py, source),
        EntityRewriteError::CoverageSidecar { source } => {
            coverage_sidecar_error_to_py(py, source, msg)
        }
        _ => TimeseriesTableError::new_err(msg),
    }
}

fn optimize_error_to_py(py: Python<'_>, err: OptimizeError, msg: String) -> PyErr {
    match err {
        OptimizeError::Rollback { source, .. } => optimize_error_to_py(py, *source, msg),
        OptimizeError::MixedSegmentRewrite { source } => {
            entity_rewrite_error_to_py(py, *source, msg)
        }
        OptimizeError::SchemaValidation { .. } => SchemaMismatchError::new_err(msg),
        OptimizeError::CoverageSidecar { source } => coverage_sidecar_error_to_py(py, *source, msg),
        OptimizeError::Commit { source } => commit_error_to_py(py, source),
        _ => TimeseriesTableError::new_err(msg),
    }
}

#[allow(dead_code)]
pub(crate) fn table_error_to_py(
    py: Python<'_>,
    err: TableError,
    entity_columns: &[String],
) -> PyErr {
    let msg = err.to_string();

    match err {
        TableError::Storage { source } => storage_error_to_py(py, source),

        TableError::Scan {
            source: ScanError::Storage { source, .. },
        } => storage_error_to_py(py, *source),

        TableError::CoverageQuery {
            source:
                CoverageQueryError::CoverageSnapshotRead { source, .. }
                | CoverageQueryError::SegmentCoverageSidecarRead { source, .. },
        } => coverage_sidecar_error_to_py(py, *source, msg),

        TableError::CoverageQuery {
            source: CoverageQueryError::SchemaCompatibility { .. },
        } => SchemaMismatchError::new_err(msg),

        TableError::TransactionLog { source } => commit_error_to_py(py, source),

        TableError::Append { source } => append_error_to_py(py, source, entity_columns, msg),

        TableError::Optimize { source } => optimize_error_to_py(py, source, msg),

        TableError::SchemaCompatibility { .. } => SchemaMismatchError::new_err(err.to_string()),

        _ => TimeseriesTableError::new_err(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Once};

    use super::*;
    use pyo3::types::PyAnyMethods;
    use timeseries_table_format::{
        coverage::index_interval::{index_interval_for_id, index_interval_id_for_value},
        storage::StorageLocation,
        transaction_log::{IndexKind, IndexValue},
    };

    fn init_python() {
        static PYTHON: Once = Once::new();
        PYTHON.call_once(Python::initialize);
    }

    fn invalid_location_error() -> CoreStorageError {
        StorageLocation::parse("").expect_err("empty storage location must fail")
    }

    #[test]
    fn scan_and_coverage_storage_errors_map_to_storage_error() {
        init_python();

        let attached = Python::try_attach(|py| {
            let errors = [
                TableError::Scan {
                    source: ScanError::Storage {
                        path: "data/missing.parquet".to_string(),
                        source: Box::new(invalid_location_error()),
                    },
                },
                TableError::CoverageQuery {
                    source: CoverageQueryError::CoverageSnapshotRead {
                        coverage_path: "_coverage/table/missing.roar".to_string(),
                        source: Box::new(CoverageSidecarError::Storage {
                            source: invalid_location_error(),
                        }),
                    },
                },
                TableError::CoverageQuery {
                    source: CoverageQueryError::SegmentCoverageSidecarRead {
                        segment_path: "data/segment.parquet".to_string(),
                        coverage_path: "_coverage/segments/missing.roar".to_string(),
                        source: Box::new(CoverageSidecarError::Storage {
                            source: invalid_location_error(),
                        }),
                    },
                },
            ];

            for error in errors {
                let python_error = table_error_to_py(py, error, &[]);
                assert!(python_error.is_instance_of::<StorageError>(py));
                let path: String = python_error
                    .value(py)
                    .getattr("path")
                    .expect("storage error path")
                    .extract()
                    .expect("string path");
                assert_eq!(path, "<empty table location>");
            }
        });
        assert!(attached.is_some());
    }

    #[test]
    fn rollback_preserves_the_primary_python_exception_category() {
        init_python();
        let kind = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(10).expect("nonzero test index granularity"),
        };
        let example_index_interval_id = index_interval_id_for_value(&kind, &IndexValue::Int64(0))
            .expect("valid test index interval ID");
        let example_index_interval = index_interval_for_id(&kind, example_index_interval_id)
            .expect("valid test index interval");
        let cleanup_error =
            StorageLocation::parse("").expect_err("empty storage location must fail");
        let error = TableError::from(AppendError::Rollback {
            source: Box::new(AppendError::PersistedIndexIntervalOverlap {
                segment_path: "data/test.parquet".to_string(),
                overlap_count: 1,
                example_identity: None,
                example_index_interval_id,
                example_index_interval: Box::new(example_index_interval),
            }),
            cleanup_errors: vec![cleanup_error],
        });

        Python::attach(|py| {
            let error = table_error_to_py(py, error, &[]);
            assert!(error.is_instance_of::<IndexIntervalOverlapError>(py));
            assert_eq!(
                error
                    .value(py)
                    .getattr("conflict_count")
                    .expect("conflict_count")
                    .extract::<u128>()
                    .expect("integer conflict_count"),
                1
            );
        });
    }

    #[test]
    fn optimize_commit_and_rollback_preserve_storage_python_categories() {
        init_python();
        let commit = TableError::from(OptimizeError::Commit {
            source: CommitError::Storage {
                source: invalid_location_error(),
            },
        });
        let rollback = TableError::from(OptimizeError::Rollback {
            source: Box::new(OptimizeError::MixedSegmentRewrite {
                source: Box::new(EntityRewriteError::Storage {
                    source: invalid_location_error(),
                }),
            }),
            cleanup_errors: vec![invalid_location_error()],
        });

        Python::attach(|py| {
            let commit = table_error_to_py(py, commit, &[]);
            assert!(commit.is_instance_of::<StorageError>(py));

            let rollback = table_error_to_py(py, rollback, &[]);
            assert!(rollback.is_instance_of::<StorageError>(py));
        });
    }
}
