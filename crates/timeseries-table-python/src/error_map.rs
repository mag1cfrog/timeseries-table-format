use std::error::Error;

use pyo3::{
    Bound, PyErr, PyResult, Python,
    types::{PyAny, PyAnyMethods, PyDict, PyDictMethods},
};

use crate::exceptions::{
    ConflictError, DataFusionError, DuplicateIndexIntervalError, IndexIntervalOverlapError,
    SchemaMismatchError, StorageError, TimeseriesTableError,
};
use timeseries_table_format::{
    coverage::{EntityIdentity, EntityValue, SegmentCoverageError, index_interval::IndexInterval},
    metadata::schema_compat::SchemaCompatibilityError,
    storage::StorageError as CoreStorageError,
    table::{AppendError, TableError},
    transaction_log::CommitError,
};

#[allow(dead_code)]
pub(crate) fn datafusion_error_to_py(
    _py: Python<'_>,
    err: datafusion::error::DataFusionError,
) -> PyErr {
    DataFusionError::new_err(err.to_string())
}

fn new_storage_py_error(py: Python<'_>, err: &CoreStorageError) -> PyErr {
    let msg = err.to_string();

    let path_attr = match err {
        CoreStorageError::NotFound { path, .. }
        | CoreStorageError::AlreadyExists { path, .. }
        | CoreStorageError::OtherIo { path, .. }
        | CoreStorageError::CleanupFailed { path, .. } => Some(path.as_str()),
        _ => None,
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

fn new_conflict_py_error(py: Python<'_>, msg: String, expected: u64, found: u64) -> PyErr {
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

fn set_index_interval_error_attributes(
    py: Python<'_>,
    exc: &Bound<'_, PyAny>,
    segment_path: &str,
    example_index_interval: &IndexInterval,
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

fn new_index_interval_overlap_py_error(
    py: Python<'_>,
    msg: String,
    segment_path: &str,
    conflict_count: u128,
    example_index_interval: &IndexInterval,
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

fn new_duplicate_index_interval_py_error(
    py: Python<'_>,
    msg: String,
    segment_path: &str,
    example_index_interval: &IndexInterval,
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

fn find_error_in_source_chain<'a, E>(
    root: &'a (dyn Error + 'static),
    mut predicate: impl FnMut(&E) -> bool,
) -> Option<&'a E>
where
    E: Error + 'static,
{
    let mut current = Some(root);

    while let Some(error) = current {
        let candidate = error
            .downcast_ref::<E>()
            .or_else(|| error.downcast_ref::<Box<E>>().map(|boxed| boxed.as_ref()));
        if let Some(candidate) = candidate
            && predicate(candidate)
        {
            return Some(candidate);
        }
        current = error.source();
    }

    None
}

#[allow(dead_code)]
pub(crate) fn table_error_to_py(
    py: Python<'_>,
    err: TableError,
    entity_columns: &[String],
) -> PyErr {
    let msg = err.to_string();
    let root = &err as &(dyn Error + 'static);

    if let Some(
        conflict @ CommitError::Conflict {
            expected, found, ..
        },
    ) = find_error_in_source_chain::<CommitError>(root, |error| {
        matches!(error, CommitError::Conflict { .. })
    }) {
        return new_conflict_py_error(py, conflict.to_string(), *expected, *found);
    }

    if let Some(AppendError::PersistedIndexIntervalOverlap {
        segment_path,
        overlap_count,
        example_identity,
        example_index_interval,
        ..
    }) = find_error_in_source_chain::<AppendError>(root, |error| {
        matches!(error, AppendError::PersistedIndexIntervalOverlap { .. })
    }) {
        return new_index_interval_overlap_py_error(
            py,
            msg,
            segment_path,
            *overlap_count,
            example_index_interval,
            entity_columns,
            example_identity.as_ref(),
        );
    }

    if let Some(SegmentCoverageError::DuplicateIndexInterval {
        path,
        example_identity,
        example_index_interval,
    }) = find_error_in_source_chain::<SegmentCoverageError>(root, |error| {
        matches!(error, SegmentCoverageError::DuplicateIndexInterval { .. })
    }) {
        return new_duplicate_index_interval_py_error(
            py,
            msg,
            path,
            example_index_interval,
            entity_columns,
            example_identity.as_ref(),
        );
    }

    if find_error_in_source_chain::<SchemaCompatibilityError>(root, |_| true).is_some() {
        return SchemaMismatchError::new_err(msg);
    }

    if let Some(storage) = find_error_in_source_chain::<CoreStorageError>(root, |_| true) {
        return new_storage_py_error(py, storage);
    }

    TimeseriesTableError::new_err(msg)
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Once};

    use super::*;
    use pyo3::types::PyAnyMethods;
    use timeseries_table_format::{
        coverage::{
            CoverageSidecarError,
            index_interval::{index_interval_for_id, index_interval_id_for_value},
        },
        storage::StorageLocation,
        table::{
            CoverageQueryError, CreateTableError, EntityRewriteError, OpenTableError,
            OptimizeError, ScanError, TableStateAccessError,
        },
        transaction_log::{IndexKind, IndexValue, SegmentError, TableProtocolError},
    };

    fn init_python() {
        static PYTHON: Once = Once::new();
        PYTHON.call_once(Python::initialize);
    }

    fn invalid_location_error() -> CoreStorageError {
        StorageLocation::parse("").expect_err("empty storage location must fail")
    }

    #[test]
    fn lifecycle_errors_preserve_python_exception_categories() {
        init_python();

        let attached = Python::try_attach(|py| {
            let storage = TableError::from(OpenTableError::Storage {
                source: invalid_location_error(),
            });
            assert!(table_error_to_py(py, storage, &[]).is_instance_of::<StorageError>(py));

            let state_storage = TableError::from(TableStateAccessError::Commit {
                source: CommitError::Storage {
                    source: invalid_location_error(),
                },
            });
            assert!(table_error_to_py(py, state_storage, &[]).is_instance_of::<StorageError>(py));

            let schema = TableError::from(CreateTableError::from(
                timeseries_table_format::metadata::schema_compat::SchemaCompatibilityError::MissingTableSchema,
            ));
            assert!(table_error_to_py(py, schema, &[]).is_instance_of::<SchemaMismatchError>(py));
        });
        assert!(attached.is_some());
    }

    #[test]
    fn unclassified_commit_errors_use_the_generic_python_exception() {
        init_python();

        Python::attach(|py| {
            let error = TableError::from(TableStateAccessError::Commit {
                source: CommitError::from(TableProtocolError::UnsupportedVersion {
                    expected: 1,
                    found: 2,
                }),
            });
            let python_error = table_error_to_py(py, error, &[]);

            assert!(
                python_error
                    .get_type(py)
                    .is(py.get_type::<TimeseriesTableError>())
            );
        });
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
    fn append_nested_storage_errors_map_to_storage_error() {
        init_python();

        let errors = [
            TableError::from(AppendError::SegmentMetadata {
                source: Box::new(SegmentError::from(invalid_location_error())),
            }),
            TableError::from(AppendError::GeneratedSegmentCoverage {
                source: Box::new(SegmentCoverageError::Storage {
                    path: "data/segment.parquet".to_string(),
                    source: invalid_location_error(),
                }),
            }),
            TableError::from(AppendError::CoverageSidecar {
                source: Box::new(CoverageSidecarError::Storage {
                    source: invalid_location_error(),
                }),
            }),
            TableError::from(AppendError::ExistingSegmentCoverageSidecarRead {
                segment_path: "data/segment.parquet".to_string(),
                coverage_path: "_coverage/segments/missing.roar".to_string(),
                source: Box::new(CoverageSidecarError::Storage {
                    source: invalid_location_error(),
                }),
            }),
        ];

        Python::attach(|py| {
            for error in errors {
                let python_error = table_error_to_py(py, error, &[]);
                assert!(python_error.is_instance_of::<StorageError>(py));
            }
        });
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
