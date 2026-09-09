use std::error::Error;

use pyo3::{
    Bound, PyErr, PyResult, Python,
    types::{PyAny, PyAnyMethods, PyDict, PyDictMethods, PyModule},
};

use crate::exceptions::{
    ConflictError, DataFusionError, DuplicateIndexIntervalError, IndexIntervalOverlapError,
    SchemaMismatchError, StorageError, TimeseriesTableError,
};
use timeseries_table_format::{
    SchemaEvolutionError, UpdateKey, UpdateKeyValue, UpdateKeyViolation, UpdatePreparationError,
    UpdateRewriteError, UpdateRowsError,
    coverage::{EntityIdentity, EntityValue, SegmentCoverageError, index_interval::IndexInterval},
    metadata::{logical_schema::LogicalTimestampUnit, schema_compat::SchemaCompatibilityError},
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

pub(crate) fn storage_error_to_py(py: Python<'_>, err: &CoreStorageError) -> PyErr {
    let msg = err.to_string();

    let path_attr = match err {
        CoreStorageError::InvalidRelativePath { path, .. }
        | CoreStorageError::NotFound { path, .. }
        | CoreStorageError::AlreadyExists { path, .. }
        | CoreStorageError::OtherIo { path, .. }
        | CoreStorageError::CleanupFailed { path, .. } => Some(path.as_str()),
        _ => None,
    };

    new_storage_py_error(py, msg, path_attr)
}

fn new_storage_py_error(py: Python<'_>, msg: String, path_attr: Option<&str>) -> PyErr {
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

fn update_key_to_python<'py>(py: Python<'py>, key: &UpdateKey) -> PyResult<Bound<'py, PyDict>> {
    let result = PyDict::new(py);
    for (name, value) in key.components() {
        match value {
            None => result.set_item(name, py.None())?,
            Some(UpdateKeyValue::Utf8(value)) => result.set_item(name, value)?,
            Some(UpdateKeyValue::Int32(value)) => result.set_item(name, value)?,
            Some(UpdateKeyValue::Int64(value)) => result.set_item(name, value)?,
            Some(UpdateKeyValue::UInt64(value)) => result.set_item(name, value)?,
            Some(UpdateKeyValue::Timestamp {
                ticks,
                unit,
                timezone,
            }) => {
                let unit = match unit {
                    LogicalTimestampUnit::Millis => "ms",
                    LogicalTimestampUnit::Micros => "us",
                    LogicalTimestampUnit::Nanos => "ns",
                };
                let arrow = PyModule::import(py, "pyarrow")?;
                let dtype = arrow
                    .getattr("timestamp")?
                    .call1((unit, timezone.as_deref()))?;
                let scalar = arrow.getattr("scalar")?.call1((*ticks, dtype))?;
                result.set_item(name, scalar)?;
            }
        }
    }
    Ok(result)
}

#[allow(dead_code)]
pub(crate) fn table_error_to_py(
    py: Python<'_>,
    err: TableError,
    entity_columns: &[String],
) -> PyErr {
    let msg = err.to_string();
    let root = &err as &(dyn Error + 'static);

    // An ambiguous commit contains storage causes, but must retain its own
    // diagnostic rather than being presented as an ordinary storage failure.
    if find_error_in_source_chain::<CommitError>(root, |error| {
        matches!(error, CommitError::AmbiguousOutcome { .. })
    })
    .is_some()
    {
        return TimeseriesTableError::new_err(msg);
    }

    if let Some(UpdateRowsError::SnapshotMismatch { expected, found }) =
        find_error_in_source_chain::<UpdateRowsError>(root, |error| {
            matches!(error, UpdateRowsError::SnapshotMismatch { .. })
        })
    {
        return new_conflict_py_error(py, msg, *expected, *found);
    }

    if let Some(UpdatePreparationError::Key {
        kind,
        input_rows_seen,
        observed_violations,
        example_key,
    }) = find_error_in_source_chain::<UpdatePreparationError>(root, |error| {
        matches!(error, UpdatePreparationError::Key { .. })
    }) {
        let reason = match kind {
            UpdateKeyViolation::DuplicateSource => "duplicate_source_key",
            UpdateKeyViolation::UnmatchedSource => "unmatched_source_key",
            UpdateKeyViolation::AmbiguousTarget => "ambiguous_target_key",
            UpdateKeyViolation::NullIdentity => "null_identity",
            _ => return TimeseriesTableError::new_err(msg),
        };
        let error = TimeseriesTableError::new_err(msg);
        let attributes = || -> PyResult<()> {
            let exc = error.value(py);
            exc.setattr("reason", reason)?;
            exc.setattr("input_rows_seen", *input_rows_seen)?;
            exc.setattr("observed_violations", *observed_violations)?;
            exc.setattr("example_key", update_key_to_python(py, example_key)?)?;
            Ok(())
        };
        return attributes().err().unwrap_or(error);
    }

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

    if find_error_in_source_chain::<SchemaCompatibilityError>(root, |_| true).is_some()
        || find_error_in_source_chain::<SchemaEvolutionError>(root, |_| true).is_some()
        || find_error_in_source_chain::<UpdatePreparationError>(root, |error| {
            matches!(error, UpdatePreparationError::InvalidInput { .. })
        })
        .is_some()
    {
        return SchemaMismatchError::new_err(msg);
    }

    if let Some(storage) = find_error_in_source_chain::<CoreStorageError>(root, |_| true) {
        return storage_error_to_py(py, storage);
    }

    if let Some(
        UpdatePreparationError::Io { path, .. } | UpdatePreparationError::Cleanup { path, .. },
    ) = find_error_in_source_chain::<UpdatePreparationError>(root, |error| {
        matches!(
            error,
            UpdatePreparationError::Io { .. } | UpdatePreparationError::Cleanup { .. }
        )
    }) {
        return new_storage_py_error(py, msg, Some(&path.to_string_lossy()));
    }
    if let Some(UpdateRewriteError::Io { path, .. }) =
        find_error_in_source_chain::<UpdateRewriteError>(root, |error| {
            matches!(error, UpdateRewriteError::Io { .. })
        })
    {
        return new_storage_py_error(py, msg, Some(&path.to_string_lossy()));
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
    fn update_publication_errors_keep_their_categories_and_attributes() {
        init_python();
        Python::attach(|py| {
            let map = |source| table_error_to_py(py, TableError::UpdateRows { source }, &[]);
            let mismatch = map(UpdateRowsError::SnapshotMismatch {
                expected: u64::MAX,
                found: 9,
            });
            assert!(mismatch.get_type(py).is(py.get_type::<ConflictError>()));
            assert_eq!(
                mismatch
                    .value(py)
                    .getattr("expected")
                    .unwrap()
                    .extract::<u64>()
                    .unwrap(),
                u64::MAX
            );
            assert_eq!(
                mismatch
                    .value(py)
                    .getattr("found")
                    .unwrap()
                    .extract::<u64>()
                    .unwrap(),
                9
            );

            let CoreStorageError::OtherIo { backtrace, .. } = invalid_location_error() else {
                panic!("expected invalid location");
            };
            let conflict = map(CommitError::Conflict {
                expected: 9,
                found: 10,
                backtrace,
            }
            .into());
            assert!(conflict.get_type(py).is(py.get_type::<ConflictError>()));
            assert_eq!(
                conflict
                    .value(py)
                    .getattr("found")
                    .unwrap()
                    .extract::<u64>()
                    .unwrap(),
                10
            );

            let CoreStorageError::OtherIo { backtrace, .. } = invalid_location_error() else {
                panic!("expected invalid location");
            };
            let race = map(CommitError::Storage {
                source: CoreStorageError::AlreadyExists {
                    path: "_timeseries_log/0000000010.json".into(),
                    source: std::io::Error::from(std::io::ErrorKind::AlreadyExists).into(),
                    backtrace,
                },
            }
            .into());
            assert!(race.get_type(py).is(py.get_type::<StorageError>()));
            assert_eq!(
                race.value(py)
                    .getattr("path")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "_timeseries_log/0000000010.json"
            );
            assert!(!race.value(py).hasattr("found").unwrap());

            let ambiguous = TableError::UpdateRows {
                source: CommitError::AmbiguousOutcome {
                    commit_path: "_timeseries_log/0000000010.json".into(),
                    operation_error: Box::new(invalid_location_error()),
                    cleanup_error: Box::new(invalid_location_error()),
                }
                .into(),
            };
            let message = ambiguous.to_string();
            let error = table_error_to_py(py, ambiguous, &[]);
            assert!(error.get_type(py).is(py.get_type::<TimeseriesTableError>()));
            assert_eq!(
                error.value(py).str().unwrap().extract::<String>().unwrap(),
                message
            );

            for source in [
                UpdateRowsError::from(UpdatePreparationError::Io {
                    path: "scratch/input".into(),
                    source: std::io::Error::other("read failed"),
                }),
                UpdateRowsError::from(UpdateRewriteError::Io {
                    path: "data/input.parquet".into(),
                    source: std::io::Error::other("read failed"),
                }),
            ] {
                let error = map(source);
                assert!(error.get_type(py).is(py.get_type::<StorageError>()));
                assert!(error.value(py).hasattr("path").unwrap());
                assert!(error.to_string().contains("read failed"));
            }
            let cleanup = map(UpdateRowsError::CleanupAfterFailure {
                source: Box::new(
                    UpdatePreparationError::Reader {
                        source: datafusion::arrow::error::ArrowError::ComputeError(
                            "source failed".into(),
                        ),
                    }
                    .into(),
                ),
                cleanup: Box::new(UpdateRewriteError::Cleanup {
                    cleanup_errors: vec![invalid_location_error()],
                }),
            });
            assert!(
                cleanup
                    .get_type(py)
                    .is(py.get_type::<TimeseriesTableError>())
            );
            assert!(cleanup.to_string().contains("source failed"));
            assert!(cleanup.to_string().contains("cleanup also failed"));
        });
    }

    #[test]
    fn column_addition_errors_preserve_schema_storage_and_ambiguity_categories() {
        use timeseries_table_format::AddColumnsError;

        init_python();
        Python::attach(|py| {
            let schema = TableError::AddColumns {
                source: AddColumnsError::from(SchemaEvolutionError::MissingSchema),
            };
            assert!(
                table_error_to_py(py, schema, &[])
                    .get_type(py)
                    .is(py.get_type::<SchemaMismatchError>())
            );

            let CoreStorageError::OtherIo { backtrace, .. } = invalid_location_error() else {
                panic!("expected an invalid location");
            };
            let storage = TableError::AddColumns {
                source: AddColumnsError::from(CommitError::Storage {
                    source: CoreStorageError::AlreadyExists {
                        path: "_timeseries_log/0000000003.json".into(),
                        source: std::io::Error::from(std::io::ErrorKind::AlreadyExists).into(),
                        backtrace,
                    },
                }),
            };
            let error = table_error_to_py(py, storage, &[]);
            assert!(error.get_type(py).is(py.get_type::<StorageError>()));
            assert_eq!(
                error
                    .value(py)
                    .getattr("path")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "_timeseries_log/0000000003.json"
            );

            let ambiguous = TableError::AddColumns {
                source: AddColumnsError::from(CommitError::AmbiguousOutcome {
                    commit_path: "_timeseries_log/0000000003.json".into(),
                    operation_error: Box::new(invalid_location_error()),
                    cleanup_error: Box::new(invalid_location_error()),
                }),
            };
            let message = ambiguous.to_string();
            let error = table_error_to_py(py, ambiguous, &[]);
            assert!(error.get_type(py).is(py.get_type::<TimeseriesTableError>()));
            assert_eq!(
                error.value(py).str().unwrap().extract::<String>().unwrap(),
                message
            );
        });
    }

    #[test]
    fn lifecycle_errors_preserve_python_exception_categories() {
        init_python();

        let attached = Python::try_attach(|py| {
            let storage = TableError::Open {
                source: OpenTableError::Commit {
                    source: CommitError::Storage {
                        source: invalid_location_error(),
                    },
                },
            };
            assert!(table_error_to_py(py, storage, &[]).is_instance_of::<StorageError>(py));

            let state_storage = TableError::StateAccess {
                source: TableStateAccessError::Commit {
                    source: CommitError::Storage {
                        source: invalid_location_error(),
                    },
                },
            };
            assert!(table_error_to_py(py, state_storage, &[]).is_instance_of::<StorageError>(py));

            let schema = TableError::Create {
                source: CreateTableError::from(
                    timeseries_table_format::metadata::schema_compat::SchemaCompatibilityError::MissingTableSchema,
                ),
            };
            assert!(table_error_to_py(py, schema, &[]).is_instance_of::<SchemaMismatchError>(py));
        });
        assert!(attached.is_some());
    }

    #[test]
    fn unclassified_commit_errors_use_the_generic_python_exception() {
        init_python();

        Python::attach(|py| {
            let error = TableError::StateAccess {
                source: TableStateAccessError::Commit {
                    source: CommitError::from(TableProtocolError::UnsupportedVersion {
                        expected: 1,
                        found: 2,
                    }),
                },
            };
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
            TableError::Append {
                source: AppendError::SegmentMetadata {
                    source: Box::new(SegmentError::from(invalid_location_error())),
                },
            },
            TableError::Append {
                source: AppendError::GeneratedSegmentCoverage {
                    source: Box::new(SegmentCoverageError::Storage {
                        path: "data/segment.parquet".to_string(),
                        source: invalid_location_error(),
                    }),
                },
            },
            TableError::Append {
                source: AppendError::CoverageSidecar {
                    source: Box::new(CoverageSidecarError::Storage {
                        source: invalid_location_error(),
                    }),
                },
            },
            TableError::Append {
                source: AppendError::ExistingSegmentCoverageSidecarRead {
                    segment_path: "data/segment.parquet".to_string(),
                    coverage_path: "_coverage/segments/missing.roar".to_string(),
                    source: Box::new(CoverageSidecarError::Storage {
                        source: invalid_location_error(),
                    }),
                },
            },
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
        let error = TableError::Append {
            source: AppendError::Rollback {
                source: Box::new(AppendError::PersistedIndexIntervalOverlap {
                    segment_path: "data/test.parquet".to_string(),
                    overlap_count: 1,
                    example_identity: None,
                    example_index_interval_id,
                    example_index_interval: Box::new(example_index_interval),
                }),
                cleanup_errors: vec![cleanup_error],
            },
        };

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
        let commit = TableError::Optimize {
            source: OptimizeError::Commit {
                source: CommitError::Storage {
                    source: invalid_location_error(),
                },
            },
        };
        let rollback = TableError::Optimize {
            source: OptimizeError::Rollback {
                source: Box::new(OptimizeError::MixedSegmentRewrite {
                    source: Box::new(EntityRewriteError::Storage {
                        source: invalid_location_error(),
                    }),
                }),
                cleanup_errors: vec![invalid_location_error()],
            },
        };

        Python::attach(|py| {
            let commit = table_error_to_py(py, commit, &[]);
            assert!(commit.is_instance_of::<StorageError>(py));

            let rollback = table_error_to_py(py, rollback, &[]);
            assert!(rollback.is_instance_of::<StorageError>(py));
        });
    }
}
