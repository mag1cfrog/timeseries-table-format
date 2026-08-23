use pyo3::{
    Bound, PyErr, PyResult, Python,
    types::{PyAny, PyAnyMethods, PyDict, PyDictMethods},
};

use crate::exceptions::{
    ConflictError, DataFusionError, DuplicateIndexIntervalError, IndexIntervalOverlapError,
    SchemaMismatchError, StorageError, TimeseriesTableError,
};
use timeseries_table_format::{
    coverage::{EntityIdentity, EntityValue, index_interval::IndexInterval},
    formats::parquet::SegmentCoverageError,
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

#[allow(dead_code)]
pub(crate) fn table_error_to_py(
    py: Python<'_>,
    err: TableError,
    entity_columns: &[String],
) -> PyErr {
    let msg = err.to_string();

    match err {
        TableError::Storage { source }
        | TableError::Append {
            source: AppendError::Storage { source },
        } => storage_error_to_py(py, source),

        TableError::TransactionLog { source }
        | TableError::Append {
            source: AppendError::Commit { source },
        } => commit_error_to_py(py, source),

        TableError::Append {
            source: AppendError::CommitAmbiguous { source, .. },
        } => commit_error_to_py(py, *source),

        TableError::Append {
            source: AppendError::GeneratedSegmentCoverage { source },
        } => match *source {
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

        TableError::Append {
            source:
                AppendError::PersistedIndexIntervalOverlap {
                    segment_path,
                    overlap_count,
                    example_identity,
                    example_index_interval_id: _,
                    example_index_interval,
                },
        } => index_interval_overlap_error_to_py(
            py,
            msg,
            segment_path,
            overlap_count,
            *example_index_interval,
            entity_columns,
            example_identity.as_ref(),
        ),

        TableError::SchemaCompatibility { .. }
        | TableError::Append {
            source:
                AppendError::InputSchemaCompatibility { .. }
                | AppendError::GeneratedSegmentSchemaCompatibility { .. },
        } => SchemaMismatchError::new_err(err.to_string()),

        _ => TimeseriesTableError::new_err(err.to_string()),
    }
}
