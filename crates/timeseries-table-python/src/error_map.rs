use pyo3::{PyErr, Python, types::PyAnyMethods};

use crate::exceptions::{
    ConflictError, CoverageOverlapError, DataFusionError, SchemaMismatchError, StorageError,
    TimeseriesTableError,
};
use timeseries_table_core::{
    storage::StorageError as CoreStorageError, table::TableError, transaction_log::CommitError,
};

pub(crate) fn datafusion_error_to_py(
    _py: Python<'_>,
    err: datafusion::error::DataFusionError,
) -> PyErr {
    DataFusionError::new_err(err.to_string())
}

pub(crate) fn storage_error_to_py(py: Python<'_>, err: CoreStorageError) -> PyErr {
    let msg = err.to_string();

    let path_attr: Option<String> = match &err {
        CoreStorageError::NotFound { path, .. } => Some(path.clone()),
        CoreStorageError::AlreadyExists { path, .. } => Some(path.clone()),
        CoreStorageError::OtherIo { path, .. } => Some(path.clone()),
        CoreStorageError::AlreadyExistsNoSource { path, .. } => Some(path.clone()),
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

fn commit_error_to_py(py: Python<'_>, err: CommitError) -> PyErr {
    match err {
        CommitError::Conflict {
            expected, found, ..
        } => {
            let py_err = ConflictError::new_err(err.to_string());
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

        CommitError::CorruptState { .. } => TimeseriesTableError::new_err(err.to_string()),
    }
}

pub(crate) fn table_error_to_py(py: Python<'_>, err: TableError) -> PyErr {
    let msg = err.to_string();

    match err {
        TableError::Storage { source } => storage_error_to_py(py, source),

        TableError::TransactionLog { source } => commit_error_to_py(py, source),

        TableError::CoverageOverlap {
            segment_path,
            overlap_count,
            example_bucket,
        } => {
            let py_err = CoverageOverlapError::new_err(msg);
            let exc = py_err.value(py);

            if let Err(e) = exc.setattr("segment_path", segment_path) {
                return e;
            }

            if let Err(e) = exc.setattr("overlap_count", overlap_count) {
                return e;
            }

            match example_bucket {
                Some(b) => {
                    if let Err(e) = exc.setattr("example_bucket", b) {
                        return e;
                    }
                }
                None => {
                    if let Err(e) = exc.setattr("example_bucket", py.None()) {
                        return e;
                    }
                }
            }

            py_err
        }

        TableError::SchemaCompatibility { .. } => SchemaMismatchError::new_err(err.to_string()),

        _ => TimeseriesTableError::new_err(err.to_string()),
    }
}
