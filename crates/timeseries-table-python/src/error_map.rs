use pyo3::{
    PyErr, Python,
    types::{PyAnyMethods, PyDict, PyDictMethods},
};

use crate::exceptions::{
    ConflictError, CoverageOverlapError, DataFusionError, SchemaMismatchError, StorageError,
    TimeseriesTableError,
};
use timeseries_table_format::{
    coverage::{EntityValue, index_interval::IndexInterval},
    storage::StorageError as CoreStorageError,
    table::TableError,
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

fn coverage_overlap_error_to_py(
    py: Python<'_>,
    msg: String,
    segment_path: String,
    overlap_count: u128,
    example_index_interval_id: Option<u64>,
    example_index_interval: IndexInterval,
    example_entity_identity: Option<(&[String], &[EntityValue])>,
) -> PyErr {
    let py_err = CoverageOverlapError::new_err(msg);
    let exc = py_err.value(py);

    if let Err(error) = exc.setattr("segment_path", segment_path) {
        return error;
    }
    if let Err(error) = exc.setattr("overlap_count", overlap_count) {
        return error;
    }
    if let Err(error) = exc.setattr("example_bucket", example_index_interval_id) {
        return error;
    }
    if let Err(error) = exc.setattr("example_bucket_range", example_index_interval.to_string()) {
        return error;
    }
    match example_entity_identity {
        Some((columns, components)) => {
            if columns.len() != components.len() {
                return TimeseriesTableError::new_err(
                    "entity overlap identity does not match configured entity columns",
                );
            }
            let identity = PyDict::new(py);
            for (column, component) in columns.iter().zip(components) {
                let result = match component {
                    EntityValue::Utf8(value) => identity.set_item(column, value),
                    EntityValue::Int32(value) => identity.set_item(column, value),
                    EntityValue::Int64(value) => identity.set_item(column, value),
                    EntityValue::UInt64(value) => identity.set_item(column, value),
                };
                if let Err(error) = result {
                    return error;
                }
            }
            if let Err(error) = exc.setattr("example_entity_identity", identity) {
                return error;
            }
        }
        None => {
            if let Err(error) = exc.setattr("example_entity_identity", py.None()) {
                return error;
            }
        }
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
        TableError::Storage { source } => storage_error_to_py(py, source),

        TableError::TransactionLog { source } => commit_error_to_py(py, source),

        TableError::IndexIntervalOverlap {
            segment_path,
            overlap_count,
            example_index_interval_id,
            example_index_interval,
        } => coverage_overlap_error_to_py(
            py,
            msg,
            segment_path,
            u128::from(overlap_count),
            example_index_interval_id,
            example_index_interval,
            None,
        ),

        TableError::EntityIndexIntervalOverlap {
            segment_path,
            overlap_count,
            example_identity,
            example_index_interval_id,
            example_index_interval,
        } => coverage_overlap_error_to_py(
            py,
            msg,
            segment_path,
            overlap_count,
            Some(example_index_interval_id),
            example_index_interval,
            Some((entity_columns, example_identity.components())),
        ),

        TableError::SchemaCompatibility { .. } | TableError::SegmentSchemaCompatibility { .. } => {
            SchemaMismatchError::new_err(err.to_string())
        }

        _ => TimeseriesTableError::new_err(err.to_string()),
    }
}
