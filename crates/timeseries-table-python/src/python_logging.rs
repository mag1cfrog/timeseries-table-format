//! Bridge native Rust diagnostics into Python's logging hierarchy.

use std::sync::OnceLock;

use log::{LevelFilter, Log, Metadata, Record};
use pyo3::{PyErr, PyResult, Python, exceptions::PyRuntimeError};
use pyo3_log::{Caching, Logger, ResetHandle};

const LOGGER_ROOT: &str = "timeseries_table_format";
static RESET_HANDLE: OnceLock<ResetHandle> = OnceLock::new();

struct PythonLogger {
    inner: Logger,
}

impl Log for PythonLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        is_project_target(metadata.target()) && self.inner.enabled(metadata)
    }

    fn log(&self, record: &Record<'_>) {
        if !is_project_target(record.target()) {
            return;
        }

        // Preserve an existing exception, but contain handler failures so logging cannot turn a
        // committed operation into a reported failure. During shutdown, drop the record instead.
        let _ = Python::try_attach(|py| {
            let pending_error = PyErr::take(py);
            self.inner.log(record);
            let _ = PyErr::take(py);
            if let Some(error) = pending_error {
                error.restore(py);
            }
        });
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

fn is_project_target(target: &str) -> bool {
    target == LOGGER_ROOT
        || target.starts_with("timeseries_table_format::")
        || target.starts_with("timeseries_table_format.")
}

pub(crate) fn install(py: Python<'_>) -> PyResult<()> {
    if RESET_HANDLE.get().is_some() {
        return Ok(());
    }

    let logger = Logger::new(py, Caching::LoggersAndLevels)
        .map_err(|err| {
            PyRuntimeError::new_err(format!("failed to create native logging bridge: {err}"))
        })?
        .filter(LevelFilter::Debug);
    let reset_handle = logger.reset_handle();

    log::set_boxed_logger(Box::new(PythonLogger { inner: logger })).map_err(|err| {
        PyRuntimeError::new_err(format!("failed to install native logging bridge: {err}"))
    })?;
    log::set_max_level(LevelFilter::Debug);

    RESET_HANDLE.set(reset_handle).map_err(|_| {
        PyRuntimeError::new_err("native logging bridge initialized without its reset handle")
    })?;

    Ok(())
}

pub(crate) fn refresh_cache() {
    if let Some(reset_handle) = RESET_HANDLE.get() {
        reset_handle.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::is_project_target;

    #[test]
    fn only_project_targets_are_forwarded_to_python() {
        assert!(is_project_target("timeseries_table_format"));
        assert!(is_project_target("timeseries_table_format::table"));
        assert!(is_project_target("timeseries_table_format.table"));
        assert!(!is_project_target("datafusion::execution"));
        assert!(!is_project_target("timeseries_table_formatting"));
        assert!(!is_project_target(""));
    }
}
