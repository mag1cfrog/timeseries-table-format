//! Bridge native Rust diagnostics into Python's logging hierarchy.

use std::{borrow::Cow, sync::OnceLock};

use log::{LevelFilter, Log, Metadata, Record};
use pyo3::{PyResult, Python, exceptions::PyRuntimeError};
use pyo3_log::{Caching, Logger, ResetHandle};

const LOGGER_ROOT: &str = "timeseries_table_format";
static RESET_HANDLE: OnceLock<ResetHandle> = OnceLock::new();

struct PythonLogger {
    inner: Logger,
}

impl Log for PythonLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        let target = namespaced_target(metadata.target());
        let metadata = Metadata::builder()
            .level(metadata.level())
            .target(target.as_ref())
            .build();
        self.inner.enabled(&metadata)
    }

    fn log(&self, record: &Record<'_>) {
        let target = namespaced_target(record.target());
        let record = Record::builder()
            .args(*record.args())
            .level(record.level())
            .target(target.as_ref())
            .module_path(record.module_path())
            .file(record.file())
            .line(record.line())
            .build();

        // Logging may outlive the Python objects that initiated background native work.
        // Drop records once the interpreter can no longer be attached safely.
        let _ = Python::try_attach(|_| self.inner.log(&record));
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

fn namespaced_target(target: &str) -> Cow<'_, str> {
    if target == LOGGER_ROOT
        || target.starts_with("timeseries_table_format::")
        || target.starts_with("timeseries_table_format.")
    {
        Cow::Borrowed(target)
    } else if target.is_empty() {
        Cow::Borrowed(LOGGER_ROOT)
    } else {
        Cow::Owned(format!("{LOGGER_ROOT}::{target}"))
    }
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
    use super::namespaced_target;

    #[test]
    fn native_targets_share_the_public_python_namespace() {
        assert_eq!(
            namespaced_target("timeseries_table_format"),
            "timeseries_table_format"
        );
        assert_eq!(
            namespaced_target("timeseries_table_format::table"),
            "timeseries_table_format::table"
        );
        assert_eq!(
            namespaced_target("timeseries_table_format.table"),
            "timeseries_table_format.table"
        );
        assert_eq!(
            namespaced_target("datafusion::execution"),
            "timeseries_table_format::datafusion::execution"
        );
        assert_eq!(
            namespaced_target("timeseries_table_formatting"),
            "timeseries_table_format::timeseries_table_formatting"
        );
        assert_eq!(namespaced_target(""), "timeseries_table_format");
    }
}
