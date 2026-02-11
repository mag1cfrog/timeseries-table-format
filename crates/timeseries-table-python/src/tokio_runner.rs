//! Tokio runtime runner helpers for calling async Rust from sync Python bindings.

use std::sync::{Arc, OnceLock};
use std::{fmt::Display, future::Future};

use pyo3::{PyErr, PyResult, Python, exceptions::PyRuntimeError};
use tokio::runtime::{Builder, Runtime};

#[allow(dead_code)]
pub(crate) fn new_runtime() -> PyResult<Runtime> {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

static GLOBAL_RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

pub(crate) fn global_runtime() -> PyResult<Arc<Runtime>> {
    if let Some(rt) = GLOBAL_RUNTIME.get() {
        return Ok(Arc::clone(rt));
    }

    let rt = Arc::new(new_runtime()?);

    // If we win the race, keep our runtime.
    if GLOBAL_RUNTIME.set(Arc::clone(&rt)).is_ok() {
        return Ok(rt);
    }

    // If we lost the race, use the already-initialized runtime.
    if let Some(rt) = GLOBAL_RUNTIME.get() {
        return Ok(Arc::clone(rt));
    }

    Err(PyRuntimeError::new_err(
        "failed to initialize global Tokio runtime",
    ))
}

#[allow(dead_code)]
pub(crate) fn run_blocking<T, E, F>(py: Python<'_>, rt: &Runtime, fut: F) -> PyResult<T>
where
    F: Future<Output = Result<T, E>> + Send,
    T: Send,
    E: Display + Send,
{
    let result = py.detach(|| rt.block_on(fut));
    result.map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[allow(dead_code)]
pub(crate) fn run_blocking_map_err<T, E, F, MapErr>(
    py: Python<'_>,
    rt: &Runtime,
    fut: F,
    map_err: MapErr,
) -> PyResult<T>
where
    F: Future<Output = Result<T, E>> + Send,
    T: Send,
    E: Send,
    MapErr: FnOnce(Python<'_>, E) -> PyErr,
{
    let result = py.detach(|| rt.block_on(fut));
    match result {
        Ok(v) => Ok(v),
        Err(e) => Err(map_err(py, e)),
    }
}

#[cfg(test)]
mod tests {
    use crate::tokio_runner::{new_runtime, run_blocking};

    #[test]
    fn run_blocking_works() -> pyo3::PyResult<()> {
        pyo3::Python::initialize();

        let rt = new_runtime()?;

        let v =
            pyo3::Python::attach(|py| run_blocking(py, &rt, async { Ok::<_, &'static str>(123) }))?;
        assert_eq!(v, 123);
        Ok(())
    }

    #[test]
    fn global_runtime_is_singleton() -> pyo3::PyResult<()> {
        let a = crate::tokio_runner::global_runtime()?;
        let b = crate::tokio_runner::global_runtime()?;

        assert!(std::sync::Arc::ptr_eq(&a, &b));

        Ok(())
    }
}
