//! Tokio runtime runner helpers for calling async Rust from sync Python bindings.

use std::{fmt::Display, future::Future};

use pyo3::{PyResult, Python, exceptions::PyRuntimeError};
use tokio::runtime::{Builder, Runtime};

#[allow(dead_code)]
pub(crate) fn new_runtime() -> PyResult<Runtime> {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
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
}
