//! Python bindings for timeseries-table-format (v0 skeleton).
mod error_map;
mod exceptions;
mod tokio_runner;

#[pyo3::pymodule]
mod timeseries_table_format {
    use pyo3::{Bound, PyResult, prelude::*, pyclass, pymethods, types::PyModule};

    #[pyclass]
    struct Session;

    #[pymethods]
    impl Session {
        #[new]
        fn new() -> Self {
            Self
        }
    }

    #[pyclass]
    struct TimeSeriesTable;

    #[pymethods]
    impl TimeSeriesTable {
        #[new]
        fn new() -> Self {
            Self
        }
    }

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add("__version__", env!("CARGO_PKG_VERSION"))?;
        Ok(())
    }
}
