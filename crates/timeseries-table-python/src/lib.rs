//! Python bindings for timeseries-table-format (v0 skeleton).
mod error_map;
mod exceptions;
mod tokio_runner;

#[pyo3::pymodule]
mod timeseries_table_format {

    use pyo3::{
        Bound, PyErr, PyResult,
        exceptions::PyRuntimeError,
        prelude::*,
        pyclass, pymethods,
        types::{PyModule, PyType},
    };

    use crate::exceptions::{
        ConflictError, CoverageOverlapError, DataFusionError, SchemaMismatchError, StorageError,
        TimeseriesTableError,
    };

    fn table_error_to_py_with_root(
        py: Python<'_>,
        table_root: &str,
        err: timeseries_table_core::table::TableError,
    ) -> PyErr {
        let base_msg = err.to_string();
        let py_err = crate::error_map::table_error_to_py(py, err);
        let exc = py_err.value(py);

        if let Err(e) = exc.setattr("table_root", table_root.to_string()) {
            return e;
        }

        let msg = format!("{base_msg} (table_root={table_root})");
        if let Err(e) = exc.setattr("args", (msg,)) {
            return e;
        }

        py_err
    }

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
    struct TimeSeriesTable {
        inner: timeseries_table_core::table::TimeSeriesTable,
        table_root: String,
    }

    #[pymethods]
    impl TimeSeriesTable {
        #[classmethod]
        #[pyo3(signature = (*, table_root, time_column, bucket, entity_columns=None, timezone=None))]
        fn create(
            _cls: &Bound<'_, PyType>,
            py: Python<'_>,
            table_root: String,
            time_column: String,
            bucket: String,
            entity_columns: Option<Vec<String>>,
            timezone: Option<String>,
        ) -> PyResult<Self> {
            use crate::tokio_runner;

            use timeseries_table_core::storage::TableLocation;
            use timeseries_table_core::table::TableError;
            use timeseries_table_core::transaction_log::{TableMeta, TimeBucket, TimeIndexSpec};

            let bucket = TimeBucket::parse(&bucket).map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "invalid bucket spec {bucket:?} (table_root={table_root}): {e}"
                ))
            })?;

            let index = TimeIndexSpec {
                timestamp_column: time_column,
                bucket,
                timezone,
                entity_columns: entity_columns.unwrap_or_default(),
            };
            let meta = TableMeta::new_time_series(index);

            let rt = tokio_runner::new_runtime()?;
            let table_root_for_err = table_root.clone();

            let table_root_for_err_cp = table_root_for_err.clone();
            let inner = tokio_runner::run_blocking_map_err(
                py,
                &rt,
                async move {
                    let location = TableLocation::parse(&table_root)
                        .map_err(|e| TableError::Storage { source: e })?;

                    let table =
                        timeseries_table_core::table::TimeSeriesTable::create(location, meta)
                            .await?;

                    Ok::<_, TableError>(table)
                },
                move |py, err| table_error_to_py_with_root(py, &table_root_for_err_cp, err),
            )?;

            Ok(Self {
                inner,
                table_root: table_root_for_err,
            })
        }

        #[classmethod]
        fn open(_cls: &Bound<'_, PyType>, py: Python<'_>, table_root: String) -> PyResult<Self> {
            use crate::tokio_runner;

            use timeseries_table_core::{storage::TableLocation, table::TableError};

            let rt = tokio_runner::new_runtime()?;
            let table_root_for_err = table_root.clone();
            let table_root_for_err_cp = table_root_for_err.clone();

            let inner = tokio_runner::run_blocking_map_err(
                py,
                &rt,
                async move {
                    let location = TableLocation::parse(&table_root)
                        .map_err(|e| TableError::Storage { source: e })?;

                    let table =
                        timeseries_table_core::table::TimeSeriesTable::open(location).await?;

                    Ok::<_, TableError>(table)
                },
                move |py, err| table_error_to_py_with_root(py, &table_root_for_err_cp, err),
            )?;

            Ok(Self {
                inner,
                table_root: table_root_for_err,
            })
        }
    }

    /// Test-only helper: creates a table at `table_root`, copies `parquet_path`
    /// under the table root if needed, appends it twice, and expects the second
    /// append to fail with a coverage overlap.
    #[pyfunction]
    fn _test_trigger_overlap(py: Python<'_>, table_root: &str, parquet_path: &str) -> PyResult<()> {
        use crate::{error_map, tokio_runner};

        let rt = tokio_runner::new_runtime()?;

        let table_root = table_root.to_string();
        let parquet_path = parquet_path.to_string();

        tokio_runner::run_blocking_map_err(
            py,
            &rt,
            async move {
                use std::path::Path;

                use timeseries_table_core::table::TimeSeriesTable;
                use timeseries_table_core::{
                    storage::TableLocation,
                    table::TableError,
                    transaction_log::{TableMeta, TimeBucket, TimeIndexSpec},
                };

                let index = TimeIndexSpec {
                    timestamp_column: "ts".to_string(),
                    bucket: TimeBucket::Minutes(60),
                    timezone: None,
                    entity_columns: Vec::new(),
                };

                let meta = TableMeta::new_time_series(index);

                let location = TableLocation::parse(&table_root)
                    .map_err(|e| TableError::Storage { source: e })?;

                let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

                let rel = location
                    .ensure_parquet_under_root(Path::new(&parquet_path))
                    .await
                    .map_err(|e| TableError::Storage { source: e })?;

                let rel_str = rel.to_string_lossy().to_string();
                let rel_str = if cfg!(windows) {
                    rel_str.replace('\\', "/")
                } else {
                    rel_str
                };

                let _v1 = table.append_parquet_segment(&rel_str, "ts").await?;

                let _v2 = table.append_parquet_segment(&rel_str, "ts").await?;

                Ok::<(), TableError>(())
            },
            error_map::table_error_to_py,
        )?;

        Ok(())
    }

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add("__version__", env!("CARGO_PKG_VERSION"))?;

        // Export classes
        m.add_class::<Session>()?;
        m.add_class::<TimeSeriesTable>()?;

        // Export exception types
        let py = m.py();
        m.add(
            "TimeseriesTableError",
            py.get_type::<TimeseriesTableError>(),
        )?;

        m.add("StorageError", py.get_type::<StorageError>())?;
        m.add("ConflictError", py.get_type::<ConflictError>())?;
        m.add(
            "CoverageOverlapError",
            py.get_type::<CoverageOverlapError>(),
        )?;
        m.add("SchemaMismatchError", py.get_type::<SchemaMismatchError>())?;
        m.add("DataFusionError", py.get_type::<DataFusionError>())?;

        // Internal test-only hook (kept under a clearly private namespace).
        let testing = PyModule::new(py, "timeseries_table_format._testing")?;
        testing.add_function(pyo3::wrap_pyfunction!(_test_trigger_overlap, py)?)?;
        m.add_submodule(&testing)?;
        Ok(())
    }
}
