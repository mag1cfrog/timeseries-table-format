//! Python bindings for timeseries-table-format (v0 skeleton).
mod error_map;
mod exceptions;
mod tokio_runner;

#[pyo3::pymodule]
mod _native {

    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use datafusion::arrow::array::Scalar;
    use datafusion::arrow::error::ArrowError;
    use datafusion::common::ScalarValue;
    use datafusion::error::DataFusionError as DFError;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

    use pyo3::{
        Bound, PyErr, PyResult, Python,
        exceptions::{PyImportError, PyRuntimeError, PyTypeError, PyValueError},
        ffi::PyObject,
        prelude::*,
        pyclass, pymethods,
        types::{PyBytes, PyDict, PyList, PyModule, PyTuple, PyType},
    };

    use timeseries_table_datafusion::TsTableProvider;

    use crate::{
        exceptions::{
            ConflictError, CoverageOverlapError, DataFusionError, SchemaMismatchError,
            StorageError, TimeseriesTableError,
        },
        tokio_runner,
    };

    enum AppendParquetError {
        Table(timeseries_table_core::table::TableError),
        ValueError(String),
    }

    enum RegisterTsTableError {
        Table(timeseries_table_core::table::TableError),
        DataFusion(DFError),
        RestoreFailed { original: DFError, restore: DFError },
        Runtime(&'static str),
    }

    enum RegisterParquetError {
        DataFusion(DFError),
        RestoreFailed { original: DFError, restore: DFError },
        Runtime(&'static str),
    }

    enum QueryParams {
        Positional(Vec<ScalarValue>),
        Named(Vec<(String, ScalarValue)>),
    }

    fn py_any_to_scalar_value(v: &Bound<'_, pyo3::types::PyAny>) -> PyResult<ScalarValue> {
        use pyo3::types;

        if v.is_none() {
            return Ok(ScalarValue::Null);
        }

        // bool must come before int (since bool is a subclass of int in Python)
        if v.is_instance_of::<types::PyBool>() {
            let b: bool = v.extract()?;
            return Ok(ScalarValue::Boolean(Some(b)));
        }

        if v.is_instance_of::<types::PyInt>() {
            let n: i64 = v.extract().map_err(|e| {
                PyValueError::new_err(format!("int parameter out of i64 range: {e}"))
            })?;
            return Ok(ScalarValue::Int64(Some(n)));
        }

        if v.is_instance_of::<types::PyFloat>() {
            let f: f64 = v.extract()?;
            return Ok(ScalarValue::Float64(Some(f)));
        }

        if v.is_instance_of::<types::PyString>() {
            let s: String = v.extract()?;
            return Ok(ScalarValue::Utf8(Some(s)));
        }

        if v.is_instance_of::<types::PyBytes>() {
            let b: Vec<u8> = v.extract()?;
            return Ok(ScalarValue::Binary(Some(b)));
        }

        Err(PyTypeError::new_err(
            "params values must be one of: None, bool, int (i64), float, str, bytes",
        ))
    }

    fn parse_query_params(params: &Bound<'_, pyo3::types::PyAny>) -> PyResult<QueryParams> {
        if let Ok(d) = params.cast::<PyDict>() {
            let mut out: Vec<(String, ScalarValue)> = Vec::with_capacity(d.len());
            for (k, v) in d.iter() {
                let key: String = k
                    .extract()
                    .map_err(|_| PyTypeError::new_err("params dict keys must be str"))?;

                let key = key.strip_prefix('$').unwrap_or(key.as_str()).to_string();
                let sv = py_any_to_scalar_value(&v)?;
                out.push((key, sv));
            }
            return Ok(QueryParams::Named(out));
        }

        if let Ok(l) = params.cast::<PyList>() {
            let mut out: Vec<ScalarValue> = Vec::with_capacity(l.len());
            for v in l.iter() {
                out.push(py_any_to_scalar_value(&v)?);
            }
            return Ok(QueryParams::Positional(out));
        }

        if let Ok(t) = params.cast::<PyTuple>() {
            let mut out: Vec<ScalarValue> = Vec::with_capacity(t.len());
            for v in t.iter() {
                out.push(py_any_to_scalar_value(&v)?);
            }
            return Ok(QueryParams::Positional(out));
        }

        Err(PyTypeError::new_err(
            "params must be a dict (named &param) or list/tuple (positional $1, $2, ...)",
        ))
    }

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

    fn datafusion_error_to_py_with_name_and_path(
        py: Python<'_>,
        name: &str,
        path: &str,
        err: DFError,
    ) -> PyErr {
        let base_msg = err.to_string();
        let py_err = crate::error_map::datafusion_error_to_py(py, err);
        let exc = py_err.value(py);

        if let Err(e) = exc.setattr("name", name.to_string()) {
            return e;
        }
        if let Err(e) = exc.setattr("path", path.to_string()) {
            return e;
        }

        let msg = format!("{base_msg} (name={name}, path={path})");
        if let Err(e) = exc.setattr("args", (msg,)) {
            return e;
        }

        py_err
    }

    #[allow(unused)]
    #[pyclass]
    struct Session {
        rt: Arc<tokio::runtime::Runtime>,
        ctx: SessionContext,
        tables: Mutex<BTreeSet<String>>,
        catalog_sema: Arc<tokio::sync::Semaphore>,
    }

    #[pymethods]
    impl Session {
        #[new]
        fn new() -> PyResult<Self> {
            let rt = tokio_runner::global_runtime()?;

            let cfg = SessionConfig::new();
            let ctx = SessionContext::new_with_config(cfg);

            Ok(Self {
                rt,
                ctx,
                tables: Mutex::new(BTreeSet::new()),
                catalog_sema: Arc::new(tokio::sync::Semaphore::new(1)),
            })
        }

        fn register_tstable(
            &self,
            py: Python<'_>,
            name: String,
            table_root: String,
        ) -> PyResult<()> {
            use timeseries_table_core::storage::TableLocation;
            use timeseries_table_core::table::{TableError, TimeSeriesTable};

            if name.is_empty() {
                return Err(PyValueError::new_err("name must be non-empty"));
            }

            let name_for_df = name.clone();

            let ctx = self.ctx.clone();
            let sema = Arc::clone(&self.catalog_sema);
            let tables = &self.tables;

            // For better error messages / attributes
            let table_root_for_err = table_root.clone();
            let name_for_err = name.clone();

            tokio_runner::run_blocking_map_err(
                py,
                self.rt.as_ref(),
                async move {
                    // 1) IO: open table (async)
                    let location = TableLocation::parse(&table_root)
                        .map_err(|e| TableError::Storage { source: e })
                        .map_err(RegisterTsTableError::Table)?;

                    let table = TimeSeriesTable::open(location)
                        .await
                        .map_err(RegisterTsTableError::Table)?;

                    // 2) provider creation (sync)
                    let provider = TsTableProvider::try_new(Arc::new(table))
                        .map_err(RegisterTsTableError::DataFusion)?;

                    // 3) atomic-ish replace (serialize this section per Session)
                    let provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(provider);

                    let _permit = sema.acquire_owned().await.map_err(|_| {
                        RegisterTsTableError::Runtime("Session catalog semaphore closed")
                    })?;

                    let old = ctx
                        .deregister_table(name_for_df.as_str())
                        .map_err(RegisterTsTableError::DataFusion)?;

                    if let Err(e) = ctx.register_table(name.as_str(), provider) {
                        if let Some(old_provider) = old
                            && let Err(restore) = ctx.register_table(name.as_str(), old_provider)
                        {
                            return Err(RegisterTsTableError::RestoreFailed {
                                original: e,
                                restore,
                            });
                        }
                        return Err(RegisterTsTableError::DataFusion(e));
                    }

                    let mut t = tables.lock().map_err(|_| {
                        RegisterTsTableError::Runtime("Session tables lock poisoned")
                    })?;
                    t.insert(name);

                    Ok::<(), RegisterTsTableError>(())
                },
                move |py, err| match err {
                    RegisterTsTableError::Table(e) => {
                        table_error_to_py_with_root(py, &table_root_for_err, e)
                    }
                    RegisterTsTableError::DataFusion(e) => {
                        crate::error_map::datafusion_error_to_py(py, e)
                    }
                    RegisterTsTableError::RestoreFailed { original, restore } => {
                        DataFusionError::new_err(format!(
                            "failed to register table {name_for_err:?}: {original}; additionally failed to restore previous registration: {restore}"
                        ))
                    }
                    RegisterTsTableError::Runtime(msg) => PyRuntimeError::new_err(msg),
                },
            )
        }

        fn register_parquet(&self, py: Python<'_>, name: String, path: String) -> PyResult<()> {
            if name.is_empty() {
                return Err(PyValueError::new_err("name must be non-empty"));
            }
            if path.is_empty() {
                return Err(PyValueError::new_err("path must be non-empty"));
            }

            let ctx = self.ctx.clone();
            let sema = Arc::clone(&self.catalog_sema);
            let tables = &self.tables;

            let name_for_err = name.clone();
            let path_for_err = path.clone();

            tokio_runner::run_blocking_map_err(
                py,
                self.rt.as_ref(),
                async move {
                    let _permit = sema.acquire_owned().await.map_err(|_| {
                        RegisterParquetError::Runtime("Session catalog semaphore closed")
                    })?;

                    // Swap with rollback.
                    let old = ctx
                        .deregister_table(name.as_str())
                        .map_err(RegisterParquetError::DataFusion)?;

                    match ctx
                        .register_parquet(
                            name.as_str(),
                            path.as_str(),
                            ParquetReadOptions::default(),
                        )
                        .await
                    {
                        Ok(()) => {}
                        Err(e) => {
                            if let Some(old_provider) = old
                                && let Err(restore) =
                                    ctx.register_table(name.as_str(), old_provider)
                            {
                                return Err(RegisterParquetError::RestoreFailed {
                                    original: e,
                                    restore,
                                });
                            }
                            return Err(RegisterParquetError::DataFusion(e));
                        }
                    }

                    let mut t = tables.lock().map_err(|_| {
                        RegisterParquetError::Runtime("Session tables lock poisoned")
                    })?;
                    t.insert(name);

                    Ok::<(), RegisterParquetError>(())
                },
                move |py, err| match err {
                    RegisterParquetError::DataFusion(e) => {
                        datafusion_error_to_py_with_name_and_path(
                            py,
                            &name_for_err,
                            &path_for_err,
                            e,
                        )
                    }
                    RegisterParquetError::RestoreFailed { original, restore } => {
                        let msg = format!(
                            "failed to register parquet: {original} (name={name_for_err}, path={path_for_err}); additionally failed to restore previous registration: {restore}"
                        );

                        let py_err = DataFusionError::new_err(msg);
                        let exc = py_err.value(py);
                        let _ = exc.setattr("name", name_for_err.clone());
                        let _ = exc.setattr("path", path_for_err.clone());
                        py_err
                    }
                    RegisterParquetError::Runtime(msg) => PyRuntimeError::new_err(msg),
                },
            )
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
                let msg = format!("invalid bucket spec {bucket:?} (table_root={table_root}): {e}");
                let py_err = TimeseriesTableError::new_err(msg);
                let exc = py_err.value(py);
                let _ = exc.setattr("table_root", table_root.clone());
                py_err
            })?;

            let index = TimeIndexSpec {
                timestamp_column: time_column,
                bucket,
                timezone,
                entity_columns: entity_columns.unwrap_or_default(),
            };
            let meta = TableMeta::new_time_series(index);

            let rt = tokio_runner::global_runtime()?;
            let table_root_for_err = table_root.clone();

            let table_root_for_err_cp = table_root_for_err.clone();
            let inner = tokio_runner::run_blocking_map_err(
                py,
                rt.as_ref(),
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

            let rt = tokio_runner::global_runtime()?;
            let table_root_for_err = table_root.clone();
            let table_root_for_err_cp = table_root_for_err.clone();

            let inner = tokio_runner::run_blocking_map_err(
                py,
                rt.as_ref(),
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

        fn root(&self) -> String {
            self.table_root.clone()
        }

        fn version(&self) -> u64 {
            self.inner.state().version
        }

        fn index_spec<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
            use timeseries_table_core::transaction_log::TimeBucket;

            let spec = self.inner.index_spec();

            let bucket = match spec.bucket {
                TimeBucket::Seconds(n) => format!("{n}s"),
                TimeBucket::Minutes(n) => format!("{n}m"),
                TimeBucket::Hours(n) => format!("{n}h"),
                TimeBucket::Days(n) => format!("{n}d"),
            };

            let d = PyDict::new(py);
            d.set_item("timestamp_column", spec.timestamp_column.clone())?;
            d.set_item("entity_columns", spec.entity_columns.clone())?;
            d.set_item("bucket", bucket)?;
            d.set_item("timezone", spec.timezone.clone())?;

            Ok(d)
        }

        #[pyo3(signature = (parquet_path, time_column=None, copy_if_outside=true))]
        fn append_parquet(
            &mut self,
            py: Python<'_>,
            parquet_path: String,
            time_column: Option<String>,
            copy_if_outside: bool,
        ) -> PyResult<u64> {
            use crate::tokio_runner;

            use std::path::{Component, Path};

            use timeseries_table_core::storage::StorageLocation;
            use timeseries_table_core::table::TableError;

            let rt = tokio_runner::global_runtime()?;

            let effective_time_column =
                time_column.unwrap_or_else(|| self.inner.index_spec().timestamp_column.clone());

            let table_root_for_err = self.table_root.clone();
            let table_root_for_err_cp = table_root_for_err.clone();

            // Clone location before taking a mutable borrow of `self.inner`.
            let location = self.inner.location().clone();
            let table = &mut self.inner;

            tokio_runner::run_blocking_map_err(
                py,
                rt.as_ref(),
                async move {
                    let rel_path = if copy_if_outside {
                        location
                            .ensure_parquet_under_root(Path::new(&parquet_path))
                            .await
                            .map_err(|source| {
                                AppendParquetError::Table(TableError::Storage { source })
                            })?
                    } else {
                        let root_path = match location.storage() {
                            StorageLocation::Local(p) => p.as_path(),
                        };

                        let src_path = Path::new(&parquet_path);

                        // 1) If caller passes a path *including* table root, strip it.
                        // (works for absolute-under-root and also some relative cases).
                        let rel = src_path
                            .strip_prefix(root_path)
                            .or_else(|_| src_path.strip_prefix(Path::new(&table_root_for_err)))
                            .ok()
                            .map(|p| p.to_path_buf());

                        // 2) Otherwise, if caller passed a relative path, treat it as already relative-to-root,
                        // but refuse parent traversal.
                        let rel = match rel {
                            Some(r) => r,
                            None if !src_path.is_absolute() => {
                                if src_path
                                    .components()
                                    .any(|c| matches!(c, Component::ParentDir))
                                {
                                    return Err(AppendParquetError::ValueError(format!(
                                        "parquet_path must not contain '..' when copy_if_outside=False (parquet_path={parquet_path:?}, table_root={table_root_for_err:?})"
                                    )));
                                }
                                src_path.to_path_buf()
                            }
                            None => {
                                return Err(AppendParquetError::ValueError(format!(
                                    "parquet_path must be under table_root when copy_if_outside=False (parquet_path={parquet_path:?}, table_root={table_root_for_err:?})"
                                )));
                            }
                        };

                        if rel.as_os_str().is_empty() {
                            return Err(AppendParquetError::ValueError(format!(
                                "parquet_path must point to a file under table_root, not the root itself (parquet_path={parquet_path:?}, table_root={table_root_for_err:?})"
                            )));
                        }

                        rel
                    };

                    let mut rel_str = rel_path.to_string_lossy().to_string();
                    if cfg!(windows) {
                        rel_str = rel_str.replace('\\', "/");
                    }

                    let version = table
                        .append_parquet_segment(&rel_str, &effective_time_column)
                        .await
                        .map_err(AppendParquetError::Table)?;

                    Ok::<u64, AppendParquetError>(version)
                },
                move |py, err| match err {
                    AppendParquetError::Table(e) => {
                        table_error_to_py_with_root(py, &table_root_for_err_cp, e)
                    }
                    AppendParquetError::ValueError(msg) => PyValueError::new_err(msg),
                },
            )
        }
    }

    /// Test-only helper: creates a table at `table_root`, copies `parquet_path`
    /// under the table root if needed, appends it twice, and expects the second
    /// append to fail with a coverage overlap.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    fn _test_trigger_overlap(py: Python<'_>, table_root: &str, parquet_path: &str) -> PyResult<()> {
        use crate::{error_map, tokio_runner};

        let rt = tokio_runner::global_runtime()?;

        let table_root = table_root.to_string();
        let parquet_path = parquet_path.to_string();

        tokio_runner::run_blocking_map_err(
            py,
            rt.as_ref(),
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

    /// Test-only helper: blocks for `millis` while releasing the GIL.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    fn _test_sleep_without_gil(py: Python<'_>, millis: u64) -> PyResult<()> {
        use std::time::Duration;

        py.detach(move || std::thread::sleep(Duration::from_millis(millis)));
        Ok(())
    }

    /// Test-only helper: checks if a table name is currently registered in the DataFusion catalog.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    fn _test_session_table_exists(
        py: Python<'_>,
        session: PyRef<'_, Session>,
        name: &str,
    ) -> PyResult<bool> {
        enum ExistsError {
            DataFusion(DFError),
            Runtime(&'static str),
        }

        let rt = Arc::clone(&session.rt);
        let ctx = session.ctx.clone();
        let sema = Arc::clone(&session.catalog_sema);
        let name = name.to_string();

        tokio_runner::run_blocking_map_err(
            py,
            rt.as_ref(),
            async move {
                let _permit = sema
                    .acquire_owned()
                    .await
                    .map_err(|_| ExistsError::Runtime("Session catalog semaphore closed"))?;

                let exists = ctx
                    .table_exist(name.as_str())
                    .map_err(ExistsError::DataFusion)?;

                Ok::<bool, ExistsError>(exists)
            },
            move |py, err| match err {
                ExistsError::DataFusion(e) => crate::error_map::datafusion_error_to_py(py, e),
                ExistsError::Runtime(msg) => PyRuntimeError::new_err(msg),
            },
        )
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

        // Feature-gated: present only when built with `--features test-utils`.
        // Always add the attribute (defaulting to None) to keep the module surface stable.
        m.add("_testing", py.None())?;

        #[cfg(feature = "test-utils")]
        {
            // Internal test-only hook (kept under a clearly private namespace).
            let py = m.py();
            let testing = PyModule::new(py, "timeseries_table_format._native._testing")?;
            testing.add_function(pyo3::wrap_pyfunction!(_test_trigger_overlap, py)?)?;
            testing.add_function(pyo3::wrap_pyfunction!(_test_sleep_without_gil, py)?)?;
            testing.add_function(pyo3::wrap_pyfunction!(_test_session_table_exists, py)?)?;
            m.add("_testing", &testing)?;
            m.add_submodule(&testing)?;
        }

        Ok(())
    }
}
