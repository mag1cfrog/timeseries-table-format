//! Python bindings for timeseries-table-format (v0 skeleton).
mod error_map;
mod exceptions;
mod tokio_runner;

#[pyo3::pymodule]
mod _native {

    use std::collections::BTreeSet;
    use std::ffi::CString;
    use std::sync::{Arc, Mutex};

    use arrow_array::ffi::FFI_ArrowSchema;
    use arrow_array::ffi_stream::FFI_ArrowArrayStream;
    use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};

    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::arrow::error::ArrowError;
    use datafusion::common::ScalarValue;
    use datafusion::error::DataFusionError as DFError;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

    use pyo3::types::PyCapsule;
    use pyo3::{
        Bound, PyErr, PyResult, PyTypeInfo, Python,
        exceptions::{
            PyAttributeError, PyImportError, PyKeyError, PyRuntimeError, PyRuntimeWarning,
            PyTypeError, PyValueError,
        },
        prelude::*,
        pyclass, pymethods,
        types::{PyBytes, PyDict, PyList, PyModule, PyTuple, PyType},
    };

    use timeseries_table_datafusion::TsTableProvider;

    use crate::error_map::datafusion_error_to_py;
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

    #[derive(Clone)]
    enum QueryParams {
        Positional(Vec<ScalarValue>),
        Named(Vec<(String, ScalarValue)>),
    }

    fn env_var_truthy(name: &str) -> bool {
        match std::env::var_os(name) {
            None => false,
            Some(v) => {
                let s = v.to_string_lossy();
                let s = s.trim().to_ascii_lowercase();
                !(s.is_empty() || s == "0" || s == "false" || s == "no" || s == "off")
            }
        }
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
            "params must be a dict (named $param) or list/tuple (positional $1, $2, ...)",
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

    /// SQL session backed by DataFusion.
    ///
    /// Use `Session` to register one or more tables (including multiple time-series tables) and
    /// run SQL queries—joins included. Query results are returned to Python as a `pyarrow.Table`.
    ///
    /// The Python API is synchronous. Internally, long-running Rust operations run on an
    /// internal Tokio runtime and release the GIL.
    #[pyclass]
    struct Session {
        rt: Arc<tokio::runtime::Runtime>,
        ctx: SessionContext,
        tables: Mutex<BTreeSet<String>>,
        catalog_sema: Arc<tokio::sync::Semaphore>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum SqlExportMode {
        Auto,
        Ipc,
        CStream,
    }

    impl SqlExportMode {
        fn from_env() -> PyResult<Self> {
            let v = match std::env::var("TTF_SQL_EXPORT_MODE") {
                Ok(v) => v.trim().to_ascii_lowercase(),
                Err(std::env::VarError::NotPresent) => return Ok(Self::CStream),
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(PyValueError::new_err(
                        "TTF_SQL_EXPORT_MODE must be valid unicode",
                    ));
                }
            };

            match v.as_str() {
                "" | "auto" => Ok(Self::Auto),
                "ipc" => Ok(Self::Ipc),
                "c_stream" | "cstream" | "c-stream" => Ok(Self::CStream),
                other => Err(PyValueError::new_err(format!(
                    "invalid TTF_SQL_EXPORT_MODE={other:?}; expected 'auto', 'ipc', or 'c_stream'"
                ))),
            }
        }
    }

    fn can_export_schema_to_c_stream(schema: &SchemaRef) -> bool {
        fn can_export_data_type(dt: &DataType) -> bool {
            match dt {
                DataType::Null
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::FixedSizeBinary(_)
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Duration(_)
                | DataType::Interval(_) => true,

                DataType::Dictionary(key, value) => {
                    matches!(
                        key.as_ref(),
                        DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::UInt8
                            | DataType::UInt16
                            | DataType::UInt32
                            | DataType::UInt64
                    ) && can_export_data_type(value.as_ref())
                }

                DataType::List(child)
                | DataType::LargeList(child)
                | DataType::FixedSizeList(child, _) => can_export_data_type(child.data_type()),
                DataType::Struct(fields) => {
                    fields.iter().all(|f| can_export_data_type(f.data_type()))
                }
                DataType::Map(field, _) => can_export_data_type(field.data_type()),

                // Keep these as separate milestones / avoid edge-case-heavy types for now.
                DataType::Union(_, _)
                | DataType::RunEndEncoded(_, _)
                | DataType::ListView(_)
                | DataType::LargeListView(_) => false,
            }
        }

        if !schema
            .fields()
            .iter()
            .all(|f| can_export_data_type(f.data_type()))
        {
            return false;
        }

        // Sanity check that arrow-rs can represent this schema via the C Data Interface.
        // This should succeed for supported types.
        FFI_ArrowSchema::try_from(schema.as_ref()).is_ok()
    }

    fn export_batches_to_c_stream(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> FFI_ArrowArrayStream {
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok::<_, ArrowError>), schema);

        FFI_ArrowArrayStream::new(Box::new(reader) as Box<dyn RecordBatchReader + Send>)
    }

    #[pyclass]
    struct ArrowCStreamWrapper {
        capsule: Option<Py<PyAny>>,
    }

    #[pymethods]
    impl ArrowCStreamWrapper {
        #[pyo3(signature = (_requested_schema=None))]
        fn __arrow_c_stream__(
            mut slf: PyRefMut<'_, Self>,
            _requested_schema: Option<Py<PyAny>>,
        ) -> PyResult<Py<PyAny>> {
            slf.capsule.take().ok_or_else(|| {
                PyRuntimeError::new_err("__arrow_c_stream__ may only be called once per object")
            })
        }
    }

    fn table_from_c_stream(py: Python<'_>, stream: FFI_ArrowArrayStream) -> PyResult<Py<PyAny>> {
        let name = CString::new("arrow_array_stream")
            .map_err(|_| PyRuntimeError::new_err("invalid capsule name"))?;

        // Capsule now owns `stream` (and therefore the underlying batch reader / buffers).
        let capsule = PyCapsule::new(py, stream, Some(name))?;

        let pa_mod = PyModule::import(py, "pyarrow").map_err(|e| {
            PyImportError::new_err(format!("pyarrow is required for Session.sql(...): {e}"))
        })?;

        let rbr = pa_mod.getattr("RecordBatchReader")?;
        // Prefer the public `RecordBatchReader.from_stream` API (PyCapsule Protocol).
        // Our capsule is a raw PyCapsule, so we wrap it in an object implementing `__arrow_c_stream__`.
        //
        // This avoids relying on PyArrow private methods like `_import_from_c_capsule`.
        let wrapper = Py::new(
            py,
            ArrowCStreamWrapper {
                capsule: Some(capsule.into_any().unbind()),
            },
        )?;
        let reader = rbr.getattr("from_stream")?.call1((wrapper,))?;

        let table_res = reader.call_method0("read_all");
        let close_res = reader.call_method0("close");

        match (table_res, close_res) {
            (Ok(table), Ok(_)) => Ok(table.into()),
            (Ok(table), Err(e_close)) => {
                // If the table was successfully read, treat a close() failure as non-fatal.
                // Falling back to IPC in auto mode would be wasteful (we already have the data).
                if std::env::var_os("TTF_SQL_EXPORT_DEBUG").is_some() {
                    let msg = format!(
                        "Session.sql: C Stream reader.close() failed after successful read_all(): {e_close}"
                    );
                    if let Ok(warnings) = PyModule::import(py, "warnings") {
                        let _ =
                            warnings.call_method1("warn", (msg, PyRuntimeWarning::type_object(py)));
                    } else {
                        eprintln!("{msg}");
                    }
                }
                Ok(table.into())
            }
            (Err(e), Ok(_)) => Err(e),
            (Err(e_read), Err(e_close)) => {
                // Preserve the primary failure (`read_all`) but do not silently discard a
                // cleanup failure.
                //
                // Prefer `BaseException.add_note` (Python 3.11+) so it shows up in the
                // traceback without changing the exception type. If it's unavailable,
                // only emit a debug warning when requested.
                let note = format!("Additionally, RecordBatchReader.close() failed: {e_close}");
                match e_read.value(py).call_method1("add_note", (note,)) {
                    Ok(_) => {}
                    Err(err) => {
                        if !err.is_instance_of::<PyAttributeError>(py)
                            && std::env::var_os("TTF_SQL_EXPORT_DEBUG").is_some()
                        {
                            let msg = format!(
                                "Session.sql: failed to attach exception note (close failure was: {e_close}): {err}"
                            );
                            if let Ok(warnings) = PyModule::import(py, "warnings") {
                                let _ = warnings
                                    .call_method1("warn", (msg, PyRuntimeWarning::type_object(py)));
                            } else {
                                eprintln!("{msg}");
                            }
                        } else if std::env::var_os("TTF_SQL_EXPORT_DEBUG").is_some() {
                            let msg = format!(
                                "Session.sql: C Stream reader.close() also failed: {e_close}"
                            );
                            if let Ok(warnings) = PyModule::import(py, "warnings") {
                                let _ = warnings
                                    .call_method1("warn", (msg, PyRuntimeWarning::type_object(py)));
                            } else {
                                eprintln!("{msg}");
                            }
                        }
                    }
                }

                Err(e_read)
            }
        }
    }

    fn ipc_bytes_from_batches(
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<Vec<u8>, ArrowError> {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut buf, schema)?;
            for batch in batches {
                w.write(batch)?;
            }
            w.finish()?;
        }
        Ok(buf)
    }

    #[pymethods]
    impl Session {
        #[new]
        /// Create a new DataFusion-backed SQL session.
        ///
        /// The session owns an internal Tokio runtime used to run async Rust internals.
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

        /// Register a time-series table under a name for SQL queries.
        ///
        /// Parameters
        /// ----------
        /// name:
        ///     SQL table name to register under.
        /// table_root:
        ///     Filesystem directory containing the table (created by `TimeSeriesTable.create`).
        ///
        /// Notes
        /// -----
        /// If `name` is already registered, it is replaced atomically (with rollback on failure).
        ///
        /// The table must have a canonical logical schema adopted (typically after the first
        /// successful append). If the table has never had data appended, registration may fail with
        /// a `DataFusionError`.
        ///
        /// Raises
        /// ------
        /// ValueError:
        ///     If `name` is empty.
        /// TimeseriesTableError:
        ///     If opening the table fails. The exception includes a `table_root` attribute.
        /// DataFusionError:
        ///     If provider creation or registration fails.
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

        /// Register a Parquet file or directory under a name for SQL queries.
        ///
        /// Parameters
        /// ----------
        /// name:
        ///     SQL table name to register under.
        /// path:
        ///     Filesystem path to a Parquet file or a directory of Parquet files.
        ///
        /// Notes
        /// -----
        /// If `name` is already registered, it is replaced atomically (with rollback on failure).
        ///
        /// Raises
        /// ------
        /// ValueError:
        ///     If `name` or `path` is empty.
        /// DataFusionError:
        ///     If registration fails. The exception includes `name` and `path` attributes.
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

        /// Run a SQL query and return the results as a `pyarrow.Table`.
        ///
        /// This method runs synchronously from Python, but uses an internal Tokio runtime and
        /// releases the GIL while planning/executing the query.
        ///
        /// Parameters
        /// ----------
        /// query:
        ///     SQL query string.
        /// params:
        ///     Optional query parameter values for DataFusion SQL placeholders:
        ///
        ///     - Positional: pass a list/tuple to bind `$1`, `$2`, ...
        ///       Example: `sess.sql("select * from t where x = $1", params=[1])`
        ///     - Named: pass a dict to bind `$name` placeholders (keys may optionally start with `$`).
        ///       Example: `sess.sql("select * from t where x = $a", params={"a": 1})`
        ///
        ///     Supported Python value types: `None`, `bool`, `int` (i64 range), `float`, `str`, `bytes`.
        ///
        /// Notes
        /// -----
        /// DataFusion infers placeholder types from context when possible (e.g. in `WHERE` clauses).
        /// If you use placeholders in a `SELECT` projection without type context, you may need an
        /// explicit cast, e.g. `SELECT CAST($1 AS BIGINT) AS x`.
        ///
        /// Raises
        /// ------
        /// ImportError:
        ///     If `pyarrow` cannot be imported.
        /// DataFusionError:
        ///     If the SQL fails to plan or execute.
        /// TypeError, ValueError:
        ///     If `params` has an invalid shape or contains unsupported value types.
        #[pyo3(signature = (query, *, params=None))]
        fn sql(
            &self,
            py: Python<'_>,
            query: String,
            params: Option<Py<PyAny>>,
        ) -> PyResult<Py<PyAny>> {
            enum SqlError {
                DataFusion(DFError),

                Runtime(&'static str),
            }

            let export_mode = SqlExportMode::from_env()?;
            let auto_rerun_fallback = env_var_truthy("TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK");

            let params = match params {
                None => None,
                Some(obj) => {
                    let bound = obj.bind(py);
                    Some(parse_query_params(bound)?)
                }
            };

            fn collect_sql<'py>(
                py: Python<'py>,
                rt: &tokio::runtime::Runtime,
                ctx: SessionContext,
                sema: Arc<tokio::sync::Semaphore>,
                query: String,
                params: Option<QueryParams>,
            ) -> PyResult<(SchemaRef, Vec<RecordBatch>)> {
                // Release GIL while planning/executing + collecting.
                tokio_runner::run_blocking_map_err(
                    py,
                    rt,
                    async move {
                        let _permit = sema
                            .acquire_owned()
                            .await
                            .map_err(|_| SqlError::Runtime("Session catalog semaphore closed"))?;

                        let mut df = ctx.sql(&query).await.map_err(SqlError::DataFusion)?;

                        if let Some(p) = params {
                            df = match p {
                                QueryParams::Positional(v) => df.with_param_values(v),
                                QueryParams::Named(v) => df.with_param_values(v),
                            }
                            .map_err(SqlError::DataFusion)?;
                        }

                        let schema = df.schema().as_arrow().clone();
                        let batches = df.collect().await.map_err(SqlError::DataFusion)?;

                        Ok::<(SchemaRef, Vec<RecordBatch>), SqlError>((schema.into(), batches))
                    },
                    move |py, err| match err {
                        SqlError::DataFusion(e) => datafusion_error_to_py(py, e),
                        SqlError::Runtime(msg) => PyRuntimeError::new_err(msg),
                    },
                )
            }

            let rt = Arc::clone(&self.rt);
            let ctx = self.ctx.clone();
            let sema = Arc::clone(&self.catalog_sema);

            let (schema, batches): (SchemaRef, Vec<RecordBatch>) = collect_sql(
                py,
                rt.as_ref(),
                ctx.clone(),
                Arc::clone(&sema),
                query.clone(),
                params.clone(),
            )?;

            let schema_ok = can_export_schema_to_c_stream(&schema);

            // Fast paths: forced mode doesn't need to preserve data for IPC fallback.
            match export_mode {
                SqlExportMode::Ipc => {}
                SqlExportMode::CStream => {
                    if !schema_ok {
                        return Err(PyRuntimeError::new_err(
                            "Session.sql: schema cannot be exported via Arrow C Stream (unsupported type)",
                        ));
                    }

                    let stream = export_batches_to_c_stream(schema, batches);
                    return table_from_c_stream(py, stream);
                }
                SqlExportMode::Auto => {
                    if schema_ok {
                        if auto_rerun_fallback {
                            // Avoid cloning on the hot path. If C Stream import fails, re-run the
                            // query for the IPC fallback path (may change results for non-deterministic queries).
                            let stream = export_batches_to_c_stream(schema, batches);
                            match table_from_c_stream(py, stream) {
                                Ok(table) => return Ok(table),
                                Err(e) => {
                                    if std::env::var_os("TTF_SQL_EXPORT_DEBUG").is_some() {
                                        let msg = format!(
                                            "Session.sql: C Stream path failed, re-running query for IPC fallback: {e}"
                                        );
                                        if let Ok(warnings) = PyModule::import(py, "warnings") {
                                            let _ = warnings.call_method1(
                                                "warn",
                                                (msg, PyRuntimeWarning::type_object(py)),
                                            );
                                        } else {
                                            eprintln!("{msg}");
                                        }
                                    }

                                    let (schema, batches) = collect_sql(
                                        py,
                                        rt.as_ref(),
                                        ctx.clone(),
                                        Arc::clone(&sema),
                                        query.clone(),
                                        params.clone(),
                                    )?;

                                    // IPC fallback path (still release GIL for encoding).
                                    let ipc_bytes: Vec<u8> = py
                                        .detach(move || ipc_bytes_from_batches(&schema, &batches))
                                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

                                    let ipc_mod =
                                        PyModule::import(py, "pyarrow.ipc").map_err(|e| {
                                            PyImportError::new_err(format!(
                                                "pyarrow is required for Session.sql(...): {e}"
                                            ))
                                        })?;

                                    let b = PyBytes::new(py, &ipc_bytes);
                                    let reader = ipc_mod.getattr("open_stream")?.call1((b,))?;
                                    let table = reader.call_method0("read_all")?;

                                    return Ok(table.into());
                                }
                            }
                        } else {
                            // Preserve data for the IPC fallback path by cloning (Arc-backed; no buffer copies).
                            let stream =
                                export_batches_to_c_stream(schema.clone(), batches.clone());
                            match table_from_c_stream(py, stream) {
                                Ok(table) => return Ok(table),
                                Err(e) => {
                                    if std::env::var_os("TTF_SQL_EXPORT_DEBUG").is_some() {
                                        let msg = format!(
                                            "Session.sql: C Stream path failed, falling back to IPC: {e}"
                                        );
                                        if let Ok(warnings) = PyModule::import(py, "warnings") {
                                            let _ = warnings.call_method1(
                                                "warn",
                                                (msg, PyRuntimeWarning::type_object(py)),
                                            );
                                        } else {
                                            eprintln!("{msg}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // IPC fallback path (still release GIL for encoding).
            let ipc_bytes: Vec<u8> = py
                .detach(move || ipc_bytes_from_batches(&schema, &batches))
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let ipc_mod = PyModule::import(py, "pyarrow.ipc").map_err(|e| {
                PyImportError::new_err(format!("pyarrow is required for Session.sql(...): {e}"))
            })?;

            let b = PyBytes::new(py, &ipc_bytes);
            let reader = ipc_mod.getattr("open_stream")?.call1((b,))?;
            let table = reader.call_method0("read_all")?;

            Ok(table.into())
        }

        /// Return the list of currently registered table names (sorted).
        fn tables(&self, py: Python<'_>) -> PyResult<Vec<String>> {
            enum TablesError {
                Runtime(&'static str),
            }

            let sema = Arc::clone(&self.catalog_sema);
            let tables = &self.tables;

            tokio_runner::run_blocking_map_err(
                py,
                self.rt.as_ref(),
                async move {
                    let _permit = sema
                        .acquire_owned()
                        .await
                        .map_err(|_| TablesError::Runtime("Session catalog semaphore closed"))?;

                    let t = tables
                        .lock()
                        .map_err(|_| TablesError::Runtime("Session tables lock poisoned"))?;

                    Ok::<Vec<String>, TablesError>(t.iter().cloned().collect())
                },
                move |_py, err| match err {
                    TablesError::Runtime(msg) => PyRuntimeError::new_err(msg),
                },
            )
        }

        /// Deregister a previously registered table name.
        ///
        /// Raises
        /// ------
        /// KeyError:
        ///     If `name` is not registered.
        /// ValueError:
        ///     If `name` is empty.
        fn deregister(&self, py: Python<'_>, name: String) -> PyResult<()> {
            enum DeregisterError {
                NotFound(String),
                Invariant(&'static str),
                DataFusion(DFError),
                Runtime(&'static str),
            }

            if name.is_empty() {
                return Err(PyValueError::new_err("name must be non-empty"));
            }

            let ctx = self.ctx.clone();
            let sema = Arc::clone(&self.catalog_sema);
            let tables = &self.tables;

            tokio_runner::run_blocking_map_err(
                py,
                self.rt.as_ref(),
                async move {
                    let _permit = sema.acquire_owned().await.map_err(|_| {
                        DeregisterError::Runtime("Session catalog semaphore closed")
                    })?;

                    {
                        let t = tables.lock().map_err(|_| {
                            DeregisterError::Runtime("Session tables lock poisoned")
                        })?;

                        if !t.contains(name.as_str()) {
                            return Err(DeregisterError::NotFound(name));
                        }
                    }

                    let removed = ctx
                        .deregister_table(name.as_str())
                        .map_err(DeregisterError::DataFusion)?;

                    if removed.is_none() {
                        return Err(DeregisterError::Invariant(
                            "invariant violation: table tracked as registered, but DataFusion had no registration",
                        ));
                    }

                    let mut t = tables
                        .lock()
                        .map_err(|_| DeregisterError::Runtime("Session tables lock poisoned"))?;
                    if !t.remove(name.as_str()) {
                        return Err(DeregisterError::Invariant(
                            "invariant violation: table existed during check but could not be removed from tracked set",
                        ));
                    }

                    Ok::<(), DeregisterError>(())
                },
                move |py, err| match err {
                    DeregisterError::NotFound(n) => PyKeyError::new_err(n),
                    DeregisterError::Invariant(msg) => PyRuntimeError::new_err(msg),
                    DeregisterError::DataFusion(e) => datafusion_error_to_py(py, e),
                    DeregisterError::Runtime(msg) => PyRuntimeError::new_err(msg),
                },
            )
        }
    }

    /// Local filesystem time-series table rooted at `table_root`.
    ///
    /// Use `TimeSeriesTable` for table lifecycle operations (create/open/append Parquet). For SQL
    /// querying across one or more registered tables, use `Session`.
    ///
    /// Appends are overlap-checked according to the table's time bucket configuration.
    #[pyclass]
    struct TimeSeriesTable {
        inner: timeseries_table_core::table::TimeSeriesTable,
        table_root: String,
    }

    #[pymethods]
    impl TimeSeriesTable {
        #[classmethod]
        #[pyo3(signature = (*, table_root, time_column, bucket, entity_columns=None, timezone=None))]
        /// Create a new time-series table at `table_root`.
        ///
        /// Parameters
        /// ----------
        /// table_root:
        ///     Filesystem directory where the table will be created.
        /// time_column:
        ///     Name of the timestamp column.
        /// bucket:
        ///     Time bucket specification string such as `"1h"`, `"5m"`, `"30s"`, `"1d"`.
        /// entity_columns:
        ///     Column names that define the entity identity for this table. For v0, a table is
        ///     effectively scoped to a single entity identity; all appended segments must match the
        ///     entity values established by the first successful append.
        /// timezone:
        ///     Optional timezone name for bucketing; `None` means no timezone normalization.
        ///
        /// Notes
        /// -----
        /// The table's canonical schema is typically adopted on the first successful append.
        ///
        /// Raises
        /// ------
        /// TimeseriesTableError:
        ///     If creation fails. The exception includes a `table_root` attribute.
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
        /// Open an existing time-series table at `table_root`.
        ///
        /// Raises
        /// ------
        /// TimeseriesTableError:
        ///     If opening fails. The exception includes a `table_root` attribute.
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

        /// Return the table root path.
        fn root(&self) -> String {
            self.table_root.clone()
        }

        /// Return the current table version.
        fn version(&self) -> u64 {
            self.inner.state().version
        }

        /// Return the index specification as a Python dict.
        ///
        /// Keys: `timestamp_column`, `entity_columns`, `bucket`, `timezone`.
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
        /// Append a Parquet segment to the table.
        ///
        /// Parameters
        /// ----------
        /// parquet_path:
        ///     Path to a Parquet file.
        /// time_column:
        ///     Optional override for the timestamp column name in the Parquet file.
        /// copy_if_outside:
        ///     If `True`, copies the file under the table root before appending.
        ///     If `False`, the path must already be under the table root (parent traversal via
        ///     `..` is rejected).
        ///
        /// Returns
        /// -------
        /// int
        ///     The new table version after the append.
        ///
        /// Raises
        /// ------
        /// CoverageOverlapError:
        ///     If the segment overlaps existing coverage.
        /// SchemaMismatchError:
        ///     If the segment schema does not match the table schema.
        /// TimeseriesTableError:
        ///     For other table errors. The exception includes a `table_root` attribute.
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
                                        "parquet_path must not contain '..' when copy_if_outside=False (parquet_path={parquet_path:?}, table_root={table_root_for_err})"
                                    )));
                                }
                                src_path.to_path_buf()
                            }
                            None => {
                                return Err(AppendParquetError::ValueError(format!(
                                    "parquet_path must be under table_root when copy_if_outside=False (parquet_path={parquet_path:?}, table_root={table_root_for_err})"
                                )));
                            }
                        };

                        if rel.as_os_str().is_empty() {
                            return Err(AppendParquetError::ValueError(format!(
                                "parquet_path must point to a file under table_root, not the root itself (parquet_path={parquet_path:?}, table_root={table_root_for_err})"
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

    /// Benchmark helper: run a SQL query and return Arrow IPC stream bytes plus Rust-side timing
    /// and sizing metrics.
    ///
    /// This is intentionally feature-gated and exported only under `_native._testing` to avoid
    /// committing to a stable public API surface.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    #[pyo3(signature = (session, query, *, ipc_compression="none"))]
    fn _bench_sql_ipc<'py>(
        py: Python<'py>,
        session: PyRef<'_, Session>,
        query: String,
        ipc_compression: &str,
    ) -> PyResult<(Bound<'py, PyBytes>, Bound<'py, PyDict>)> {
        use std::time::Instant;

        enum BenchSqlIpcError {
            DataFusion(DFError),
            Arrow(ArrowError),
            Runtime(&'static str),
        }

        let compression = ipc_compression.trim().to_ascii_lowercase();
        let (compression_label, compression_type): (String, Option<arrow_ipc::CompressionType>) =
            match compression.as_str() {
                "" | "none" => ("none".to_string(), None),
                "zstd" => {
                    #[cfg(feature = "ipc-zstd")]
                    {
                        ("zstd".to_string(), Some(arrow_ipc::CompressionType::ZSTD))
                    }
                    #[cfg(not(feature = "ipc-zstd"))]
                    {
                        return Err(PyValueError::new_err(
                            "ipc_compression='zstd' requires building the extension with --features ipc-zstd",
                        ));
                    }
                }
                other => {
                    return Err(PyValueError::new_err(format!(
                        "invalid ipc_compression={other:?}; expected 'none' or 'zstd'"
                    )));
                }
            };

        let rt = Arc::clone(&session.rt);
        let ctx = session.ctx.clone();
        let sema = Arc::clone(&session.catalog_sema);
        let query = query.to_string();

        struct BenchResult {
            ipc_bytes: Vec<u8>,
            plan_ms: f64,
            collect_ms: f64,
            ipc_encode_ms: f64,
            total_ms: f64,
            arrow_mem_bytes: usize,
            row_count: usize,
            batch_count: usize,
        }

        let result: BenchResult = tokio_runner::run_blocking_map_err(
            py,
            rt.as_ref(),
            async move {
                let t_total = Instant::now();

                let _permit = sema
                    .acquire_owned()
                    .await
                    .map_err(|_| BenchSqlIpcError::Runtime("Session catalog semaphore closed"))?;

                let t_plan = Instant::now();
                let df = ctx
                    .sql(&query)
                    .await
                    .map_err(BenchSqlIpcError::DataFusion)?;
                let plan_ms = t_plan.elapsed().as_secs_f64() * 1000.0;

                let schema: SchemaRef = df.schema().as_arrow().clone().into();

                let t_collect = Instant::now();
                let batches = df.collect().await.map_err(BenchSqlIpcError::DataFusion)?;
                let collect_ms = t_collect.elapsed().as_secs_f64() * 1000.0;

                let arrow_mem_bytes: usize =
                    batches.iter().map(|b| b.get_array_memory_size()).sum();
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                let batch_count = batches.len();

                let t_ipc = Instant::now();
                let mut buf: Vec<u8> = Vec::new();
                {
                    let mut write_options = arrow_ipc::writer::IpcWriteOptions::default();
                    if let Some(ct) = compression_type {
                        write_options = write_options
                            .try_with_compression(Some(ct))
                            .map_err(BenchSqlIpcError::Arrow)?;
                    }

                    let mut w = arrow_ipc::writer::StreamWriter::try_new_with_options(
                        &mut buf,
                        &schema,
                        write_options,
                    )
                    .map_err(BenchSqlIpcError::Arrow)?;
                    for batch in &batches {
                        w.write(batch).map_err(BenchSqlIpcError::Arrow)?;
                    }
                    w.finish().map_err(BenchSqlIpcError::Arrow)?;
                }
                let ipc_encode_ms = t_ipc.elapsed().as_secs_f64() * 1000.0;

                let total_ms = t_total.elapsed().as_secs_f64() * 1000.0;

                Ok::<BenchResult, BenchSqlIpcError>(BenchResult {
                    ipc_bytes: buf,
                    plan_ms,
                    collect_ms,
                    ipc_encode_ms,
                    total_ms,
                    arrow_mem_bytes,
                    row_count,
                    batch_count,
                })
            },
            move |py, err| match err {
                BenchSqlIpcError::DataFusion(e) => datafusion_error_to_py(py, e),
                BenchSqlIpcError::Arrow(e) => PyRuntimeError::new_err(e.to_string()),
                BenchSqlIpcError::Runtime(msg) => PyRuntimeError::new_err(msg),
            },
        )?;

        let metrics = PyDict::new(py);
        metrics.set_item("ipc_compression", compression_label)?;
        metrics.set_item("ipc_bytes_len", result.ipc_bytes.len())?;
        metrics.set_item("arrow_mem_bytes", result.arrow_mem_bytes)?;
        metrics.set_item("row_count", result.row_count)?;
        metrics.set_item("batch_count", result.batch_count)?;
        metrics.set_item("plan_ms", result.plan_ms)?;
        metrics.set_item("collect_ms", result.collect_ms)?;
        metrics.set_item("ipc_encode_ms", result.ipc_encode_ms)?;
        metrics.set_item("total_ms", result.total_ms)?;

        let b = PyBytes::new(py, &result.ipc_bytes);
        Ok((b, metrics))
    }

    /// Benchmark helper: run a SQL query and return an Arrow C Stream capsule plus Rust-side timing
    /// and sizing metrics.
    ///
    /// Python usage:
    ///
    /// - `capsule, m = ttf._native._testing._bench_sql_c_stream(sess, sql)`
    /// - `reader = pyarrow.RecordBatchReader.from_stream(_Wrapper(capsule))`
    /// - `table = reader.read_all(); reader.close()`
    ///
    /// Note: the returned capsule must remain alive until `reader.close()` completes.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    fn _bench_sql_c_stream<'py>(
        py: Python<'py>,
        session: PyRef<'_, Session>,
        query: String,
    ) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyDict>)> {
        use std::time::Instant;

        enum BenchSqlCStreamError {
            DataFusion(DFError),
            Runtime(&'static str),
        }

        let rt = Arc::clone(&session.rt);
        let ctx = session.ctx.clone();
        let sema = Arc::clone(&session.catalog_sema);
        let query = query.to_string();

        struct BenchResult {
            stream: FFI_ArrowArrayStream,
            plan_ms: f64,
            collect_ms: f64,
            c_stream_export_ms: f64,
            total_ms: f64,
            arrow_mem_bytes: usize,
            row_count: usize,
            batch_count: usize,
        }

        let result: BenchResult = tokio_runner::run_blocking_map_err(
            py,
            rt.as_ref(),
            async move {
                let t_total = Instant::now();

                let _permit = sema.acquire_owned().await.map_err(|_| {
                    BenchSqlCStreamError::Runtime("Session catalog semaphore closed")
                })?;

                let t_plan = Instant::now();
                let df = ctx
                    .sql(&query)
                    .await
                    .map_err(BenchSqlCStreamError::DataFusion)?;
                let plan_ms = t_plan.elapsed().as_secs_f64() * 1000.0;

                let schema: SchemaRef = df.schema().as_arrow().clone().into();

                let t_collect = Instant::now();
                let batches = df
                    .collect()
                    .await
                    .map_err(BenchSqlCStreamError::DataFusion)?;
                let collect_ms = t_collect.elapsed().as_secs_f64() * 1000.0;

                let arrow_mem_bytes: usize =
                    batches.iter().map(|b| b.get_array_memory_size()).sum();
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                let batch_count = batches.len();

                let t_export = Instant::now();
                let stream = export_batches_to_c_stream(schema, batches);
                let c_stream_export_ms = t_export.elapsed().as_secs_f64() * 1000.0;

                let total_ms = t_total.elapsed().as_secs_f64() * 1000.0;

                Ok::<BenchResult, BenchSqlCStreamError>(BenchResult {
                    stream,
                    plan_ms,
                    collect_ms,
                    c_stream_export_ms,
                    total_ms,
                    arrow_mem_bytes,
                    row_count,
                    batch_count,
                })
            },
            move |py, err| match err {
                BenchSqlCStreamError::DataFusion(e) => datafusion_error_to_py(py, e),
                BenchSqlCStreamError::Runtime(msg) => PyRuntimeError::new_err(msg),
            },
        )?;

        let name = CString::new("arrow_array_stream")
            .map_err(|_| PyValueError::new_err("invalid capsule name"))?;
        let capsule = PyCapsule::new(py, result.stream, Some(name))?;

        let metrics = PyDict::new(py);
        metrics.set_item("arrow_mem_bytes", result.arrow_mem_bytes)?;
        metrics.set_item("row_count", result.row_count)?;
        metrics.set_item("batch_count", result.batch_count)?;
        metrics.set_item("plan_ms", result.plan_ms)?;
        metrics.set_item("collect_ms", result.collect_ms)?;
        metrics.set_item("c_stream_export_ms", result.c_stream_export_ms)?;
        metrics.set_item("total_ms", result.total_ms)?;

        Ok((capsule, metrics))
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
            testing.add_function(pyo3::wrap_pyfunction!(_bench_sql_ipc, py)?)?;
            testing.add_function(pyo3::wrap_pyfunction!(_bench_sql_c_stream, py)?)?;
            m.add("_testing", &testing)?;
            m.add_submodule(&testing)?;
        }

        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::SqlExportMode;
        use pyo3::Py;
        use pyo3::Python;
        use std::ffi::OsString;
        use std::sync::{Mutex, MutexGuard, Once, OnceLock};

        fn init_python() {
            static ONCE: Once = Once::new();
            ONCE.call_once(Python::initialize);
        }

        fn env_lock() -> &'static Mutex<()> {
            static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
            LOCK.get_or_init(|| Mutex::new(()))
        }

        struct EnvGuard {
            _lock: MutexGuard<'static, ()>,
            key: &'static str,
            old: Option<OsString>,
        }

        impl EnvGuard {
            fn set(key: &'static str, value: Option<OsString>) -> Self {
                let lock = match env_lock().lock() {
                    Ok(g) => g,
                    Err(e) => e.into_inner(),
                };
                let old = std::env::var_os(key);
                match value {
                    None => unsafe { std::env::remove_var(key) },
                    Some(v) => unsafe { std::env::set_var(key, v) },
                }
                Self {
                    _lock: lock,
                    key,
                    old,
                }
            }
        }

        impl Drop for EnvGuard {
            fn drop(&mut self) {
                match self.old.take() {
                    None => unsafe { std::env::remove_var(self.key) },
                    Some(v) => unsafe { std::env::set_var(self.key, v) },
                }
            }
        }

        #[test]
        fn sql_export_mode_defaults_to_c_stream_when_unset() {
            init_python();
            let _g = EnvGuard::set("TTF_SQL_EXPORT_MODE", None);
            assert!(matches!(
                SqlExportMode::from_env(),
                Ok(SqlExportMode::CStream)
            ));
        }

        #[test]
        fn sql_export_mode_trims_and_is_case_insensitive() {
            init_python();

            {
                let _g = EnvGuard::set("TTF_SQL_EXPORT_MODE", Some(OsString::from("  IPC  ")));
                assert!(matches!(SqlExportMode::from_env(), Ok(SqlExportMode::Ipc)));
            }

            {
                let _g = EnvGuard::set("TTF_SQL_EXPORT_MODE", Some(OsString::from("C-STREAM")));
                assert!(matches!(
                    SqlExportMode::from_env(),
                    Ok(SqlExportMode::CStream)
                ));
            }
        }

        #[test]
        fn sql_export_mode_rejects_invalid_values() {
            init_python();
            let _g = EnvGuard::set("TTF_SQL_EXPORT_MODE", Some(OsString::from("nope")));

            let msg = match SqlExportMode::from_env() {
                Ok(v) => unreachable!("expected error, got {v:?}"),
                Err(e) => e.to_string(),
            };

            assert!(msg.contains("TTF_SQL_EXPORT_MODE"));
            assert!(msg.contains("auto"));
            assert!(msg.contains("ipc"));
            assert!(msg.contains("c_stream"));
        }

        #[test]
        #[cfg(unix)]
        fn sql_export_mode_rejects_non_unicode() {
            use std::os::unix::ffi::OsStringExt;

            init_python();
            let _g = EnvGuard::set("TTF_SQL_EXPORT_MODE", Some(OsString::from_vec(vec![0xFF])));
            let msg = match SqlExportMode::from_env() {
                Ok(v) => unreachable!("expected error, got {v:?}"),
                Err(e) => e.to_string(),
            };
            assert!(msg.contains("valid unicode"));
        }

        #[test]
        fn c_stream_schema_support_rejects_union() {
            use datafusion::arrow::datatypes::{DataType, Field, Schema, UnionFields, UnionMode};
            use std::sync::Arc;

            let uf = UnionFields::try_new(
                vec![1, 3],
                vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("b", DataType::Utf8, true),
                ],
            )
            .unwrap();

            let schema = Arc::new(Schema::new(vec![Field::new(
                "u",
                DataType::Union(uf, UnionMode::Dense),
                true,
            )]));

            assert!(!super::can_export_schema_to_c_stream(&schema));
        }

        #[test]
        fn env_var_truthy_parses_common_values() {
            init_python();

            {
                let _g = EnvGuard::set("TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK", None);
                assert!(!super::env_var_truthy("TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK"));
            }
            {
                let _g = EnvGuard::set(
                    "TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK",
                    Some(OsString::from("0")),
                );
                assert!(!super::env_var_truthy("TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK"));
            }
            {
                let _g = EnvGuard::set(
                    "TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK",
                    Some(OsString::from("false")),
                );
                assert!(!super::env_var_truthy("TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK"));
            }
            {
                let _g = EnvGuard::set(
                    "TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK",
                    Some(OsString::from("1")),
                );
                assert!(super::env_var_truthy("TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK"));
            }
            {
                let _g = EnvGuard::set(
                    "TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK",
                    Some(OsString::from("yes")),
                );
                assert!(super::env_var_truthy("TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK"));
            }
        }

        #[test]
        fn arrow_c_stream_wrapper_is_single_use() {
            use pyo3::types::PyAnyMethods;
            use pyo3::types::PyCapsule;
            use std::ffi::CString;

            init_python();
            let ok = Python::try_attach(|py| {
                let name = CString::new("arrow_array_stream").unwrap();
                let capsule = PyCapsule::new(py, 123usize, Some(name)).unwrap();

                let wrapper = Py::new(
                    py,
                    super::ArrowCStreamWrapper {
                        capsule: Some(capsule.into_any().unbind()),
                    },
                )
                .unwrap();

                let wrapper = wrapper.bind(py);
                wrapper.call_method0("__arrow_c_stream__").unwrap();

                let err = wrapper.call_method0("__arrow_c_stream__").unwrap_err();
                let msg = err.to_string();
                assert!(msg.contains("only be called once"));
            });
            assert!(ok.is_some());
        }
    }
}
