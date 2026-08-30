//! Python bindings for timeseries-table-format (v0 skeleton).
mod error_map;
mod exceptions;
mod python_logging;
#[allow(dead_code)]
mod sql_stream_reader;
mod tokio_runner;

#[pyo3::pymodule]
mod _native {

    use std::collections::BTreeSet;
    use std::ffi::{c_char, c_void};
    use std::sync::{Arc, Mutex};

    use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
    use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};

    use chrono::{DateTime, Utc};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::arrow::error::ArrowError;
    use datafusion::common::ScalarValue;
    use datafusion::error::DataFusionError as DFError;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

    use pyo3::PyAny;
    use pyo3::types::{PyCapsule, PyCapsuleMethods};
    use pyo3::{
        Bound, PyErr, PyResult, PyTypeInfo, Python,
        exceptions::{
            PyAttributeError, PyException, PyImportError, PyKeyError, PyNotImplementedError,
            PyRuntimeError, PyRuntimeWarning, PyTypeError, PyValueError,
        },
        prelude::*,
        pyclass, pymethods,
        types::{PyBytes, PyDateTime, PyDict, PyList, PyModule, PyTuple, PyType},
    };

    use timeseries_table_format::{
        AppendRequest, ParquetCompression,
        datafusion::TsTableProvider,
        table::{
            OptimizeReport as CoreOptimizeReport, VacuumArtifact as CoreVacuumArtifact,
            VacuumArtifactReason as CoreVacuumArtifactReason, VacuumError as CoreVacuumError,
            VacuumMode as CoreVacuumMode, VacuumReport as CoreVacuumReport,
        },
    };

    use crate::error_map::{datafusion_error_to_py, storage_error_to_py};
    use crate::sql_stream_reader::SqlStreamRecordBatchReader;
    use crate::{
        exceptions::{
            ConflictError, DataFusionError, DuplicateIndexIntervalError, IndexIntervalOverlapError,
            SchemaMismatchError, StorageError, TimeseriesTableError, VacuumApplyError,
        },
        tokio_runner,
    };

    /// Refresh cached native logging levels after changing Python logging configuration.
    #[pyfunction]
    fn refresh_logging_cache() {
        crate::python_logging::refresh_cache();
    }

    enum RegisterTsTableError {
        Table(timeseries_table_format::table::TableError),
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
                // Treat non-unicode environment values as falsy. This avoids surprising behavior
                // where `to_string_lossy()` would replace invalid bytes with � and then interpret
                // the value as truthy.
                let Some(s) = v.to_str() else {
                    return false;
                };
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

    fn py_error_with_table_root(
        py: Python<'_>,
        table_root: &str,
        message: String,
        py_err: PyErr,
    ) -> PyErr {
        let exc = py_err.value(py);

        if let Err(e) = exc.setattr("table_root", table_root.to_string()) {
            return e;
        }

        let msg = format!("{message} (table_root={table_root})");
        if let Err(e) = exc.setattr("args", (msg,)) {
            return e;
        }

        py_err
    }

    fn table_error_to_py_with_root(
        py: Python<'_>,
        table_root: &str,
        entity_columns: &[String],
        err: timeseries_table_format::table::TableError,
    ) -> PyErr {
        let message = err.to_string();
        let py_err = crate::error_map::table_error_to_py(py, err, entity_columns);
        py_error_with_table_root(py, table_root, message, py_err)
    }

    fn storage_error_to_py_with_root(
        py: Python<'_>,
        table_root: &str,
        err: timeseries_table_format::storage::StorageError,
    ) -> PyErr {
        let message = err.to_string();
        let py_err = storage_error_to_py(py, &err);
        py_error_with_table_root(py, table_root, message, py_err)
    }

    /// Own the imported stream behind callbacks that always provide an error message.
    ///
    /// The Arrow C Stream contract permits a null error description, but arrow-rs 59.2.0
    /// unwraps it. Every callback below is installed together, and `private_data` remains a valid
    /// boxed adapter until the release callback clears and drops it.
    struct ArrowStreamErrorMessageAdapter {
        source: FFI_ArrowArrayStream,
    }

    // Only malformed callback invocations reach this path; any nonzero value reports failure.
    const INVALID_ARROW_STREAM_CALLBACK: i32 = 1;
    const ARROW_STREAM_ERROR_WITHOUT_DETAILS: &std::ffi::CStr =
        c"Arrow C Stream operation failed without error details";

    unsafe extern "C" fn error_message_adapter_get_schema(
        stream: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowSchema,
    ) -> i32 {
        let Some(stream) = (unsafe { stream.as_mut() }) else {
            return INVALID_ARROW_STREAM_CALLBACK;
        };
        let Some(adapter) = (unsafe {
            stream
                .private_data
                .cast::<ArrowStreamErrorMessageAdapter>()
                .as_mut()
        }) else {
            return INVALID_ARROW_STREAM_CALLBACK;
        };
        let Some(get_schema) = adapter.source.get_schema else {
            return INVALID_ARROW_STREAM_CALLBACK;
        };

        unsafe { get_schema(&mut adapter.source, out) }
    }

    unsafe extern "C" fn error_message_adapter_get_next(
        stream: *mut FFI_ArrowArrayStream,
        out: *mut FFI_ArrowArray,
    ) -> i32 {
        let Some(stream) = (unsafe { stream.as_mut() }) else {
            return INVALID_ARROW_STREAM_CALLBACK;
        };
        let Some(adapter) = (unsafe {
            stream
                .private_data
                .cast::<ArrowStreamErrorMessageAdapter>()
                .as_mut()
        }) else {
            return INVALID_ARROW_STREAM_CALLBACK;
        };
        let Some(get_next) = adapter.source.get_next else {
            return INVALID_ARROW_STREAM_CALLBACK;
        };

        unsafe { get_next(&mut adapter.source, out) }
    }

    unsafe extern "C" fn error_message_adapter_get_last_error(
        stream: *mut FFI_ArrowArrayStream,
    ) -> *const c_char {
        let Some(stream) = (unsafe { stream.as_mut() }) else {
            return ARROW_STREAM_ERROR_WITHOUT_DETAILS.as_ptr();
        };
        let Some(adapter) = (unsafe {
            stream
                .private_data
                .cast::<ArrowStreamErrorMessageAdapter>()
                .as_mut()
        }) else {
            return ARROW_STREAM_ERROR_WITHOUT_DETAILS.as_ptr();
        };
        let Some(get_last_error) = adapter.source.get_last_error else {
            return ARROW_STREAM_ERROR_WITHOUT_DETAILS.as_ptr();
        };
        let message = unsafe { get_last_error(&mut adapter.source) };
        if message.is_null() {
            ARROW_STREAM_ERROR_WITHOUT_DETAILS.as_ptr()
        } else {
            message
        }
    }

    unsafe extern "C" fn release_error_message_adapter(stream: *mut FFI_ArrowArrayStream) {
        let Some(stream) = (unsafe { stream.as_mut() }) else {
            return;
        };
        if stream.release.is_none() {
            return;
        }

        stream.get_schema = None;
        stream.get_next = None;
        stream.get_last_error = None;
        stream.release = None;
        let private_data = std::mem::replace(&mut stream.private_data, std::ptr::null_mut());
        if !private_data.is_null() {
            drop(unsafe { Box::from_raw(private_data.cast::<ArrowStreamErrorMessageAdapter>()) });
        }
    }

    fn ensure_arrow_stream_error_messages(
        source: FFI_ArrowArrayStream,
    ) -> Result<FFI_ArrowArrayStream, &'static str> {
        if source.release.is_none() {
            return Err("input stream is already released");
        }
        if source.get_schema.is_none() {
            return Err("input stream has no get_schema callback");
        }
        if source.get_next.is_none() {
            return Err("input stream has no get_next callback");
        }

        Ok(FFI_ArrowArrayStream {
            get_schema: Some(error_message_adapter_get_schema),
            get_next: Some(error_message_adapter_get_next),
            get_last_error: Some(error_message_adapter_get_last_error),
            release: Some(release_error_message_adapter),
            private_data: Box::into_raw(Box::new(ArrowStreamErrorMessageAdapter { source }))
                .cast::<c_void>(),
        })
    }

    fn record_batch_reader_from_python(
        source: &Bound<'_, PyAny>,
    ) -> PyResult<ArrowArrayStreamReader> {
        let py = source.py();
        let exporter = source.getattr("__arrow_c_stream__").map_err(|error| {
            if error.is_instance_of::<PyAttributeError>(py) {
                PyTypeError::new_err(
                    "source must be a pyarrow.RecordBatch, pyarrow.Table, \
                     pyarrow.RecordBatchReader, or an object implementing __arrow_c_stream__",
                )
            } else {
                error
            }
        })?;
        let capsule = exporter.call0().map_err(|error| {
            if !error.is_instance_of::<PyException>(py) {
                return error;
            }
            let mapped = PyValueError::new_err("source.__arrow_c_stream__() failed");
            mapped.set_cause(py, Some(error));
            mapped
        })?;
        let capsule = capsule.cast::<PyCapsule>().map_err(|error| {
            PyValueError::new_err(format!(
                "source.__arrow_c_stream__() must return an Arrow C Stream capsule: {error}"
            ))
        })?;
        let stream_pointer = capsule
            .pointer_checked(Some(c"arrow_array_stream"))
            .map_err(|error| {
                let mapped = PyValueError::new_err("invalid Arrow C Stream capsule");
                mapped.set_cause(py, Some(error));
                mapped
            })?
            .cast::<FFI_ArrowArrayStream>();

        // SAFETY: the Arrow PyCapsule protocol requires a capsule with this checked name to point
        // to a valid, aligned, initialized FFI_ArrowArrayStream. `from_raw` moves the stream and
        // replaces the capsule's value with a released stream.
        let stream = unsafe { FFI_ArrowArrayStream::from_raw(stream_pointer.as_ptr()) };
        let stream = ensure_arrow_stream_error_messages(stream).map_err(|error| {
            PyValueError::new_err(format!("failed to import Arrow C Stream: {error}"))
        })?;

        ArrowArrayStreamReader::try_new(stream).map_err(|error| {
            PyValueError::new_err(format!("failed to import Arrow C Stream: {error}"))
        })
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

    fn export_stream_to_c_stream(
        rt: &tokio::runtime::Runtime,
        stream: SendableRecordBatchStream,
    ) -> Result<FFI_ArrowArrayStream, ArrowError> {
        let reader = SqlStreamRecordBatchReader::spawn(rt, stream)?;
        Ok(FFI_ArrowArrayStream::new(
            Box::new(reader) as Box<dyn RecordBatchReader + Send>
        ))
    }

    fn pyarrow_record_batch_reader_from_c_stream(
        py: Python<'_>,
        stream: FFI_ArrowArrayStream,
        api_name: &str,
    ) -> PyResult<Py<PyAny>> {
        let capsule = PyCapsule::new_with_value(py, stream, c"arrow_array_stream")?;

        let pyarrow = PyModule::import(py, "pyarrow").map_err(|error| {
            PyImportError::new_err(format!(
                "pyarrow is required for Session.{api_name}(...): {error}"
            ))
        })?;

        let record_batch_reader = pyarrow.getattr("RecordBatchReader")?;
        let wrapper = Py::new(
            py,
            ArrowCStreamWrapper {
                capsule: Some(capsule.into_any().unbind()),
            },
        )?;

        let from_stream = match record_batch_reader.getattr("from_stream") {
            Ok(from_stream) => from_stream,
            Err(error) => {
                if error.is_instance_of::<PyAttributeError>(py) {
                    let mut message = format!(
                        "pyarrow.RecordBatchReader.from_stream is required for Session.{api_name}(...). \
This project requires pyarrow>=23.0.0, so please upgrade your pyarrow installation."
                    );

                    if let Ok(version) = pyarrow.getattr("__version__")
                        && let Ok(version) = version.extract::<String>()
                    {
                        message = format!("{message} (detected pyarrow=={version})");
                    }
                    return Err(PyImportError::new_err(message));
                }
                return Err(error);
            }
        };

        Ok(from_stream.call1((wrapper,))?.into())
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
            if _requested_schema.is_some() {
                return Err(PyNotImplementedError::new_err(
                    "__arrow_c_stream__ schema negotiation is not supported",
                ));
            }
            slf.capsule.take().ok_or_else(|| {
                PyRuntimeError::new_err("__arrow_c_stream__ may only be called once per object")
            })
        }
    }

    fn table_from_c_stream(py: Python<'_>, stream: FFI_ArrowArrayStream) -> PyResult<Py<PyAny>> {
        let reader = pyarrow_record_batch_reader_from_c_stream(py, stream, "sql")?;
        let reader = reader.bind(py);

        let table_res = reader.call_method0("read_all");
        let close_res = reader.call_method0("close");
        let debug = std::env::var_os("TTF_SQL_EXPORT_DEBUG").is_some();

        match (table_res, close_res) {
            (Ok(table), Ok(_)) => Ok(table.into()),
            (Ok(table), Err(e_close)) => {
                // If the table was successfully read, treat a close() failure as non-fatal.
                // Falling back to IPC in auto mode would be wasteful (we already have the data).
                if debug {
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
                        if debug {
                            if err.is_instance_of::<PyAttributeError>(py) {
                                // Python < 3.11: BaseException.add_note isn't available.
                                let msg = format!(
                                    "Session.sql: C Stream reader.close() also failed: {e_close}"
                                );
                                if let Ok(warnings) = PyModule::import(py, "warnings") {
                                    let _ = warnings.call_method1(
                                        "warn",
                                        (msg, PyRuntimeWarning::type_object(py)),
                                    );
                                } else {
                                    eprintln!("{msg}");
                                }
                            } else {
                                let msg = format!(
                                    "Session.sql: failed to attach exception note (close failure was: {e_close}): {err}"
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
            use timeseries_table_format::storage::TableLocation;
            use timeseries_table_format::table::TimeSeriesTable;

            if name.is_empty() {
                return Err(PyValueError::new_err("name must be non-empty"));
            }

            let location = TableLocation::parse(&table_root)
                .map_err(|err| storage_error_to_py_with_root(py, &table_root, err))?;

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
                        table_error_to_py_with_root(py, &table_root_for_err, &[], e)
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
        /// Directories must contain at least one Parquet file so their schema can be inferred.
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

            // Only needed if `auto_rerun_fallback` triggers. Keep a copy of the inputs so we can
            // re-run without requiring `query`/`params` to be cloned on every call.
            let mut rerun_args: Option<(String, Option<QueryParams>)> =
                (export_mode == SqlExportMode::Auto && auto_rerun_fallback)
                    .then(|| (query.clone(), params.clone()));

            let (schema, batches): (SchemaRef, Vec<RecordBatch>) = collect_sql(
                py,
                rt.as_ref(),
                ctx.clone(),
                Arc::clone(&sema),
                query,
                params,
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

                                    let (query, params) = rerun_args.take().unwrap_or_else(|| {
                                        unreachable!(
                                            "rerun_args must be present when auto_rerun_fallback is enabled"
                                        )
                                    });
                                    let (schema, batches) = collect_sql(
                                        py,
                                        rt.as_ref(),
                                        ctx.clone(),
                                        Arc::clone(&sema),
                                        query,
                                        params,
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

        /// Run a SQL query and return a streaming `pyarrow.RecordBatchReader`.
        ///
        /// This method runs synchronously from Python, but uses an internal Tokio runtime and
        /// releases the GIL while planning the query and starting the stream.
        ///
        /// Parameters
        /// ----------
        /// query:
        ///     SQL query string.
        /// params:
        ///     Optional query parameter values for DataFusion SQL placeholders:
        ///
        ///     - Positional: pass a list/tuple to bind `$1`, `$2`, ...
        ///       Example: `sess.sql_reader("select * from t where x = $1", params=[1])`
        ///     - Named: pass a dict to bind `$name` placeholders (keys may optionally start with `$`).
        ///       Example: `sess.sql_reader("select * from t where x = $a", params={"a": 1})`
        ///
        ///     Supported Python value types: `None`, `bool`, `int` (i64 range), `float`, `str`, `bytes`.
        ///
        /// Notes
        /// -----
        /// Unlike `Session.sql(...)`, this does not materialize the full result eagerly.
        /// Iterate batches incrementally or call `reader.read_all()` if you want a
        /// `pyarrow.Table`.
        ///
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
        /// RuntimeError:
        ///     If the result schema cannot be exported via Arrow C Stream.
        /// TypeError, ValueError:
        ///     If `params` has an invalid shape or contains unsupported value types.
        #[pyo3(signature = (query, *, params=None))]
        fn sql_reader(
            &self,
            py: Python<'_>,
            query: String,
            params: Option<Py<PyAny>>,
        ) -> PyResult<Py<PyAny>> {
            enum SqlReaderError {
                DataFusion(DFError),
                Arrow(ArrowError),
                UnsupportedSchema,
                Runtime(&'static str),
            }

            let params = match params {
                None => None,
                Some(obj) => {
                    let bound = obj.bind(py);
                    Some(parse_query_params(bound)?)
                }
            };

            let rt = Arc::clone(&self.rt);
            let rt_for_stream = Arc::clone(&self.rt);
            let ctx = self.ctx.clone();
            let sema = Arc::clone(&self.catalog_sema);

            let stream = tokio_runner::run_blocking_map_err(
                py,
                rt.as_ref(),
                async move {
                    let _permit = sema
                        .acquire_owned()
                        .await
                        .map_err(|_| SqlReaderError::Runtime("Session catalog semaphore closed"))?;

                    let mut df = ctx.sql(&query).await.map_err(SqlReaderError::DataFusion)?;

                    if let Some(p) = params {
                        df = match p {
                            QueryParams::Positional(v) => df.with_param_values(v),
                            QueryParams::Named(v) => df.with_param_values(v),
                        }
                        .map_err(SqlReaderError::DataFusion)?;
                    }

                    let schema: SchemaRef = df.schema().as_arrow().clone().into();
                    if !can_export_schema_to_c_stream(&schema) {
                        return Err(SqlReaderError::UnsupportedSchema);
                    }

                    let stream = df
                        .execute_stream()
                        .await
                        .map_err(SqlReaderError::DataFusion)?;

                    export_stream_to_c_stream(rt_for_stream.as_ref(), stream)
                        .map_err(SqlReaderError::Arrow)
                },
                move |py, err| match err {
                    SqlReaderError::DataFusion(e) => datafusion_error_to_py(py, e),
                    SqlReaderError::Arrow(e) => PyRuntimeError::new_err(e.to_string()),
                    SqlReaderError::UnsupportedSchema => PyRuntimeError::new_err(
                        "Session.sql_reader: schema cannot be exported via Arrow C Stream (unsupported type). \
Cast unsupported columns to supported Arrow types, or use Session.sql(...) to materialize a pyarrow.Table instead.",
                    ),
                    SqlReaderError::Runtime(msg) => PyRuntimeError::new_err(msg),
                },
            )?;

            pyarrow_record_batch_reader_from_c_stream(py, stream, "sql_reader")
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

    /// Result of one entity-layout optimization operation.
    #[pyclass(frozen, get_all)]
    struct OptimizeReport {
        /// Table version used to select optimization candidates.
        starting_version: u64,
        /// Committed replacement version, or `starting_version` for a no-op.
        committed_version: u64,
        /// Mixed source segments selected from the starting snapshot.
        candidate_source_segments: u64,
        /// Selected source segments removed by the committed rewrite.
        source_segments_replaced: u64,
        /// Verified single-entity replacement segments written.
        replacement_segments_written: u64,
        /// Unique complete identities represented by the replacements.
        distinct_identities_materialized: u64,
        /// Logical rows read from selected source segments.
        rows_read: u64,
        /// Logical rows written to committed replacement segments.
        rows_written: u64,
        /// Whether no mixed live segments required rewriting.
        no_op: bool,
    }

    impl From<CoreOptimizeReport> for OptimizeReport {
        fn from(report: CoreOptimizeReport) -> Self {
            let CoreOptimizeReport {
                starting_version,
                committed_version,
                candidate_source_segments,
                source_segments_replaced,
                replacement_segments_written,
                distinct_identities_materialized,
                rows_read,
                rows_written,
                no_op,
            } = report;
            Self {
                starting_version,
                committed_version,
                candidate_source_segments,
                source_segments_replaced,
                replacement_segments_written,
                distinct_identities_materialized,
                rows_read,
                rows_written,
                no_op,
            }
        }
    }

    /// Vacuum classification for one file below a scanned directory.
    #[derive(Clone)]
    #[pyclass(frozen, get_all, skip_from_py_object)]
    struct VacuumArtifact {
        /// Canonical table-relative path.
        path: String,
        /// Latest file size observed by this invocation.
        size_bytes: u64,
        /// Latest modification time observed by this invocation.
        modified_at: DateTime<Utc>,
        /// `retained`, `removable`, or `deleted`.
        disposition: String,
        /// Stable snake-case classification reason.
        reason: String,
        /// Earliest retained commit referencing the path, when applicable.
        referenced_by_commit_version: Option<u64>,
    }

    impl From<CoreVacuumArtifact> for VacuumArtifact {
        fn from(artifact: CoreVacuumArtifact) -> Self {
            let referenced_by_commit_version = match &artifact.reason {
                CoreVacuumArtifactReason::ReferencedByCommit { version } => Some(*version),
                _ => None,
            };
            Self {
                path: artifact.path,
                size_bytes: artifact.size_bytes,
                modified_at: artifact.modified_at,
                disposition: artifact.disposition.as_str().to_string(),
                reason: artifact.reason.as_str().to_string(),
                referenced_by_commit_version,
            }
        }
    }

    /// Structured result of one vacuum invocation.
    #[pyclass(frozen)]
    struct VacuumReport {
        #[pyo3(get)]
        /// Latest transaction-log version used for deletion safety.
        table_version: u64,
        #[pyo3(get)]
        /// Exclusive retention cutoff.
        older_than: DateTime<Utc>,
        #[pyo3(get)]
        /// `dry_run` or `apply`.
        mode: String,
        artifacts: Vec<VacuumArtifact>,
        #[pyo3(get)]
        /// Number of files considered by this invocation.
        considered_files: usize,
        #[pyo3(get)]
        /// Number of files retained by this invocation.
        retained_files: usize,
        #[pyo3(get)]
        /// Number of files reported as removable by dry-run.
        removable_files: usize,
        #[pyo3(get)]
        /// Number of files removed by apply mode.
        deleted_files: usize,
        #[pyo3(get)]
        /// Bytes across every considered file.
        considered_bytes: u128,
        #[pyo3(get)]
        /// Bytes retained by this invocation.
        retained_bytes: u128,
        #[pyo3(get)]
        /// Bytes reported as removable by dry-run.
        removable_bytes: u128,
        #[pyo3(get)]
        /// Bytes removed by apply mode.
        deleted_bytes: u128,
    }

    #[pymethods]
    impl VacuumReport {
        /// Every regular file considered below `data/` and `_coverage/`.
        #[getter]
        fn artifacts(&self, py: Python<'_>) -> PyResult<Vec<Py<VacuumArtifact>>> {
            self.artifacts
                .iter()
                .cloned()
                .map(|artifact| Py::new(py, artifact))
                .collect()
        }
    }

    impl From<CoreVacuumReport> for VacuumReport {
        fn from(report: CoreVacuumReport) -> Self {
            Self {
                table_version: report.table_version,
                older_than: report.older_than,
                mode: report.mode.as_str().to_string(),
                artifacts: report.artifacts.into_iter().map(Into::into).collect(),
                considered_files: report.considered_files,
                retained_files: report.retained_files,
                removable_files: report.removable_files,
                deleted_files: report.deleted_files,
                considered_bytes: report.considered_bytes,
                retained_bytes: report.retained_bytes,
                removable_bytes: report.removable_bytes,
                deleted_bytes: report.deleted_bytes,
            }
        }
    }

    fn vacuum_error_to_py_with_root(
        py: Python<'_>,
        table_root: &str,
        entity_columns: &[String],
        error: timeseries_table_format::table::TableError,
    ) -> PyErr {
        if matches!(
            error,
            timeseries_table_format::table::TableError::Vacuum {
                source: CoreVacuumError::FutureCutoff { .. }
            }
        ) {
            let message = error.to_string();
            return py_error_with_table_root(
                py,
                table_root,
                message.clone(),
                PyValueError::new_err(message),
            );
        }
        let partial = match &error {
            timeseries_table_format::table::TableError::Vacuum {
                source:
                    CoreVacuumError::Delete {
                        path,
                        partial_report,
                        ..
                    },
            } => Some((path.clone(), partial_report.as_ref().clone())),
            _ => None,
        };
        let Some((path, partial_report)) = partial else {
            return table_error_to_py_with_root(py, table_root, entity_columns, error);
        };

        let message = error.to_string();
        let py_error = VacuumApplyError::new_err(message.clone());
        let exception = py_error.value(py);
        if let Err(error) = exception.setattr("path", path) {
            return error;
        }
        let partial_report = match Py::new(py, VacuumReport::from(partial_report)) {
            Ok(partial_report) => partial_report,
            Err(error) => return error,
        };
        if let Err(error) = exception.setattr("partial_report", partial_report) {
            return error;
        }
        py_error_with_table_root(py, table_root, message, py_error)
    }

    fn positive_append_limit(name: &str, value: Option<isize>) -> PyResult<Option<usize>> {
        value
            .map(|value| {
                usize::try_from(value)
                    .ok()
                    .filter(|value| *value > 0)
                    .ok_or_else(|| PyValueError::new_err(format!("{name} must be positive")))
            })
            .transpose()
    }

    fn datetime_to_utc(value: &Bound<'_, PyAny>) -> PyResult<DateTime<Utc>> {
        let value = value.cast::<PyDateTime>()?;
        if value.getattr("tzinfo")?.is_none() {
            return Err(PyTypeError::new_err(
                "older_than must be a timezone-aware datetime",
            ));
        }
        let utc = Utc.into_pyobject(value.py())?;
        value.call_method1("astimezone", (utc,))?.extract()
    }

    /// Local filesystem time-series table rooted at `table_root`.
    ///
    /// Use `TimeSeriesTable` for table lifecycle operations (create/open/append Arrow data). For SQL
    /// querying across one or more registered tables, use `Session`.
    ///
    /// Appends are overlap-checked according to the table's persisted index granularity.
    #[pyclass]
    struct TimeSeriesTable {
        inner: timeseries_table_format::table::TimeSeriesTable,
        table_root: String,
    }

    #[pymethods]
    impl TimeSeriesTable {
        #[classmethod]
        #[pyo3(signature = (*, table_root, index_column, index_type, index_granularity, entity_columns=None, timezone=None))]
        /// Create a new time-series table at `table_root`.
        ///
        /// Parameters
        /// ----------
        /// table_root:
        ///     Filesystem directory where the table will be created.
        /// index_column:
        ///     Name of the ascending ordered-index column.
        /// index_type:
        ///     One of `"timestamp"`, `"int64"`, or `"uint64"`.
        /// index_granularity:
        ///     Timestamp interval string such as `"1h"`, or a positive Python integer for
        ///     `"int64"` and `"uint64"` indexes.
        /// entity_columns:
        ///     Column names that define ordered entity identities for this table. Segments may
        ///     contain multiple identities; coverage is tracked independently for each identity.
        ///     Supported canonical Arrow types are string, large_string, int32, int64, and uint64.
        ///     Actual entity values must be non-null. After schema adoption, append may losslessly
        ///     widen narrower integers but never converts between signed and unsigned values.
        /// timezone:
        ///     Optional timestamp timezone; rejected for integer indexes.
        ///
        /// Notes
        /// -----
        /// The table's canonical schema is typically adopted on the first successful append.
        ///
        /// Raises
        /// ------
        /// TimeseriesTableError:
        ///     If creation fails. The exception includes a `table_root` attribute.
        #[allow(clippy::too_many_arguments)]
        fn create(
            _cls: &Bound<'_, PyType>,
            py: Python<'_>,
            table_root: String,
            index_column: String,
            index_type: String,
            index_granularity: &Bound<'_, PyAny>,
            entity_columns: Option<Vec<String>>,
            timezone: Option<String>,
        ) -> PyResult<Self> {
            use crate::tokio_runner;

            use std::num::NonZeroU64;
            use timeseries_table_format::storage::TableLocation;
            use timeseries_table_format::table::TableError;
            use timeseries_table_format::transaction_log::{
                IndexKind, IndexSpec, TableMeta, TimeIndexGranularity,
            };

            let invalid = |field: &str, reason: String| {
                let msg = format!(
                    "invalid {field} for index_type {index_type:?} \
                     (table_root={table_root}): {reason}"
                );
                let py_err = TimeseriesTableError::new_err(msg);
                let exc = py_err.value(py);
                let _ = exc.setattr("table_root", table_root.clone());
                let _ = exc.setattr("index_type", index_type.clone());
                py_err
            };

            let kind = match index_type.as_str() {
                "timestamp" => {
                    if !index_granularity.is_instance_of::<pyo3::types::PyString>() {
                        return Err(invalid(
                            "index_granularity",
                            "must be a string using s, m, h, or d units for timestamp indexes"
                                .to_string(),
                        ));
                    }
                    let value = index_granularity.extract::<String>().map_err(|_| {
                        invalid(
                            "index_granularity",
                            "must be a string using s, m, h, or d units for timestamp indexes"
                                .to_string(),
                        )
                    })?;
                    let index_granularity = TimeIndexGranularity::parse(&value)
                        .map_err(|error| {
                            invalid(
                                "index_granularity",
                                format!(
                                    "must be a string using s, m, h, or d units for timestamp indexes: {error}"
                                ),
                            )
                        })?;
                    IndexKind::Timestamp {
                        index_granularity,
                        timezone,
                    }
                }
                "int64" | "uint64" => {
                    if timezone.is_some() {
                        return Err(invalid(
                            "timezone",
                            "is only valid for timestamp indexes".to_string(),
                        ));
                    }
                    if index_granularity.is_instance_of::<pyo3::types::PyBool>()
                        || !index_granularity.is_instance_of::<pyo3::types::PyInt>()
                    {
                        return Err(invalid(
                            "index_granularity",
                            format!(
                                "must be a Python int in 1..={} for integer indexes; bool is not accepted",
                                u64::MAX
                            ),
                        ));
                    }
                    let value = index_granularity.extract::<u64>().map_err(|_| {
                        invalid(
                            "index_granularity",
                            format!(
                                "must be a Python int in 1..={} for integer indexes",
                                u64::MAX
                            ),
                        )
                    })?;
                    let index_granularity = NonZeroU64::new(value).ok_or_else(|| {
                        invalid(
                            "index_granularity",
                            format!(
                                "must be a Python int in 1..={} for integer indexes",
                                u64::MAX
                            ),
                        )
                    })?;

                    if index_type == "int64" {
                        IndexKind::Int64 { index_granularity }
                    } else {
                        IndexKind::UInt64 { index_granularity }
                    }
                }
                _ => {
                    return Err(invalid(
                        "index_type",
                        "expected 'timestamp', 'int64', or 'uint64'".to_string(),
                    ));
                }
            };

            let index = IndexSpec {
                column: index_column,
                entity_columns: entity_columns.unwrap_or_default(),
                kind,
            };
            let meta = TableMeta::new_time_series(index);

            let table_root_for_err = table_root.clone();
            let location = TableLocation::parse(&table_root)
                .map_err(|err| storage_error_to_py_with_root(py, &table_root_for_err, err))?;
            let rt = tokio_runner::global_runtime()?;

            let table_root_for_err_cp = table_root_for_err.clone();
            let inner = tokio_runner::run_blocking_map_err(
                py,
                rt.as_ref(),
                async move {
                    let table =
                        timeseries_table_format::table::TimeSeriesTable::create(location, meta)
                            .await?;

                    Ok::<_, TableError>(table)
                },
                move |py, err| table_error_to_py_with_root(py, &table_root_for_err_cp, &[], err),
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

            use timeseries_table_format::{storage::TableLocation, table::TableError};

            let table_root_for_err = table_root.clone();
            let location = TableLocation::parse(&table_root)
                .map_err(|err| storage_error_to_py_with_root(py, &table_root_for_err, err))?;
            let rt = tokio_runner::global_runtime()?;
            let table_root_for_err_cp = table_root_for_err.clone();

            let inner = tokio_runner::run_blocking_map_err(
                py,
                rt.as_ref(),
                async move {
                    let table =
                        timeseries_table_format::table::TimeSeriesTable::open(location).await?;

                    Ok::<_, TableError>(table)
                },
                move |py, err| table_error_to_py_with_root(py, &table_root_for_err_cp, &[], err),
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
        /// Returns
        /// -------
        /// dict[str, object]
        ///     Exactly one variant-specific shape:
        ///
        ///     Timestamp:
        ///
        ///     ```python
        ///     {
        ///         "index_column": str,
        ///         "entity_columns": list[str],
        ///         "index_type": "timestamp",
        ///         "index_granularity": str,
        ///         "timezone": str | None,
        ///     }
        ///     ```
        ///
        ///     Int64:
        ///
        ///     ```python
        ///     {
        ///         "index_column": str,
        ///         "entity_columns": list[str],
        ///         "index_type": "int64",
        ///         "index_granularity": int,
        ///     }
        ///     ```
        ///
        ///     UInt64:
        ///
        ///     ```python
        ///     {
        ///         "index_column": str,
        ///         "entity_columns": list[str],
        ///         "index_type": "uint64",
        ///         "index_granularity": int,
        ///     }
        ///     ```
        fn index_spec<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
            use timeseries_table_format::transaction_log::{IndexKind, TimeIndexGranularity};

            let spec = self.inner.index_spec();
            let d = PyDict::new(py);
            d.set_item("index_column", spec.column.clone())?;
            d.set_item("entity_columns", spec.entity_columns.clone())?;
            d.set_item("index_type", spec.kind.name())?;
            match &spec.kind {
                IndexKind::Timestamp {
                    index_granularity,
                    timezone,
                } => {
                    let index_granularity = match index_granularity {
                        TimeIndexGranularity::Seconds(n) => format!("{n}s"),
                        TimeIndexGranularity::Minutes(n) => format!("{n}m"),
                        TimeIndexGranularity::Hours(n) => format!("{n}h"),
                        TimeIndexGranularity::Days(n) => format!("{n}d"),
                    };
                    d.set_item("index_granularity", index_granularity)?;
                    d.set_item("timezone", timezone.clone())?;
                }
                IndexKind::Int64 { index_granularity }
                | IndexKind::UInt64 { index_granularity } => {
                    d.set_item("index_granularity", index_granularity.get())?;
                }
            }

            Ok(d)
        }

        /// Append Arrow data to the table and return the committed version.
        ///
        /// `source` must be a `pyarrow.RecordBatch`, `pyarrow.Table`,
        /// `pyarrow.RecordBatchReader`, or another object implementing `__arrow_c_stream__`.
        /// The stream is consumed lazily while the GIL is released.
        #[pyo3(signature = (source, *, compression=None, max_rows_per_row_group=None, max_bytes_per_row_group=None))]
        fn append(
            &mut self,
            py: Python<'_>,
            source: &Bound<'_, PyAny>,
            compression: Option<&str>,
            max_rows_per_row_group: Option<isize>,
            max_bytes_per_row_group: Option<isize>,
        ) -> PyResult<u64> {
            let table_root_for_err = self.table_root.clone();
            let entity_columns_for_err = self.inner.index_spec().entity_columns.clone();
            self.inner.ensure_append_supported().map_err(|error| {
                table_error_to_py_with_root(py, &table_root_for_err, &entity_columns_for_err, error)
            })?;

            let compression = match compression.map(|value| value.trim().to_ascii_lowercase()) {
                None => None,
                Some(value) => Some(match value.as_str() {
                    "uncompressed" => ParquetCompression::Uncompressed,
                    "snappy" => ParquetCompression::Snappy,
                    "zstd" => ParquetCompression::Zstd,
                    _ => {
                        return Err(PyValueError::new_err(format!(
                            "invalid compression={value:?}; expected 'uncompressed', 'snappy', or 'zstd'"
                        )));
                    }
                }),
            };
            let max_rows_per_row_group =
                positive_append_limit("max_rows_per_row_group", max_rows_per_row_group)?;
            let max_bytes_per_row_group =
                positive_append_limit("max_bytes_per_row_group", max_bytes_per_row_group)?;

            let reader = record_batch_reader_from_python(source)?;
            let mut request = AppendRequest::new(reader);
            if let Some(compression) = compression {
                request = request.compression(compression);
            }
            if let Some(max_rows_per_row_group) = max_rows_per_row_group {
                request = request.max_rows_per_row_group(max_rows_per_row_group);
            }
            if let Some(max_bytes_per_row_group) = max_bytes_per_row_group {
                request = request.max_bytes_per_row_group(max_bytes_per_row_group);
            }
            let rt = tokio_runner::global_runtime()?;
            let table = &mut self.inner;

            tokio_runner::run_blocking_map_err(
                py,
                rt.as_ref(),
                async move { table.append(request).await },
                move |py, err| {
                    table_error_to_py_with_root(
                        py,
                        &table_root_for_err,
                        &entity_columns_for_err,
                        err,
                    )
                },
            )
        }

        /// Rewrite every mixed-entity segment into single-entity segments.
        ///
        /// Preserves logical rows, schema, and per-entity coverage, but may change physical row
        /// order. Query results are unordered unless the SQL query uses ORDER BY.
        ///
        /// Returns
        /// -------
        /// OptimizeReport
        ///     Complete counts and versions for the operation. A successful no-op returns a
        ///     report with `no_op=True` and equal starting and committed versions.
        ///
        /// Raises
        /// ------
        /// TimeseriesTableError
        ///     If optimization is not applicable or rewriting, validation, commit, or cleanup
        ///     fails. The exception includes a `table_root` attribute.
        fn optimize(&mut self, py: Python<'_>) -> PyResult<OptimizeReport> {
            let rt = tokio_runner::global_runtime()?;
            let table_root_for_err = self.table_root.clone();
            let entity_columns_for_err = self.inner.index_spec().entity_columns.clone();
            let table = &mut self.inner;

            let report = tokio_runner::run_blocking_map_err(
                py,
                rt.as_ref(),
                table.optimize(),
                move |py, err| {
                    table_error_to_py_with_root(
                        py,
                        &table_root_for_err,
                        &entity_columns_for_err,
                        err,
                    )
                },
            )?;
            Ok(report.into())
        }

        /// Inspect or delete expired files unreachable from retained table history.
        ///
        /// `older_than` must be timezone-aware and must not be in the future. Files modified at
        /// or after it are retained. Choose a cutoff older than the longest expected writer
        /// duration. This operation does not expire snapshots, rewrite history, or delete
        /// transaction-log files.
        ///
        /// Parameters
        /// ----------
        /// older_than:
        ///     Exclusive retention cutoff as a timezone-aware `datetime.datetime`.
        /// apply:
        ///     Delete candidates when true. The default is a non-mutating dry-run.
        ///
        /// Returns
        /// -------
        /// VacuumReport
        ///     Per-file classifications and aggregate byte counts.
        ///
        /// Raises
        /// ------
        /// TimeseriesTableError
        ///     If retained history cannot be validated or storage access fails. Exceptions include
        ///     a `table_root` attribute. A deletion failure raises `VacuumApplyError`, whose
        ///     `partial_report` records deletions completed before the failure.
        /// TypeError
        ///     If `older_than` is not a timezone-aware `datetime.datetime`.
        /// ValueError
        ///     If `older_than` is in the future.
        #[pyo3(signature = (older_than, *, apply=false))]
        fn vacuum(
            &self,
            py: Python<'_>,
            older_than: &Bound<'_, PyAny>,
            apply: bool,
        ) -> PyResult<VacuumReport> {
            let older_than = datetime_to_utc(older_than)?;
            let rt = tokio_runner::global_runtime()?;
            let table_root_for_err = self.table_root.clone();
            let entity_columns_for_err = self.inner.index_spec().entity_columns.clone();
            let mode = if apply {
                CoreVacuumMode::Apply
            } else {
                CoreVacuumMode::DryRun
            };

            let report = tokio_runner::run_blocking_map_err(
                py,
                rt.as_ref(),
                self.inner.vacuum(older_than, mode),
                move |py, err| {
                    vacuum_error_to_py_with_root(
                        py,
                        &table_root_for_err,
                        &entity_columns_for_err,
                        err,
                    )
                },
            )?;
            Ok(report.into())
        }
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

    /// Test-only helper: raise the same unsupported-schema error used by `Session.sql_reader(...)`.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    fn _test_sql_reader_unsupported_schema(py: Python<'_>) -> PyResult<()> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema, UnionFields, UnionMode};

        let uf = UnionFields::try_new(
            vec![1, 3],
            vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ],
        )
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::Union(uf, UnionMode::Dense),
            true,
        )]));

        if !can_export_schema_to_c_stream(&schema) {
            return Err(PyRuntimeError::new_err(
                "Session.sql_reader: schema cannot be exported via Arrow C Stream (unsupported type). \
Cast unsupported columns to supported Arrow types, or use Session.sql(...) to materialize a pyarrow.Table instead.",
            ));
        }

        let _ = py;
        Ok(())
    }

    #[cfg(feature = "test-utils")]
    fn test_reader_from_stream(
        py: Python<'_>,
        stream: SendableRecordBatchStream,
    ) -> PyResult<Py<PyAny>> {
        let rt = tokio_runner::global_runtime()?;
        let stream = export_stream_to_c_stream(rt.as_ref(), stream)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        pyarrow_record_batch_reader_from_c_stream(py, stream, "_test_sql_reader")
    }

    #[cfg(feature = "test-utils")]
    fn make_test_i64_batch(
        schema: &SchemaRef,
        start: i64,
        len: usize,
    ) -> Result<RecordBatch, ArrowError> {
        use arrow_array::Int64Array;

        let values: Vec<i64> = (start..start + len as i64).collect();
        let array = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(schema.clone(), vec![array])
    }

    #[cfg(feature = "test-utils")]
    #[pyclass(name = "_AppendStreamReleaseCounter")]
    struct AppendStreamReleaseCounter {
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[cfg(feature = "test-utils")]
    #[pymethods]
    impl AppendStreamReleaseCounter {
        #[getter]
        fn count(&self) -> usize {
            self.count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// Test-only helper: return a native stream whose reader drop count is observable.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    #[pyo3(signature = (*, fail_after_first, with_error_details=true))]
    fn _test_append_stream_with_release_counter(
        py: Python<'_>,
        fail_after_first: bool,
        with_error_details: bool,
    ) -> PyResult<(Py<ArrowCStreamWrapper>, Py<AppendStreamReleaseCounter>)> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct ReleaseCountingReader {
            schema: SchemaRef,
            batch: Option<RecordBatch>,
            fail_after_first: bool,
            count: Arc<AtomicUsize>,
        }

        impl Iterator for ReleaseCountingReader {
            type Item = Result<RecordBatch, ArrowError>;

            fn next(&mut self) -> Option<Self::Item> {
                if let Some(batch) = self.batch.take() {
                    return Some(Ok(batch));
                }
                if std::mem::take(&mut self.fail_after_first) {
                    return Some(Err(ArrowError::CDataInterface(
                        "test append stream failure".to_string(),
                    )));
                }
                None
            }
        }

        impl RecordBatchReader for ReleaseCountingReader {
            fn schema(&self) -> SchemaRef {
                Arc::clone(&self.schema)
            }
        }

        impl Drop for ReleaseCountingReader {
            fn drop(&mut self) {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }

        unsafe extern "C" fn no_error_details(_stream: *mut FFI_ArrowArrayStream) -> *const c_char {
            std::ptr::null()
        }

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = make_test_i64_batch(&schema, 1, 2)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let count = Arc::new(AtomicUsize::new(0));
        let mut stream = FFI_ArrowArrayStream::new(Box::new(ReleaseCountingReader {
            schema,
            batch: Some(batch),
            fail_after_first,
            count: Arc::clone(&count),
        }));
        if !with_error_details {
            stream.get_last_error = Some(no_error_details);
        }
        let capsule = PyCapsule::new_with_value(py, stream, c"arrow_array_stream")?;
        let source = Py::new(
            py,
            ArrowCStreamWrapper {
                capsule: Some(capsule.into_any().unbind()),
            },
        )?;
        let counter = Py::new(py, AppendStreamReleaseCounter { count })?;

        Ok((source, counter))
    }

    /// Test-only helper: return a stream that fails schema import and counts its release.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    fn _test_append_stream_with_schema_import_error(
        py: Python<'_>,
    ) -> PyResult<(Py<ArrowCStreamWrapper>, Py<AppendStreamReleaseCounter>)> {
        use std::ffi::{c_char, c_void};
        use std::sync::atomic::{AtomicUsize, Ordering};

        const C_STREAM_ERROR_CODE: i32 = 22;

        unsafe extern "C" fn fail_schema(
            _stream: *mut FFI_ArrowArrayStream,
            _out: *mut FFI_ArrowSchema,
        ) -> i32 {
            C_STREAM_ERROR_CODE
        }

        unsafe extern "C" fn fail_next(
            _stream: *mut FFI_ArrowArrayStream,
            _out: *mut FFI_ArrowArray,
        ) -> i32 {
            C_STREAM_ERROR_CODE
        }

        unsafe extern "C" fn last_error(_stream: *mut FFI_ArrowArrayStream) -> *const c_char {
            c"test schema import failure".as_ptr()
        }

        unsafe extern "C" fn release(stream: *mut FFI_ArrowArrayStream) {
            if stream.is_null() {
                return;
            }
            // SAFETY: the callback receives the stream and private pointer created below.
            let stream = unsafe { &mut *stream };
            if stream.release.is_none() {
                return;
            }
            if !stream.private_data.is_null() {
                // SAFETY: `private_data` came from `Box::into_raw` below and is reclaimed once.
                let count =
                    unsafe { Box::from_raw(stream.private_data.cast::<Arc<AtomicUsize>>()) };
                count.fetch_add(1, Ordering::SeqCst);
            }
            stream.get_schema = None;
            stream.get_next = None;
            stream.get_last_error = None;
            stream.release = None;
            stream.private_data = std::ptr::null_mut();
        }

        let count = Arc::new(AtomicUsize::new(0));
        let stream = FFI_ArrowArrayStream {
            get_schema: Some(fail_schema),
            get_next: Some(fail_next),
            get_last_error: Some(last_error),
            release: Some(release),
            private_data: Box::into_raw(Box::new(Arc::clone(&count))).cast::<c_void>(),
        };
        let capsule = PyCapsule::new_with_value(py, stream, c"arrow_array_stream")?;
        let source = Py::new(
            py,
            ArrowCStreamWrapper {
                capsule: Some(capsule.into_any().unbind()),
            },
        )?;
        let counter = Py::new(py, AppendStreamReleaseCounter { count })?;

        Ok((source, counter))
    }

    /// Test-only helper: return a reader that yields one batch, then raises a mid-stream error.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    fn _test_sql_reader_midstream_error(py: Python<'_>) -> PyResult<Py<PyAny>> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures_util::stream;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = make_test_i64_batch(&schema, 1, 2)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let source = stream::iter(vec![
            Ok(batch),
            Err(DFError::Execution("mid-stream boom".to_string())),
        ]);
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, source));

        test_reader_from_stream(py, stream)
    }

    /// Test-only helper: return a reader that yields one batch, then never produces another
    /// batch unless the reader is closed and the producer task is aborted.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    fn _test_sql_reader_pending_after_first_batch(py: Python<'_>) -> PyResult<Py<PyAny>> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::RecordBatchStream;
        use futures_util::Stream;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct PendingAfterFirstBatchStream {
            schema: SchemaRef,
            first_batch: Option<RecordBatch>,
        }

        impl Stream for PendingAfterFirstBatchStream {
            type Item = Result<RecordBatch, DFError>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if let Some(batch) = self.first_batch.take() {
                    Poll::Ready(Some(Ok(batch)))
                } else {
                    Poll::Pending
                }
            }
        }

        impl RecordBatchStream for PendingAfterFirstBatchStream {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = make_test_i64_batch(&schema, 1, 2)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let stream: SendableRecordBatchStream = Box::pin(PendingAfterFirstBatchStream {
            schema,
            first_batch: Some(batch),
        });

        test_reader_from_stream(py, stream)
    }

    /// Test-only helper: return a reader that yields `batch_count` delayed batches of
    /// sequential `Int64` values. Used to test time-to-first-batch behavior.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    #[pyo3(signature = (*, batch_count, rows_per_batch, delay_millis))]
    fn _test_sql_reader_delayed_batches(
        py: Python<'_>,
        batch_count: usize,
        rows_per_batch: usize,
        delay_millis: u64,
    ) -> PyResult<Py<PyAny>> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures_util::{StreamExt, stream};
        use std::time::Duration;

        if batch_count == 0 {
            return Err(PyValueError::new_err("batch_count must be >= 1"));
        }
        if rows_per_batch == 0 {
            return Err(PyValueError::new_err("rows_per_batch must be >= 1"));
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let schema_for_stream = schema.clone();
        let source = stream::iter(0..batch_count).then(move |batch_index| {
            let schema = schema_for_stream.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(delay_millis)).await;
                make_test_i64_batch(
                    &schema,
                    (batch_index * rows_per_batch) as i64,
                    rows_per_batch,
                )
                .map_err(|e| DFError::Execution(e.to_string()))
            }
        });
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, source));

        test_reader_from_stream(py, stream)
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
    /// - `obj, m = ttf._native._testing._bench_sql_c_stream(sess, sql)`
    /// - `reader = pyarrow.RecordBatchReader.from_stream(obj)`
    /// - `table = reader.read_all(); reader.close()`
    ///
    /// Note: the returned object must remain alive until `reader.close()` completes.
    #[cfg(feature = "test-utils")]
    #[pyfunction]
    fn _bench_sql_c_stream<'py>(
        py: Python<'py>,
        session: PyRef<'_, Session>,
        query: String,
    ) -> PyResult<(Py<ArrowCStreamWrapper>, Bound<'py, PyDict>)> {
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

        let capsule = PyCapsule::new_with_value(py, result.stream, c"arrow_array_stream")?;

        let wrapper = Py::new(
            py,
            ArrowCStreamWrapper {
                capsule: Some(capsule.into_any().unbind()),
            },
        )?;

        let metrics = PyDict::new(py);
        metrics.set_item("arrow_mem_bytes", result.arrow_mem_bytes)?;
        metrics.set_item("row_count", result.row_count)?;
        metrics.set_item("batch_count", result.batch_count)?;
        metrics.set_item("plan_ms", result.plan_ms)?;
        metrics.set_item("collect_ms", result.collect_ms)?;
        metrics.set_item("c_stream_export_ms", result.c_stream_export_ms)?;
        metrics.set_item("total_ms", result.total_ms)?;

        Ok((wrapper, metrics))
    }

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        crate::python_logging::install(m.py())?;
        m.add("__version__", env!("CARGO_PKG_VERSION"))?;
        m.add_function(pyo3::wrap_pyfunction!(refresh_logging_cache, m)?)?;

        // Export classes
        m.add_class::<Session>()?;
        m.add_class::<OptimizeReport>()?;
        m.add_class::<VacuumArtifact>()?;
        m.add_class::<VacuumReport>()?;
        m.add_class::<TimeSeriesTable>()?;

        // Export exception types
        let py = m.py();
        m.add(
            "TimeseriesTableError",
            py.get_type::<TimeseriesTableError>(),
        )?;

        m.add("StorageError", py.get_type::<StorageError>())?;
        m.add("VacuumApplyError", py.get_type::<VacuumApplyError>())?;
        m.add("ConflictError", py.get_type::<ConflictError>())?;
        m.add(
            "IndexIntervalOverlapError",
            py.get_type::<IndexIntervalOverlapError>(),
        )?;
        m.add(
            "DuplicateIndexIntervalError",
            py.get_type::<DuplicateIndexIntervalError>(),
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
            testing.add_function(pyo3::wrap_pyfunction!(_test_sleep_without_gil, py)?)?;
            testing.add_function(pyo3::wrap_pyfunction!(_test_session_table_exists, py)?)?;
            testing.add_function(pyo3::wrap_pyfunction!(
                _test_sql_reader_unsupported_schema,
                py
            )?)?;
            testing.add_function(pyo3::wrap_pyfunction!(
                _test_sql_reader_midstream_error,
                py
            )?)?;
            testing.add_class::<AppendStreamReleaseCounter>()?;
            testing.add_function(pyo3::wrap_pyfunction!(
                _test_append_stream_with_release_counter,
                py
            )?)?;
            testing.add_function(pyo3::wrap_pyfunction!(
                _test_append_stream_with_schema_import_error,
                py
            )?)?;
            testing.add_function(pyo3::wrap_pyfunction!(
                _test_sql_reader_pending_after_first_batch,
                py
            )?)?;
            testing.add_function(pyo3::wrap_pyfunction!(
                _test_sql_reader_delayed_batches,
                py
            )?)?;
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
        #[cfg(unix)]
        fn env_var_truthy_treats_non_unicode_as_falsy() {
            use std::os::unix::ffi::OsStringExt;

            init_python();
            let _g = EnvGuard::set(
                "TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK",
                Some(OsString::from_vec(vec![0xFF])),
            );
            assert!(!super::env_var_truthy("TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK"));
        }

        #[test]
        fn arrow_c_stream_wrapper_is_single_use() {
            use pyo3::types::PyAnyMethods;
            use pyo3::types::PyCapsule;

            init_python();
            let ok = Python::try_attach(|py| {
                let capsule =
                    PyCapsule::new_with_value(py, 123usize, c"arrow_array_stream").unwrap();

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

        #[test]
        fn arrow_c_stream_wrapper_rejects_requested_schema_without_consuming() {
            use pyo3::types::PyAnyMethods;
            use pyo3::types::PyCapsule;
            use pyo3::types::PyDict;

            init_python();
            let ok = Python::try_attach(|py| {
                let capsule =
                    PyCapsule::new_with_value(py, 123usize, c"arrow_array_stream").unwrap();

                let wrapper = Py::new(
                    py,
                    super::ArrowCStreamWrapper {
                        capsule: Some(capsule.into_any().unbind()),
                    },
                )
                .unwrap();

                let wrapper = wrapper.bind(py);

                // Passing any non-None object as `requested_schema` should raise NotImplementedError
                // and must not consume the capsule.
                let err = wrapper
                    .call_method1("__arrow_c_stream__", (PyDict::new(py),))
                    .unwrap_err();
                assert!(err.is_instance_of::<pyo3::exceptions::PyNotImplementedError>(py));

                // The capsule should still be available after the NotImplementedError.
                wrapper.call_method0("__arrow_c_stream__").unwrap();
            });
            assert!(ok.is_some());
        }
    }
}
