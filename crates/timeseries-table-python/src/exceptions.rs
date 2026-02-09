//! doc placeholder

use pyo3::{create_exception, exceptions::PyException};

create_exception!(
    timeseries_table_format,
    TimeseriesTableError,
    PyException,
    "Based exception for timeseries_table_format."
);

create_exception!(
    timeseries_table_format,
    StorageError,
    TimeseriesTableError,
    "Raised when a storage operation fails."
);

create_exception!(
    timeseries_table_format,
    ConflictError,
    TimeseriesTableError,
    "Raised when an optimistic concurrency control (OCC) conflict is detected."
);

create_exception!(
    timeseries_table_format,
    CoverageOverlapError,
    TimeseriesTableError,
    "Raised when appending a segment would overlap existing coverage."
);

create_exception!(
    timeseries_table_format,
    SchemaMismatchError,
    TimeseriesTableError,
    "Raised when an appended segment schema does not match the table schema."
);

create_exception!(
    timeseries_table_format,
    DataFusionError,
    TimeseriesTableError,
    "Raised when a DataFusion query or planning operation fails."
);
