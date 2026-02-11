//! Python exception types exposed by the `timeseries_table_format` module.

use pyo3::{create_exception, exceptions::PyException};

create_exception!(
    _native,
    TimeseriesTableError,
    PyException,
    "Based exception for timeseries_table_format."
);

create_exception!(
    _native,
    StorageError,
    TimeseriesTableError,
    "Raised when a storage operation fails."
);

create_exception!(
    _native,
    ConflictError,
    TimeseriesTableError,
    "Raised when an optimistic concurrency control (OCC) conflict is detected."
);

create_exception!(
    _native,
    CoverageOverlapError,
    TimeseriesTableError,
    "Raised when appending a segment would overlap existing coverage."
);

create_exception!(
    _native,
    SchemaMismatchError,
    TimeseriesTableError,
    "Raised when an appended segment schema does not match the table schema."
);

create_exception!(
    _native,
    DataFusionError,
    TimeseriesTableError,
    "Raised when a DataFusion query or planning operation fails."
);
