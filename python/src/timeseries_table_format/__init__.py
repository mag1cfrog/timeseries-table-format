from timeseries_table_format.notebook_display import _auto_enable_notebook_display
from timeseries_table_format.notebook_display import (
    disable_notebook_display,
    enable_notebook_display,
)
from . import _native as _native

__doc__ = _native.__doc__

__version__ = _native.__version__

TimeseriesTableError = _native.TimeseriesTableError
StorageError = _native.StorageError
ConflictError = _native.ConflictError
CoverageOverlapError = _native.CoverageOverlapError
SchemaMismatchError = _native.SchemaMismatchError
DataFusionError = _native.DataFusionError

Session = _native.Session
TimeSeriesTable = _native.TimeSeriesTable

_auto_enable_notebook_display()
del _auto_enable_notebook_display

__all__ = [
    "__version__",
    "TimeseriesTableError",
    "StorageError",
    "ConflictError",
    "CoverageOverlapError",
    "SchemaMismatchError",
    "DataFusionError",
    "Session",
    "TimeSeriesTable",
    "enable_notebook_display",
    "disable_notebook_display",
]
