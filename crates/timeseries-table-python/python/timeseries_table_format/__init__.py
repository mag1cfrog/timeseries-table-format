from timeseries_table_format.notebook_display import _auto_enable_notebook_display
from timeseries_table_format.notebook_display import (
    disable_notebook_display,
    enable_notebook_display,
    load_notebook_display_config,
)
from . import _native as _native
from ._native import (
    __version__,
    ConflictError,
    DataFusionError,
    DuplicateIndexIntervalError,
    IndexIntervalOverlapError,
    OptimizeReport,
    refresh_logging_cache,
    SchemaMismatchError,
    Session,
    StorageError,
    TimeSeriesTable,
    TimeseriesTableError,
    VacuumArtifact,
    VacuumApplyError,
    VacuumReport,
)

__doc__ = _native.__doc__

_auto_enable_notebook_display()
del _auto_enable_notebook_display

__all__ = [
    "__version__",
    "TimeseriesTableError",
    "StorageError",
    "ConflictError",
    "IndexIntervalOverlapError",
    "DuplicateIndexIntervalError",
    "SchemaMismatchError",
    "DataFusionError",
    "OptimizeReport",
    "VacuumArtifact",
    "VacuumApplyError",
    "VacuumReport",
    "refresh_logging_cache",
    "Session",
    "TimeSeriesTable",
    "enable_notebook_display",
    "disable_notebook_display",
    "load_notebook_display_config",
]
