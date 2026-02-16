from ._native import (
    __version__,
    ConflictError,
    CoverageOverlapError,
    DataFusionError,
    SchemaMismatchError,
    Session,
    StorageError,
    TimeSeriesTable,
    TimeseriesTableError,
)

def enable_notebook_display(
    *,
    max_rows: int = 20,
    max_cols: int = 50,
    max_cell_chars: int = 2000,
    align: str = "right",
) -> bool: ...
def disable_notebook_display() -> bool: ...
def load_notebook_display_config(path: str) -> bool: ...

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
    "load_notebook_display_config",
]
