from types import ModuleType
__version__: str

class TimeseriesTableError(Exception): ...
class StorageError(TimeseriesTableError): ...
class ConflictError(TimeseriesTableError): ...

class CoverageOverlapError(TimeseriesTableError):
    segment_path: str
    overlap_count: int
    example_bucket: int | None

class SchemaMismatchError(TimeseriesTableError): ...
class DataFusionError(TimeseriesTableError): ...

class Session:
    def __init__(self) -> None: ...

class TimeSeriesTable:
    @classmethod
    def create(
        cls,
        *,
        table_root: str,
        time_column: str,
        bucket: str,
        entity_columns: list[str] | None = None,
        timezone: str | None = None,
    ) -> TimeSeriesTable: ...

    @classmethod
    def open(cls, table_root: str) -> TimeSeriesTable: ...

class _TestingModule(ModuleType):
    def _test_trigger_overlap(self, table_root: str, parquet_path: str) -> None: ...

_testing: _TestingModule
