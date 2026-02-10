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
    @classmethod
    def append_parquet(
        self,
        parquet_path: str,
        time_column: str | None = None,
        copy_if_outside: bool = True,
    ) -> int: ...

class _TestingModule(ModuleType):
    def _test_trigger_overlap(self, table_root: str, parquet_path: str) -> None: ...

# Feature-gated: present only when built with `--features test-utils`.
_testing: _TestingModule | None
