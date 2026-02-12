from types import ModuleType

import pyarrow

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
    def register_tstable(self, name: str, table_root: str) -> None: ...
    def register_parquet(self, name: str, path: str) -> None: ...
    def sql(self, query: str, *, params: object | None = None) -> pyarrow.Table:
        """Run a SQL query and return the results as a `pyarrow.Table`.

        Parameters
        ----------
        query:
            SQL query string.
        params:
            Optional query parameter values for DataFusion SQL placeholders:

            - Positional: pass a list/tuple to bind `$1`, `$2`, ...
              Example: `sess.sql("select * from t where x = $1", params=[1])`
            - Named: pass a dict to bind `$name` placeholders (keys may optionally start with `$`).
              Example: `sess.sql("select * from t where x = $a", params={"a": 1})`

        Notes
        -----
        DataFusion infers placeholder types from context when possible (e.g. in `WHERE` clauses).
        If you use placeholders in a `SELECT` projection without type context, you may need an
        explicit cast, e.g. `SELECT CAST($1 AS BIGINT) AS x`.
        """
        ...

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
    def append_parquet(
        self,
        parquet_path: str,
        time_column: str | None = None,
        copy_if_outside: bool = True,
    ) -> int: ...
    def root(self) -> str: ...
    def version(self) -> int: ...
    def index_spec(self) -> dict[str, object]: ...

class _TestingModule(ModuleType):
    def _test_trigger_overlap(self, table_root: str, parquet_path: str) -> None: ...
    def _test_sleep_without_gil(self, millis: int) -> None: ...
    def _test_session_table_exists(self, session: Session, name: str) -> bool: ...

# Feature-gated: present only when built with `--features test-utils`.
_testing: _TestingModule | None
