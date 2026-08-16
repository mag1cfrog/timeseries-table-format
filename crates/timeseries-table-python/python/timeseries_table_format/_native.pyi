from types import ModuleType
from typing import Literal

import pyarrow

__version__: str

class TimeseriesTableError(Exception): ...
class StorageError(TimeseriesTableError): ...
class ConflictError(TimeseriesTableError): ...

class CoverageOverlapError(TimeseriesTableError):
    segment_path: str
    """Path to the Parquet segment that triggered the overlap."""
    overlap_count: int
    """Number of (entity, bucket) pairs that already have coverage."""
    example_bucket: int | None
    """One example bucket (epoch microseconds) that overlapped, for debugging."""

class SchemaMismatchError(TimeseriesTableError): ...
class DataFusionError(TimeseriesTableError): ...

class Session:
    def __init__(self) -> None:
        """Create a new DataFusion-backed SQL session.

        The session runs async Rust internals on an internal Tokio runtime and releases the GIL
        while executing queries.
        """
        ...

    def register_tstable(self, name: str, table_root: str) -> None:
        """Register a time-series table under a name for SQL queries.

        Parameters
        ----------
        name:
            SQL table name to register under.
        table_root:
            Filesystem directory containing the table.

        Notes
        -----
        If `name` is already registered, it is replaced atomically (with rollback on failure).
        """
        ...

    def register_parquet(self, name: str, path: str) -> None:
        """Register a Parquet file or directory under a name for SQL queries.

        Parameters
        ----------
        name:
            SQL table name to register under.
        path:
            Path to a Parquet file or a directory of Parquet files.

        Notes
        -----
        If `name` is already registered, it is replaced atomically (with rollback on failure).
        """
        ...

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

            Supported Python value types: `None`, `bool`, `int` (i64 range), `float`, `str`, `bytes`.

        Notes
        -----
        DataFusion infers placeholder types from context when possible (e.g. in `WHERE` clauses).
        If you use placeholders in a `SELECT` projection without type context, you may need an
        explicit cast, e.g. `SELECT CAST($1 AS BIGINT) AS x`.
        """
        ...

    def sql_reader(
        self,
        query: str,
        *,
        params: object | None = None,
    ) -> pyarrow.RecordBatchReader:
        """Run a SQL query and return a streaming `pyarrow.RecordBatchReader`.

        Parameters
        ----------
        query:
            SQL query string.
        params:
            Optional query parameter values for DataFusion SQL placeholders.

        Notes
        -----
        Unlike `Session.sql(...)`, this does not materialize the full result eagerly.
        Iterate batches incrementally or call `reader.read_all()` if you want a
        `pyarrow.Table`.
        """
        ...

    def tables(self) -> list[str]:
        """Return the list of currently registered table names (sorted)."""
        ...

    def deregister(self, name: str) -> None:
        """Deregister a previously registered table name.

        Raises
        ------
        ValueError:
            If `name` is empty.
        KeyError:
            If `name` is not registered.
        """
        ...

class TimeSeriesTable:
    @classmethod
    def create(
        cls,
        *,
        table_root: str,
        index_column: str,
        index_type: Literal["timestamp", "int64", "uint64"],
        entity_columns: list[str] | None = None,
        bucket: str | None = None,
        bucket_width: int | None = None,
        timezone: str | None = None,
    ) -> TimeSeriesTable:
        """Create a new time-series table at `table_root`.

        Parameters
        ----------
        table_root:
            Filesystem directory where the table will be created.
        index_column:
            Name of the ascending ordered-index column.
        index_type:
            One of `"timestamp"`, `"int64"`, or `"uint64"`.
        entity_columns:
            Column names that define the entity identity for the table.
        bucket:
            Required timestamp bucket such as `"1h"`, `"5m"`, `"30s"`, or `"1d"`.
        bucket_width:
            Required positive integer width for `"int64"` and `"uint64"` indexes.
        timezone:
            Optional timestamp timezone; rejected for integer indexes.

        Notes
        -----
        The table's canonical schema is typically adopted on the first successful append.
        """
        ...

    @classmethod
    def open(cls, table_root: str) -> TimeSeriesTable:
        """Open an existing time-series table at `table_root`."""
        ...

    def append_parquet(
        self,
        parquet_path: str,
        copy_if_outside: bool = True,
    ) -> int:
        """Append a Parquet segment to the table and return the new table version.

        Parameters
        ----------
        parquet_path:
            Path to a Parquet file.
        copy_if_outside:
            If `True`, copies the file under the table root before appending.
            If `False`, `parquet_path` must be a table-relative storage key.
        """
        ...

    def root(self) -> str:
        """Return the table root path."""
        ...

    def version(self) -> int:
        """Return the current table version."""
        ...

    def index_spec(self) -> dict[str, object]:
        """Return the variant-specific ordered-index specification dict."""
        ...

class _TestingModule(ModuleType):
    def _test_trigger_overlap(
        self, table_root: str, first_parquet_path: str, second_parquet_path: str
    ) -> None: ...
    def _test_sleep_without_gil(self, millis: int) -> None: ...
    def _test_session_table_exists(self, session: Session, name: str) -> bool: ...
    def _test_sql_reader_unsupported_schema(self) -> None: ...
    def _test_sql_reader_midstream_error(self) -> pyarrow.RecordBatchReader: ...
    def _test_sql_reader_pending_after_first_batch(
        self,
    ) -> pyarrow.RecordBatchReader: ...
    def _test_sql_reader_delayed_batches(
        self,
        *,
        batch_count: int,
        rows_per_batch: int,
        delay_millis: int,
    ) -> pyarrow.RecordBatchReader: ...
    def _bench_sql_ipc(
        self,
        session: Session,
        query: str,
        *,
        ipc_compression: str = "none",
    ) -> tuple[bytes, dict[str, object]]: ...
    def _bench_sql_c_stream(
        self,
        session: Session,
        query: str,
    ) -> tuple[object, dict[str, object]]: ...

# Feature-gated: present only when built with `--features test-utils`.
_testing: _TestingModule | None
