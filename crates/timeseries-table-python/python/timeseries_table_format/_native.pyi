from types import ModuleType
from typing import Literal, Protocol

import pyarrow

class _ArrowStreamExportable(Protocol):
    def __arrow_c_stream__(
        self, requested_schema: object | None = None, /
    ) -> object: ...

__version__: str

def refresh_logging_cache() -> None:
    """Refresh native logging levels after changing Python logging configuration."""
    ...

class TimeseriesTableError(Exception): ...
class StorageError(TimeseriesTableError): ...
class ConflictError(TimeseriesTableError): ...

class IndexIntervalOverlapError(TimeseriesTableError):
    segment_path: str
    """Path to the Parquet segment that triggered the overlap."""
    conflict_count: int
    """Number of conflicting intervals, or identity and interval pairs."""
    example_identity: dict[str, str | int] | None
    """One complete identity, or `None` for a table without entity columns."""
    example_index_interval: str
    """One conflicting logical ordered-index interval."""

class DuplicateIndexIntervalError(TimeseriesTableError):
    segment_path: str
    """Path to the generated Parquet segment that contains the duplicate."""
    example_identity: dict[str, str | int] | None
    """One complete identity, or `None` for a table without entity columns."""
    example_index_interval: str
    """One duplicated logical ordered-index interval."""

class SchemaMismatchError(TimeseriesTableError): ...
class DataFusionError(TimeseriesTableError): ...

class OptimizeReport:
    """Result of one entity-layout optimization operation."""

    @property
    def starting_version(self) -> int:
        """Table version used to select optimization candidates."""
        ...

    @property
    def committed_version(self) -> int:
        """Committed replacement version, or `starting_version` for a no-op."""
        ...

    @property
    def candidate_source_segments(self) -> int:
        """Mixed source segments selected from the starting snapshot."""
        ...

    @property
    def source_segments_replaced(self) -> int:
        """Selected source segments removed by the committed rewrite."""
        ...

    @property
    def replacement_segments_written(self) -> int:
        """Verified single-entity replacement segments written."""
        ...

    @property
    def distinct_identities_materialized(self) -> int:
        """Unique complete identities represented by the replacements."""
        ...

    @property
    def rows_read(self) -> int:
        """Logical rows read from selected source segments."""
        ...

    @property
    def rows_written(self) -> int:
        """Logical rows written to committed replacement segments."""
        ...

    @property
    def no_op(self) -> bool:
        """Whether no mixed live segments required rewriting."""
        ...

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
        index_granularity: str | int,
        entity_columns: list[str] | None = None,
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
        index_granularity:
            Timestamp interval string such as `"1h"`, or a positive integer for `"int64"`
            and `"uint64"` indexes.
        entity_columns:
            Ordered column names that define independent identities within the table. One
            Parquet segment may contain multiple identities.
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

    def append(
        self,
        source: pyarrow.RecordBatch
        | pyarrow.Table
        | pyarrow.RecordBatchReader
        | _ArrowStreamExportable,
    ) -> int:
        """Append Arrow data and return the committed table version.

        Parameters
        ----------
        source:
            A `pyarrow.RecordBatch`, `pyarrow.Table`, `pyarrow.RecordBatchReader`, or another
            object implementing `__arrow_c_stream__`. File paths, pandas objects, NumPy arrays,
            mappings, row iterables, and arbitrary batch iterables are not converted implicitly.

        Returns
        -------
        int
            The newly committed table version.

        Notes
        -----
        Arrow streams are consumed lazily without staging or collecting the complete input in
        Python. `RecordBatch` and `Table` sources remain usable after append; readers and other
        single-use streams are consumed. After importing the stream, append releases the GIL.

        Raises
        ------
        TypeError
            If `source` is not one of the supported Arrow forms.
        ValueError
            If the Arrow C Stream exporter or capsule is invalid.
        IndexIntervalOverlapError
            If incoming coverage overlaps committed coverage for the same entity.
        DuplicateIndexIntervalError
            If two incoming rows for one entity occupy the same index interval.
        SchemaMismatchError
            If the Arrow schema does not match the table's established schema.
        TimeseriesTableError
            For other table, storage, transaction, or stream failures. The exception includes a
            `table_root` attribute.
        """
        ...

    def optimize(self) -> OptimizeReport:
        """Rewrite every mixed-entity segment into single-entity segments.

        Returns
        -------
        OptimizeReport
            Complete counts and versions for the operation. A successful no-op returns a
            report with `no_op=True` and equal starting and committed versions.

        Raises
        ------
        TimeseriesTableError
            If optimization is not applicable or rewriting, validation, commit, or cleanup
            fails. The exception includes a `table_root` attribute.
        """
        ...

    def root(self) -> str:
        """Return the table root path."""
        ...

    def version(self) -> int:
        """Return the current table version."""
        ...

    def index_spec(self) -> dict[str, object]:
        """Return exactly one variant-specific ordered-index specification.

        Timestamp:

            {
                "index_column": str,
                "entity_columns": list[str],
                "index_type": "timestamp",
                "index_granularity": str,
                "timezone": str | None,
            }

        Int64:

            {
                "index_column": str,
                "entity_columns": list[str],
                "index_type": "int64",
                "index_granularity": int,
            }

        UInt64:

            {
                "index_column": str,
                "entity_columns": list[str],
                "index_type": "uint64",
                "index_granularity": int,
            }
        """
        ...

class _TestingModule(ModuleType):
    class _AppendStreamReleaseCounter:
        @property
        def count(self) -> int: ...

    def _test_sleep_without_gil(self, millis: int) -> None: ...
    def _test_session_table_exists(self, session: Session, name: str) -> bool: ...
    def _test_sql_reader_unsupported_schema(self) -> None: ...
    def _test_sql_reader_midstream_error(self) -> pyarrow.RecordBatchReader: ...
    def _test_append_stream_with_release_counter(
        self, *, fail_after_first: bool, with_error_details: bool = True
    ) -> tuple[object, _AppendStreamReleaseCounter]: ...
    def _test_append_stream_with_schema_import_error(
        self,
    ) -> tuple[object, _AppendStreamReleaseCounter]: ...
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
