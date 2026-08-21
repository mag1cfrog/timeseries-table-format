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

class CoverageOverlapError(TimeseriesTableError):
    segment_path: str
    """Path to the Parquet segment that triggered the overlap."""
    overlap_count: int
    """Number of overlapping buckets, or entity/bucket pairs for an entity-aware table."""
    example_entity_identity: dict[str, str | int] | None
    """One typed identity in configured column order, or `None` for global coverage."""
    example_bucket: int | None
    """Internal overlapping bucket identifier, retained for compatibility."""
    example_bucket_range: str
    """Logical ordered-index interval covered by `example_bucket`."""

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
            Ordered column names that define independent identities within the table. One
            Parquet segment may contain multiple identities.
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

        The persisted ordered-index specification determines the required column and exact
        Arrow type; append accepts no index override.

        Parameters
        ----------
        parquet_path:
            Path to a Parquet file.
        copy_if_outside:
            If `True`, copies the file under the table root before appending.
            If `False`, `parquet_path` must be a table-relative storage key.

        Raises
        ------
        CoverageOverlapError:
            If the same complete entity identity already covers an incoming bucket. Different
            identities may reuse the same bucket.
        """
        ...

    def append(
        self,
        source: pyarrow.RecordBatch
        | pyarrow.Table
        | pyarrow.RecordBatchReader
        | _ArrowStreamExportable,
    ) -> int:
        """Append an Arrow source and return the committed table version."""
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
                "column": str,
                "entity_columns": list[str],
                "kind": "timestamp",
                "bucket": str,
                "timezone": str | None,
            }

        Int64:

            {
                "column": str,
                "entity_columns": list[str],
                "kind": "int64",
                "bucket_width": int,
            }

        UInt64:

            {
                "column": str,
                "entity_columns": list[str],
                "kind": "uint64",
                "bucket_width": int,
            }
        """
        ...

class _TestingModule(ModuleType):
    class _AppendStreamReleaseCounter:
        @property
        def count(self) -> int: ...

    def _test_trigger_overlap(
        self, table_root: str, first_parquet_path: str, second_parquet_path: str
    ) -> None: ...
    def _test_sleep_without_gil(self, millis: int) -> None: ...
    def _test_session_table_exists(self, session: Session, name: str) -> bool: ...
    def _test_sql_reader_unsupported_schema(self) -> None: ...
    def _test_sql_reader_midstream_error(self) -> pyarrow.RecordBatchReader: ...
    def _test_append_release_counted_stream(
        self, *, fail_after_first: bool
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
