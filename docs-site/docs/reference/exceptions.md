# Exceptions reference

All library-specific errors inherit from `TimeseriesTableError`, so you can catch any library
error with a single `except ttf.TimeseriesTableError`.

## Exception hierarchy

```
TimeseriesTableError
|-- StorageError                - filesystem or I/O problem
|-- ConflictError               - concurrent table metadata modification
|-- IndexIntervalOverlapError   - incoming interval conflicts with committed data
|-- DuplicateIndexIntervalError - incoming rows duplicate an identity and interval
|-- SchemaMismatchError         - incoming Arrow schema does not match the table schema
`-- DataFusionError             - SQL query failed inside DataFusion
```

## When you'll see each error

**`StorageError`** - raised when the filesystem operation fails. Common causes: the table root
directory doesn't exist, a file is missing, or a permissions problem. The error message includes
the path that caused the problem.

**`IndexIntervalOverlapError`** - raised by `append(...)` when an incoming row uses an identity
and logical index interval already present in committed data. The exception carries:

- `segment_path` - the generated table-relative path for the rejected segment
- `conflict_count` - the number of conflicting intervals, or identity and interval pairs
- `example_identity` - one complete identity as a dictionary, or `None` for a table without
  entity columns
- `example_index_interval` - one conflicting logical interval, such as
  `[-20, -10)`, `[50460, 50470)`, or
  `[1970-01-01T00:00:00Z, 1970-01-01T01:00:00Z)`

**`DuplicateIndexIntervalError`** - raised when two rows in one incoming append use the same
complete identity and logical index interval. It carries `segment_path`, `example_identity`, and
`example_index_interval`. It does not expose `conflict_count`.

These attributes are stable diagnostics. Inspect them directly instead of parsing exception
messages. See [Index granularity and conflicts](../concepts/index_granularity_and_conflicts.md)
for the uniqueness rule.

**`SchemaMismatchError`** - raised when an Arrow source you try to append has a schema that
conflicts with the table's established schema (set on the first successful append).

**`ConflictError`** - raised when a concurrent modification to the table metadata is detected.
In typical single-process usage this is rare; it can happen if two processes are appending to the
same table root simultaneously.

**`DataFusionError`** - raised when `Session.sql(...)` or `Session.sql_reader(...)` encounters a
SQL error (syntax error, type error, unknown column, etc.).

---

## API reference

::: timeseries_table_format.TimeseriesTableError
    options:
      show_source: false

::: timeseries_table_format.StorageError
    options:
      show_source: false

::: timeseries_table_format.ConflictError
    options:
      show_source: false

::: timeseries_table_format.IndexIntervalOverlapError
    options:
      show_source: false

::: timeseries_table_format.DuplicateIndexIntervalError
    options:
      show_source: false

::: timeseries_table_format.SchemaMismatchError
    options:
      show_source: false

::: timeseries_table_format.DataFusionError
    options:
      show_source: false
