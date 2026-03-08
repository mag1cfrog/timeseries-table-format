# Reference: exceptions

All library-specific errors inherit from `TimeseriesTableError`, so you can catch any library
error with a single `except ttf.TimeseriesTableError`.

## Exception hierarchy

```
TimeseriesTableError
├── StorageError          — filesystem or I/O problem (e.g. missing path, permission denied)
├── ConflictError         — concurrent modification conflict on table metadata
├── CoverageOverlapError  — append rejected because its time window is already covered
├── SchemaMismatchError   — Parquet schema does not match the table's established schema
└── DataFusionError       — SQL query failed inside the DataFusion engine
```

## When you'll see each error

**`StorageError`** — raised when the filesystem operation fails. Common causes: the table root
directory doesn't exist, a file is missing, or a permissions problem. The error message includes
the path that caused the problem.

**`CoverageOverlapError`** — raised by `append_parquet(...)` when the incoming segment covers a
time bucket that is already occupied for the same entity. This is intentional — it protects you
from double-ingesting data. The exception carries:
- `segment_path` — which file triggered the rejection
- `overlap_count` — how many (entity, bucket) pairs already had coverage
- `example_bucket` — one example overlapping bucket timestamp (epoch microseconds), useful for debugging

See [Buckets + overlap](../concepts/bucketing_and_overlap.md) for background.

**`SchemaMismatchError`** — raised when a Parquet file you try to append has a schema that
conflicts with the table's established schema (set on the first successful append).

**`ConflictError`** — raised when a concurrent modification to the table metadata is detected.
In typical single-process usage this is rare; it can happen if two processes are appending to the
same table root simultaneously.

**`DataFusionError`** — raised when `Session.sql(...)` or `Session.sql_reader(...)` encounters a
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

::: timeseries_table_format.CoverageOverlapError
    options:
      show_source: false

::: timeseries_table_format.SchemaMismatchError
    options:
      show_source: false

::: timeseries_table_format.DataFusionError
    options:
      show_source: false

