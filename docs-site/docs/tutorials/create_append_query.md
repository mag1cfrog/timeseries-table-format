# Tutorial: create, append, query

This tutorial walks through:
1) Creating a time-series table
2) Appending a Parquet segment
3) Querying it with SQL via `Session`

The full example below is the exact code used in docs (kept in sync with the repo):

```python
--8<-- "python/examples/quickstart_create_append_query.py"
```

## What happens in the example?

### Create a table

`TimeSeriesTable.create(...)` initializes a table root directory and writes initial metadata.

### Append a Parquet segment

`append_parquet(...)` adds the Parquet file as a new segment.

By default, if the Parquet file is outside the table root, it is copied under the table root
before being committed (so the table is self-contained on disk).

### Query with SQL

`Session` is a DataFusion-backed SQL session. You register a table under a name and then query it.

`Session.sql(...)` returns a `pyarrow.Table`.

!!! note
    The Python API is synchronous. Internally, long-running Rust operations run on an internal
    Tokio runtime and release the GIL.

