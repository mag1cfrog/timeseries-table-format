# Tutorial: create, append, query

**Goal:** Create a table on disk, append a Parquet segment, then query it with SQL.

**Prereqs:** Installed `timeseries-table-format` (see [Installation](../install.md)).

**What you'll learn:**
- How a table root is created and stays self-contained on disk
- How appends work (and what overlap detection is protecting you from)
- How `Session` queries registered tables and returns a `pyarrow.Table`

!!! tip "Mental model"
    - `TimeSeriesTable` manages the on-disk table and appends.
    - `Session` runs SQL over what you register (tables, Parquet datasets, etc.).

## Steps

1) Create a table root (`TimeSeriesTable.create`)
2) Write a tiny Parquet segment (toy data)
3) Append it (`append_parquet`)
4) Create a SQL session (`Session`)
5) Register the table (`register_tstable`)
6) Query (`Session.sql`) -> `pyarrow.Table`

The full example below is the exact code used in docs (kept in sync with the repo):

```python
--8<-- "crates/timeseries-table-python/examples/quickstart_create_append_query.py"
```

## What happens in the example?

### Create a table

`TimeSeriesTable.create(...)` initializes a table root directory and writes initial metadata.

!!! note "`entity_columns` explained"
    `entity_columns=["symbol"]` tells the table that coverage is tracked **per symbol independently**.
    That means AAPL at 10:00 and NVDA at 10:00 are considered separate coverage - appending data
    for one symbol never blocks appends for a different symbol in the same time window.

#### Use an integer index

The main example uses a Timestamp. For logical time, use these index configurations:

```python
signed = ttf.TimeSeriesTable.create(
    table_root="signed_ticks",
    index_column="tick",
    index_type="int64",
    bucket_width=10,
)

unsigned = ttf.TimeSeriesTable.create(
    table_root="unsigned_counters",
    index_column="counter",
    index_type="uint64",
    bucket_width=100,
)
```

The Parquet columns must be Arrow `int64` and `uint64`, respectively. Append and register these
tables exactly as in the main example. Use signed SQL expressions for Int64 and an explicit cast
for UInt64 literals above `i64::MAX`:

```sql
SELECT * FROM signed_ticks WHERE tick >= -20 AND tick < 0;
SELECT * FROM unsigned_counters
WHERE counter >= CAST('9223372036854775808' AS BIGINT UNSIGNED);
```

### Append a Parquet segment

`append_parquet(...)` adds the Parquet file as a new segment.

By default, if the Parquet file is outside the table root, it is copied under the table root
before being committed (so the table is self-contained on disk).

!!! warning "What happens if you run this twice?"
    If you run the example a second time against the same table root, `append_parquet(...)` will
    raise `CoverageOverlapError`. That's intentional - the table already has coverage for those
    hour buckets, so it refuses to re-ingest the same window. This is the overlap detection
    working as designed.

    To reset for experimentation, delete the table root directory and start fresh.

### Query with SQL

`Session` is a DataFusion-backed SQL session. You register a table under a name and then query it.

`Session.sql(...)` returns a `pyarrow.Table`.

!!! tip "Streaming large results"
    For large result sets, `Session.sql_reader(...)` returns a streaming `pyarrow.RecordBatchReader`
    instead of materializing the full result into memory. See [Reference: Session](../reference/session.md).

!!! tip "Notebook display"
    Notebook results use a bounded HTML preview by default. See
    [Configure notebook display](../guides/notebook_display.md) to change or
    disable it.

!!! note
    The Python API is synchronous. Internally, long-running Rust operations run on an internal
    Tokio runtime and release the GIL.

Next:
- Tutorial: [Register + join](register_and_join.md)
- Concept: [Buckets + overlap](../concepts/bucketing_and_overlap.md)
- Reference: [Session](../reference/session.md)
