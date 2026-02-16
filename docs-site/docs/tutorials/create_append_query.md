# Tutorial: create, append, query

**Goal:** Create a table on disk, append a Parquet segment, then query it with SQL.

**Prereqs:** Installed `timeseries-table-format` (see [Installation](../install.md)).

**What you’ll learn:**
- How a table root is created and stays self-contained on disk
- How appends work (and what overlap detection is protecting you from)
- How `Session` queries registered tables and returns Arrow

!!! tip "Mental model"
    - `TimeSeriesTable` manages the on-disk table and appends.
    - `Session` runs SQL over what you register (tables, Parquet datasets, etc.).

## Steps

1) Create a table root (`TimeSeriesTable.create`)
2) Write a tiny Parquet segment (toy data)
3) Append it (`append_parquet`)
4) Create a SQL session (`Session`)
5) Register the table (`register_tstable`)
6) Query (`Session.sql`) → `pyarrow.Table`

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

!!! tip "Notebook display"
    In IPython/Jupyter (including VS Code notebooks), `pyarrow.Table` results display as a bounded HTML preview by default (the return type is still a real `pyarrow.Table`).

    - Opt-out: set `TTF_NOTEBOOK_DISPLAY=0` before importing `timeseries_table_format`, or call `timeseries_table_format.disable_notebook_display()`
    - Configure: call `timeseries_table_format.enable_notebook_display(max_rows=..., max_cols=..., max_cell_chars=..., align=...)`
    - Alignment: set `TTF_NOTEBOOK_ALIGN=auto|left|right` before importing `timeseries_table_format` (or pass `align=...` to `enable_notebook_display(...)`)

!!! note
    The Python API is synchronous. Internally, long-running Rust operations run on an internal
    Tokio runtime and release the GIL.

Next:
- Tutorial: [Register + join](register_and_join.md)
- Concept: [Buckets + overlap](../concepts/bucketing_and_overlap.md)
- Reference: [Session](../reference/session.md)
