# timeseries-table-format (Python)

`timeseries-table-format` provides a Python-first workflow for:

- Creating/opening local filesystem time-series tables
- Appending Parquet segments (with overlap detection)
- Querying with SQL across multiple registered tables (including joins)
- Getting results back as a `pyarrow.Table`

Non-goals for v0: S3/object-store backends, compaction, schema evolution, upserts/merges.

## Quickstart: create → append → query

```python
--8<-- "python/examples/quickstart_create_append_query.py"
```

## Join across two registered tables

```python
--8<-- "python/examples/register_and_join_two_tables.py"
```

## Arrow interop (optional)

`Session.sql(...)` returns a `pyarrow.Table`. If you want to use Polars:

```python
import polars as pl

df = pl.from_arrow(out)
```

Next:
- Follow the tutorials for step-by-step explanations
- See the Reference section for `Session` and `TimeSeriesTable`

