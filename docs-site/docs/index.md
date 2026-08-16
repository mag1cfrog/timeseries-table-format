# timeseries-table-format (Python)

`timeseries-table-format` helps you manage **local, append-only time-series tables** on disk.
You append new data files over time and query across all of them with SQL, getting results back as
a Python table object. It also prevents you from loading the same chronological window twice.

!!! tip "New here? Start with the [Key concepts](#key-concepts-quick-reference) table below, then jump into the [Quickstart](#quickstart-create-append-query)."

## A quick example of what this solves

Imagine you collect hourly price bars (open/high/low/close/volume) for a set of stock symbols — one Parquet file arrives each day.
You want to query across 90 days of history with SQL, without building your own directory-scanning
logic and without accidentally re-ingesting a day you already loaded. That's exactly the problem
this library handles: it tracks what chronological windows are already covered (per symbol) and rejects
duplicates automatically.

## When to use this

- You ingest time-series data incrementally (new files arrive over time).
- You want SQL across segments/tables without building your own dataset plumbing.
- You want guardrails against accidentally re-ingesting the same time window (overlap detection).

??? note "When not to use this (v0)"
    - You need S3/object storage backends.
    - You need compaction, schema evolution, or upserts/merges.
    - You want a centralized database/server.

## Key concepts (quick reference)

| Term | What it means |
|---|---|
| **table root** | A local directory that holds a time-series table: metadata, segments, and coverage data. |
| **segment** | A single Parquet file appended to a table. A table is made up of one or more segments. |
| **chronological index** | One ascending Timestamp, Int64, or UInt64 column that orders the table's logical time. Public APIs call it the ordered index. |
| **bucket** | The coverage granularity: a duration for Timestamp or an index-value width for Int64 and UInt64. Does not resample data. |
| **entity** | The logical identity of a time series (e.g. a stock symbol). Defined by `entity_columns`. |
| **overlap detection** | The guard that prevents you from appending data for the same entity + bucket twice. |
| **Session** | A DataFusion SQL session. Register tables into it and run SQL queries returning `pyarrow.Table`. |
| **DataFusion** | The SQL engine used internally. You don't need to install it separately. |
| **Parquet** | A columnar file format commonly used for analytics data. Your segments are Parquet files. |
| **pyarrow** | A Python library for working with columnar data. Installed as a dependency; query results are returned as `pyarrow.Table`. |

## Install

```bash
pip install timeseries-table-format
```

See [Installation](install.md) for verification and notes about building from source.

## Quickstart: create → append → query

In this example:

- `TimeSeriesTable` manages the on-disk table and appends.
- `Session` runs SQL over what you register and returns a `pyarrow.Table`.

```python
--8<-- "crates/timeseries-table-python/examples/quickstart_create_append_query.py"
```

Next:
- Tutorial: [Create, append, query](tutorials/create_append_query.md)
- Tutorial: [Register + join](tutorials/register_and_join.md)
- Tutorial: [Parameterized queries](tutorials/parameterized_queries.md)
- Tutorial: [Real-world workflow](tutorials/real_world_workflow.md)
- Concept: [Chronological indexes, buckets + overlap](concepts/bucketing_and_overlap.md)
- Reference: [Session](reference/session.md)
