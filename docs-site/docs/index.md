# timeseries-table-format (Python)

`timeseries-table-format` helps you manage **local, append-only time-series tables** stored as Parquet
segments on disk.

You append new segments (with **overlap detection**) and query one or many tables using **SQL
(DataFusion)**, getting results back as a `pyarrow.Table`.

## When to use this

- You ingest time-series data incrementally (new files arrive over time).
- You want SQL across segments/tables without building your own dataset plumbing.
- You want guardrails against accidentally re-ingesting the same time window (overlap detection).

??? note "When not to use this (v0)"
    - You need S3/object storage backends.
    - You need compaction, schema evolution, or upserts/merges.
    - You want a centralized database/server.

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
--8<-- "python/examples/quickstart_create_append_query.py"
```

Next:
- Tutorial: [Create, append, query](tutorials/create_append_query.md)
- Tutorial: [Register + join](tutorials/register_and_join.md)
- Tutorial: [Parameterized queries](tutorials/parameterized_queries.md)
- Concept: [Buckets + overlap](concepts/bucketing_and_overlap.md)
- Reference: [Session](reference/session.md)
