# Decide if this project fits

`timeseries-table-format` manages local, append-only time-series tables built
from Parquet segments. It tracks coverage, rejects overlapping appends, and
queries committed segments with DataFusion SQL.

## A good fit

Use it when:

- New time-series Parquet files arrive over time.
- You want one managed table root instead of custom file-discovery code.
- Each entity and time bucket should be ingested at most once.
- You want SQL results as `pyarrow.Table` or `pyarrow.RecordBatchReader` objects.

## Choose another tool when

- You only need ad hoc queries over a few files. Query Parquet directly with a
  tool such as DuckDB or Polars.
- You need row updates or a central database server. Use a database.
- You need object storage, compaction, schema evolution, or merge operations.
  Use a lakehouse format designed for those workflows.

The current release supports local filesystems and append-only ingestion. If
that matches your workload, continue with [Installation](../install.md).
