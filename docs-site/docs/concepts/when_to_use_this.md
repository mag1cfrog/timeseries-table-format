# Decide if this project fits

`timeseries-table-format` manages local time-series tables built
from immutable Parquet segments. It tracks coverage, rejects overlapping appends, and
queries committed segments with DataFusion SQL.

## A good fit

Use it when:

- New time-series Parquet files arrive over time.
- You want one managed table root instead of custom file-discovery code.
- Each complete entity identity should have at most one row per index interval.
- You want SQL results as `pyarrow.Table` or `pyarrow.RecordBatchReader` objects.
- You need occasional backfills or corrections to selected values by complete row key.

## Choose another tool when

- You only need ad hoc queries over a few files. Query Parquet directly with a
  tool such as DuckDB or Polars.
- You need frequent low-latency point updates or a central database server. Use a database;
  keyed updates here rewrite affected Parquet segments.
- You need object storage, small-file compaction, column dropping/renaming, automatic schema merging, or merge operations.
  Use a lakehouse format designed for those workflows.

The current release supports local filesystems, append ingestion, and explicit keyed updates. If
that matches your workload, continue with [Installation](../install.md).
