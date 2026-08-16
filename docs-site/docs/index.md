---
title: timeseries-table-format Python documentation
description: Build local, append-only time-series tables and query them with DataFusion SQL.
---

# timeseries-table-format

Build local, append-only time-series tables from Parquet files. The Python API
tracks coverage, rejects overlapping appends, and queries your tables with
DataFusion SQL.

!!! note "Project status"
    This project is an early MVP. APIs and on-disk layouts may change before
    v1.0.

## Start here

Follow these pages in order:

1. [Check whether the project fits your workload](concepts/when_to_use_this.md).
2. [Install and verify the Python package](install.md).
3. [Create, append, and query your first table](tutorials/create_append_query.md).

## The basic workflow

1. Create a table root with an ordered index, bucket size, and optional entity
   columns.
2. Append Parquet files as new segments. The table rejects time windows that
   overlap existing coverage for the same entity.
3. Register the table in a `Session` and query it with SQL. Results are returned
   as `pyarrow.Table` objects.

After the first-table tutorial, continue with the **Common tasks** section in
the navigation.
