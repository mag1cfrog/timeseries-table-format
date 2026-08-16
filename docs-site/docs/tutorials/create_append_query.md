# Create, append, and query your first table

This tutorial creates a timestamp-indexed table, appends one Parquet file, and
queries the result with SQL.

Before you start, [install and verify the Python package](../install.md).

## Run the example

Run this example from an empty working directory. It creates a table at
`./my_table` and prints three rows.

```python
--8<-- "crates/timeseries-table-python/examples/quickstart_create_append_query.py"
```

## 1. Create the table

`TimeSeriesTable.create(...)` initializes the table root and its metadata:

- `index_column="ts"` selects the ascending timestamp column.
- `bucket="1h"` tracks coverage in one-hour windows. It does not resample data.
- `entity_columns=["symbol"]` tracks coverage independently for each symbol.

This means NVDA and AAPL can cover the same hour without conflicting.

## 2. Append a Parquet segment

`append_parquet(...)` adds the file as a table segment. The Parquet `ts` column
must be an Arrow timestamp because that is the index type stored in the table
metadata.

If the incoming file covers a bucket that already exists for the same entity,
the append raises `CoverageOverlapError`. This prevents duplicate ingestion.

!!! warning "Run the example once"
    Running the example again fails because `./my_table` already contains a
    table. Delete that directory before repeating the tutorial.

## 3. Query with SQL

`Session` provides the DataFusion SQL engine. The example registers the table
as `prices`, runs a query, and returns the result as a `pyarrow.Table`.

You now have a self-contained table root that can accept more non-overlapping
Parquet segments.

Next, learn how to [append files incrementally](real_world_workflow.md).
