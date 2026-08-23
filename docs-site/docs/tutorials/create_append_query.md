# Create, append, and query your first table

This tutorial creates a timestamp-indexed table, appends one Parquet file that
contains two symbols, and queries both identities through one logical table.

Before you start, [install and verify the Python package](../install.md).

## Run the example

Run this example from an empty working directory. It creates a table at
`./my_table` and prints four rows.

```python
--8<-- "crates/timeseries-table-python/examples/quickstart_create_append_query.py"
```

## 1. Create the table

`TimeSeriesTable.create(...)` initializes the table root and its metadata:

- `index_column="ts"` selects the ascending timestamp column.
- `index_granularity="1h"` divides the timestamp domain into one-hour logical intervals.
- `entity_columns=["exchange_id", "symbol"]` tracks coverage independently for each
  composite identity.

This means NVDA and AAPL can use the same hour without conflicting. Each
complete identity may have at most one row in that hour. The
entity columns are an ordered identity definition, not instructions to create
one table per entity. `exchange_id` is an Arrow Int32 column, so the example
also shows a numeric identity component.

## 2. Append a Parquet segment

The example opens the Parquet file lazily as a `RecordBatchReader` and passes
it to `append(...)`. Append consumes the batches incrementally and writes one
table-owned segment. The source contains both NVDA and AAPL rows in the same
hourly intervals; a segment does not need to contain only one identity. The
Parquet `ts` column must be an Arrow timestamp because that is the index type
stored in the table metadata.

If another file uses an existing interval for NVDA, the append raises
`IndexIntervalOverlapError`. AAPL may independently use that same interval.
Two NVDA rows in the same incoming interval raise `DuplicateIndexIntervalError`.

!!! warning "Run the example once"
    Running the example again fails because `./my_table` already contains a
    table. Delete that directory before repeating the tutorial.

## 3. Optimize the entity layout

`optimize()` explicitly rewrites each mixed source segment into one replacement
segment per complete identity. In this example, the source segment becomes one
NVDA segment and one AAPL segment inside the same logical table.

Optimization is optional. It preserves the logical rows, schema, and per-entity
coverage. The returned `OptimizeReport` records the versions, source and
replacement segment counts, distinct identities, row counts, and whether the
operation was a no-op.

Calling `optimize()` again in the example returns a successful no-op. Its
starting and committed versions are equal, so it does not create another table
version. Replaced source files may remain on disk until a future vacuum
operation removes unreferenced files.

This operation is specific to entity layout. It does not combine small files or
accept a target file size.

## 4. Query with SQL

`Session` provides the DataFusion SQL engine. The example registers the table
as `prices` before and after optimization. It verifies that both queries return
the same rows, then returns the optimized result as a `pyarrow.Table`.

Both entity columns remain normal SQL columns. You can use
`WHERE exchange_id = 1 AND symbol = 'NVDA'` to select one identity or group by
both columns to calculate independent aggregates.

You now have a locally queryable table root that can accept more non-overlapping
Parquet segments.

Next, learn how to [append files incrementally](real_world_workflow.md).
