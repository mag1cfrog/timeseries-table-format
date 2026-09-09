# Python examples

These scripts are meant to be runnable in a normal Python environment where the
`timeseries-table-format` wheel is installed (import `timeseries_table_format`).

## `add_nullable_columns.py`

Creates a temporary table, appends historical rows, adds nullable fields, replaces the SQL
registration, and appends values and omissions. Run with
`python crates/timeseries-table-python/examples/add_nullable_columns.py` from the repository root.

## `update_rows.py`

Adds a nullable destination, captures the version before reading and computing values, then
updates selected rows and checks the report and reopened SQL results. Run with
`python crates/timeseries-table-python/examples/update_rows.py` from the repository root.

## `create_append_sql.py`

Creates a table in a temporary directory, writes a tiny Parquet file with
`pyarrow`, appends it, registers the table in a `Session`, then runs a SQL query
and prints the result.

Run from the repo root:

```bash
crates/timeseries-table-python/.venv/bin/python crates/timeseries-table-python/examples/create_append_sql.py
```

To keep the temp directory (useful for inspecting files on disk):

```bash
crates/timeseries-table-python/.venv/bin/python crates/timeseries-table-python/examples/create_append_sql.py --keep
```

To write into a specific location:

```bash
crates/timeseries-table-python/.venv/bin/python crates/timeseries-table-python/examples/create_append_sql.py --table-root ./demo_table
```
