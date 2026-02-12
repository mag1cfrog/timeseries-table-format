# Python examples

These scripts are meant to be runnable in a normal Python environment where the
`timeseries-table-format` wheel is installed (import `timeseries_table_format`).

## `create_append_sql.py`

Creates a table in a temporary directory, writes a tiny Parquet file with
`pyarrow`, appends it, registers the table in a `Session`, then runs a SQL query
and prints the result.

Run from the repo root:

```bash
python/.venv/bin/python python/examples/create_append_sql.py
```

To keep the temp directory (useful for inspecting files on disk):

```bash
python/.venv/bin/python python/examples/create_append_sql.py --keep
```

To write into a specific location:

```bash
python/.venv/bin/python python/examples/create_append_sql.py --table-root ./demo_table
```

