# Register and join tables

Register multiple tables in one `Session` to join them with DataFusion SQL.
Complete the [first-table tutorial](create_append_query.md) before starting.

## Run the example

Run this example from an empty working directory. It creates `prices` and
`volumes` tables, registers both, and joins them on `(ts, symbol)`.

```python
--8<-- "crates/timeseries-table-python/examples/register_and_join_two_tables.py"
```

Use stable SQL names when registering tables so application queries do not
depend on filesystem paths.

You can also mix managed tables and plain Parquet data in one session:

- `register_tstable(...)` registers a managed table root.
- `register_parquet(...)` registers an unmanaged Parquet file or directory.

See the [Session reference](../reference/session.md) for the complete API.
