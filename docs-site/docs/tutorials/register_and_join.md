# Tutorial: register multiple tables and join

**Goal:** Register multiple tables in one `Session` and join them with SQL.

**Prereqs:** Finished [Create, append, query](create_append_query.md) (or equivalent).

**What you’ll learn:**
- How `Session` can hold multiple registered tables at once
- How to structure joins on `(time, entity)` columns

`Session` supports registering multiple tables into one SQL session and running joins.

This tutorial creates two time-series tables, registers them as `prices` and `volumes`,
and joins them on `(ts, symbol)`.

```python
--8<-- "python/examples/register_and_join_two_tables.py"
```

## Tips

- Keep your SQL table names stable (e.g. `prices`, `volumes`, `symbols`) and register them at startup.
- You can mix time-series tables (`register_tstable`) and plain Parquet datasets (`register_parquet`) in
  the same `Session`.

Next:
- Tutorial: [Parameterized queries](parameterized_queries.md)
- Reference: [Session](../reference/session.md)
