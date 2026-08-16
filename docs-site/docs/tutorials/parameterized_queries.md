# Use SQL parameters

Pass values through `params` instead of inserting them into a SQL string.
`Session.sql(...)` and `Session.sql_reader(...)` support the same placeholder
styles.

| Style | SQL placeholder | Python value |
|---|---|---|
| Positional | `$1`, `$2`, ... | `list` or `tuple` |
| Named | `$name` | `dict` |

This example runs one query with each style:

```python
--8<-- "crates/timeseries-table-python/examples/parameterized_queries.py"
```

## Supported values

Parameters accept `None`, `bool`, `int` in the Int64 range, `float`, `str`, and
`bytes`.

DataFusion usually infers a parameter type from its context. A placeholder in
a `SELECT` projection may need an explicit cast:

```sql
SELECT CAST($1 AS BIGINT) AS value;
```

For UInt64 values above `i64::MAX`, see
[Integer ordered indexes](../guides/integer_indexes.md).
