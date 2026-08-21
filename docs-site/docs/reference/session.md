# Session reference

`Session` registers data sources and runs DataFusion SQL queries.

## Register data sources

- `register_tstable(...)` registers every committed segment in a managed table root.
- `register_parquet(...)` registers an unmanaged Parquet file or directory.

Both sources use the same SQL query API.

A directory passed to `register_parquet(...)` must contain at least one Parquet
file so DataFusion can infer its schema. Registering an empty directory raises
`DataFusionError`.

## Choose a result type

| Method | Return type | Use when |
|---|---|---|
| `sql(...)` | `pyarrow.Table` | The complete result fits in memory |
| `sql_reader(...)` | `pyarrow.RecordBatchReader` | You want to process large results in batches |

See [Stream query results](../guides/stream_query_results.md) for usage and
[Streaming query performance](../performance.md) for benchmarks.
For integer-index query rules, see
[Integer ordered indexes](../guides/integer_indexes.md).

`Session.sql(...)` returns a normal `pyarrow.Table`, so it can be passed to
other Arrow-compatible libraries. For example, after installing Polars:

```python
import polars as pl

frame = pl.from_arrow(session.sql("SELECT * FROM prices"))
```

SQL result rows have no guaranteed order unless the query includes an
`ORDER BY` clause.

Queries use DataFusion SQL. See the
[DataFusion SQL reference](https://datafusion.apache.org/user-guide/sql/index.html)
for supported syntax and functions.

## API

::: timeseries_table_format.Session
    options:
      members: true
      show_source: false
