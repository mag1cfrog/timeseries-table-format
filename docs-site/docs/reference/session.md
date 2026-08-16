# Session reference

`Session` registers data sources and runs DataFusion SQL queries.

## Register data sources

- `register_tstable(...)` registers every committed segment in a managed table root.
- `register_parquet(...)` registers an unmanaged Parquet file or directory.

Both sources use the same SQL query API.

## Choose a result type

| Method | Return type | Use when |
|---|---|---|
| `sql(...)` | `pyarrow.Table` | The complete result fits in memory |
| `sql_reader(...)` | `pyarrow.RecordBatchReader` | You want to process large results in batches |

See [Performance](../performance.md) for benchmarks and streaming examples.
For integer-index query rules, see
[Integer ordered indexes](../guides/integer_indexes.md).

Queries use DataFusion SQL. See the
[DataFusion SQL reference](https://datafusion.apache.org/user-guide/sql/index.html)
for supported syntax and functions.

## API

::: timeseries_table_format.Session
    options:
      members: true
      show_source: false
