# Stream query results

Use `Session.sql_reader(...)` when a query result is too large to keep in
memory or when processing should begin before the full query completes.

```python
import timeseries_table_format as ttf

session = ttf.Session()
session.register_tstable("prices", "prices_table")

reader = session.sql_reader(
    "SELECT * FROM prices WHERE ts > TIMESTAMP '2024-05-01 00:00:00'"
)
try:
    for batch in reader:
        print(batch.num_rows)
finally:
    reader.close()
```

Each item is a `pyarrow.RecordBatch`. Process or write each batch inside the
loop so earlier batches can be released from memory.

Always close the reader, including when batch processing raises an exception.

## Choose the result API

| Need | Method |
|---|---|
| A `pyarrow.Table` that fits in memory | `Session.sql(...)` |
| Incremental processing | `Session.sql_reader(...)` |
| Lower time to first result | `Session.sql_reader(...)` |

If you plan to call `reader.read_all()`, use `Session.sql(...)` directly. Both
paths materialize the complete result, and `sql(...)` is simpler.

See [Streaming query performance](../performance.md) for measured latency and
memory results.
