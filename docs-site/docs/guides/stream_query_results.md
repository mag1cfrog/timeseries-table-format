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

## Reduce memory for wide rows

Streaming avoids collecting the full result in Python when you process and
discard each batch. It does not set a byte limit on decoded batches or on
DataFusion's parallel execution. Wide binary or string columns can make a
single batch large, and several batches can be in flight. Compressed Parquet
file size is not a decoded memory budget.

To try a smaller memory footprint, apply these settings before creating the
reader:

```python
session.sql("SET datafusion.execution.batch_size = 1024")
session.sql("SET datafusion.execution.target_partitions = 1")
```

`batch_size` sets the target number of rows per batch. Some operators, such
as `UNNEST`, can emit larger batches. Reducing `target_partitions` reduces
execution parallelism and can lower throughput. Measure both settings with
your query; neither sets a strict process memory ceiling. Select only the
columns you need, and avoid retaining processed batches in a list.

If you stop reading early, close the reader in the `finally` block above.
Closing releases the reader's resources and requests cancellation of its
producer task, but does not guarantee that the process immediately returns
memory to the operating system.

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
