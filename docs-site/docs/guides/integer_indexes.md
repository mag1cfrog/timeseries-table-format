# Use an integer ordered index

Use an Int64 or UInt64 ordered index when your data follows a logical clock,
sequence number, or counter instead of a timestamp. The units are defined by
your application.

## Create the table

Set `index_type` and provide a positive `bucket_width` in index-value units:

```python
import timeseries_table_format as ttf

signed = ttf.TimeSeriesTable.create(
    table_root="signed_ticks",
    index_column="tick",
    index_type="int64",
    bucket_width=10,
)

unsigned = ttf.TimeSeriesTable.create(
    table_root="unsigned_counters",
    index_column="counter",
    index_type="uint64",
    bucket_width=100,
)
```

The corresponding Parquet columns must be Arrow `int64` and `uint64`. The
library does not convert between signed and unsigned values.

Append and register integer-indexed tables in the same way as timestamp-indexed
tables.

## Query large UInt64 values

Signed integer expressions work normally for Int64 indexes:

```sql
SELECT * FROM signed_ticks WHERE tick >= -20 AND tick < 0;
```

For UInt64 literals above `i64::MAX`, use an explicit unsigned cast:

```sql
SELECT * FROM unsigned_counters
WHERE counter >= CAST('9223372036854775808' AS BIGINT UNSIGNED);
```

See [Buckets and overlap](../concepts/bucketing_and_overlap.md) for integer
bucketing semantics and current index limitations.
