# Reference: Session

`Session` is a DataFusion-backed SQL session. It supports registering multiple tables and running
SQL queries that return `pyarrow.Table`.

## Query ordered indexes

Registered tables preserve their Timestamp, Int64, or UInt64 Arrow index type. Use SQL expressions
that match the domain:

```sql
-- Timestamp
SELECT ts FROM events
WHERE ts >= TIMESTAMP '2026-01-01 00:00:00';

-- Int64
SELECT tick FROM signed_ticks
WHERE tick >= -20 AND tick < 0;

-- UInt64 above i64::MAX
SELECT counter FROM unsigned_counters
WHERE counter >= CAST('9223372036854775808' AS BIGINT UNSIGNED);
```

Given `2**63`, `2**63 + 1`, and `2**64 - 1` in `unsigned_counters`, the default Arrow
result path remains exact:

```python
import pyarrow as pa

out = sess.sql(
    "SELECT counter FROM unsigned_counters "
    "WHERE counter >= CAST('9223372036854775808' AS BIGINT UNSIGNED) "
    "ORDER BY counter"
)
assert out.schema.field("counter").type == pa.uint64()
assert out["counter"].to_pylist() == [2**63, 2**63 + 1, 2**64 - 1]
```

Python query parameters accept integers in the Int64 range. Use the explicit unsigned cast above
for larger literals. `Session` SQL is the Python query surface; direct range-scan and coverage
bindings are not exposed.

## `sql` vs `sql_reader`: which to use?

`Session` provides two query APIs:

- **`Session.sql(...)`** — returns a fully materialized `pyarrow.Table`. Simple and convenient;
use this when the result fits comfortably in memory.
- **`Session.sql_reader(...)`** — returns a streaming `pyarrow.RecordBatchReader`. Batches arrive
as the engine produces them; use this when you want lower memory usage or faster time-to-first-row.

For large queries (~10M rows), benchmarks show `sql_reader(...)` delivers the first batch
**~80% earlier** and uses **24–36% less peak RSS** in process-as-you-go workloads.
See [Performance](../performance.md) for full benchmark results.

## SQL result export mode

`Session.sql(...)` exports Arrow results to Python in one of two ways:

- Arrow C Data Interface (C Stream) when supported (preferred; avoids IPC serialization + large bytes copies)
- Arrow IPC stream as a fallback

The C Stream exporter supports common nested Arrow types like `List`, `Struct`, and `Map`.
Some edge-case-heavy types (e.g. `Union`, `ListView`) are not enabled yet: `auto` mode falls back to IPC, and
`c_stream` mode errors.

You can control the behavior via environment variables (set before calling `Session.sql(...)`):

- `TTF_SQL_EXPORT_MODE=auto|ipc|c_stream` (default: `c_stream`)
  - `auto`: try C Stream, fall back to IPC if C Stream export/import fails
  - `ipc`: force IPC
  - `c_stream`: force C Stream (no IPC fallback; errors propagate)
- `TTF_SQL_EXPORT_DEBUG=1` to emit a debug warning when `auto` falls back from C Stream → IPC
- `TTF_SQL_EXPORT_AUTO_RERUN_FALLBACK=1` to re-run the query when C Stream fails in `auto` mode (avoids cloning the collected batches, but may change results for non-deterministic queries)

::: timeseries_table_format.Session
    options:
      members: true
      show_source: false
