# timeseries-table-datafusion

This crate lets you query a `timeseries-table-core` table using DataFusion SQL.
It focuses on time-series workloads and includes **compile-time pruning**:
when a query has a time predicate, we try to determine which data segments
cannot possibly match, and skip them before execution.

The goal is simple: keep SQL queries fast without changing your data.

## What works well today

### Basic time comparisons
We recognize direct comparisons between the timestamp column and a literal:

- `ts < '2024-01-01T00:00:00Z'`
- `ts >= '2024-01-01T00:00:00Z'`
- `ts = '2024-01-01T00:00:00Z'`
- `ts != '2024-01-01T00:00:00Z'`

The literal can be an RFC3339 string or a timestamp literal. Date-only strings
like `'2024-01-01'` are treated as midnight UTC.

### BETWEEN / IN / NOT / AND / OR
These are supported as long as they ultimately boil down to time comparisons:

- `ts BETWEEN '...' AND '...'`
- `ts IN ('...', '...')`
- `ts NOT IN ('...')`
- `NOT (ts < '...')`
- Combined with `AND` / `OR`

When a predicate can’t be understood, we **do not prune** (safe fallback).

### Interval arithmetic on `ts`
We support SQL interval arithmetic as long as it’s based on **interval literals**:

- `ts + INTERVAL '1 day' < '...'`
- `ts - INTERVAL '2 hours' <= '...'`
- `ts + INTERVAL '1 day' + INTERVAL '1 hour' < '...'`
- `INTERVAL '1 day' + ts < '...'`

Intervals can include common units (seconds, millis, micros, nanos, minutes,
hours, days, months, years). Mixed intervals are handled by combining months,
days, and sub-day nanos.

### `to_timestamp*` scalar functions
We recognize DataFusion’s timestamp helper functions when they use **literals**:

- `to_timestamp(...)`
- `to_timestamp_seconds(...)`
- `to_timestamp_millis(...)`
- `to_timestamp_micros(...)`
- `to_timestamp_nanos(...)`

Both string literals and numeric epoch literals are supported.

Example:

```
WHERE ts < to_timestamp_millis(1704672000123)
```

### `to_unixtime(ts)`
We also recognize `to_unixtime(ts)` when it is compared to a **numeric** literal:

- `to_unixtime(ts) >= 1704672000`
- `1704672000 < to_unixtime(ts)`

This uses seconds since epoch. String literals are not supported here.

### `to_date(ts)`
We recognize `to_date(ts)` when it is compared to a **date literal**:

- `to_date(ts) = '2024-01-01'`
- `to_date(ts) < '2024-01-01'`
- `to_date(ts) >= '2024-01-01'`

The date literal should be `YYYY-MM-DD`. The comparison is expanded into a
timestamp range for that whole day. If the timestamp column has a timezone,
day boundaries are computed in that timezone.

## What does NOT prune (yet)

These are intentionally treated as “unknown” to avoid incorrect pruning:

- `ts + 1 < ...` (numeric, not interval)
- `ts + other_column < ...`
- `to_timestamp(...) + ts < ...`
- `interval - ts` (non-commutative, ambiguous for our matcher)
- `to_unixtime(ts) < '1704672000'` (string literal)
- `to_date(ts) = 'not-a-date'` (invalid date literal)

Queries still run correctly; they just won’t be pruned.

## Notes for contributors

- The pruning logic is in `src/ts_table_provider/`.
- Unit tests for predicate logic live in `src/ts_table_provider/tests.rs`.
- Integration tests (SQL-level) live in `tests/ts_table_provider_tests.rs`.
- Test helpers for integration tests are in `src/test_utils.rs`.

## Quick example

```sql
SELECT *
FROM t
WHERE ts >= '2024-01-01T00:00:00Z'
  AND ts <  '2024-01-02T00:00:00Z'
```

This should prune any segment that is fully outside the range.
