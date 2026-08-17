# DataFusion integration

The optional `datafusion` feature lets you query a `timeseries-table-format` table using DataFusion SQL.
It focuses on time-series workloads and includes **segment-level pruning**:
when a query has a time predicate, we determine which data segments cannot
possibly match (based on each segment's `ts_min`/`ts_max`), and skip them
before execution.

The goal is simple: keep SQL queries fast without changing your data.

## Getting Started

### Installation

```toml
[dependencies]
timeseries-table-format = { version = "0.1", features = ["datafusion"] }
datafusion = "51"
tokio = { version = "1", features = ["rt-multi-thread"] }
```

### Rust API Example

```rust
use datafusion::prelude::*;
use std::sync::Arc;
use timeseries_table_format::{
    datafusion::TsTableProvider,
    storage::TableLocation,
    table::TimeSeriesTable,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open an existing table
    let table = TimeSeriesTable::open(TableLocation::local("./my_table")).await?;

    // 2. Create a TsTableProvider (implements DataFusion's TableProvider)
    let provider = TsTableProvider::try_new(Arc::new(table))?;

    // 3. Register it in a DataFusion SessionContext
    let ctx = SessionContext::new();
    ctx.register_table("my_table", Arc::new(provider))?;

    // 4. Run a query with time filters + projection
    //    Time filters enable segment pruning automatically
    let df = ctx.sql("
        SELECT ts, symbol, close
        FROM my_table
        WHERE ts >= '2024-01-01T00:00:00Z'
          AND ts <  '2024-02-01T00:00:00Z'
        ORDER BY ts
        LIMIT 100
    ").await?;

    // 5. Collect and print results
    let batches = df.collect().await?;
    for batch in &batches {
        println!("{:?}", batch);
    }

    // Or use show() for a formatted table
    // df.show().await?;

    Ok(())
}
```

### How Pruning Works

When you include time predicates in your query (e.g., `WHERE ts >= '...' AND ts < '...'`),
the `TsTableProvider` extracts those filters and compares them against each segment's
metadata (`ts_min`, `ts_max`). Segments that fall entirely outside the query range
are skipped—no I/O is performed for them.

This happens automatically; you don't need to do anything special.

### Current Limitations

- **Read-only**: This integration is for querying only. Use `timeseries-table-format` or the CLI for writes.
- **Best-effort filter extraction**: Complex predicates may not be fully recognized (see below). Unrecognized predicates fall back to scanning all segments—correctness is preserved.
- **No custom execution plan**: We use DataFusion's default `ParquetExec` after pruning. Future versions may add custom operators.

---

## Pruning Reference

Below is a detailed reference of which SQL predicates enable segment pruning.

### Entity equality pruning

For tables with configured entity columns, direct equality predicates with an
exact string, Int32, Int64, or UInt64 literal can prune segments from
transaction-log metadata alone. No Parquet file, footer, or coverage sidecar
is opened to make this decision.

For example, with string `symbol` or Int32 `device_id` configured as an entity
column:

```sql
WHERE symbol = 'A'
```

```sql
WHERE device_id = -1
```

- A `Single(identity)` segment is retained when its `symbol` component is `A`.
- A `Single(identity)` segment is skipped when its `symbol` component conflicts.
- Entity metadata alone never excludes a `Mixed` segment because it may contain
  `A`; ordered-index metadata may still exclude it independently.

Composite entities follow the configured entity-column order. A predicate may
specify every component or only some components. Any specified conflict can
exclude a `Single(identity)` segment; unspecified components do not prevent a
compatible segment from remaining a candidate.

Entity equality pruning composes with ordered-index pruning. For example,
`WHERE symbol = 'A' AND ts >= '2024-01-01T00:00:00Z'` can exclude a segment
because its identity conflicts, its ordered-index range cannot match, or both.

Reversed equality operands and qualified column references are supported, such
as `WHERE 'A' = prices.symbol` or `WHERE -1 = readings.device_id`. The literal
must have the exact entity-column type. Mismatched or unsupported literals,
range predicates, inequalities, functions, and other expressions do not enable
entity-level segment pruning.

Segment pruning is only an internal optimization. Filter pushdown remains
inexact, and DataFusion still evaluates the original predicate against the
rows from every retained segment.

### What works well today

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

### `date_trunc(precision, ts)`
We recognize `date_trunc` when it is compared to a **timestamp literal**:

- `date_trunc('hour', ts) = '2024-01-01T10:00:00Z'`
- `date_trunc('minute', ts) > '2024-01-01T10:30:00Z'`
- `date_trunc('day', ts) <= '2024-01-01T00:00:00Z'`

Supported precisions: `second`, `minute`, `hour`, `day`.

Behavior notes:
- Non-aligned literals (e.g. `10:30` for hour) are handled by moving the
  comparison to the next bucket boundary.
- If the timestamp column has an Olson timezone, hour/day boundaries are
  computed in that timezone (DST-aware). Fine granularity (second/minute) uses
  UTC arithmetic, matching DataFusion’s fast path.

### `date_bin(interval, ts[, origin])`
We recognize `date_bin` when all arguments are **literals**:

- `date_bin(interval '15 minutes', ts) = '2024-01-01T10:30:00Z'`
- `date_bin(interval '1 day', ts, '2024-01-01T03:00:00Z') > '2024-01-02T03:00:00Z'`

Supported interval forms:
- Day/time intervals (days, hours, minutes, seconds, millis, micros, nanos)
- Month intervals (months only; no mixed month+day+nanos)

Behavior notes:
- Default origin is the Unix epoch (`1970-01-01T00:00:00Z`) when omitted.
- Binning uses UTC arithmetic (timezone is not applied to bin boundaries).

## What does NOT prune (yet)

These are intentionally treated as “unknown” to avoid incorrect pruning:

- `ts + 1 < ...` (numeric, not interval)
- `ts + other_column < ...`
- `to_timestamp(...) + ts < ...`
- `interval - ts` (non-commutative, ambiguous for our matcher)
- `to_unixtime(ts) < '1704672000'` (string literal)
- `to_date(ts) = 'not-a-date'` (invalid date literal)

Queries still run correctly; they just won’t be pruned.

---

## Related

- `tstable` - Command-line tool shipped by this crate behind the `cli` feature
- `timeseries-table-format` - Canonical Rust crate with DataFusion enabled by default
