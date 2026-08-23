# Ordered indexes, granularity, and conflicts

Every table has one ordered index. It can be a physical Timestamp or an integer-valued logical
clock with application-defined units.

| Index domain | Required Arrow type | `index_granularity` |
|---|---|---|
| Timestamp | `timestamp` with an explicit unit | A time interval such as `"1m"` or `"1h"` |
| Int64 | Signed `int64` | A positive integer in index-value units |
| UInt64 | Unsigned `uint64` | A positive integer in index-value units |

The incoming column must match the configured Arrow type or use a supported
[lossless widening](../reference/timeseries_table.md#append-arrow-data). The library does not
infer timestamps from integers or convert between signed and unsigned values.

## Index granularity

Granularity divides the ordered-index domain into logical intervals. Intervals are half-open,
written `[start, end)`, except that the final interval includes the index domain maximum.

- Timestamp granularity `"1h"` creates one-hour intervals.
- Integer granularity `10` creates intervals ten index values wide.
- Int64 uses Euclidean division. With granularity 10, `-11` belongs to `[-20, -10)` and `-1`
  belongs to `[-10, 0)`.

Granularity does not resample, aggregate, sort, or repair input data. Parquet rows do not need to
be sorted before append.

## Entity identities

`entity_columns` is an ordered list of columns that identifies independent time series inside one
logical table. For `entity_columns=["exchange", "symbol"]`, the identity
`("NASDAQ", "NVDA")` is distinct from `("NYSE", "NVDA")`. Each entity column name must be
unique, and the ordered-index column cannot also be an entity column.

Registered entity columns support Arrow `string`, `large_string`, `int32`, `int64`, and `uint64`.
Values must be non-null. Incoming integers may use a supported
[lossless widening](../reference/timeseries_table.md#append-arrow-data), but signedness changes
and unsupported scalar domains are rejected.

One Parquet segment may contain rows for several identities. Entity columns remain normal SQL
columns for filtering and grouping.

## One row per identity and interval

A complete entity identity may have at most one row in each logical index interval. The same rule
applies everywhere:

- Two rows in one append that use the same identity and interval raise
  `DuplicateIndexIntervalError`.
- A row whose identity and interval already exist in committed table data raises
  `IndexIntervalOverlapError`.
- Different complete identities may use the same interval.
- A table without entity columns has one table-wide implicit identity.

For example, `10:05` and `10:55` share the `10:00` to `11:00` interval when granularity is
`"1h"`. Both rows cannot belong to NVDA, whether they arrive together or in separate appends. An
NVDA row and an AAPL row may use that same interval.

Coverage records which identity and interval pairs are occupied. It does not claim that every
possible index value inside an interval exists.

## Choosing granularity

Choose a granularity that matches the smallest interval in which each identity is expected to
have at most one row:

- One row per hour: `index_granularity="1h"`
- One row per minute: `index_granularity="1m"`
- One row per integer index value: `index_granularity=1`

Use a finer granularity if legitimate rows for one identity can occur in the same wider interval.

## Entity-layout optimization

Mixed-entity segments are valid and queryable. Rust `TimeSeriesTable::optimize`, Python
`table.optimize()`, and CLI `tstable optimize --table <path>` can rewrite each mixed segment into
one segment per complete identity while preserving rows, schema, and coverage.

Entity-layout optimization is not small-file compaction and does not accept a target file size.
Removed source objects may remain in storage, so optimization is not vacuum.

## Current limitations

Indexes cannot currently use floats, decimals, strings, multiple columns, descending order, or
implicit signedness conversion. Entity columns cannot currently use booleans, floats, decimals,
timestamps, binary, dictionary, nested, or null values.
