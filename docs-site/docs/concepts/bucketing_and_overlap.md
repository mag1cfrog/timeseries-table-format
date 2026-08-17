# Ordered indexes, buckets, and overlap

Every table has one ascending chronological index. It can be a physical Timestamp or an
integer-valued logical clock with application-defined units. Public APIs and metadata call this
the ordered index.

| Index domain | Required Arrow type | Bucket configuration |
|---|---|---|
| Timestamp | `timestamp` with an explicit unit | A duration such as `"1m"` or `"1h"` |
| Int64 | Signed `int64` | A positive width in index-value units |
| UInt64 | Unsigned `uint64` | A positive width in index-value units |

The Parquet column must match the configured Arrow type exactly. The library does not infer
timestamps from integers or convert between signed and unsigned values.

## Buckets

Buckets drive coverage and overlap checks; they do not resample data.

- `bucket="1h"` groups Timestamp values into one-hour windows.
- `bucket_width=10` groups integer values in application-defined units.
- Int64 uses Euclidean division. With width 10, `-11` belongs to `[-20, -10)` and `-1`
  belongs to `[-10, 0)`.

Bucket boundaries and core range operations are half-open: the start is included and the end is
excluded, written `[start, end)`.

## Entity identities

`entity_columns` is an ordered list of columns that identifies independent time series inside one
logical table. For `entity_columns=["exchange", "symbol"]`, the identity `("NASDAQ", "NVDA")`
is distinct from `("NYSE", "NVDA")`. The configured column order defines the composite identity.

One Parquet segment may contain rows for several identities. The table remains one
`TimeSeriesTable`, and a registered DataFusion provider exposes every row. Entity columns remain
normal SQL columns, so use `WHERE` to select an identity and `GROUP BY` to aggregate identities
independently.

## Coverage and overlap

Coverage is bucket-level evidence. A covered bucket contains at least one value, but coverage does
not prove that every possible value inside the bucket exists.

When a segment is appended, the table computes its covered buckets for each entity. The append is
rejected with `CoverageOverlapError` only if a bucket is already covered for the same complete
identity. Different identities may reuse the same ordered-index value or bucket.

For example, Timestamp values `10:05` and `10:55` share the `10:00` to `11:00` bucket when
`bucket="1h"`. If an existing segment covers that bucket for `NVDA`, a later segment covering it
for `NVDA` is rejected. Coverage for another entity remains independent.

The table does not physically repartition a mixed-entity Parquet file. Compaction, repartitioning,
and an `optimize` command are outside the current feature.

## Choosing a bucket

Choose the granularity expected to be unique per entity:

- Hourly bars: `bucket="1h"`
- Minute bars: `bucket="1m"`
- Integer ticks grouped by hundreds: `bucket_width=100`

Use a finer bucket when independent segments may contain values in the same wider bucket.

## Current limitations

Indexes cannot currently use floats, decimals, strings, multiple columns, descending order, or
implicit signedness conversion.
