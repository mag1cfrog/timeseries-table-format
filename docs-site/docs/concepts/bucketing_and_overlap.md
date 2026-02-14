# Concept: buckets and overlap detection

`bucket="1h"` (or `"5m"`, `"1d"`, etc.) defines the time grid used for overlap detection.

It does **not** resample your data. Instead, it affects how the table decides whether a new segment
overlaps existing coverage.

Example: with `bucket="1h"`, timestamps `10:05` and `10:55` fall into the same bucket window
(10:00–11:00). In v0, appending two rows for the same entity in the same bucket is treated as
overlap and will be rejected.

## Overlap behavior (v0)

When you append a segment, the table computes which time buckets are covered (per entity identity)
and rejects the append if it would overlap existing coverage at the bucket granularity.

If an overlap is detected, `append_parquet(...)` raises `CoverageOverlapError`.

## Choosing a bucket

Pick a bucket that matches the granularity where you expect coverage to be unique for an entity.

Examples:
- Hourly bars → `bucket="1h"`
- Minute bars → `bucket="1m"`

If you expect multiple rows per entity within the same bucket window, choose a finer bucket.
