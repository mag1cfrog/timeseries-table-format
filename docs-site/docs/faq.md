# FAQ

## Do I need Rust installed?

Usually no. Most users on supported platforms should get a prebuilt wheel from PyPI.

If `pip install` tries to compile from source, you’ll need a Rust toolchain. See
[Installation](install.md) and [Troubleshooting](troubleshooting.md).

## What doesn’t v0 support?

v0 (the current initial version) focuses on local filesystem tables. It does not (yet) include
S3/object storage backends, compaction, schema evolution, or upserts/merges.

## What is a “table root”?

A table root is the directory that stores a time-series table on disk (metadata + data segments).
See [Table root layout](concepts/table_root.md).

## Why did `append_parquet(...)` raise `CoverageOverlapError`?

Your incoming segment overlaps existing coverage at the configured bucket granularity. See
[Buckets + overlap](concepts/bucketing_and_overlap.md).

## How do I choose `bucket`?

Pick a bucket that matches the granularity where you expect coverage to be unique for an entity
(e.g. hourly bars → `bucket="1h"`). See [Buckets + overlap](concepts/bucketing_and_overlap.md).

## Where does the data live after append?

By default, if the Parquet file is outside the table root, it’s copied under the table root so the
table is self-contained on disk.

## Can I update/delete rows (upserts/merges)?

Not in v0. Tables are append-only, and the library does not provide in-place updates/deletes or
merge/upsert semantics.

## What SQL syntax does DataFusion support?

DataFusion supports most ANSI SQL: `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `JOIN`, window
functions (`OVER (PARTITION BY ... ORDER BY ...)`), and timestamp functions like `date_trunc` and
`date_bin`. For the full reference, see the
[DataFusion SQL documentation](https://datafusion.apache.org/user-guide/sql/index.html).

## How do I see what's already in my table?

Open the table, create a `Session`, register it, and run a quick summary query:

```python
import timeseries_table_format as ttf

tbl = ttf.TimeSeriesTable.open("./my_table")
sess = ttf.Session()
sess.register_tstable("t", tbl.root())
print(sess.sql("SELECT min(ts), max(ts), count(*) FROM t"))
```

## What's the difference between `register_tstable` and `register_parquet`?

`register_tstable` opens a managed table root (created by `TimeSeriesTable.create`) and registers
all its committed segments. `register_parquet` registers a plain Parquet file or directory
directly, with no segment tracking or overlap metadata. Both are queryable with the same
`Session.sql(...)` API.

## What is `timezone` in `TimeSeriesTable.create`?

`timezone` is an optional IANA timezone name (e.g. `"America/New_York"`, `"UTC"`) used when
bucketing timestamps. If your data uses timezone-aware timestamps and you want bucket boundaries
aligned to wall-clock hours or days in a specific timezone, pass that timezone here.

For UTC or timezone-naive timestamps, leave it as `None` (the default).
