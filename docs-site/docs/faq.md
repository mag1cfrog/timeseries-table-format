# FAQ

## Do I need Rust installed?

Usually no. Most users on supported platforms should get a prebuilt wheel from PyPI.

If `pip install` tries to compile from source, you’ll need a Rust toolchain. See
[Installation](install.md) and [Troubleshooting](troubleshooting.md).

## What doesn’t v0 support?

v0 focuses on local filesystem tables. It does not (yet) include S3/object storage backends,
compaction, schema evolution, or upserts/merges.

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
