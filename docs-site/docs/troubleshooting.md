# Troubleshooting

## `pip install` tries to build from source

PyPI does not have a compatible wheel for your Python version or platform.

- Use a supported Python version. The project requires Python 3.10 or later.
- To build from source, install a stable Rust toolchain and retry.

## SQL placeholder type errors

DataFusion may not infer the type of a placeholder in a `SELECT` projection.
Add an explicit cast:

```python
session.sql("SELECT CAST($1 AS BIGINT) AS value", params=[1])
```

See [Use SQL parameters](tutorials/parameterized_queries.md) for supported
values and placeholder styles.

## `CoverageOverlapError` during append

The incoming segment covers at least one bucket already present for the same
entity. The rejected append is not committed.

1. Check whether the entire file is a duplicate or only part of it overlaps.
2. For a duplicate file, leave it unappended.
3. For a partial overlap, create a segment containing only uncovered buckets.

Do not automatically catch and ignore every overlap error. That can discard
new data contained in a partially overlapping file.

The bucket configuration is stored when the table is created and cannot be
changed in place. If it is too coarse, create a new table with a finer `bucket`
or `bucket_width`, then re-append the original source files.

See [Buckets and overlap](concepts/bucketing_and_overlap.md) for the coverage
model.

## Append rejects the ordered-index column

The Parquet ordered-index column must exactly match the type stored in the
table metadata:

- Timestamp indexes must preserve their Arrow timestamp unit and timezone.
- Int64 indexes require Arrow `int64`.
- UInt64 indexes require Arrow `uint64`.

The package does not infer timestamps from integers or convert between signed
and unsigned indexes. Inspect the table with `index_spec()` and write the
incoming Parquet column with the same Arrow type.

See [Ordered indexes, buckets, and overlap](concepts/bucketing_and_overlap.md)
for the complete index rules.

## `SchemaMismatchError` during append

The first successful append adopts the table's canonical Parquet schema. Every
later segment must use the same column names and Arrow types. Compare the new
file with an accepted segment and correct the producer schema before retrying.

The rejected append is not committed. Avoid casting data automatically unless
the conversion is part of the intended table schema.

## A table root no longer opens

Do not create a new table over the same directory. Preserve the damaged root
for diagnosis and check filesystem permissions first.

There is no repair tool in v0. If table metadata or managed data is missing,
rebuild the table in a new directory from the original Parquet sources. See
[Table root layout](concepts/table_root.md).

## Native diagnostics do not appear

Configure `logging.getLogger("timeseries_table_format")` before the first table
operation and attach a handler to that logger or one of its ancestors. Check
the logger level and normal Python propagation settings.

If you changed a logger level after native records were already emitted, call:

```python
import timeseries_table_format as ttf

ttf.refresh_logging_cache()
```

`RUST_LOG` does not control the Python extension. See
[Configure native logging](guides/native_logging.md) for the complete setup and
cache behavior.

## Native diagnostics appear twice

The package forwards each native record once and does not install a handler.
Check whether both `timeseries_table_format` and an ancestor such as the root
logger have handlers. Remove one handler or set
`logging.getLogger("timeseries_table_format").propagate = False` if the record
should not reach the ancestor.
