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
