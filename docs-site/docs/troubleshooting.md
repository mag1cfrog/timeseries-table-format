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

## Index interval conflict during append

`IndexIntervalOverlapError` means an incoming identity and interval pair already exists in
committed data. `DuplicateIndexIntervalError` means the same pair occurs twice within the incoming
append. The rejected append is not committed.

1. Inspect `example_identity` and `example_index_interval` on the exception.
2. Remove duplicated input if the row is redundant.
3. If both rows are legitimate, use a finer granularity in a newly created table.

Do not automatically catch and ignore interval errors. A rejected append may also contain valid
new rows that would be silently discarded.

Index granularity is stored when the table is created and cannot be changed in place. It does not
resample, aggregate, sort, or repair input.

See [Index granularity and conflicts](concepts/index_granularity_and_conflicts.md) for the complete
uniqueness model.

## Append rejects the ordered-index column

The ordered-index type stored in table metadata is authoritative. The incoming
column must either match it or use a supported
[lossless widening](reference/timeseries_table.md#append-arrow-data). Timestamp
units and timezones must match exactly, and signed and unsigned indexes are
never converted into each other.

See [Ordered indexes, granularity, and conflicts](concepts/index_granularity_and_conflicts.md)
for the complete index rules.

## `SchemaMismatchError` during append

The first successful append adopts the table's canonical schema when one is not
already registered. Later appends must match its field names and nullability;
types must match except for the documented
[lossless widenings](reference/timeseries_table.md#append-arrow-data). Compare
the new source with an accepted segment and correct its schema before retrying.

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
