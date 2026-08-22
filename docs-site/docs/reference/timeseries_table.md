# TimeSeriesTable reference

`TimeSeriesTable` manages table lifecycle (create/open/append/optimize) on the local filesystem.

`entity_columns` is an ordered identity definition. A table may contain many identities, and one
Parquet segment may contain rows for several identities. Ordered-index values and buckets may
repeat across different identities; an append is rejected only when the same complete identity
already covers the bucket.

Supported entity column types are Arrow `string`, `large_string`, `int32`, `int64`, and `uint64`.
Actual values must be non-null. Composite identity components follow the configured
`entity_columns` order. Signed and unsigned integers retain their exact types and are never
stringified for comparison. Unsupported domains and type mismatches are rejected rather than
cast.

Python exposes one `TimeSeriesTable`, not child tables per identity. After registration, entity
columns remain ordinary SQL columns for filtering and grouping.

## Append Arrow data

`TimeSeriesTable.append(source)` accepts these sources:

| Source | Behavior |
|---|---|
| `pyarrow.RecordBatch` | Appended as one batch without copying its arrays in Python |
| `pyarrow.Table` | Its existing chunks are streamed without calling `combine_chunks()` |
| `pyarrow.RecordBatchReader` | Batches are consumed lazily |
| Object implementing `__arrow_c_stream__` | One schema-bearing Arrow C Stream is requested and consumed |

The method returns the newly committed table version as an `int`. It does not accept file paths,
pandas or NumPy objects, mappings, row iterables, or arbitrary iterables of batches. Convert those
inputs to one of the supported Arrow forms explicitly.

After the table has a canonical schema, append matches top-level fields by name and writes them in
canonical order. Nullability must match. Types must match except for these lossless widenings:
`int8` to `int32` or `int64`, `int16` to `int32` or `int64`, `int32` to `int64`, `uint8`, `uint16`,
or `uint32` to `uint64`, and `float32` to `float64`. Signedness changes, timestamp changes, and
nested widening are rejected.

For a materialized source, pass a table or record batch directly. This example assumes the target
table expects the shown schema:

```python
import pyarrow as pa

source = pa.table(
    {
        "ts": pa.array([0, 3_600_000_000], type=pa.timestamp("us")),
        "symbol": pa.array(["A", "A"]),
        "value": pa.array([1.0, 2.0]),
    }
)
new_version = table.append(source)
```

For streaming ingestion, pass a `RecordBatchReader`:

```python
batch = pa.record_batch(
    {
        "ts": pa.array([7_200_000_000], type=pa.timestamp("us")),
        "symbol": pa.array(["A"]),
        "value": pa.array([3.0]),
    }
)
reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
new_version = table.append(reader)
```

Append imports the source through Arrow C Stream and writes a table-owned Parquet segment. It does
not stage or collect the complete input in Python. A `RecordBatch` or `Table` remains usable after
the call; a reader or other single-use stream is consumed. Once Rust owns the stream, append runs
with the Python GIL released.

Unsupported sources raise `TypeError`, invalid stream exporters or capsules raise `ValueError`,
and table failures use the library's existing [exception hierarchy](exceptions.md). Boundary and
mid-stream source failures do not commit a new version.

::: timeseries_table_format.TimeSeriesTable
    options:
      members: true
      show_source: false

## OptimizeReport

`TimeSeriesTable.optimize()` returns this immutable report for both rewrites and successful
no-ops.

Optimization may change physical row order. It preserves logical rows, schema, and per-entity
coverage, but it does not combine small files or accept a target file size. Replaced source files
may remain on disk until a future vacuum operation removes unreferenced files.

::: timeseries_table_format.OptimizeReport
    options:
      members: true
      show_source: false
