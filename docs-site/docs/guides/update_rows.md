# Update values by row key

Use `table.update_rows(source, columns=[...], expected_version=...)` to backfill or overwrite
selected payload columns. Compute the values outside the table engine and supply an Arrow
source containing complete row keys and the selected values. Every source key must match
exactly one existing row.

## Run the example

This example adds a nullable column, captures the source version, computes values from a SQL
read, and updates four rows in one transaction. It checks the report and reopened SQL results.
The script also runs in the documentation tests.

```python
--8<-- "crates/timeseries-table-python/examples/update_rows.py"
```

Run it from the repository root with
`python crates/timeseries-table-python/examples/update_rows.py`.

## Keep the version that belongs to your computation

Add any new nullable columns first, then capture `table.version()` before reading and computing
assignments. Pass that same version as `expected_version`. This is a provenance check supplied
by the caller: the engine cannot establish which snapshot you used to compute arbitrary values.
Reading old values and attaching a freshly fetched version to them is invalid.

Both the selected handle and the published table must match the expected version. Any intervening
commit, including an unrelated append, optimization, or column addition, causes a conflict.
The operation does not refresh the handle, rebase assignments, or retry automatically. Reopen
and reconcile the source computation before trying again. A later SQL scan can observe a newer
commit than the version you captured; that also makes the update conflict.

Errors leave the handle unchanged. An ambiguous commit may have published the replacements;
reopen and reconcile it before retrying. Do not treat an exception as permission to delete
replacement files. Ordinary validation failures publish no partial update.

## Supply exact keys and selected fields

Supported sources are `pyarrow.RecordBatch`, `pyarrow.Table`, `pyarrow.RecordBatchReader`, and
objects implementing `__arrow_c_stream__`. Readers are consumed once and incrementally with the
GIL released during the Rust operation. Filesystem paths, row lists/dictionaries, and callbacks
that compute assignments are not alternative input forms. Exporters retain their normal Arrow
stream callbacks. Arrow import failures preserve the existing append boundary's errors.

`columns` and `expected_version` are required keyword arguments. Columns must be a sequence of
strings, such as a list or tuple; names are not coerced or deduplicated. Versions must be Python
integers in `1..=18446744073709551615`; booleans are rejected. Representation errors use
`TypeError`, or `ValueError` for integers outside this range.

The source must contain every configured entity column, the actual ordered-index column, and
exactly the selected destination fields. Field order can differ. Missing or extra fields are
errors. Keys use their exact stored values, including timestamp precision, rather than the
coverage bucket. Equal indexes on different entities are distinct keys. A table without entity
columns uses only its ordered index. Null keys, duplicate source keys, missing targets, and
ambiguous targets are rejected.

Keep canonical field types and nullable annotations even when a batch contains no nulls.
The existing lossless scalar widenings are accepted; see the
[ingestion schema rules](../reference/timeseries_table.md). Ordinary Arrow metadata follows
append ingestion and does not change table metadata. Key-column assignments and implicit
schema addition are not supported.

An assignment overwrites the selected value, whether or not it was null. An explicit null clears
a nullable destination. Unselected fields remain unchanged. For a null-only backfill, filter the
source rows before constructing assignments. Dots are literal field-name characters; supported
nested values replace the entire selected top-level field.

## Interpret the result and budget for rewrites

`UpdateRowsReport.rows_updated` counts addressed rows, including assignments equal to stored
values. A nonempty source still commits when every assignment is equal. A fully validated empty
stream rechecks the version and returns `no_op=True`, zero counts, and equal starting/committed
versions. `table.version()` agrees with a successful report.

Affected immutable Parquet segments are rewritten. Even a sparse update can replace a large
file; it is not a metadata-only operation. `source_file_bytes` and `replacement_file_bytes` sum
affected source and completed replacement Parquet file sizes. They exclude scratch, coverage
sidecars, and discovery I/O, so they do not measure total bytes read or written.

Plan disk space for preparation scratch and replacements while original files remain retained.
The native operation spills preparation data and streams rewrites, but caller batches, individual
values, Parquet pages, and metadata prevent a hard process-memory guarantee. The
[native benchmark report](https://github.com/mag1cfrog/timeseries-table-format/blob/main/docs/benchmarks/update-rows-2026-09-07.md)
measures the completed Rust operation on a 262,144-row workload with about 1 GiB of selected values.
The Python adapter uses that operation without collecting the input in Python.

Existing retained history protects replaced files and sidecars. A vacuum age cutoff does not
expire those snapshots or guarantee that their files will be reclaimed. This API does not add
time-travel queries, row insertion/deletion, merge/upsert, or automatic recomputation.

Value updates preserve schema and coverage. Newly planned SQL queries through an existing
registration see the new values; already planned scans retain their snapshots. Re-register only
after an explicit schema addition. No new reader/writer feature is activated by value updates;
existing requirements still apply. See the Rust-owned
[operation and protocol contract](https://github.com/mag1cfrog/timeseries-table-format/blob/main/crates/timeseries-table-format/ENGINE.md#keyed-row-updates-rust)
and the [Python exception reference](../reference/exceptions.md).
