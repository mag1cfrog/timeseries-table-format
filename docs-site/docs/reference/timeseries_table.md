# TimeSeriesTable reference

`TimeSeriesTable` manages table lifecycle (create/open/append/add_columns/optimize/vacuum) on the local
filesystem.

`entity_columns` is an ordered identity definition. A table may contain many identities, and one
Parquet segment may contain rows for several identities. Different identities may use the same
index interval. One complete identity may have at most one row per interval, both within one
append and across committed appends.

Registered entity column types are Arrow `string`, `large_string`, `int32`, `int64`, and `uint64`.
Actual values must be non-null. Composite identity components follow the configured
`entity_columns` order. Incoming integer columns may use the lossless widenings documented below,
but signedness changes and unsupported domains are rejected. Persisted identities retain their
registered types and are never stringified for comparison.

Python exposes one `TimeSeriesTable`, not child tables per identity. After registration, entity
columns remain ordinary SQL columns for filtering and grouping.

## Append Arrow data

`TimeSeriesTable.append(source, *, compression=None, max_rows_per_row_group=None,
max_bytes_per_row_group=None)` accepts these sources:

| Source | Behavior |
|---|---|
| `pyarrow.RecordBatch` | Appended as one batch without copying its arrays in Python |
| `pyarrow.Table` | Its existing chunks are streamed without calling `combine_chunks()` |
| `pyarrow.RecordBatchReader` | Batches are consumed lazily |
| Object implementing `__arrow_c_stream__` | One schema-bearing Arrow C Stream is requested and consumed |

The method returns an immutable `AppendReport` describing the committed segment and its effective
writer settings. Use `table.append(source).committed_version` when only the new table version is
needed. The method does not accept file paths, pandas or NumPy objects, mappings, row iterables, or
arbitrary iterables of batches. Convert those inputs to one of the supported Arrow forms explicitly.

The keyword-only settings control the physical layout of the new table-owned Parquet segment:

| Setting | Default | Meaning |
|---|---|---|
| `compression` | `"zstd"` | `"uncompressed"`, `"snappy"`, or `"zstd"` |
| `max_rows_per_row_group` | 1,048,576 | Maximum rows per output row group |
| `max_bytes_per_row_group` | 128 MiB | Maximum estimated encoded bytes per output row group |

Both row-group limits apply, and the first one reached closes the active row group. The byte limit
is an estimate, not a strict process-memory ceiling, and a single oversized value may exceed it.
Settings apply only to the current append and are not persisted in table metadata.

After the table has a canonical schema, append matches top-level fields by name and writes them in
canonical order. Nullability must match. Types must match except for these lossless widenings:
`int8` to `int32` or `int64`, `int16` to `int32` or `int64`, `int32` to `int64`, `uint8`, `uint16`,
or `uint32` to `uint64`, and `float32` to `float64`. Signedness changes, timestamp changes, and
nested widening are rejected.

After an explicit `add_columns` operation, append may omit nullable payload fields, including
pre-existing nullable fields. Missing keys and non-nullable fields remain errors. Baseline tables
retain their strict missing-field behavior until the first addition.

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
report = table.append(source)
new_version = report.committed_version
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
new_version = table.append(
    reader,
    compression="zstd",
    max_rows_per_row_group=4_096,
    max_bytes_per_row_group=128 * 1024 * 1024,
).committed_version
```

Append imports the source through Arrow C Stream and writes a table-owned Parquet segment. It does
not stage or collect the complete input in Python. A `RecordBatch` or `Table` remains usable after
the call; a reader or other single-use stream is consumed. Once Rust owns the stream, append runs
with the Python GIL released.

Unsupported sources raise `TypeError`. Invalid writer settings, stream exporters, or capsules
raise `ValueError`; writer settings are validated before the source is exported or consumed.
Table failures use the library's existing [exception hierarchy](exceptions.md). Boundary and
mid-stream source failures do not commit a new version.

## Update row values

```python
report = table.update_rows(source, columns=["quality"], expected_version=computed_from_version)
```

Both keyword arguments are required. `columns` accepts a sequence of strings and
`expected_version` accepts an integer in `1..=18446744073709551615`, excluding bool.
The source accepts the same Arrow C Stream inputs as append. It contains exactly all entity
columns, the raw ordered-index column, and the selected payload fields. The core validates
schema compatibility and exact one-to-one key matches.

Capture the version before reading and computing assignments. Both the handle and published
table must still match it. The operation never refreshes or retries. It overwrites selected
values, including explicit nulls in nullable fields; unselected values remain unchanged.
Nested destinations replace complete top-level values. A valid empty stream is a version-checked
no-op; equal-value nonempty input still commits. See the
[workflow guide](../guides/update_rows.md) for provenance, error handling, and resource costs.

`UpdateRowsReport` is read-only. Its seven fields are:

| Field | Meaning |
| --- | --- |
| `starting_version` | Snapshot used to compute assignments |
| `committed_version` | Published version, or starting version for a no-op |
| `rows_updated` | Addressed rows, including equal-value assignments |
| `segments_rewritten` | Affected source segments replaced |
| `source_file_bytes` | Total affected source Parquet file sizes |
| `replacement_file_bytes` | Total completed replacement Parquet file sizes |
| `no_op` | True only for a fully validated source with zero rows |

Byte fields exclude scratch, sidecars, and discovery I/O; they are not total I/O measurements.
All counts are zero for empty input. The successful committed version equals `table.version()`.

::: timeseries_table_format.UpdateRowsReport
    options:
      members: true
      show_source: false

## Add nullable columns

`TimeSeriesTable.add_columns(columns: pyarrow.Schema) -> int` adds the fields in one commit and
updates the handle to that version. Pass only new nullable top-level fields. The table must have
a canonical schema, normally established by its first successful append. Field order, exact
case-sensitive names, types, and nullability are preserved. Dots are literal name characters;
use SQL quoting when needed.

Schema/field metadata, including nested metadata, raises `ValueError`. A non-schema argument raises
`TypeError`. Core validation failures use `SchemaMismatchError`, including empty schemas,
duplicates, existing/key names, unsupported types, and non-nullable additions. Existing fields and
key definitions cannot change. Addition fills no historical values and rewrites no data or coverage.

The operation uses the handle's selected version without refreshing or retrying. A stale handle
raises `ConflictError` with `expected` and `found`. A create-only commit race retains `StorageError`
and its `path`; it does not invent a newer observed version. Table failures preserve `table_root`.
Reopen and reconcile before retrying a stale or ambiguous operation. `TimeseriesTableError` with
an ambiguous-commit diagnostic does not guarantee rollback.

After adding fields, replace SQL registrations with `Session.register_tstable(name, table_root)`.
An old registration rejects newly planned scans when it detects a schema change. See
[Add nullable columns](../guides/add_nullable_columns.md) for a complete runnable workflow and
[Table protocol compatibility](../concepts/table_protocol.md) for unsupported-client rejection.

::: timeseries_table_format.TimeSeriesTable
    options:
      members: true
      show_source: false

## AppendReport

`TimeSeriesTable.append(...)` returns this immutable report after a successful commit. Its segment
path is table-relative. The row count and file size match the committed segment metadata, while the
row-group count comes from the completed Parquet footer.

::: timeseries_table_format.AppendReport
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

## Vacuum expired orphan files

An interrupted append can leave an incomplete Parquet file under `data/_managed/append/` without
adding it to the transaction log. An interrupted entity rewrite can leave files under
`data/_staged/entity-rewrite/`. `TimeSeriesTable.vacuum(older_than, *, apply=False)` finds expired
files in these reserved directories that no valid retained commit references. The default dry-run
does not modify the table.

Choose a timezone-aware cutoff older than the longest writer operation you expect, then inspect
the plan:

```python
from datetime import datetime, timedelta, timezone

cutoff = datetime.now(timezone.utc) - timedelta(days=7)
plan = table.vacuum(cutoff)

for artifact in plan.artifacts:
    if artifact.disposition == "removable":
        print(artifact.path, artifact.size_bytes, artifact.reason)
```

Apply the same retention policy after reviewing the plan:

```python
report = table.vacuum(cutoff, apply=True)
print(report.deleted_files, report.deleted_bytes)
```

The cutoff is exclusive. Files modified at or after it are retained, and a future cutoff raises
`ValueError`. Vacuum also retains files referenced anywhere in valid retained history and
unrecognized files. Apply rechecks each candidate's size and modification time before deletion and
retains it if either value differs from planning. This check is best effort, not atomic with
deletion, so leave enough retention time for active writers to finish.

Parquet files elsewhere under `data/` are not vacuum candidates. This includes append source files
inside the table root.

`VacuumReport.artifacts` contains every regular file considered under `data/` and `_coverage/`.
Each artifact has a disposition (`retained`, `removable`, `deleted`, or `already_absent`) and a
reason. `already_absent` means vacuum found a candidate missing before deletion; its last observed
size is counted in `already_absent_bytes`, not `deleted_bytes`. The report also provides matching
file and byte totals.

Apply mode can remove some files before a later deletion fails. In that case,
`VacuumApplyError.partial_report` records the completed deletions and the remaining candidates;
`VacuumApplyError.path` identifies the file that failed. The exception is also a `StorageError`,
so existing storage-error handlers continue to catch it.

Vacuum is orphan-file cleanup. It does not expire snapshots, choose a transaction-log retention
boundary, rewrite history, or delete transaction-log files. It scans `data/` and `_coverage/`, but
only reserved Parquet paths and recognized coverage paths can be removed.

::: timeseries_table_format.VacuumArtifact
    options:
      members: true
      show_source: false

::: timeseries_table_format.VacuumReport
    options:
      members: true
      show_source: false
