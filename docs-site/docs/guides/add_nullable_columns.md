# Add nullable columns to an existing table

Use `table.add_columns(pyarrow.Schema)` to introduce new payload fields after the first successful
append establishes the table's schema. The fields must be nullable and top-level. Existing types,
nullability, names, and keys cannot change. This operation adds fields; it does not compute or
backfill historical values. Use a subsequent [keyed row update](update_rows.md) to assign
externally computed values to those columns.

## Run the example

The example creates a temporary table, appends historical rows, adds two fields, re-registers its
SQL table, then appends values and omissions. The `quality` field keeps its `unit` metadata even
when later batches omit the annotation. The example prints six rows; historical and omitted
values are null. The same script runs in the documentation tests.

```python
--8<-- "crates/timeseries-table-python/examples/add_nullable_columns.py"
```

## Preserve schema annotations

Pass a schema describing only the new fields. Field metadata is preserved, including annotations
on Struct children, List elements, and Map entries, keys, and values. Metadata keys and values
must be valid UTF-8. Complete supported structs, lists, and maps may be added as nullable top-level
fields. Dots in names are literal; they do not select children of an existing field.

The supplied schema must have no schema-level metadata. `add_columns` raises `ValueError` if it
does, even when those annotations match the table. This operation adds fields and keeps the
table's existing schema-level metadata unchanged.

Later appends and updates may omit some or all metadata keys from an annotated field. The writer
restores them from the table schema. A different value or an additional key is rejected before
publication. See [Schema and field metadata](../concepts/table_protocol.md#schema-and-field-metadata)
for inheritance and legacy-table behavior.

Provided fields must match the canonical types and nullable annotations on later appends,
subject to the existing lossless scalar widenings. An array without null values can still have a
nullable field; keep `nullable=True` for an added field even when that batch supplies every value.
After the first addition, any nullable payload may be omitted and becomes null. Every key remains
required. A table without an explicit addition retains baseline strict missing-field behavior.

## Re-register queries and handle conflicts

Call `Session.register_tstable` again with the same name and root after each addition. It replaces
the registration and exposes the latest schema. Already built query plans keep their snapshots;
newly planned scans through a stale registration report a schema change. A query naming a new field
may instead fail earlier during name resolution.

`add_columns` uses the current handle's version and does not refresh or retry. On `ConflictError`,
reopen the table, inspect the current state, and reconcile the request before retrying. If a
`TimeseriesTableError` reports an ambiguous commit, neither success nor rollback is guaranteed;
reopen and reconcile the log before taking further mutation steps.

Adding fields requires a compatible client. The first successful addition declares the
`schema_add_columns` reader feature atomically, so older clients reject the evolved table. Adding
annotated fields also declares `schema_metadata` for readers and writers. Installing this release
and ordinary appends do not activate `schema_add_columns`. See
[Table protocol compatibility](../concepts/table_protocol.md)
for the persistent contract and [TimeSeriesTable reference](../reference/timeseries_table.md) for
argument and error details.
