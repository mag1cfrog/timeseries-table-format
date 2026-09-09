# Add nullable columns to an existing table

Use `table.add_columns(pyarrow.Schema)` to introduce new payload fields after the first successful
append establishes the table's schema. The fields must be nullable and top-level. Existing types,
nullability, names, and keys cannot change. This operation adds fields; it does not compute or
backfill historical values. Use a subsequent [keyed row update](update_rows.md) to assign
externally computed values to those columns.

## Run the example

The example creates a temporary table, appends historical rows, adds two fields, re-registers its
SQL table, then appends values and omissions. It prints six rows; historical and omitted values
are null. The same script runs in the documentation tests.

```python
--8<-- "crates/timeseries-table-python/examples/add_nullable_columns.py"
```

## Preserve schema annotations

Pass a schema describing only the new fields. Schema and field metadata, including metadata on
nested children, is rejected because the logical table model cannot persist it. Complete supported
structs, lists, and maps may be added as nullable top-level fields. Dots in names are literal;
they do not select children of an existing field.

Later appends must match the canonical types and nullable annotations of every provided field,
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

Adding fields requires a compatible client. The first successful addition declares a reader
feature atomically, so older clients reject the evolved table. Installing this release and ordinary
appends do not activate that feature. See [Table protocol compatibility](../concepts/table_protocol.md)
for the persistent contract and [TimeSeriesTable reference](../reference/timeseries_table.md) for
argument and error details.
