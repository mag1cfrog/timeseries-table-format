# TimeSeriesTable reference

`TimeSeriesTable` manages table lifecycle (create/open/append/optimize) on the local filesystem.

`entity_columns` is an ordered identity definition. A table may contain many identities, and one
Parquet segment may contain rows for several identities. Ordered-index values and buckets may
repeat across different identities; an append is rejected only when the same complete identity
already covers the bucket.

Python exposes one `TimeSeriesTable`, not child tables per identity. After registration, entity
columns remain ordinary SQL columns for filtering and grouping.

::: timeseries_table_format.TimeSeriesTable
    options:
      members: true
      show_source: false

## OptimizeReport

`TimeSeriesTable.optimize()` returns this immutable report for both rewrites and successful
no-ops.

::: timeseries_table_format.OptimizeReport
    options:
      members: true
      show_source: false
