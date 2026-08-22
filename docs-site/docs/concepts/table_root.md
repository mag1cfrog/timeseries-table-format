# Table root layout

A time-series table is stored under a local directory called the *table root*.

You typically create it with `TimeSeriesTable.create(table_root=...)` and later open it with
`TimeSeriesTable.open(table_root)`.

## What's inside the table root?

At a high level, you'll see directories like:

- `_timeseries_log/` - transaction log / table metadata history
- `data/` - table-managed Parquet segment paths
- `_coverage/` - coverage/overlap tracking data (created after appends)

Example (after a first append):

```text
my_table/
  _timeseries_log/
    CURRENT
    0000000001.json
    0000000002.json
  data/
    some_segment.parquet
  _coverage/
    table/
      ...
    segments/
      ...
```

!!! note
    Exact filenames under `_coverage/` are implementation details and may change. Table metadata
    records paths relative to the table root, and the local filesystem resolves those paths,
    including symlinks.

## Protect the table root

Do not manually edit or delete files inside a table root. Missing segment,
transaction-log, or coverage files can prevent the table from opening,
querying, or accepting new appends.

If the table root uses symlinks, keep their targets available when moving or
backing up the table.

There is no repair tool in v0. If a table root is damaged, rebuild it in a new
directory from the original Parquet sources.
