# Concept: table root layout

A time-series table is stored under a local directory called the *table root*.

You typically create it with `TimeSeriesTable.create(table_root=...)` and later open it with
`TimeSeriesTable.open(table_root)`.

## What's inside the table root?

At a high level, you'll see directories like:

- `_timeseries_log/` - transaction log / table metadata history
- `data/` - segment files (for appends that copy data under the root)
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
    Exact filenames under `_coverage/` are implementation details and may change; the important part
    is that the table root is self-contained and queryable locally.

## Protect the table root

Do not manually edit or delete files inside a table root. Missing segment,
transaction-log, or coverage files can prevent the table from opening,
querying, or accepting new appends.

There is no repair tool in v0. If a table root is damaged, rebuild it in a new
directory from the original Parquet sources.
