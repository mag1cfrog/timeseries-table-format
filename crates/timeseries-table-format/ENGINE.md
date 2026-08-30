# timeseries-table-format engine

Core engine for a log-structured time-series table format. It owns table metadata,
append rules, coverage math, storage IO, and the `TimeSeriesTable` API that
higher-level integrations build on.

Without optional features, this crate exposes metadata and scan streams without
including a query engine. Enable the `datafusion` feature for SQL queries with
time-based segment pruning.

## Layers (module layout)
- `metadata`: pure metadata model + validation (logical schema, table metadata, segment types). No IO.
- `transaction_log`: append-only metadata log APIs (OCC) + table state materialization.
- `table`: user-facing `TimeSeriesTable` API (create/open/append/scan).
- `storage`: local backend + table-root IO helpers.
- `coverage`: coverage math and gap analysis.
- `formats`: format-specific helpers (currently `formats::parquet`).
- `datafusion` (optional): DataFusion `TableProvider` and pruning integration.

See the [DataFusion integration guide](DATAFUSION.md) for setup and examples.

## Responsibilities
- **Transaction log + metadata**: versioned commits, optimistic concurrency, table schema.
- **Segment metadata**: min/max timestamps, row counts, file format, coverage sidecars.
- **Coverage math**: RoaringBitmap overlap checks and gap analysis over index interval IDs.
- **Storage access**: local filesystem backend and atomic IO helpers.
- **User API**: create/open/append/scan plus coverage/gap queries.

## On-disk layout (local backend)
```
<table_root>/
  _timeseries_log/
    CURRENT                  # latest committed version
    0000000001.json          # commit files (LogAction list)
    ...
  _coverage/
    segments/<id>.roar       # per-segment RoaringBitmap coverage
    table/<ver>-<id>.roar    # table snapshot coverage
  data/_managed/append/...   # append-generated Parquet segments
  data/_staged/entity-rewrite/... # entity-rewrite Parquet segments
```

## Transaction log and OCC
- Commits are append-only JSON files under `_timeseries_log/`.
- `CURRENT` stores the latest committed version.
- Commit flow is optimistic:
  1. Read current version `N`.
  2. Build a commit with `expected_version = N`.
  3. Write version `N+1` only if `CURRENT` is still `N`.
- On conflict, the caller reloads and retries.

Log actions:
- `AddSegment`: adds a new segment descriptor (and `coverage_path` if enabled).
- `UpdateTableMeta`: updates table-level metadata (schema adoption, entity pinning).
- `UpdateTableCoverage`: points to the latest table coverage snapshot.

## Table lifecycle
### Create
`TimeSeriesTable::create` validates `TableMeta`, writes the initial
`UpdateTableMeta` commit, then rebuilds `TableState`.

### Open
`TimeSeriesTable::open` rebuilds `TableState` from the log and extracts
`IndexSpec`. Empty logs return `TableError::Open` containing
`OpenTableError::EmptyTable`.

### Append (Arrow to Parquet)
1. Validate the Arrow reader schema before consuming batches or creating output.
2. On the first append, preserve and adopt that schema. Otherwise, match fields by name and
   normalize exact or explicitly allowlisted lossless scalar types into registered field order.
3. Stream normalized non-empty batches into one uniquely named table-owned Parquet segment.
4. Inspect the finished segment and verify its exact registered `LogicalSchema`.
5. Compute coverage, reject overlaps, and write attempt-owned coverage sidecars.
6. Commit `AddSegment` + optional `UpdateTableMeta` + `UpdateTableCoverage`.

## Coverage and gaps
- **Index interval IDs**: ordered-index values are mapped to discrete interval IDs using the
  configured index granularity.
- **Overlap checks**: tables with entity columns reject an existing complete identity/index
  interval pair; tables without entity columns reject an existing index interval.
- **Snapshots**: table coverage snapshots union segment coverage while preserving entity identity.
- **Recovery**: if the snapshot sidecar is missing/corrupt, it is rebuilt from
  segment coverage sidecars when possible.
- **Read-side metrics**: coverage ratio, missing runs, max gap length, and
  "last fully covered window" are computed against the current snapshot.

### Coverage sidecar lifecycle (v0.1)
1. Write per-segment coverage to `_coverage/segments/<id>.roar`.
2. Union with the current snapshot to build the next snapshot.
3. Write snapshot to `_coverage/table/<ver>-<id>.roar`.
4. Commit both references in a single log entry.

## Range scans
1. Select segments whose `[ts_min, ts_max]` intersect `[ts_start, ts_end)`.
2. Read each segment, build a Parquet `RecordBatch` reader, and filter by time.
3. Stream filtered batches as `TimeSeriesScan`. Input rows need not be chronological, and returned
   batches and rows have no ordering guarantee. Callers must sort when they require ordered results.

## Schema rules (v0.1)
- No schema evolution: the registered schema remains authoritative and every committed segment
  matches it exactly. Incoming top-level scalar fields may use the append allowlist for lossless
  widening before they are written.
- Time column must exist and have a supported timestamp type.
- If entity columns are configured, segments may contain multiple identities.
  Coverage overlap is checked independently for each `(identity, index interval)` pair.

## Error behavior (high level)
- Missing coverage snapshot when segments exist yields a clear error.
- Overlaps surface a dedicated overlap error with context.
- Invalid ordered-index scan ranges return `TableError::Scan` containing
  `ScanError::InvalidRange`.
- Schema mismatches fail before any commit is attempted.

## Extension points
- **Storage**: extend `TableLocation` and `storage` for new backends.
- **Segment formats**: extend `FileFormat` and `SegmentMeta::new_validated`.
- **Schema evolution**: rules live in `metadata::schema_compat`.
