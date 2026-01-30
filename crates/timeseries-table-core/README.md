# timeseries-table-core

Core engine for a log-structured time-series table format. It owns table metadata,
append rules, coverage math, storage IO, and the `TimeSeriesTable` API that
higher-level integrations build on.

This crate does **not** implement a query engine. It exposes metadata and scan
streams that other layers (DataFusion, Polars, custom code) can consume.

## Layers (module layout)
- `metadata`: pure metadata model + validation (logical schema, table metadata, segment types). No IO.
- `transaction_log`: append-only metadata log APIs (OCC) + table state materialization.
- `table`: user-facing `TimeSeriesTable` API (create/open/append/scan).
- `storage`: local backend + table-root IO helpers.
- `coverage`: coverage math and gap analysis.
- `formats`: format-specific helpers (currently `formats::parquet`).

During the refactor, older module paths remain available as compatibility
re-exports (for example, `transaction_log`, `time_series_table`, `helpers`).

## Responsibilities
- **Transaction log + metadata**: versioned commits, optimistic concurrency, table schema.
- **Segment metadata**: min/max timestamps, row counts, file format, coverage sidecars.
- **Coverage math**: RoaringBitmap overlap checks and gap analysis in bucket space.
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
  data/...                   # Parquet segments (convention)
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
`TimeIndexSpec`. Empty logs return `TableError::EmptyTable`.

### Append (Parquet)
1. Read Parquet bytes from storage.
2. Extract metadata and derive a `LogicalSchema`.
3. If this is the first segment, adopt its schema; otherwise enforce exact match.
4. Validate entity identity (if `TimeIndexSpec.entity_columns` is set).
5. Compute coverage, reject overlaps, and write coverage sidecars.
6. Commit `AddSegment` + optional `UpdateTableMeta` + `UpdateTableCoverage`.

## Coverage and gaps
- **Bucket IDs**: timestamps are mapped to discrete bucket IDs using `TimeBucket`.
- **Overlap checks**: a new segment is rejected if any bucket ID already exists.
- **Snapshots**: table coverage snapshots are the union of all segment coverages.
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
3. Stream filtered batches in chronological order as `TimeSeriesScan`.

## Schema rules (v0.1)
- No schema evolution: all segments must match the canonical schema exactly.
- Time column must exist and have a supported timestamp type.
- If entity columns are configured, each segment must be single-entity and match
  the pinned table identity.

## Error behavior (high level)
- Missing coverage snapshot when segments exist yields a clear error.
- Overlaps surface a dedicated overlap error with context.
- Invalid time ranges return `TableError::InvalidRange`.
- Schema mismatches fail before any commit is attempted.

## Extension points
- **Storage**: extend `TableLocation` and `storage` for new backends.
- **Segment formats**: extend `FileFormat` and `SegmentMeta::new_validated`.
- **Schema evolution**: rules live in `helpers::schema`.
