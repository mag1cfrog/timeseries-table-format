# timeseries-table-core

Core engine for the log-structured time-series table format. This crate owns the
storage layout, transaction log, coverage math, and the user-facing
`TimeSeriesTable` API that higher-level integrations build on.

## What this crate is responsible for
- **Table metadata + log**: versioned commits, optimistic concurrency, table
  schema, and table-level invariants.
- **Segment metadata**: min/max timestamps, row counts, file format, and optional
  coverage sidecar pointers.
- **Coverage math**: RoaringBitmap-based coverage, overlap detection, and gap
  analysis in bucket space.
- **Storage access**: local filesystem backend and atomic/consistent IO helpers.
- **User API**: create/open/append/scan and coverage/gap query helpers.

This crate does not implement a query engine; it exposes metadata and scan
streams that higher layers can plug into DataFusion or custom backtest code.

## Module map (with responsibilities)
- `transaction_log/*`
  - `actions.rs`: typed log actions (AddSegment, UpdateTableMeta, UpdateTableCoverage).
  - `log_store.rs`: append-only log store with version-guard OCC commits.
  - `table_state.rs`: in-memory snapshot rebuilt from the log.
  - `table_metadata.rs`: typed table metadata (`TableMeta`, `TimeIndexSpec`).
  - `segments.rs`: segment descriptors (`SegmentMeta`, `SegmentDescriptor`).
- `storage.rs`
  - Local filesystem backend with `write_atomic`, `write_new`, and small reads
    (`read_head_tail_4`). All on-disk reads/writes flow through this layer.
- `coverage/*`
  - `coverage.rs`: in-memory coverage wrapper around `roaring::RoaringBitmap`.
  - `serde.rs`: bitmap <-> bytes serialization in Roaring's binary format.
  - `layout/coverage.rs`: canonical sidecar layout and deterministic ID helpers.
- `helpers/*`
  - `parquet.rs`: derive `SegmentMeta` and `LogicalSchema` from Parquet bytes,
    with stats-based fast path and scan fallback for timestamp min/max.
  - `segment_coverage.rs`: compute per-segment coverage by streaming Arrow
    batches and mapping timestamps to bucket IDs.
  - `coverage_sidecar.rs`: read/write coverage sidecars via `storage`.
  - `schema.rs`: v0.1 schema rules (exact match, time column validation).
  - `time_bucket.rs`: deterministic timestamp -> bucket ID mapping and expected
    range helpers.
  - `segment_entity_identity.rs`: extract and validate entity identity from
    segment Parquet data, with stats-based fast path and scan fallback.
- `common/time_column.rs`
  - Shared errors for validating time columns in Parquet/Arrow schemas.
- `time_series_table/*`
  - Public `TimeSeriesTable` API, append/scan wiring, and error types.

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
  3. Write the next version `N+1` only if `CURRENT` is still `N`.
- On OCC conflict, the caller can reload and retry.

### Log actions (core types)
- `AddSegment`: adds a new segment descriptor, including `coverage_path` when
  coverage sidecars are enabled.
- `UpdateTableMeta`: updates table-level metadata (schema adoption, entity
  identity pinning).
- `UpdateTableCoverage`: points to the latest table coverage snapshot.

## Table creation and open
1. `TimeSeriesTable::create` validates `TableMeta` (must be
   `TableKind::TimeSeries`), writes the initial `UpdateTableMeta` commit using
   `TransactionLogStore::commit_with_expected_version`, then rebuilds
   `TableState` through `TransactionLogStore::rebuild_table_state`.
2. `TimeSeriesTable::open` rebuilds `TableState` from the log and extracts
   `TimeIndexSpec`. Empty logs return `TableError::EmptyTable`.

### TableState rebuild rules
- All log actions are replayed in order, producing a consistent in-memory view.
- The latest `UpdateTableCoverage` at or before the current version is used as
  the table snapshot pointer.
- If the table has segments but no coverage pointer, this is treated as an
  error for overlap checks and coverage queries.

## Append pipeline (Parquet)
1. **Read bytes**: storage layer loads the Parquet file
   (`storage::read_all_bytes`).
2. **Metadata + schema**:
   - `helpers::parquet::segment_meta_from_parquet_bytes` extracts `ts_min`,
     `ts_max`, and `row_count`.
   - `helpers::parquet::logical_schema_from_parquet_bytes` derives a
     `LogicalSchema`.
3. **Schema rules**:
   - If the table lacks a canonical schema and is at version 1, adopt this
     segment schema.
   - Otherwise enforce exact match via `helpers::schema::ensure_schema_exact_match`.
4. **Entity identity (v0.1)**:
   - If `TimeIndexSpec.entity_columns` is non-empty, validate the segment is a
     single-entity segment and pin/enforce table identity in `TableMeta`.
5. **Coverage and overlap checks**:
   - Load existing table snapshot coverage (or recover by unioning segment
     sidecars) with `load_table_snapshot_coverage`.
   - Compute new segment coverage using
     `helpers::segment_coverage::compute_segment_coverage_from_parquet_bytes`.
   - Reject overlaps if `segment_cov ∩ table_cov` is non-empty.
   - Persist per-segment coverage to `_coverage/segments/<id>.roar`.
   - Pre-write the next table snapshot to `_coverage/table/<ver>-<id>.roar`.
6. **Commit**:
   - Build `LogAction::AddSegment` (with `coverage_path`),
     optional `UpdateTableMeta` (schema adoption / entity pinning),
     and `UpdateTableCoverage`.
   - Commit with OCC via `TransactionLogStore`. In-memory `TableState` updates
     to the committed version.

## Coverage and gap semantics
- **Bucket IDs**: timestamps are mapped to discrete bucket IDs using
  `helpers::time_bucket` and the table's `TimeBucket`.
- **Overlap detection**: v0.1 overlap checks are time-bucket-only. A new segment
  is rejected if any of its bucket IDs already exist in the table snapshot.
- **Snapshots**: table coverage snapshots are the union of all segment
  coverages present in the current `TableState`.
- **Recovery**: if the snapshot sidecar is missing/corrupt, append/open attempts
  to recover by unioning segment coverage sidecars. If recovery is impossible,
  a clear error is returned.
- **Read-side metrics**: coverage ratio, missing runs, max gap length, and
  "last fully covered window" are computed against the current snapshot using
  expected-bucket domains derived from a time range.

### Coverage sidecar lifecycle (v0.1)
1. Segment coverage is computed from the incoming Parquet bytes and written to
   `_coverage/segments/<id>.roar`.
2. New table snapshot coverage is computed by unioning the current snapshot and
   the new segment coverage.
3. Snapshot is written to `_coverage/table/<ver>-<id>.roar`.
4. A single commit references both sidecars (`AddSegment` + `UpdateTableCoverage`).
5. If the commit fails, sidecars may be orphaned but are ignored by readers.

### Expected bucket domains (read-side)
- Expected buckets are derived from a half-open time range `[start, end)`.
- For each TimeBucket, a deterministic mapping defines which discrete bucket
  IDs intersect the range.
- Coverage metrics compare the table snapshot to this expected domain without
  rescanning Parquet.

## Range scans
1. Caller requests `[ts_start, ts_end)`. `scan::segments_for_range` selects
   segments whose `[ts_min, ts_max]` intersect the half-open window.
2. Each segment is read via the storage layer, turned into an in-memory Parquet
   reader (`ParquetRecordBatchReaderBuilder`), and filtered by the time column
   using Arrow scalar comparisons (`scan::filter_ts_batch!`) that preserve
   timezone metadata.
3. Filtered `RecordBatch` values are streamed in chronological order
   (by `ts_min`) as `TimeSeriesScan`.

## Schema and invariants (v0.1)
- **No schema evolution**: all appended segments must match the canonical schema.
- **Time column validation**: time column must exist and have a supported
  timestamp type.
- **Single-entity tables**: if `TimeIndexSpec.entity_columns` is non-empty,
  each segment must contain exactly one entity value per column, and all
  appends must match the pinned table identity.
- **Coverage uniqueness**: overlap checks enforce uniqueness at the selected
  time bucket resolution.

## Error behavior (high level)
- Missing table coverage snapshot when segments exist yields a clear error.
- Overlap detection failures surface a dedicated overlap error with context.
- Invalid time ranges return `TableError::InvalidRange`.
- Schema mismatches or invalid time columns fail before any commit is attempted.

## Extension points
- **Storage**: extend `TableLocation` and `storage` to add new backends without
  touching log or table logic.
- **Segment formats**: extend `FileFormat` and `SegmentMeta::new_validated`,
  reuse the append pipeline.
- **Schema evolution**: rules live in `helpers::schema`.
