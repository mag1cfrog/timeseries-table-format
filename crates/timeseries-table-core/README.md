# timeseries-table-core

Core engine for the log‑structured time-series table format. This crate owns the storage layout, transaction log, coverage math, and the user-facing `TimeSeriesTable` API that higher-level integrations build on.

## High-level architecture
- **Transaction log (`transaction_log`)**: Delta-inspired append-only metadata log stored under `_timeseries_log/`. Encapsulates commit files (`actions.rs`), the log store (`log_store.rs`), and the in-memory snapshot (`table_state.rs`). Strongly-typed metadata lives in `table_metadata.rs` and segment descriptors in `segments.rs`.
- **Storage (`storage.rs`)**: Local-filesystem backend that reads/writes relative paths, performs atomic writes (`write_atomic`), create-new writes (`write_new`), and small probes (`read_head_tail_4`). All on-disk access flows through this layer.
- **Coverage (`coverage.rs`, `coverage/serde.rs`, `layout/coverage.rs`)**: RoaringBitmap-backed gap/overlap math plus serialization helpers and canonical layout (`_coverage/segments/*.roar`, `_coverage/table/<ver>-<id>.roar`). Coverage IDs are derived deterministically to make writes idempotent.
- **Helpers (`helpers/*`)**: Bridges between raw data and metadata.
  - `parquet.rs`: Derive `SegmentMeta` and `LogicalSchema` from Parquet bytes, with stats/scan fallback for min/max timestamps.
  - `segment_coverage.rs`: Compute per-segment coverage by streaming Arrow batches and mapping timestamps to bucket IDs.
  - `coverage_sidecar.rs`: Read/write coverage sidecars via the storage layer.
  - `schema.rs`: Enforce the v0.1 “no schema evolution” rule (exact match, time column checked separately).
  - `time_bucket.rs`: Deterministic mapping from timestamps to discrete bucket IDs and expected ranges.
- **Common (`common/time_column.rs`)**: Shared errors for validating timestamp columns in Parquet/Arrow schemas.
- **Time-series table API (`time_series_table/*`)**: User-facing `TimeSeriesTable` that wires the above pieces into create/open/append/scan flows, plus error types.

## How the pieces work together
### Table creation and opening
1. `TimeSeriesTable::create` validates the provided `TableMeta` (must be `TableKind::TimeSeries`), writes the initial `UpdateTableMeta` commit via `TransactionLogStore::commit_with_expected_version`, then rebuilds state through `TransactionLogStore::rebuild_table_state`.
2. `TimeSeriesTable::open` rebuilds `TableState` from the log and extracts the `TimeIndexSpec`. Empty logs return `TableError::EmptyTable`.

### Append pipeline (Parquet)
1. **Read bytes**: Storage layer loads the Parquet file (`storage::read_all_bytes`).
2. **Metadata & schema**: `helpers::parquet::segment_meta_from_parquet_bytes` extracts `ts_min`, `ts_max`, `row_count`; `logical_schema_from_parquet_bytes` derives a `LogicalSchema`.
3. **Schema rules**: If the table lacks a canonical schema and is at version 1, adopt this segment schema; otherwise enforce exact-match via `helpers::schema::ensure_schema_exact_match`.
4. **Coverage**:
   - Load existing table snapshot coverage (or recover from per-segment sidecars) with `load_table_snapshot_coverage`.
   - Compute new segment coverage using `helpers::segment_coverage::compute_segment_coverage_from_parquet_bytes`.
   - Reject overlaps against current table coverage.
   - Persist per-segment coverage sidecar at `_coverage/segments/<id>.roar` and pre-write the next table snapshot at `_coverage/table/<version>-<id>.roar` using `coverage_sidecar` helpers and deterministic IDs from `layout::coverage`.
5. **Commit**: Build `LogAction::AddSegment` (with `coverage_path`), optional `UpdateTableMeta` (schema adoption), and `UpdateTableCoverage`, then commit with OCC through `TransactionLogStore`. In-memory `TableState` is updated to match the committed version.

### Coverage story
- Bucket mapping uses `TimeBucket` from the table index and `helpers::time_bucket` utilities.
- Segment coverage sidecars capture bucket IDs present in each segment. Table snapshot coverage is the union of all segment coverages and is pointed to by `TableState::table_coverage`.
- If the snapshot cannot be read, `append` attempts recovery by unioning all segment sidecars and rewriting the snapshot best-effort.

### Range scans
1. Caller requests `[ts_start, ts_end)`. `scan::segments_for_range` selects segments whose `[ts_min, ts_max]` intersect the half-open window.
2. Each segment is read via the storage layer, turned into an in-memory Parquet reader (`ParquetRecordBatchReaderBuilder`), and filtered by the time column using Arrow scalar comparisons (`scan::filter_ts_batch!` macro) that preserve timezone metadata.
3. Filtered `RecordBatch` values are streamed in chronological order (by `ts_min`) as `TimeSeriesScan`.

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

## Extending the crate
- New storage backends can slot in by extending `TableLocation` and the `storage` module without touching log or table logic.
- Additional segment formats can be added by extending `FileFormat` and `SegmentMeta::new_validated`, reusing the append pipeline.
- Schema evolution rules live in `helpers::schema`; relaxing them would start there.
