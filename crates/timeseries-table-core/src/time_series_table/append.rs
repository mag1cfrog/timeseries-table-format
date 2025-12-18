//! Append pipeline for `TimeSeriesTable`.
//!
//! This module contains the core append implementation plus the public
//! wrappers. It is responsible for:
//! - loading/deriving segment metadata and logical schema,
//! - enforcing v0.1 schema rules (adopt on first append, otherwise exact match),
//! - computing segment coverage, detecting overlaps, and writing coverage sidecars,
//! - optimistic commit to the transaction log and in-memory state update.
//!   Keep new append-time invariants here so the flow remains centralized.

use std::path::Path;

use bytes::Bytes;
use snafu::prelude::*;

use crate::{
    coverage::{Coverage, serde::coverage_to_bytes},
    helpers::{
        coverage_sidecar::{
            CoverageError, read_coverage_sidecar, write_coverage_sidecar_new_bytes,
        },
        parquet::{logical_schema_from_parquet_bytes, segment_meta_from_parquet_bytes},
        schema::ensure_schema_exact_match,
        segment_coverage::compute_segment_coverage_from_parquet_bytes,
    },
    layout::coverage::{
        segment_coverage_id_v1, segment_coverage_path, table_coverage_id_v1, table_snapshot_path,
    },
    storage::{self, StorageError, TableLocation},
    time_series_table::{
        SegmentMetaSnafu, TimeSeriesTable,
        error::{
            CoverageOverlapSnafu, CoverageSidecarSnafu, ExistingSegmentMissingCoverageSnafu,
            MissingCanonicalSchemaSnafu, MissingTableCoveragePointerSnafu,
            SchemaCompatibilitySnafu, SegmentCoverageSnafu, StorageSnafu,
            TableCoverageBucketMismatchSnafu, TableError, TransactionLogSnafu,
        },
    },
    transaction_log::{
        LogAction, SegmentId, TableState, TimeBucket, segments::segment_id_v1,
        table_state::TableCoveragePointer,
    },
};

fn ensure_existing_segments_have_coverage(state: &TableState) -> Result<(), TableError> {
    for seg in state.segments.values() {
        if seg.coverage_path.is_none() {
            return ExistingSegmentMissingCoverageSnafu {
                segment_id: seg.segment_id.clone(),
            }
            .fail();
        }
    }

    Ok(())
}

async fn load_table_snapshot_coverage(
    location: &TableLocation,
    state: &TableState,
    bucket_spec: &TimeBucket,
) -> Result<Coverage, TableError> {
    match &state.table_coverage {
        None => {
            // If there are no segments, treat as empty (first append case).
            // If there are segments, this is suspicious in v0.1: fail.
            if state.segments.is_empty() {
                Ok(Coverage::empty())
            } else {
                MissingTableCoveragePointerSnafu.fail()
            }
        }
        Some(ptr) => {
            // Extra safety: ensure bucket spec matches current index.bucket
            ensure!(
                ptr.bucket_spec == *bucket_spec,
                TableCoverageBucketMismatchSnafu {
                    expected: bucket_spec.clone(),
                    actual: ptr.bucket_spec.clone(),
                    pointer_version: ptr.version,
                }
            );

            read_coverage_sidecar(location, Path::new(&ptr.coverage_path))
                .await
                .context(CoverageSidecarSnafu)
        }
    }
}

impl TimeSeriesTable {
    /// Core append implementation that operates on already-loaded Parquet bytes.
    ///
    /// This contains the full v0.1 append flow (schema adoption/enforcement,
    /// coverage computation + overlap detection, sidecar writes, OCC commit,
    /// and in-memory state update). The public
    /// `append_parquet_segment_with_id` wrapper is responsible only for
    /// fetching the bytes from storage before delegating here.
    ///
    /// Callers must ensure `data` corresponds to `relative_path`; the function
    /// does not re-read from storage.
    async fn append_parquet_segment_with_id_and_bytes(
        &mut self,
        segment_id: SegmentId,
        relative_path: &str,
        time_column: &str,
        data: Bytes,
    ) -> Result<u64, TableError> {
        let rel_path = Path::new(relative_path);
        let expected_version = self.state.version;
        let bucket_spec = self.index.bucket.clone();

        // 0) Coverage readiness checks.
        ensure_existing_segments_have_coverage(&self.state)?;

        // 1) Segment meta + schema.
        let mut segment_meta =
            segment_meta_from_parquet_bytes(rel_path, segment_id, time_column, data.clone())
                .context(SegmentMetaSnafu)?;

        let segment_schema =
            logical_schema_from_parquet_bytes(rel_path, data.clone()).context(SegmentMetaSnafu)?;

        // 2) Schema behavior (return maybe_updated_meta, but do NOT build actions yet).
        //
        // - logical_schema == None && version == 1:
        //     first append after create() — adopt this segment’s schema.
        // - logical_schema == None && version != 1:
        //     table is in a bad state for v0.1 → error.
        // - logical_schema == Some(..):
        //     enforce “no schema evolution” via ensure_schema_exact_match.
        let maybe_table_schema = self.state.table_meta.logical_schema.as_ref();

        let maybe_updated_meta = match maybe_table_schema {
            None if expected_version == 1 => {
                let mut updated_meta = self.state.table_meta.clone();
                updated_meta.logical_schema = Some(segment_schema.clone());
                Some(updated_meta)
            }
            None => {
                return MissingCanonicalSchemaSnafu {
                    version: expected_version,
                }
                .fail();
            }
            Some(table_schema) => {
                ensure_schema_exact_match(table_schema, &segment_schema, &self.index)
                    .context(SchemaCompatibilitySnafu)?;
                None
            }
        };

        // 3) Load current table snapshot coverage (or empty if first append).
        let table_cov =
            load_table_snapshot_coverage(&self.location, &self.state, &bucket_spec).await?;

        // 4) Compute segment coverage.
        let segment_cov = compute_segment_coverage_from_parquet_bytes(
            rel_path,
            time_column,
            &bucket_spec,
            data.clone(),
        )
        .context(SegmentCoverageSnafu)?;

        // 5) Overlap detection.
        let overlap = segment_cov.intersect(&table_cov);
        let overlap_count = overlap.cardinality();
        if overlap_count > 0 {
            let example_bucket = overlap.present().iter().next();
            return CoverageOverlapSnafu {
                segment_path: relative_path.to_string(),
                overlap_count,
                example_bucket,
            }
            .fail();
        }
        let seg_cov_bytes =
            coverage_to_bytes(&segment_cov).map_err(|source| TableError::CoverageSidecar {
                source: CoverageError::Serde { source },
            })?;

        // 6) Write sidecars BEFORE commit (orphan files OK on commit failure)
        let coverage_id = segment_coverage_id_v1(&bucket_spec, time_column, &seg_cov_bytes);
        let seg_cov_path =
            segment_coverage_path(&coverage_id).map_err(|source| TableError::CoverageSidecar {
                source: CoverageError::Layout { source },
            })?;
        match write_coverage_sidecar_new_bytes(&self.location, &seg_cov_path, &seg_cov_bytes).await
        {
            Ok(()) => {}
            Err(CoverageError::Storage {
                source: StorageError::AlreadyExists { .. },
            }) => {
                // ok: same id implies same intended content
            }
            Err(e) => return Err(TableError::CoverageSidecar { source: e }),
        }

        let new_version_guess = expected_version + 1;

        let new_table_cov = table_cov.union(&segment_cov);

        let new_snap_cov_bytes =
            coverage_to_bytes(&new_table_cov).map_err(|source| TableError::CoverageSidecar {
                source: CoverageError::Serde { source },
            })?;
        let snapshot_id = table_coverage_id_v1(&bucket_spec, time_column, &new_snap_cov_bytes);

        let snapshot_path = table_snapshot_path(new_version_guess, &snapshot_id).map_err(|e| {
            TableError::CoverageSidecar {
                source: CoverageError::Layout { source: e },
            }
        })?;

        match write_coverage_sidecar_new_bytes(&self.location, &snapshot_path, &new_snap_cov_bytes)
            .await
        {
            Ok(()) => {}
            Err(CoverageError::Storage {
                source: StorageError::AlreadyExists { .. },
            }) => {
                // ok: same id implies same intended content
            }
            Err(e) => return Err(TableError::CoverageSidecar { source: e }),
        }

        // 7) Build actions and commit.
        segment_meta.coverage_path = Some(seg_cov_path.to_string_lossy().to_string());

        let mut actions = Vec::new();
        if let Some(updated_meta) = maybe_updated_meta.clone() {
            actions.push(LogAction::UpdateTableMeta(updated_meta));
        }

        actions.push(LogAction::AddSegment(segment_meta.clone()));
        actions.push(LogAction::UpdateTableCoverage {
            bucket_spec: bucket_spec.clone(),
            coverage_path: snapshot_path.to_string_lossy().to_string(),
        });

        let new_version = self
            .log
            .commit_with_expected_version(expected_version, actions)
            .await
            .context(TransactionLogSnafu)?;

        // OCC invariant: a successful commit_with_expected_version must return
        // the same "next" version we predicted when constructing `snapshot_path`.
        // If this ever diverges, it indicates a severe bug between snapshot path
        // construction and the transaction log implementation, so we panic rather
        // than continuing with an inconsistent in-memory state.
        assert_eq!(
            new_version, new_version_guess,
            "transaction log returned unexpected version: expected {}, got {}",
            new_version_guess, new_version
        );

        // 8) Update in-memory state.
        self.state.version = new_version;

        if let Some(updated_meta) = maybe_updated_meta {
            self.state.table_meta = updated_meta
        }

        self.state
            .segments
            .insert(segment_meta.segment_id.clone(), segment_meta);

        // Also update the snapshot pointer in state.
        self.state.table_coverage = Some(TableCoveragePointer {
            bucket_spec,
            coverage_path: snapshot_path.to_string_lossy().to_string(),
            version: new_version,
        });

        Ok(new_version)
    }

    /// Append a new Parquet segment with a caller-provided `segment_id`, registering it in the transaction log.
    ///
    /// v0.1 behavior:
    /// - Build SegmentMeta from the Parquet file (ts_min, ts_max, row_count).
    /// - Derive the segment logical schema from the Parquet file.
    /// - If the table has no logical_schema yet, adopt this segment schema
    ///   as canonical and write an UpdateTableMeta + AddSegment commit.
    /// - Otherwise, enforce "no schema evolution" via schema_helpers.
    /// - Compute coverage for the segment and table; reject if coverage overlaps.
    /// - Write the segment coverage sidecar before committing (safe to orphan on failure).
    /// - Commit with OCC on the current version.
    /// - Update in-memory TableState on success.
    ///
    /// v0.1: duplicates (same segment_id/path) are allowed if their coverage
    /// does not overlap existing data; otherwise overlap is rejected.
    ///
    /// This wrapper reads the Parquet bytes from storage, then delegates to
    /// `append_parquet_segment_with_id_and_bytes` for the core logic.
    pub async fn append_parquet_segment_with_id(
        &mut self,
        segment_id: SegmentId,
        relative_path: &str,
        time_column: &str,
    ) -> Result<u64, TableError> {
        let rel_path = Path::new(relative_path);

        let bytes = storage::read_all_bytes(&self.location, rel_path)
            .await
            .context(StorageSnafu)?;

        self.append_parquet_segment_with_id_and_bytes(
            segment_id,
            relative_path,
            time_column,
            Bytes::from(bytes),
        )
        .await
    }

    /// Append a Parquet segment using a deterministic, content-derived `segment_id`.
    ///
    /// This wrapper reads the Parquet bytes from storage, derives `segment_id`
    /// via `segment_id_v1(relative_path, bytes)`, then delegates to
    /// `append_parquet_segment_with_id_and_bytes` for the core logic.
    /// Behavior (schema adoption/enforcement, coverage, OCC, state updates)
    /// matches `append_parquet_segment_with_id`.
    pub async fn append_parquet_segment(
        &mut self,
        relative_path: &str,
        time_column: &str,
    ) -> Result<u64, TableError> {
        let rel_path = Path::new(relative_path);
        let bytes = storage::read_all_bytes(&self.location, rel_path)
            .await
            .context(StorageSnafu)?;
        let data = Bytes::from(bytes);

        let segment_id = segment_id_v1(relative_path, &data);
        self.append_parquet_segment_with_id_and_bytes(segment_id, relative_path, time_column, data)
            .await
    }
}
