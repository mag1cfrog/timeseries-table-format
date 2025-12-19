//! Coverage state helpers for `TimeSeriesTable`.
//!
//! This module reads and repairs table coverage bitmaps persisted alongside
//! the table. It is responsible for:
//! - Loading coverage snapshots via the transaction log pointer and enforcing
//!   bucket compatibility.
//! - Falling back to unioning segment coverage sidecars when the snapshot
//!   pointer is missing or unreadable (strict vs recovery modes).
//! - Optionally healing the snapshot file on disk after a successful recovery
//!   without touching the transaction log.

use std::path::Path;

use log::warn;

use crate::{
    coverage::Coverage,
    helpers::coverage_sidecar::{read_coverage_sidecar, write_coverage_sidecar_atomic},
    time_series_table::{TimeSeriesTable, error::TableError},
    transaction_log::table_state::TableCoveragePointer,
};

impl TimeSeriesTable {
    /// Rebuild table coverage by reading each segment's coverage sidecar.
    ///
    /// This is used as a fallback when the table snapshot coverage is missing or
    /// unreadable. Requires every segment to have a `coverage_path`.
    pub(crate) async fn recover_table_coverage_from_segments(
        &self,
    ) -> Result<Coverage, TableError> {
        let mut acc = Coverage::empty();

        for seg in self.state.segments.values() {
            let path = seg.coverage_path.as_ref().ok_or_else(|| {
                TableError::ExistingSegmentMissingCoverage {
                    segment_id: seg.segment_id.clone(),
                }
            })?;

            let cov = read_coverage_sidecar(&self.location, Path::new(path))
                .await
                .map_err(|source| TableError::SegmentCoverageSidecarRead {
                    segment_id: seg.segment_id.clone(),
                    coverage_path: path.clone(),
                    source: Box::new(source),
                })?;

            // Prefer an in-place union to avoid repeated allocations.
            acc.union_inplace(&cov);
        }

        Ok(acc)
    }

    fn ensure_table_coverage_bucket_matches(
        &self,
        ptr: &TableCoveragePointer,
    ) -> Result<(), TableError> {
        if ptr.bucket_spec != self.index.bucket {
            return Err(TableError::TableCoverageBucketMismatch {
                expected: self.index.bucket.clone(),
                actual: ptr.bucket_spec.clone(),
                pointer_version: ptr.version,
            });
        }
        Ok(())
    }

    /// Load table coverage using the snapshot pointer only.
    ///
    /// - If there is no snapshot pointer:
    ///   - If table has zero segments: returns empty coverage.
    ///   - Else: returns MissingTableCoveragePointer (strict mode).
    /// - If snapshot exists but is missing/corrupt: returns the snapshot read error.
    pub async fn load_table_coverage_snapshot_only(&self) -> Result<Coverage, TableError> {
        match &self.state.table_coverage {
            None => {
                if self.state.segments.is_empty() {
                    return Ok(Coverage::empty());
                }
                Err(TableError::MissingTableCoveragePointer)
            }
            Some(ptr) => {
                self.ensure_table_coverage_bucket_matches(ptr)?;
                read_coverage_sidecar(&self.location, Path::new(&ptr.coverage_path))
                    .await
                    .map_err(|source| TableError::CoverageSidecar { source })
            }
        }
    }

    /// Load table coverage for read paths (no writes).
    ///
    /// - If snapshot pointer is absent:
    ///   - If table has zero segments: returns empty coverage.
    ///   - Else: recovers by unioning segment sidecars.
    /// - If snapshot pointer exists but snapshot is missing/corrupt:
    ///   - Recovers by unioning segment sidecars.
    pub(crate) async fn load_table_snapshot_coverage_readonly(
        &self,
    ) -> Result<Coverage, TableError> {
        match &self.state.table_coverage {
            None => {
                if self.state.segments.is_empty() {
                    return Ok(Coverage::empty());
                }
                self.recover_table_coverage_from_segments().await
            }
            Some(ptr) => {
                self.ensure_table_coverage_bucket_matches(ptr)?;

                match read_coverage_sidecar(&self.location, Path::new(&ptr.coverage_path)).await {
                    Ok(cov) => Ok(cov),
                    Err(snapshot_err) => {
                        warn!(
                            "Failed to read table coverage snapshot at {} (version {}): {:?}. \
                             Attempting recovery from segment sidecars (readonly).",
                            ptr.coverage_path, ptr.version, snapshot_err
                        );
                        self.recover_table_coverage_from_segments().await
                    }
                }
            }
        }
    }

    /// Load table coverage and (optionally) heal the snapshot best-effort.
    ///
    /// Same as readonly, but if recovery succeeds and a snapshot pointer exists,
    /// attempts to overwrite the snapshot file with the recovered bitmap.
    ///
    /// IMPORTANT: This does NOT update the transaction log; it only rewrites the
    /// referenced snapshot path best-effort.
    pub(crate) async fn load_table_snapshot_coverage_with_heal(
        &self,
    ) -> Result<Coverage, TableError> {
        match &self.state.table_coverage {
            None => {
                // If there are no segments, treat as empty (first append case).
                // If there are segments, this is suspicious in v0.1: fail.
                if self.state.segments.is_empty() {
                    return Ok(Coverage::empty());
                }

                // No snapshot pointer, but segments exist -> recover from segments.
                self.recover_table_coverage_from_segments().await
            }

            Some(ptr) => {
                self.ensure_table_coverage_bucket_matches(ptr)?;

                match read_coverage_sidecar(&self.location, Path::new(&ptr.coverage_path)).await {
                    Ok(cov) => Ok(cov),

                    Err(snapshot_err) => {
                        warn!(
                            "Failed to read table coverage snapshot at {} (version {}): {snapshot_err:?}. \
                         Attempting recovery from segment sidecars.",
                            ptr.coverage_path, ptr.version
                        );

                        // Try recovery from segments.
                        let recovered = self.recover_table_coverage_from_segments().await?;

                        // Optional: heal snapshot best-effort (do not fail open if this fails)
                        let _ = write_coverage_sidecar_atomic(
                            &self.location,
                            Path::new(&ptr.coverage_path),
                            &recovered,
                        )
                        .await;

                        Ok(recovered)
                    }
                }
            }
        }
    }
}
