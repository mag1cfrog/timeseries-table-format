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
    coverage::io::{read_coverage_sidecar, write_coverage_sidecar_atomic},
    transaction_log::table_state::TableCoveragePointer,
};

use super::{TimeSeriesTable, error::TableError};

impl TimeSeriesTable {
    /// Rebuild table coverage by reading each segment's coverage sidecar.
    ///
    /// This is used as a fallback when the table snapshot coverage is missing or
    /// unreadable. Requires every segment to have a `coverage_path`.
    pub(crate) async fn recover_table_coverage_from_segments(
        &self,
    ) -> Result<Coverage, TableError> {
        let mut acc = Coverage::empty();

        for seg in self.state().segments.values() {
            let path = seg.coverage_path.as_ref().ok_or_else(|| {
                TableError::ExistingSegmentMissingCoverage {
                    segment_id: seg.segment_id.clone(),
                }
            })?;

            let cov = read_coverage_sidecar(self.location(), Path::new(path))
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
        let expected = self.index_spec().bucket.clone();
        if ptr.bucket_spec != expected {
            return Err(TableError::TableCoverageBucketMismatch {
                expected,
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
        match &self.state().table_coverage {
            None => {
                if self.state().segments.is_empty() {
                    return Ok(Coverage::empty());
                }
                Err(TableError::MissingTableCoveragePointer)
            }
            Some(ptr) => {
                self.ensure_table_coverage_bucket_matches(ptr)?;
                read_coverage_sidecar(self.location(), Path::new(&ptr.coverage_path))
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
        match &self.state().table_coverage {
            None => {
                if self.state().segments.is_empty() {
                    return Ok(Coverage::empty());
                }
                self.recover_table_coverage_from_segments().await
            }
            Some(ptr) => {
                self.ensure_table_coverage_bucket_matches(ptr)?;

                match read_coverage_sidecar(self.location(), Path::new(&ptr.coverage_path)).await {
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
        match &self.state().table_coverage {
            None => {
                // If there are no segments, treat as empty (first append case).
                // If there are segments, this is suspicious in v0.1: fail.
                if self.state().segments.is_empty() {
                    return Ok(Coverage::empty());
                }

                // No snapshot pointer, but segments exist -> recover from segments.
                self.recover_table_coverage_from_segments().await
            }

            Some(ptr) => {
                self.ensure_table_coverage_bucket_matches(ptr)?;

                match read_coverage_sidecar(self.location(), Path::new(&ptr.coverage_path)).await {
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
                            self.location(),
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
// Coverage query APIs for TimeSeriesTable.
//
// These APIs:
// - derive an "expected" bucket domain from a timestamp range (half-open [start, end))
// - load table coverage (readonly recovery)
// - reuse crate::coverage APIs (coverage_ratio, max_gap_len, last_window_at_or_before)
use std::ops::RangeInclusive;

use chrono::{DateTime, Duration, Utc};
use roaring::RoaringBitmap;
use snafu::ensure;

use crate::{
    coverage::Bucket,
    coverage::bucket::{bucket_id, bucket_range},
    time_series_table::error::{BucketDomainOverflowSnafu, InvalidRangeSnafu},
};

impl TimeSeriesTable {
    fn expected_bitmap_for_bucket_range_checked(
        &self,
        first: u64,
        last: u64,
    ) -> Result<RoaringBitmap, TableError> {
        if first > u32::MAX as u64 {
            return Err(TableError::BucketDomainOverflow {
                last_bucket_id: first,
                max: u32::MAX,
            });
        }
        if last > u32::MAX as u64 {
            return Err(TableError::BucketDomainOverflow {
                last_bucket_id: last,
                max: u32::MAX,
            });
        }
        Ok(RoaringBitmap::from_iter(
            (first..=last).map(|b| b as Bucket),
        ))
    }

    /// Build an "expected" bitmap for `[start, end)` with validation.
    ///
    /// - Uses [`bucket_range`] to compute the inclusive set of bucket ids
    ///   intersecting the half-open interval `[start, end)`.
    /// - Returns [`TableError::InvalidRange`] if `start >= end`.
    /// - Returns [`TableError::BucketDomainOverflow`] if any bucket id
    ///   would exceed `u32::MAX` (our Roaring bitmap domain in v0.1).
    fn expected_bitmap_for_time_range_checked(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<RoaringBitmap, TableError> {
        ensure!(start < end, InvalidRangeSnafu { start, end });

        let range = bucket_range(&self.index_spec().bucket, start, end);
        let first = *range.start();
        let last = *range.end();

        self.expected_bitmap_for_bucket_range_checked(first, last)
    }

    fn end_bucket_for_half_open_end(&self, ts_end: DateTime<Utc>) -> Result<u64, TableError> {
        // For half-open semantics [.., ts_end), subtract 1ns so we pick the
        // last bucket that still intersects the interval.

        let end_adj = ts_end.checked_sub_signed(Duration::nanoseconds(1)).ok_or(
            TableError::InvalidRange {
                start: ts_end,
                end: ts_end,
            },
        )?;
        Ok(bucket_id(&self.index_spec().bucket, end_adj))
    }

    // ---- public query APIs ----

    /// Coverage ratio in [0.0, 1.0] for the half-open time range [start, end).
    ///
    /// Uses the table-level coverage snapshot (with readonly recovery from segments if needed).
    ///
    /// # Errors
    /// - [`TableError::InvalidRange`] if `start >= end`.
    /// - [`TableError::BucketDomainOverflow`] if the derived bucket ids exceed `u32::MAX`.
    ///
    /// # Examples
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_core::{time_series_table::TimeSeriesTable, storage::TableLocation};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_core::time_series_table::error::TableError> {
    /// let start = Utc.timestamp_opt(0, 0).single().unwrap();
    /// let end = Utc.timestamp_opt(120, 0).single().unwrap();
    /// let ratio = table.coverage_ratio_for_range(start, end).await?;
    /// # let _ = ratio;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn coverage_ratio_for_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<f64, TableError> {
        let expected = self.expected_bitmap_for_time_range_checked(start, end)?;
        let cov = self.load_table_snapshot_coverage_readonly().await?;
        Ok(cov.coverage_ratio(&expected))
    }

    /// Maximum contiguous missing run length (in buckets) for the half-open time range [start, end).
    ///
    /// # Errors
    /// - [`TableError::InvalidRange`] if `start >= end`.
    /// - [`TableError::BucketDomainOverflow`] if the derived bucket ids exceed `u32::MAX`.
    ///
    /// # Examples
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_core::{time_series_table::TimeSeriesTable, storage::TableLocation};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_core::time_series_table::error::TableError> {
    /// let start = Utc.timestamp_opt(0, 0).single().unwrap();
    /// let end = Utc.timestamp_opt(180, 0).single().unwrap();
    /// let gap = table.max_gap_len_for_range(start, end).await?;
    /// # let _ = gap;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn max_gap_len_for_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<u64, TableError> {
        let expected = self.expected_bitmap_for_time_range_checked(start, end)?;
        let cov = self.load_table_snapshot_coverage_readonly().await?;
        Ok(cov.max_gap_len(&expected))
    }

    /// Return the last fully covered contiguous window (in bucket space) of length >= window_len_buckets,
    /// ending at or before ts_end.
    ///
    /// Notes:
    /// - This returns a bucket-id RangeInclusive in the v0.1 bucket domain (u32).
    /// - Returns `None` when `window_len_buckets == 0` or when no fully covered window is found.
    ///
    /// # Errors
    /// - [`TableError::BucketDomainOverflow`] if `ts_end` maps beyond the u32 bucket domain.
    ///
    /// # Examples
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_core::{time_series_table::TimeSeriesTable, storage::TableLocation};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_core::time_series_table::error::TableError> {
    /// let ts_end = Utc.timestamp_opt(360, 0).single().unwrap(); // end of bucket 5
    /// let window = table.last_fully_covered_window(ts_end, 2).await?;
    /// # let _ = window;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn last_fully_covered_window(
        &self,
        ts_end: DateTime<Utc>,
        window_len_buckets: u64,
    ) -> Result<Option<RangeInclusive<Bucket>>, TableError> {
        if window_len_buckets == 0 {
            return Ok(None);
        }

        let cov = self.load_table_snapshot_coverage_readonly().await?;
        let end_bucket_u64 = self.end_bucket_for_half_open_end(ts_end)?;

        ensure!(
            end_bucket_u64 <= u32::MAX as u64,
            BucketDomainOverflowSnafu {
                last_bucket_id: end_bucket_u64,
                max: u32::MAX,
            }
        );

        let end_bucket = end_bucket_u64 as Bucket;
        Ok(cov.last_window_at_or_before(end_bucket, window_len_buckets))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        storage::TableLocation,
        time_series_table::test_util::{
            TestResult, TestRow, make_basic_table_meta, utc_datetime, write_test_parquet,
        },
        transaction_log::TimeBucket,
    };
    use chrono::TimeZone;
    use tempfile::TempDir;

    type HelperResult<T> = Result<T, Box<dyn std::error::Error>>;

    fn ts_from_secs(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0)
            .single()
            .expect("valid timestamp")
    }

    async fn make_table() -> HelperResult<(TempDir, TimeSeriesTable)> {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let table = TimeSeriesTable::create(location, make_basic_table_meta()).await?;
        Ok((tmp, table))
    }

    async fn append_segment(
        table: &mut TimeSeriesTable,
        tmp: &TempDir,
        rel_path: &str,
        rows: &[TestRow],
    ) -> HelperResult<()> {
        let abs = tmp.path().join(rel_path);
        write_test_parquet(&abs, true, false, rows)?;
        table.append_parquet_segment(rel_path, "ts").await?;
        Ok(())
    }

    async fn table_with_sparse_coverage() -> HelperResult<(TempDir, TimeSeriesTable)> {
        // Buckets covered: 0, 1, 3 (gap at 2).
        let (tmp, mut table) = make_table().await?;
        append_segment(
            &mut table,
            &tmp,
            "data/sparse.parquet",
            &[
                TestRow {
                    ts_millis: 1_000,
                    symbol: "A",
                    price: 1.0,
                },
                TestRow {
                    ts_millis: 61_000,
                    symbol: "A",
                    price: 2.0,
                },
                TestRow {
                    ts_millis: 180_000,
                    symbol: "A",
                    price: 3.0,
                },
            ],
        )
        .await?;
        Ok((tmp, table))
    }

    async fn table_with_contiguous_run() -> HelperResult<(TempDir, TimeSeriesTable)> {
        // Buckets covered: 4 and 5 (contiguous run).
        let (tmp, mut table) = make_table().await?;
        append_segment(
            &mut table,
            &tmp,
            "data/window.parquet",
            &[
                TestRow {
                    ts_millis: 240_000,
                    symbol: "A",
                    price: 1.0,
                },
                TestRow {
                    ts_millis: 300_000,
                    symbol: "A",
                    price: 2.0,
                },
            ],
        )
        .await?;
        Ok((tmp, table))
    }

    #[tokio::test]
    async fn expected_bitmap_rejects_invalid_range() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let ts = utc_datetime(2024, 1, 1, 0, 0, 0);

        let err = table
            .expected_bitmap_for_time_range_checked(ts, ts)
            .expect_err("start >= end should be invalid");
        assert!(matches!(err, TableError::InvalidRange { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn expected_bitmap_errors_on_bucket_overflow() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let start = ts_from_secs(0);
        // Choose an end far enough in the future that the bucket id exceeds u32::MAX.
        let end = ts_from_secs(((u32::MAX as i64) + 2) * 60);

        let err = table
            .expected_bitmap_for_time_range_checked(start, end)
            .expect_err("bucket domain overflow should error");

        match err {
            TableError::BucketDomainOverflow { last_bucket_id, .. } => {
                assert!(last_bucket_id > u32::MAX as u64);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn expected_bitmap_errors_on_first_bucket_overflow() -> TestResult {
        let (_tmp, table) = make_table().await?;

        let first = (u32::MAX as u64) + 1;
        let err = table
            .expected_bitmap_for_bucket_range_checked(first, first)
            .expect_err("first bucket overflow should error");

        match err {
            TableError::BucketDomainOverflow { last_bucket_id, .. } => {
                assert_eq!(last_bucket_id, first);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn expected_bitmap_covers_inclusive_bucket_range() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(180); // covers buckets 0,1,2 with 1-minute bucket spec

        let bitmap = table.expected_bitmap_for_time_range_checked(start, end)?;
        let first = bucket_id(&table.index_spec().bucket, start);
        let last = bucket_id(&table.index_spec().bucket, end - Duration::nanoseconds(1));
        assert_eq!(bitmap.len(), (last - first + 1) as u64);
        for b in first..=last {
            assert!(bitmap.contains(b as Bucket));
        }
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_uses_snapshot_when_present() -> TestResult {
        let (_tmp, table) = table_with_sparse_coverage().await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(240); // buckets 0,1,2,3 expected

        let ratio = table.coverage_ratio_for_range(start, end).await?;
        assert!((ratio - 0.75).abs() < 1e-12);
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_recovers_when_snapshot_missing() -> TestResult {
        let (_tmp, mut table) = table_with_sparse_coverage().await?;
        table.state_mut().table_coverage = None;

        let ratio = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await?;
        assert!((ratio - 0.75).abs() < 1e-12);
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_errors_when_recovery_missing_segment_coverage_path() -> TestResult {
        let (_tmp, mut table) = table_with_sparse_coverage().await?;
        table.state_mut().table_coverage = None;
        let seg_id = table
            .state()
            .segments
            .keys()
            .next()
            .cloned()
            .expect("segment present");
        table
            .state_mut()
            .segments
            .get_mut(&seg_id)
            .expect("segment present")
            .coverage_path = None;

        let err = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await
            .expect_err("missing segment coverage_path should bubble up");
        assert!(matches!(
            err,
            TableError::ExistingSegmentMissingCoverage { segment_id } if segment_id == seg_id
        ));
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_errors_on_bucket_mismatch() -> TestResult {
        let (_tmp, mut table) = table_with_sparse_coverage().await?;
        let mut ptr = table
            .state()
            .table_coverage
            .clone()
            .expect("snapshot pointer present");
        ptr.bucket_spec = TimeBucket::Hours(1);
        table.state_mut().table_coverage = Some(ptr.clone());

        let err = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await
            .expect_err("mismatched bucket spec should error");

        match err {
            TableError::TableCoverageBucketMismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, table.index_spec().bucket.clone());
                assert_eq!(actual, ptr.bucket_spec);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_handles_empty_table() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let ratio = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(60))
            .await?;
        assert_eq!(ratio, 0.0);
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_errors_when_bucket_domain_overflows() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(((u32::MAX as i64) + 3) * 60);

        let err = table
            .coverage_ratio_for_range(start, end)
            .await
            .expect_err("overflow should error");
        assert!(matches!(err, TableError::BucketDomainOverflow { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn max_gap_len_reports_missing_run() -> TestResult {
        let (_tmp, table) = table_with_sparse_coverage().await?;
        let gap = table
            .max_gap_len_for_range(ts_from_secs(0), ts_from_secs(240))
            .await?;
        assert_eq!(gap, 1);
        Ok(())
    }

    #[tokio::test]
    async fn last_window_returns_none_for_zero_length() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let res = table.last_fully_covered_window(ts_from_secs(0), 0).await?;
        assert!(res.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn last_window_errors_when_bucket_domain_overflows() -> TestResult {
        let (_tmp, table) = make_table().await?;
        // Pick an end timestamp that maps past the u32 bucket domain to force an error.
        let ts_end = ts_from_secs(((u32::MAX as i64) + 2) * 60);

        let err = table
            .last_fully_covered_window(ts_end, 1)
            .await
            .expect_err("overflow should error");
        assert!(matches!(err, TableError::BucketDomainOverflow { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn last_window_respects_half_open_end_and_run_length() -> TestResult {
        let (_tmp, table) = table_with_contiguous_run().await?;
        let ts_end = ts_from_secs(360); // exactly at the start of bucket 6

        let win = table
            .last_fully_covered_window(ts_end, 2)
            .await?
            .expect("window should be present");
        assert_eq!(win, 4u32..=5u32);

        let none = table.last_fully_covered_window(ts_end, 3).await?;
        assert!(none.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn last_window_errors_when_recovery_fails() -> TestResult {
        let (_tmp, mut table) = table_with_contiguous_run().await?;
        table.state_mut().table_coverage = None;
        let seg_id = table
            .state()
            .segments
            .keys()
            .next()
            .cloned()
            .expect("segment present");
        table
            .state_mut()
            .segments
            .get_mut(&seg_id)
            .expect("segment present")
            .coverage_path = None;

        let err = table
            .last_fully_covered_window(ts_from_secs(360), 1)
            .await
            .expect_err("missing coverage_path should bubble up");
        assert!(matches!(
            err,
            TableError::ExistingSegmentMissingCoverage { segment_id } if segment_id == seg_id
        ));
        Ok(())
    }
}
