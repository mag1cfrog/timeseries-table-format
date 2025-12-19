//! Coverage query APIs for TimeSeriesTable.
//!
//! These APIs:
//! - derive an "expected" bucket domain from a timestamp range (half-open [start, end))
//! - load table coverage (readonly recovery)
//! - reuse crate::coverage APIs (coverage_ratio, max_gap_len, last_run_with_min_len)
use std::ops::RangeInclusive;

use chrono::{DateTime, Duration, Utc};
use roaring::RoaringBitmap;
use snafu::ensure;

use crate::{
    coverage::Bucket,
    helpers::time_bucket::{bucket_id, bucket_range},
    time_series_table::{
        TimeSeriesTable,
        error::{InvalidRangeSnafu, TableError},
    },
};

impl TimeSeriesTable {
    fn expected_bitmap_for_bucket_range_checked(
        &self,
        first: u64,
        last: u64,
    ) -> Result<RoaringBitmap, TableError> {
        if last > u32::MAX as u64 {
            return Err(TableError::BucketDomainOverflow {
                last_bucket_id: last,
                max: u32::MAX,
            });
        }
        Ok(RoaringBitmap::from_iter((first..last).map(|b| b as Bucket)))
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

        let range = bucket_range(&self.index.bucket, start, end);
        let first = *range.start();
        let last = *range.end();

        self.expected_bitmap_for_bucket_range_checked(first, last)
    }

    fn end_bucket_for_half_open_end(&self, ts_end: DateTime<Utc>) -> Result<u64, TableError> {
        // For half-open semantics [.., ts_end), subtract 1ns so we pick the
        // last bucket that still intersects the interval.

        let end_adj = ts_end - Duration::nanoseconds(1);
        Ok(bucket_id(&self.index.bucket, end_adj))
    }

    // ---- public query APIs ----

    /// Coverage ratio in [0.0, 1.0] for the half-open time range [start, end).
    ///
    /// Uses the table-level coverage snapshot (with readonly recovery from segments if needed).
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
    /// - We search within a bounded lookback window in bucket space to keep it cheap.
    pub async fn last_fully_covered_window(
        &self,
        ts_end: DateTime<Utc>,
        window_len_buckets: u64,
    ) -> Result<Option<RangeInclusive<Bucket>>, TableError> {
        if window_len_buckets == 0 {
            return Ok(None);
        }

        let cov = self.load_table_snapshot_coverage_readonly().await?;
        let end_bucket = self.end_bucket_for_half_open_end(ts_end)?;

        Ok(cov.last_window_at_or_before(end_bucket as u32, window_len_buckets))
    }
}
