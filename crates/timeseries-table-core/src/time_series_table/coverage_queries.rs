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
        error::{BucketDomainOverflowSnafu, InvalidRangeSnafu, TableError},
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
                    symbol: "B",
                    price: 2.0,
                },
                TestRow {
                    ts_millis: 180_000,
                    symbol: "C",
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
                    symbol: "B",
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
    async fn expected_bitmap_covers_inclusive_bucket_range() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(180); // covers buckets 0,1,2 with 1-minute bucket spec

        let bitmap = table.expected_bitmap_for_time_range_checked(start, end)?;
        let first = bucket_id(&table.index.bucket, start);
        let last = bucket_id(&table.index.bucket, end - Duration::nanoseconds(1));
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
        table.state.table_coverage = None;

        let ratio = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await?;
        assert!((ratio - 0.75).abs() < 1e-12);
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_errors_when_recovery_missing_segment_coverage_path() -> TestResult {
        let (_tmp, mut table) = table_with_sparse_coverage().await?;
        table.state.table_coverage = None;
        let seg_id = table
            .state
            .segments
            .keys()
            .next()
            .cloned()
            .expect("segment present");
        table
            .state
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
            .state
            .table_coverage
            .clone()
            .expect("snapshot pointer present");
        ptr.bucket_spec = TimeBucket::Hours(1);
        table.state.table_coverage = Some(ptr.clone());

        let err = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await
            .expect_err("mismatched bucket spec should error");

        match err {
            TableError::TableCoverageBucketMismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, table.index.bucket);
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
        table.state.table_coverage = None;
        let seg_id = table
            .state
            .segments
            .keys()
            .next()
            .cloned()
            .expect("segment present");
        table
            .state
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
