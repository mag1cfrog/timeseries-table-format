//! Coverage state helpers for `TimeSeriesTable`.
//!
//! This module reads table coverage bitmaps persisted alongside
//! the table. It is responsible for:
//! - Loading coverage snapshots via the transaction log pointer and enforcing
//!   bucket compatibility.
//! - Falling back to unioning segment coverage sidecars when the snapshot
//!   pointer is missing or unreadable (strict vs recovery modes).

use std::path::Path;

use log::warn;

use crate::{
    coverage::Coverage, coverage::io::read_coverage_sidecar,
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
                    path: seg.path.clone(),
                }
            })?;

            let cov = read_coverage_sidecar(self.location(), Path::new(path))
                .await
                .map_err(|source| TableError::SegmentCoverageSidecarRead {
                    path: seg.path.clone(),
                    coverage_path: path.clone(),
                    source: Box::new(source),
                })?;

            // Prefer an in-place union to avoid repeated allocations.
            acc.union_inplace(&cov);
        }

        Ok(acc)
    }

    fn ensure_table_coverage_index_matches(
        &self,
        ptr: &TableCoveragePointer,
    ) -> Result<(), TableError> {
        let expected = self.index_spec().kind.clone();
        if ptr.index_kind != expected {
            return Err(TableError::TableCoverageIndexKindMismatch {
                expected,
                actual: ptr.index_kind.clone(),
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
                self.ensure_table_coverage_index_matches(ptr)?;
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
                self.ensure_table_coverage_index_matches(ptr)?;

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
}
// Coverage query APIs for TimeSeriesTable.
//
// These APIs:
// - derive an inclusive bucket range from a timestamp range (half-open [start, end))
// - load table coverage (readonly recovery)
// - reuse crate::coverage APIs (coverage_ratio, max_gap_len, last_window_at_or_before)
use std::ops::RangeInclusive;

use chrono::{DateTime, Duration, Utc};
use snafu::{ResultExt, ensure};

use crate::{
    coverage::Bucket,
    coverage::bucket::{bucket_id, bucket_range},
    metadata::table_metadata::{IndexKind, IndexValue},
    table::error::{CoverageBucketSnafu, InvalidRangeSnafu},
};

impl TimeSeriesTable {
    fn bucket_range_for_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<RangeInclusive<Bucket>, TableError> {
        ensure!(start < end, InvalidRangeSnafu { start, end });
        self.ensure_timestamp_index("time coverage query")?;
        bucket_range(
            &self.index_spec().kind,
            &IndexValue::Timestamp(start),
            &IndexValue::Timestamp(end),
        )
        .context(CoverageBucketSnafu)
    }

    fn end_bucket_for_half_open_end(&self, ts_end: DateTime<Utc>) -> Result<u64, TableError> {
        self.ensure_timestamp_index("last fully covered time window")?;

        // For half-open semantics [.., ts_end), subtract 1ns so we pick the
        // last bucket that still intersects the interval.
        let end_adj = ts_end.checked_sub_signed(Duration::nanoseconds(1)).ok_or(
            TableError::InvalidRange {
                start: ts_end,
                end: ts_end,
            },
        )?;
        bucket_id(&self.index_spec().kind, &IndexValue::Timestamp(end_adj))
            .context(CoverageBucketSnafu)
    }

    fn ensure_timestamp_index(&self, operation: &'static str) -> Result<(), TableError> {
        if matches!(self.index_spec().kind, IndexKind::Timestamp { .. }) {
            Ok(())
        } else {
            Err(TableError::UnsupportedIndexKind {
                operation,
                actual: self.index_spec().kind.name(),
            })
        }
    }

    // ---- public query APIs ----

    /// Coverage ratio in [0.0, 1.0] for the half-open time range [start, end).
    ///
    /// Uses the table-level coverage snapshot (with readonly recovery from segments if needed).
    ///
    /// # Errors
    /// - [`TableError::InvalidRange`] if `start >= end`.
    ///
    /// # Examples
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_format::{storage::TableLocation, table::TimeSeriesTable};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_format::table::TableError> {
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
        let range = self.bucket_range_for_time_range(start, end)?;
        let cov = self.load_table_snapshot_coverage_readonly().await?;
        Ok(cov.coverage_ratio(&range))
    }

    /// Maximum contiguous missing run length (in buckets) for the half-open time range [start, end).
    ///
    /// # Errors
    /// - [`TableError::InvalidRange`] if `start >= end`.
    ///
    /// # Examples
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_format::{storage::TableLocation, table::TimeSeriesTable};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_format::table::TableError> {
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
    ) -> Result<u128, TableError> {
        let range = self.bucket_range_for_time_range(start, end)?;
        let cov = self.load_table_snapshot_coverage_readonly().await?;
        Ok(cov.max_gap_len(&range))
    }

    /// Return the last fully covered contiguous window (in bucket space) of length >= window_len_buckets,
    /// ending at or before ts_end.
    ///
    /// Notes:
    /// - This returns a bucket-id RangeInclusive in the 64-bit bucket domain.
    /// - Returns `None` when `window_len_buckets == 0` or when no fully covered window is found.
    ///
    /// # Errors
    ///
    /// # Examples
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_format::{storage::TableLocation, table::TimeSeriesTable};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_format::table::TableError> {
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

        let end_bucket = self.end_bucket_for_half_open_end(ts_end)?;
        let cov = self.load_table_snapshot_coverage_readonly().await?;
        Ok(cov.last_window_at_or_before(end_bucket, window_len_buckets))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::{
        metadata::table_metadata::{IndexKind, IndexSpec, TableMeta, TimeBucket},
        storage::TableLocation,
        table::test_util::{
            TestResult, TestRow, make_basic_table_meta, utc_datetime, write_test_parquet,
        },
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
        table.append_parquet_segment(rel_path).await?;
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
    async fn bucket_range_rejects_invalid_range() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let ts = utc_datetime(2024, 1, 1, 0, 0, 0);

        let err = table
            .bucket_range_for_time_range(ts, ts)
            .expect_err("start >= end should be invalid");
        assert!(matches!(err, TableError::InvalidRange { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn bucket_range_uses_64_bit_signed_timestamp_mapping() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(180); // covers buckets 0,1,2 with 1-minute bucket spec

        let range = table.bucket_range_for_time_range(start, end)?;
        assert_eq!(range, 0x8000_0000_0000_0000..=0x8000_0000_0000_0002);
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
        let segment = table
            .state_mut()
            .segments
            .values_mut()
            .next()
            .expect("segment present");
        let segment_path = segment.path.clone();
        segment.coverage_path = None;

        let err = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await
            .expect_err("missing segment coverage_path should bubble up");
        assert!(matches!(
            err,
            TableError::ExistingSegmentMissingCoverage { path } if path == segment_path
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
        ptr.index_kind = IndexKind::Timestamp {
            bucket: TimeBucket::Hours(1),
            timezone: None,
        };
        table.state_mut().table_coverage = Some(ptr.clone());

        let err = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await
            .expect_err("mismatched bucket spec should error");

        match err {
            TableError::TableCoverageIndexKindMismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, table.index_spec().kind);
                assert_eq!(actual, ptr.index_kind);
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
    async fn coverage_ratio_handles_bucket_ids_above_u32() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(((u32::MAX as i64) + 3) * 60);

        let ratio = table.coverage_ratio_for_range(start, end).await?;
        assert_eq!(ratio, 0.0);
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
    async fn last_window_respects_half_open_end_and_run_length() -> TestResult {
        let (_tmp, table) = table_with_contiguous_run().await?;
        let ts_end = ts_from_secs(360); // exactly at the start of bucket 6

        let win = table
            .last_fully_covered_window(ts_end, 2)
            .await?
            .expect("window should be present");
        assert_eq!(win, 0x8000_0000_0000_0004..=0x8000_0000_0000_0005);

        let none = table.last_fully_covered_window(ts_end, 3).await?;
        assert!(none.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn last_window_rejects_integer_index_before_reading_coverage() -> TestResult {
        let tmp = TempDir::new()?;
        let kind = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        };
        let meta = TableMeta::new_time_series(IndexSpec {
            column: "offset".to_string(),
            entity_columns: Vec::new(),
            kind: kind.clone(),
        });
        let mut table = TimeSeriesTable::create(TableLocation::local(tmp.path()), meta).await?;
        let version = table.state().version;
        table.state_mut().table_coverage = Some(TableCoveragePointer {
            index_kind: kind,
            coverage_path: "_coverage/table/missing.roar".to_string(),
            version,
        });

        let error = table
            .last_fully_covered_window(ts_from_secs(1), 1)
            .await
            .expect_err("integer indexes are unsupported by the timestamp API");
        assert!(matches!(error, TableError::UnsupportedIndexKind { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn last_window_errors_when_recovery_fails() -> TestResult {
        let (_tmp, mut table) = table_with_contiguous_run().await?;
        table.state_mut().table_coverage = None;
        let segment = table
            .state_mut()
            .segments
            .values_mut()
            .next()
            .expect("segment present");
        let segment_path = segment.path.clone();
        segment.coverage_path = None;

        let err = table
            .last_fully_covered_window(ts_from_secs(360), 1)
            .await
            .expect_err("missing coverage_path should bubble up");
        assert!(matches!(
            err,
            TableError::ExistingSegmentMissingCoverage { path } if path == segment_path
        ));
        Ok(())
    }
}
