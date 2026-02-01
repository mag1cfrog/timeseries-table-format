//! In-memory coverage and gap analysis over a discrete bucket domain.
//!
//! This module is intentionally small and generic:
//!
//! - It wraps `roaring::RoaringBitmap` in a `Coverage` struct.
//! - It does not know about timestamps, tables, or storage.
//! - Callers are expected to map their own domain (for example, time buckets)
//!   into `u32` bucket ids.
//!
//! Typical usage:
//!
//! ```
//! use timeseries_table_core::coverage::{Coverage, RoaringBitmap};
//!
//! // "expected" domain: buckets 0..10
//! let expected: RoaringBitmap = (0u32..10).collect();
//!
//! // "present" coverage: everything except bucket 5
//! let mut present = RoaringBitmap::new();
//! for b in 0u32..10 {
//!     if b != 5 {
//!         present.insert(b);
//!     }
//! }
//!
//! let cov = Coverage::from_bitmap(present);
//!
//! let missing = cov.missing_points(&expected);
//! assert!(missing.contains(5));
//!
//! let ratio = cov.coverage_ratio(&expected);
//! assert!((ratio - 0.9).abs() < 1e-9);
//! ```
pub mod bucket;
pub mod io;
pub mod layout;
pub mod serde;

use std::ops::RangeInclusive;

// Re-export to let callers use `RoaringBitmap` without declaring `roaring` explicitly.
pub use roaring::RoaringBitmap;

/// Type alias for bucket ids used by Coverage.
///
/// For v0.1 we use `u32`, which is enough for common time-bucket domains
/// (for example, minutes since epoch).
pub type Bucket = u32;

/// In-memory coverage over a discrete set of bucket ids.
///
/// This is a thin wrapper over `RoaringBitmap` that adds convenience
/// methods for gap analysis.
#[derive(Debug, Clone, Default)]
pub struct Coverage {
    bitmap: RoaringBitmap,
}

impl Coverage {
    /// Construct an empty coverage set (no bucket present).
    pub fn empty() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }

    /// Wrap an existing RoaringBitmap as Coverage.
    pub fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        Self { bitmap }
    }

    /// Borrow the underlying bitmap of present buckets.
    pub fn present(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    /// Consume the Coverage and return the underlying bitmap.
    pub fn into_bitmap(self) -> RoaringBitmap {
        self.bitmap
    }

    /// Return the union of `self` and `other`.
    pub fn union(&self, other: &Coverage) -> Coverage {
        let bitmap = &self.bitmap | &other.bitmap;
        Coverage { bitmap }
    }

    /// Return the intersection of `self` and `other`.
    pub fn intersect(&self, other: &Coverage) -> Coverage {
        let bitmap = &self.bitmap & &other.bitmap;
        Coverage { bitmap }
    }

    /// Number of buckets present in this coverage.
    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    /// Return bucket ids that are expected but not present in this coverage.
    ///
    /// This is `expected - present`.
    pub fn missing_points(&self, expected: &RoaringBitmap) -> RoaringBitmap {
        let mut missing = expected.clone();
        missing -= &self.bitmap;
        missing
    }

    /// Group missing buckets into contiguous runs, optionally splitting
    /// long runs into chunks of at most `max_run_len`.
    ///
    /// - `expected` defines the domain we care about.
    /// - Missing buckets are `expected - present`.
    /// - Each returned range is inclusive in bucket space.
    pub fn missing_runs(
        &self,
        expected: &RoaringBitmap,
        max_run_len: Option<u64>,
    ) -> Vec<RangeInclusive<Bucket>> {
        let missing = self.missing_points(expected);
        let base_runs = runs_from_bitmap(&missing);

        if let Some(max_len) = max_run_len {
            split_runs_by_len(base_runs, max_len)
        } else {
            base_runs
        }
    }

    /// Return the last (highest) contiguous run of coverage of length
    /// at least `min_len`, relative to `expected`.
    ///
    /// "Coverage" here means buckets that are both in `expected` and in
    /// `self.present()`.
    pub fn last_run_with_min_len(
        &self,
        expected: &RoaringBitmap,
        min_len: u64,
    ) -> Option<RangeInclusive<Bucket>> {
        if min_len == 0 {
            // Return None for min_len == 0 as it's a degenerate case.
            // Callers can avoid this case if they prefer a different meaning.
            return None;
        }

        let covered = &self.bitmap & expected;
        let runs = runs_from_bitmap(&covered);

        for range in runs.into_iter().rev() {
            let (start, end) = (*range.start() as u64, *range.end() as u64);
            let len = end.saturating_sub(start) + 1;
            if len >= min_len {
                return Some(start as Bucket..=end as Bucket);
            }
        }

        None
    }

    /// Coverage ratio in `[0.0, 1.0]` relative to `expected`.
    ///
    /// Defined as:
    ///
    /// `(|present ∩ expected| as f64) / (|expected| as f64)`
    ///
    /// For `expected.is_empty()`, this returns `1.0` by convention
    /// (vacuous full coverage).
    pub fn coverage_ratio(&self, expected: &RoaringBitmap) -> f64 {
        let expected_count = expected.len();
        if expected_count == 0 {
            return 1.0;
        }

        let covered = &self.bitmap & expected;
        let covered_count = covered.len();
        covered_count as f64 / expected_count as f64
    }

    /// Maximum gap length (in buckets) relative to `expected`.
    ///
    /// This is the length (number of buckets) of the longest missing run.
    /// If there are no missing buckets, returns 0.
    pub fn max_gap_len(&self, expected: &RoaringBitmap) -> u64 {
        let missing = self.missing_points(expected);
        let runs = runs_from_bitmap(&missing);

        runs.into_iter()
            .map(|r| {
                let (start, end) = (*r.start(), *r.end());
                (end as u64).saturating_sub(start as u64) + 1
            })
            .max()
            .unwrap_or(0)
    }

    /// Merge another coverage bitmap into this one, accumulating all buckets.
    ///
    /// This performs an in-place union, so after the call `self` contains every
    /// bucket that was set in either `self` or `other`.
    pub fn union_inplace(&mut self, other: &Coverage) {
        self.bitmap |= other.present();
    }

    /// Return the last fully-covered contiguous window of length `len`
    /// ending at or before `end_bucket`, based on *present buckets only*.
    pub fn last_window_at_or_before(
        &self,
        end_bucket: Bucket,
        len: u64,
    ) -> Option<RangeInclusive<Bucket>> {
        if len == 0 {
            return None;
        }

        let it = self.bitmap.iter().rev();

        let mut started = false;
        let mut run_end: Bucket = 0;
        let mut prev_seen: Option<Bucket> = None;
        let mut run_len: u64 = 0;

        for b in it {
            if b > end_bucket {
                continue;
            }

            if !started {
                started = true;
                run_end = b;
                run_len = 1;
            } else if prev_seen == Some(b + 1) {
                run_len += 1;
            } else {
                // break in coverage -> start a new run
                run_end = b;
                run_len = 1;
            }

            prev_seen = Some(b);

            if run_len >= len {
                let end_u64 = run_end as u64;
                let start_u64 = end_u64.checked_add(1)?.checked_sub(len)?;

                if start_u64 > u32::MAX as u64 {
                    return None;
                }
                return Some(start_u64 as Bucket..=run_end);
            }
        }

        None
    }
}

impl FromIterator<Bucket> for Coverage {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Bucket>,
    {
        let bitmap: RoaringBitmap = iter.into_iter().collect();
        Self { bitmap }
    }
}

/// Convert a bitmap into contiguous runs of bucket ids.
///
/// Each run is returned as a `RangeInclusive<Bucket>`.
fn runs_from_bitmap(bitmap: &RoaringBitmap) -> Vec<RangeInclusive<Bucket>> {
    let mut out = Vec::new();
    let mut iter = bitmap.iter();

    let Some(mut start) = iter.next() else {
        return out;
    };
    let mut prev = start;

    for v in iter {
        if prev.checked_add(1) == Some(v) {
            // still in the same contiguous run
            prev = v;
        } else {
            // close previous run and start a new one
            out.push(start..=prev);
            start = v;
            prev = v;
        }
    }

    // finalize last run
    out.push(start..=prev);
    out
}

/// Split runs into smaller runs of at most `max_len` buckets.
fn split_runs_by_len(
    runs: Vec<RangeInclusive<Bucket>>,
    max_len: u64,
) -> Vec<RangeInclusive<Bucket>> {
    if max_len == 0 {
        return Vec::new();
    }

    let mut out = Vec::new();
    let step = max_len - 1;

    for range in runs {
        let (start, end) = (*range.start() as u64, *range.end() as u64);
        let mut cur = start;
        while cur <= end {
            let chunk_end = match cur.checked_add(step) {
                Some(v) => v.min(end),
                None => break, // overflow would only happen at u64::MAX; stop splitting
            };
            out.push(cur as Bucket..=chunk_end as Bucket);

            if chunk_end == end {
                break;
            }

            cur = chunk_end + 1;
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;

    fn bm_from_range(start: u32, end_exclusive: u32) -> RoaringBitmap {
        (start..end_exclusive).collect()
    }

    #[test]
    fn from_iterator_builds_expected_bitmap() {
        let cov: Coverage = (10u32..15u32).collect();
        assert_eq!(cov.cardinality(), 5);
        for b in 10u32..15u32 {
            assert!(cov.present().contains(b));
        }
    }

    #[test]
    fn basic_api_cardinality_union_intersect_into_bitmap() {
        let cov_a = Coverage::from_bitmap(bm_from_range(0, 5)); // {0,1,2,3,4}
        let cov_b = Coverage::from_bitmap(bm_from_range(3, 8)); // {3,4,5,6,7}

        // cardinality
        assert_eq!(cov_a.cardinality(), 5);

        // union: {0..8} without 8
        let union = cov_a.union(&cov_b);
        assert_eq!(union.cardinality(), 8);
        assert!(union.present().contains(0));
        assert!(union.present().contains(7));

        // intersection: {3,4}
        let inter = cov_a.intersect(&cov_b);
        assert_eq!(inter.cardinality(), 2);
        assert!(inter.present().contains(3));
        assert!(inter.present().contains(4));
        assert!(!inter.present().contains(2));

        // into_bitmap returns underlying set
        let bitmap = inter.into_bitmap();
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(4));
        assert_eq!(bitmap.len(), 2);
    }

    #[test]
    fn full_coverage_continuous() {
        // expected = {0..10}, present = {0..10}
        let expected = bm_from_range(0, 10);
        let present = bm_from_range(0, 10);

        let cov = Coverage::from_bitmap(present);

        let missing = cov.missing_points(&expected);
        assert!(missing.is_empty());

        let runs = cov.missing_runs(&expected, None);
        assert!(runs.is_empty());

        for min_len in 1..=10 {
            let run = cov.last_run_with_min_len(&expected, min_len).unwrap();
            assert_eq!(*run.start(), 0);
            assert_eq!(*run.end(), 9);
        }

        // Past the run length, there should be no run.
        assert!(cov.last_run_with_min_len(&expected, 11).is_none());

        assert!((cov.coverage_ratio(&expected) - 1.0).abs() < 1e-12);
        assert_eq!(cov.max_gap_len(&expected), 0);
    }

    #[test]
    fn single_gap_in_middle() {
        // expected = {0..10}, present = {0..10} \ {5}
        let expected = bm_from_range(0, 10);

        let mut present = bm_from_range(0, 10);
        present.remove(5);

        let cov = Coverage::from_bitmap(present);

        let missing = cov.missing_points(&expected);
        assert_eq!(missing.len(), 1);
        assert!(missing.contains(5));

        let runs = cov.missing_runs(&expected, None);
        assert_eq!(runs.len(), 1);
        let r = &runs[0];
        assert_eq!((*r.start(), *r.end()), (5, 5));

        assert_eq!(cov.max_gap_len(&expected), 1);
    }

    #[test]
    fn multiple_gaps_and_run_splitting() {
        // expected = {0..20}
        // present = {0..20} \ {3,4,10,11,12,18}
        let expected = bm_from_range(0, 20);

        let mut present = bm_from_range(0, 20);
        for b in [3, 4, 10, 11, 12, 18] {
            present.remove(b);
        }

        let cov = Coverage::from_bitmap(present);

        let missing = cov.missing_points(&expected);
        let mut missing_vec: Vec<_> = missing.iter().collect();
        missing_vec.sort_unstable();
        assert_eq!(missing_vec, vec![3, 4, 10, 11, 12, 18]);

        // Without max_run_len, we get contiguous runs.
        let runs = cov.missing_runs(&expected, None);
        assert_eq!(runs.len(), 3);
        assert_eq!((*runs[0].start(), *runs[0].end()), (3, 4)); // len 2
        assert_eq!((*runs[1].start(), *runs[1].end()), (10, 12)); // len 3
        assert_eq!((*runs[2].start(), *runs[2].end()), (18, 18)); // len 1

        // With max_run_len = 2, the {10..12} run should be split.
        let runs_split = cov.missing_runs(&expected, Some(2));
        // Gaps: [3,4], [10,11], [12,12], [18,18]
        assert_eq!(runs_split.len(), 4);
        assert_eq!((*runs_split[0].start(), *runs_split[0].end()), (3, 4));
        assert_eq!((*runs_split[1].start(), *runs_split[1].end()), (10, 11));
        assert_eq!((*runs_split[2].start(), *runs_split[2].end()), (12, 12));
        assert_eq!((*runs_split[3].start(), *runs_split[3].end()), (18, 18));

        // With max_run_len = 1, every gap is split into singletons.
        let runs_split_single = cov.missing_runs(&expected, Some(1));
        let expected_singletons = vec![3, 4, 10, 11, 12, 18];
        assert_eq!(runs_split_single.len(), expected_singletons.len());
        for (range, expected_bucket) in runs_split_single.iter().zip(expected_singletons.iter()) {
            assert_eq!(
                (*range.start(), *range.end()),
                (*expected_bucket, *expected_bucket)
            );
        }

        // With max_run_len = 0, nothing is returned per contract.
        let runs_zero = cov.missing_runs(&expected, Some(0));
        assert!(runs_zero.is_empty());
    }

    #[test]
    fn edge_cases_empty_expected() {
        let expected = RoaringBitmap::new();

        // Nonempty present, but expected is empty.
        let present = bm_from_range(0, 10);
        let cov = Coverage::from_bitmap(present);

        let missing = cov.missing_points(&expected);
        assert!(missing.is_empty());

        let runs = cov.missing_runs(&expected, None);
        assert!(runs.is_empty());

        let ratio = cov.coverage_ratio(&expected);
        assert!((ratio - 1.0).abs() < 1e-12);

        assert_eq!(cov.max_gap_len(&expected), 0);
        assert!(cov.last_run_with_min_len(&expected, 1).is_none());
    }

    #[test]
    fn edge_cases_empty_present() {
        // Nonempty expected, empty present.
        let expected = bm_from_range(0, 5);
        let cov = Coverage::empty();

        let missing = cov.missing_points(&expected);
        assert_eq!(missing.len(), expected.len());

        let runs = cov.missing_runs(&expected, None);
        assert_eq!(runs.len(), 1);
        let r = &runs[0];
        assert_eq!((*r.start(), *r.end()), (0, 4));

        assert_eq!(cov.coverage_ratio(&expected), 0.0);
        assert_eq!(cov.max_gap_len(&expected), 5);

        assert!(cov.last_run_with_min_len(&expected, 6).is_none());
        assert!(cov.last_run_with_min_len(&expected, 3).is_none());
    }

    #[test]
    fn single_point_cases() {
        let mut expected = RoaringBitmap::new();
        expected.insert(42);

        // Present covers the single point.
        let mut present = RoaringBitmap::new();
        present.insert(42);
        let cov = Coverage::from_bitmap(present);

        assert!(cov.missing_points(&expected).is_empty());
        assert!(cov.missing_runs(&expected, None).is_empty());
        assert_eq!(cov.coverage_ratio(&expected), 1.0);
        assert_eq!(cov.max_gap_len(&expected), 0);

        let run = cov.last_run_with_min_len(&expected, 1).unwrap();
        assert_eq!((*run.start(), *run.end()), (42, 42));

        // Present is empty.
        let cov_empty = Coverage::empty();
        let missing = cov_empty.missing_points(&expected);
        assert!(missing.contains(42));
    }

    #[test]
    fn last_window_contiguous_runs() {
        let cov = Coverage::from_bitmap(bm_from_range(0, 10)); // 0..=9

        assert_eq!(cov.last_window_at_or_before(9, 3), Some(7u32..=9u32));

        // Move the end back by one; window should slide accordingly.
        assert_eq!(cov.last_window_at_or_before(8, 3), Some(6u32..=8u32));

        // Request longer than available -> None.
        assert!(cov.last_window_at_or_before(9, 11).is_none());
    }

    #[test]
    fn last_window_skips_over_gaps() {
        // Coverage with a gap between 4 and 7.
        let mut bm = bm_from_range(0, 5); // 0..=4
        bm.extend((7u32..=10u32).collect::<RoaringBitmap>()); // 7..=10
        let cov = Coverage::from_bitmap(bm);

        // Highest contiguous run ends at 10.
        assert_eq!(cov.last_window_at_or_before(10, 3), Some(8u32..=10u32));
        assert_eq!(cov.last_window_at_or_before(10, 4), Some(7u32..=10u32));

        // If the end is inside the gap, we fall back to the previous run.
        assert_eq!(cov.last_window_at_or_before(6, 2), Some(3u32..=4u32));

        // End inside the run but request longer than that run; should fall back
        // to the earlier run if it satisfies the length.
        assert_eq!(cov.last_window_at_or_before(9, 5), Some(0u32..=4u32));
    }

    #[test]
    fn last_window_handles_len_zero_and_empty() {
        let cov = Coverage::empty();
        assert!(cov.last_window_at_or_before(100, 1).is_none());
        assert!(cov.last_window_at_or_before(100, 0).is_none());
    }
}
