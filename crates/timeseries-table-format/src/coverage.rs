//! In-memory coverage and gap analysis over the 64-bit bucket domain.

pub mod bucket;
pub mod io;
pub mod layout;
pub mod serde;

use std::ops::RangeInclusive;

pub use roaring::RoaringTreemap;

/// Ordered 64-bit coverage bucket identity.
pub type Bucket = u64;

/// In-memory coverage over a discrete set of bucket identities.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    present: RoaringTreemap,
}

impl Coverage {
    /// Construct an empty coverage set.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Wrap an existing treemap.
    pub fn from_treemap(present: RoaringTreemap) -> Self {
        Self { present }
    }

    /// Borrow the present buckets.
    pub fn present(&self) -> &RoaringTreemap {
        &self.present
    }

    /// Consume the coverage and return its treemap.
    pub fn into_treemap(self) -> RoaringTreemap {
        self.present
    }

    /// Return the union of two coverage sets.
    pub fn union(&self, other: &Self) -> Self {
        Self::from_treemap(&self.present | &other.present)
    }

    /// Merge another coverage set into this one.
    pub fn union_inplace(&mut self, other: &Self) {
        self.present |= other.present();
    }

    /// Return the intersection of two coverage sets.
    pub fn intersect(&self, other: &Self) -> Self {
        Self::from_treemap(&self.present & &other.present)
    }

    /// Count buckets present in both coverage sets without materializing them.
    pub fn intersection_cardinality(&self, other: &Self) -> u64 {
        self.present.intersection_len(&other.present)
    }

    /// Number of present buckets.
    pub fn cardinality(&self) -> u64 {
        self.present.len()
    }

    /// Number of bucket identities in an inclusive range.
    pub fn range_cardinality(range: &RangeInclusive<Bucket>) -> u128 {
        if range.is_empty() {
            return 0;
        }
        u128::from(*range.end()) - u128::from(*range.start()) + 1
    }

    /// Count present buckets in an inclusive range without materializing it.
    pub fn covered_cardinality(&self, range: &RangeInclusive<Bucket>) -> u64 {
        if range.is_empty() {
            0
        } else {
            self.present.range_cardinality(range.clone())
        }
    }

    /// Return missing contiguous runs in an inclusive requested range.
    ///
    /// Long runs are optionally split into chunks of at most `max_run_len`.
    /// Work is proportional to present buckets and returned runs, not the size
    /// of the requested range.
    pub fn missing_runs(
        &self,
        range: &RangeInclusive<Bucket>,
        max_run_len: Option<u64>,
    ) -> Vec<RangeInclusive<Bucket>> {
        if range.is_empty() || max_run_len == Some(0) {
            return Vec::new();
        }

        let start = *range.start();
        let end = *range.end();
        let mut cursor = Some(start);
        let mut runs = Vec::new();
        let mut present = self.present.iter();
        present.advance_to(start);

        for bucket in present {
            if bucket > end {
                break;
            }
            let Some(missing_start) = cursor else {
                break;
            };
            if missing_start < bucket {
                runs.push(missing_start..=bucket - 1);
            }
            cursor = bucket.checked_add(1);
        }

        if let Some(missing_start) = cursor.filter(|value| *value <= end) {
            runs.push(missing_start..=end);
        }

        match max_run_len {
            Some(max_len) => split_runs_by_len(runs, max_len),
            None => runs,
        }
    }

    /// Return the last covered contiguous run of at least `min_len` buckets.
    pub fn last_run_with_min_len(
        &self,
        range: &RangeInclusive<Bucket>,
        min_len: u64,
    ) -> Option<RangeInclusive<Bucket>> {
        if range.is_empty() || min_len == 0 {
            return None;
        }

        let mut iter = self.present.iter();
        iter.advance_to(*range.start());
        iter.advance_back_to(*range.end());

        let mut current: Option<(Bucket, Bucket)> = None;
        let mut last = None;
        for bucket in iter {
            match current {
                Some((start, end)) if end.checked_add(1) == Some(bucket) => {
                    current = Some((start, bucket));
                }
                Some((start, end)) => {
                    if inclusive_len(start, end) >= u128::from(min_len) {
                        last = Some(start..=end);
                    }
                    current = Some((bucket, bucket));
                }
                None => current = Some((bucket, bucket)),
            }
        }

        if let Some((start, end)) = current
            && inclusive_len(start, end) >= u128::from(min_len)
        {
            last = Some(start..=end);
        }
        last
    }

    /// Coverage ratio in `[0.0, 1.0]` for an inclusive range.
    pub fn coverage_ratio(&self, range: &RangeInclusive<Bucket>) -> f64 {
        let expected = Self::range_cardinality(range);
        if expected == 0 {
            return 1.0;
        }
        self.covered_cardinality(range) as f64 / expected as f64
    }

    /// Length of the largest missing run in an inclusive range.
    pub fn max_gap_len(&self, range: &RangeInclusive<Bucket>) -> u128 {
        self.missing_runs(range, None)
            .into_iter()
            .map(|run| inclusive_len(*run.start(), *run.end()))
            .max()
            .unwrap_or(0)
    }

    /// Return the last fully-covered contiguous window ending at or before a bucket.
    pub fn last_window_at_or_before(
        &self,
        end_bucket: Bucket,
        len: u64,
    ) -> Option<RangeInclusive<Bucket>> {
        if len == 0 {
            return None;
        }

        let mut iter = self.present.iter();
        iter.advance_back_to(end_bucket);
        let mut run_end = None;
        let mut previous: Option<Bucket> = None;
        let mut run_len = 0u64;

        for bucket in iter.rev() {
            if previous.and_then(|value| value.checked_sub(1)) == Some(bucket) {
                run_len += 1;
            } else {
                run_end = Some(bucket);
                run_len = 1;
            }
            previous = Some(bucket);

            if run_len >= len {
                return run_end.map(|end| bucket..=end);
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
        Self::from_treemap(iter.into_iter().collect())
    }
}

fn inclusive_len(start: Bucket, end: Bucket) -> u128 {
    u128::from(end) - u128::from(start) + 1
}

fn split_runs_by_len(
    runs: Vec<RangeInclusive<Bucket>>,
    max_len: u64,
) -> Vec<RangeInclusive<Bucket>> {
    if max_len == 0 {
        return Vec::new();
    }

    let mut out = Vec::new();
    for range in runs {
        let mut start = *range.start();
        let end = *range.end();
        loop {
            let chunk_end = start.saturating_add(max_len - 1).min(end);
            out.push(start..=chunk_end);
            if chunk_end == end {
                break;
            }
            start = chunk_end + 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_set_operations_use_u64_domain() {
        let a: Coverage = [0, u64::from(u32::MAX) + 1, u64::MAX].into_iter().collect();
        let b: Coverage = [u64::from(u32::MAX) + 1, 7].into_iter().collect();

        assert_eq!(a.cardinality(), 3);
        assert_eq!(a.intersection_cardinality(&b), 1);
        assert_eq!(a.intersect(&b).cardinality(), 1);
        assert_eq!(a.union(&b).cardinality(), 4);
    }

    #[test]
    fn sparse_huge_ranges_do_not_require_expected_bitmap() {
        let coverage: Coverage = [0, 2, u64::MAX].into_iter().collect();
        let range = 0..=u64::MAX;

        assert_eq!(Coverage::range_cardinality(&range), 1u128 << 64);
        assert_eq!(coverage.covered_cardinality(&range), 3);
        assert_eq!(
            coverage.missing_runs(&range, None),
            vec![1..=1, 3..=u64::MAX - 1]
        );
        assert_eq!(coverage.max_gap_len(&range), u128::from(u64::MAX) - 3);
    }

    #[test]
    fn missing_runs_split_without_enumerating_missing_points() {
        let coverage: Coverage = [2, 7].into_iter().collect();
        assert_eq!(
            coverage.missing_runs(&(0..=9), Some(2)),
            vec![0..=1, 3..=4, 5..=6, 8..=9]
        );
    }

    #[test]
    fn range_queries_respect_requested_bounds() {
        let coverage: Coverage = [2, 3, 4, 7, 8, 9].into_iter().collect();
        let range = 1..=8;

        assert_eq!(coverage.covered_cardinality(&range), 5);
        assert_eq!(coverage.missing_runs(&range, None), vec![1..=1, 5..=6]);
        assert_eq!(coverage.max_gap_len(&range), 2);
        assert_eq!(coverage.last_run_with_min_len(&range, 2), Some(7..=8));
        assert_eq!(coverage.coverage_ratio(&range), 5.0 / 8.0);
    }

    #[test]
    fn last_window_handles_zero_and_u64_max() {
        let coverage: Coverage = [0, 1, u64::MAX - 2, u64::MAX - 1, u64::MAX]
            .into_iter()
            .collect();

        assert_eq!(
            coverage.last_window_at_or_before(u64::MAX, 3),
            Some(u64::MAX - 2..=u64::MAX)
        );
        assert_eq!(coverage.last_window_at_or_before(1, 2), Some(0..=1));
        assert_eq!(coverage.last_window_at_or_before(u64::MAX, 0), None);
    }
}
