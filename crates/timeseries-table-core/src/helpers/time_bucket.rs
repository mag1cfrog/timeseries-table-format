//! Helpers for mapping timestamps into discrete bucket ids.
//!
//! These helpers are intentionally independent of Roaring / coverage.
//! They just define a stable, documented mapping:
//!
//! - Bucket ids are `u64`, counted forward from the Unix epoch
//!   (1970-01-01T00:00:00Z).
//! - The bucket size is determined by [`TimeBucket`].
//! - `bucket_id` is monotonic in time: later timestamps never map to
//!   a smaller bucket id than earlier timestamps.
//! - `bucket_range` works on half-open time ranges `[start, end)` and
//!   returns an *inclusive* range of bucket ids that intersect that
//!   interval.

use std::ops::RangeInclusive;

use chrono::{DateTime, Duration, Utc};
use roaring::RoaringBitmap;

use crate::{coverage::Bucket, transaction_log::TimeBucket};

const SECONDS_PER_MINUTE: i64 = 60;
const SECONDS_PER_HOUR: i64 = 60 * 60;
const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

/// Return the bucket length in whole seconds for a given [`TimeBucket`].
///
/// This is an internal helper; in v0.1 we only support second-or-larger
/// bucket granularities.
fn bucket_len_secs(spec: &TimeBucket) -> i64 {
    match *spec {
        TimeBucket::Seconds(n) => n as i64,
        TimeBucket::Minutes(n) => (n as i64) * SECONDS_PER_MINUTE,
        TimeBucket::Hours(n) => (n as i64) * SECONDS_PER_HOUR,
        TimeBucket::Days(n) => (n as i64) * SECONDS_PER_DAY,
    }
}

/// Map a `DateTime<Utc>` into a discrete bucket id according to `spec`.
///
/// Semantics:
///
/// - Bucket 0 starts at the Unix epoch `1970-01-01T00:00:00Z`.
/// - Buckets are contiguous, non-overlapping half-open intervals:
///   `[0 * len, 1 * len)`, `[1 * len, 2 * len)`, ...
///   where `len` is the bucket length in seconds.
/// - The returned id is `floor((ts - epoch) / len)`.
///
/// Assumptions / caveats:
///
/// - v0.1 assumes timestamps are **not earlier** than the Unix epoch.
///   If a timestamp before 1970-01-01 is passed, the bucket id is
///   clamped to 0 (with a debug assertion).
pub fn bucket_id(spec: &TimeBucket, ts: DateTime<Utc>) -> u64 {
    let len_secs = bucket_len_secs(spec);
    debug_assert!(len_secs > 0, "TimeBucket width must be positive");

    // Seconds since epoch; `timestamp()` is i64.
    let secs_since_epoch = ts.timestamp();

    // Euclidean division so this stays monotonic even if we ever see
    // pre-epoch timestamps.
    let bucket_i64 = secs_since_epoch.div_euclid(len_secs);

    debug_assert!(
        bucket_i64 >= 0,
        "bucket_id received pre-epoch timestamp: {ts:?} -> bucket {bucket_i64}"
    );
    if bucket_i64 < 0 {
        // v0.1: clamp to 0; we don't expect pre-epoch data in practice.
        0
    } else {
        bucket_i64 as u64
    }
}

/// Map seconds since the Unix epoch into a discrete bucket id according to `spec`.
///
/// This is a lower-level variant of [`bucket_id`] that accepts `i64` seconds
/// directly instead of a `DateTime<Utc>`.
pub fn bucket_id_from_epoch_secs(spec: &TimeBucket, secs_since_epoch: i64) -> u64 {
    let len_secs = bucket_len_secs(spec);
    debug_assert!(len_secs > 0);

    let bucket_i64 = secs_since_epoch.div_euclid(len_secs);

    if bucket_i64 < 0 { 0 } else { bucket_i64 as u64 }
}

/// Return the *inclusive* range of bucket ids intersecting `[start, end)`.
///
/// - The time interval is half-open: it includes `start` and excludes `end`.
/// - All buckets whose interval intersects `[start, end)` are included.
/// - Requires `start < end`; behavior is undefined if `start >= end`.
///
/// Example (1-minute buckets):
///
/// - `start = 10:00:10`, `end = 10:03:00`
/// - Buckets covering 10:00, 10:01, and 10:02 are included.
/// - The bucket starting at 10:03 is *not* included.
pub fn bucket_range(
    spec: &TimeBucket,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> RangeInclusive<u64> {
    debug_assert!(
        start < end,
        "bucket_range expects start < end; got start={start:?}, end={end:?}"
    );

    let first = bucket_id(spec, start);

    // We want the *last* bucket that still intersects [start, end).
    //
    // A bucket intersects [start, end) iff:
    //   bucket_start < end  &&  bucket_end > start
    //
    // Rather than reason about bucket boundaries here, we take a simpler
    // approach: shift `end` back by 1 nanosecond so it definitely falls
    // inside the half-open interval (as long as start < end), and map that
    // timestamp to a bucket id.
    //
    // This works for all bucket sizes >= 1 second and preserves the
    // half-open semantics.
    let end_adj = end - Duration::nanoseconds(1);
    let last = bucket_id(spec, end_adj);

    first..=last
}

/// Build an "expected" bitmap for the half-open time range `[start, end)`,
/// given a bucket spec.
///
/// This is just:
///   bucket_range(spec, start, end) -> RoaringBitmap::from_iter(...)
pub fn expected_buckets_for_range(
    spec: &TimeBucket,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> RoaringBitmap {
    let range = bucket_range(spec, start, end);

    // In v0.1 we standardize on u32 bucket ids for RoaringBitmap and
    // assume bucket ids fit in u32::MAX.
    RoaringBitmap::from_iter(range.map(|b| {
        debug_assert!(b <= u32::MAX as u64, "bucket ID {} exceeds u32::MAX", b);
        b as Bucket
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn bucket_id_monotonic_seconds() {
        let spec = TimeBucket::Seconds(60); // 1-minute buckets
        let base = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        let t0 = base;
        let t1 = base + Duration::seconds(1);
        let t2 = base + Duration::seconds(60);
        let t3 = base + Duration::seconds(61);

        let b0 = bucket_id(&spec, t0);
        let b1 = bucket_id(&spec, t1);
        let b2 = bucket_id(&spec, t2);
        let b3 = bucket_id(&spec, t3);

        assert!(b0 <= b1);
        assert!(b1 <= b2);
        assert!(b2 <= b3);
        assert_eq!(b0, b1); // still bucket 0
        assert_eq!(b2, b3); // both in bucket 1
        assert_eq!(b2, b0 + 1); // next bucket after 60s
    }

    #[test]
    fn bucket_range_simple_minutes() {
        let spec = TimeBucket::Minutes(1);

        let start = Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 10).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 1, 1, 10, 3, 0).unwrap();

        let range = bucket_range(&spec, start, end);
        let first = *range.start();
        let last = *range.end();

        // Should cover 10:00, 10:01, 10:02 buckets (3 buckets)
        assert_eq!(last, first + 2);
    }

    #[test]
    fn bucket_range_single_bucket() {
        let spec = TimeBucket::Hours(1);

        let start = Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap();
        let end = start + Duration::seconds(30); // stays inside same hour

        let range = bucket_range(&spec, start, end);
        assert_eq!(*range.start(), *range.end());
    }

    #[test]
    fn bucket_len_secs_covers_variants() {
        assert_eq!(bucket_len_secs(&TimeBucket::Seconds(7)), 7);
        assert_eq!(bucket_len_secs(&TimeBucket::Minutes(3)), 3 * 60);
        assert_eq!(bucket_len_secs(&TimeBucket::Hours(2)), 2 * 60 * 60);
        assert_eq!(bucket_len_secs(&TimeBucket::Days(1)), 24 * 60 * 60);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic]
    fn bucket_id_pre_epoch_panics_in_debug() {
        let spec = TimeBucket::Minutes(1);
        let pre_epoch = Utc.with_ymd_and_hms(1969, 12, 31, 23, 59, 0).unwrap();
        let _ = bucket_id(&spec, pre_epoch);
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn bucket_id_pre_epoch_clamps_to_zero_in_release() {
        let spec = TimeBucket::Minutes(1);
        let pre_epoch = Utc.with_ymd_and_hms(1969, 12, 31, 23, 59, 0).unwrap();
        assert_eq!(bucket_id(&spec, pre_epoch), 0);
    }

    #[test]
    fn bucket_range_excludes_end_bucket_on_boundary() {
        let spec = TimeBucket::Minutes(1);
        let start = Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 1, 1, 10, 2, 0).unwrap();

        let range = bucket_range(&spec, start, end);
        let first = *range.start();
        let last = *range.end();

        // Should include 10:00 and 10:01 buckets, but not 10:02.
        assert_eq!(last, first + 1);
    }

    #[test]
    fn bucket_range_minimal_interval_inside_bucket() {
        let spec = TimeBucket::Minutes(1);
        let start = Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap();
        let end = start + Duration::nanoseconds(1);

        let range = bucket_range(&spec, start, end);
        assert_eq!(*range.start(), *range.end());
    }

    #[test]
    fn bucket_range_multi_bucket_with_wider_granularity() {
        let spec = TimeBucket::Minutes(5);
        let start = Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 1, 1, 10, 17, 0).unwrap();

        let range = bucket_range(&spec, start, end);
        let first = *range.start();
        let last = *range.end();

        // Buckets at 10:00, 10:05, 10:10, 10:15 -> four buckets
        assert_eq!(last, first + 3);
    }

    #[test]
    fn expected_buckets_matches_bucket_range_minutes() {
        let spec = TimeBucket::Minutes(1);
        let start = Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 10).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 1, 1, 10, 3, 0).unwrap();

        let range = bucket_range(&spec, start, end);
        let bitmap = expected_buckets_for_range(&spec, start, end);

        let manual: RoaringBitmap = range.clone().map(|b| b as Bucket).collect();
        assert_eq!(bitmap, manual);
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(*range.start() as Bucket));
        assert!(bitmap.contains(*range.end() as Bucket));
    }

    #[test]
    fn expected_buckets_respects_end_boundary() {
        let spec = TimeBucket::Minutes(1);
        let start = Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 1, 1, 10, 2, 0).unwrap();

        let bitmap = expected_buckets_for_range(&spec, start, end);

        let buckets_present: Vec<Bucket> = bitmap.iter().collect();
        assert_eq!(buckets_present.len(), 2);

        let first = bucket_id(&spec, start) as Bucket; // 10:00 bucket
        let second = bucket_id(&spec, start + Duration::minutes(1)) as Bucket; // 10:01 bucket
        let excluded = bucket_id(&spec, end) as Bucket; // 10:02 bucket (end boundary, should be excluded)

        assert!(bitmap.contains(first));
        assert!(bitmap.contains(second));
        assert!(!bitmap.contains(excluded));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic]
    fn bucket_range_panics_when_start_not_before_end_in_debug() {
        let spec = TimeBucket::Minutes(1);
        let t = Utc.with_ymd_and_hms(2020, 1, 1, 10, 0, 0).unwrap();
        // In debug builds the debug_assert! should fire.
        let _ = bucket_range(&spec, t, t);
    }
}
