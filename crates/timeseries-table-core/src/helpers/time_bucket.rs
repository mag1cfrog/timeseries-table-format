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

use chrono::{DateTime, Utc};

use crate::transaction_log::TimeBucket;

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

    if bucket_i64 < 0 {
        // v0.1: clamp to 0; we don't expect pre-epoch data in practice.
        debug_assert!(
            false,
            "bucket_id received pre-epoch timestamp: {ts:?} -> bucket {bucket_i64}"
        );
        0
    } else {
        bucket_i64 as u64
    }
}
