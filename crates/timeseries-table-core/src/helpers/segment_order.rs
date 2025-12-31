//! Segment ordering helpers.
//!
//! Provides a deterministic comparison for `SegmentMeta` values used when
//! sorting segments by time. Ordering is defined by `ts_min`, then `ts_max`,
//! and finally `segment_id` as a stable tie-breaker.

use std::cmp::Ordering;

use crate::transaction_log::SegmentMeta;

pub(crate) fn cmp_segment_meta_by_time(a: &SegmentMeta, b: &SegmentMeta) -> Ordering {
    a.ts_min
        .cmp(&b.ts_min)
        .then_with(|| a.ts_max.cmp(&b.ts_max))
        .then_with(|| a.segment_id.0.cmp(&b.segment_id.0))
}

#[cfg(test)]
mod tests {
    use crate::transaction_log::{FileFormat, SegmentId};

    use super::*;
    use chrono::{TimeZone, Utc};

    fn seg(id: &str, ts_min: i64, ts_max: i64) -> SegmentMeta {
        SegmentMeta {
            segment_id: SegmentId(id.to_string()),
            path: format!("data/{id}.parquet"),
            format: FileFormat::Parquet,
            ts_min: Utc.timestamp_opt(ts_min, 0).single().unwrap(),
            ts_max: Utc.timestamp_opt(ts_max, 0).single().unwrap(),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        }
    }

    #[test]
    fn ordering_is_deterministic_with_tie_breakers() {
        // Same ts_min, different ts_max, and then segment_id as final tie breaker
        let mut v = vec![
            seg("c", 10, 20),
            seg("b", 10, 20),
            seg("a", 10, 30),
            seg("d", 5, 7),
        ];

        v.sort_unstable_by(cmp_segment_meta_by_time);

        let ids: Vec<String> = v.into_iter().map(|s| s.segment_id.0).collect();
        assert_eq!(ids, vec!["d", "b", "c", "a"]);
    }

    #[test]
    fn ordering_is_equal_for_identical_segments() {
        let a = seg("same", 10, 20);
        let b = seg("same", 10, 20);
        assert_eq!(cmp_segment_meta_by_time(&a, &b), Ordering::Equal);
        assert_eq!(cmp_segment_meta_by_time(&b, &a), Ordering::Equal);
    }

    #[test]
    fn ordering_primary_key_ts_min_dominates() {
        let mut v = vec![seg("z", 20, 30), seg("a", 10, 50), seg("m", 15, 10)];

        v.sort_unstable_by(cmp_segment_meta_by_time);

        let ids: Vec<String> = v.into_iter().map(|s| s.segment_id.0).collect();
        assert_eq!(ids, vec!["a", "m", "z"]);
    }

    #[test]
    fn ordering_uses_segment_id_as_final_tie_breaker() {
        let mut v = vec![seg("b", 10, 20), seg("a", 10, 20), seg("c", 10, 20)];

        v.sort_unstable_by(cmp_segment_meta_by_time);

        let ids: Vec<String> = v.into_iter().map(|s| s.segment_id.0).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }
}
