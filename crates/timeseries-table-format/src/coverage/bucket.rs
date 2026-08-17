//! Stable order-preserving mappings from ordered-index values to 64-bit buckets.

use std::{fmt, ops::RangeInclusive};

use chrono::{DateTime, Duration, SecondsFormat, TimeZone, Utc};
use snafu::Snafu;

use crate::{
    coverage::Bucket,
    metadata::table_metadata::{
        IndexKind, IndexValue, IndexValueError, TimeBucket, validate_index_range,
    },
};

const SIGN_BIT: u64 = 0x8000_0000_0000_0000;
const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * 60;
const SECONDS_PER_DAY: u64 = 24 * 60 * 60;

/// Errors produced while mapping ordered values to coverage buckets.
#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum BucketError {
    /// The value or range does not match the registered index domain.
    #[snafu(display("Invalid ordered index value: {source}"))]
    IndexValue {
        /// Domain or range validation error.
        source: IndexValueError,
    },
    /// A directly constructed timestamp bucket has a zero width.
    #[snafu(display("Timestamp bucket width must be nonzero"))]
    ZeroTimeBucket,
    /// A validated range end could not be adjusted to the final included value.
    #[snafu(display("Ordered range end cannot be adjusted to its predecessor: {end}"))]
    RangeEndUnderflow {
        /// Exclusive range end.
        end: IndexValue,
    },
    /// A bucket identity cannot occur in the configured logical index domain.
    #[snafu(display("Coverage bucket {bucket} is outside the logical {kind} index domain"))]
    BucketOutsideDomain {
        /// Registered ordered-index domain.
        kind: &'static str,
        /// Internal coverage bucket identity.
        bucket: Bucket,
    },
}

/// Logical ordered-index interval represented by one coverage bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalBucketRange {
    start: IndexValue,
    end: IndexValue,
    end_inclusive: bool,
}

impl LogicalBucketRange {
    fn new(start: IndexValue, end: IndexValue, end_inclusive: bool) -> Self {
        Self {
            start,
            end,
            end_inclusive,
        }
    }

    /// Logical start value, always included.
    pub fn start(&self) -> &IndexValue {
        &self.start
    }

    /// Logical end value.
    pub fn end(&self) -> &IndexValue {
        &self.end
    }

    /// Whether the end is included because the bucket reaches the domain maximum.
    pub fn end_inclusive(&self) -> bool {
        self.end_inclusive
    }
}

impl fmt::Display for LogicalBucketRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let close = if self.end_inclusive { ']' } else { ')' };
        match (&self.start, &self.end) {
            (IndexValue::Timestamp(start), IndexValue::Timestamp(end)) => write!(
                f,
                "[{}, {}{close}",
                start.to_rfc3339_opts(SecondsFormat::AutoSi, true),
                end.to_rfc3339_opts(SecondsFormat::AutoSi, true)
            ),
            (IndexValue::Int64(start), IndexValue::Int64(end)) => {
                write!(f, "[{start}, {end}{close}")
            }
            (IndexValue::UInt64(start), IndexValue::UInt64(end)) => {
                write!(f, "[{start}, {end}{close}")
            }
            _ => unreachable!("logical bucket range endpoints share one index domain"),
        }
    }
}

fn time_bucket_width_seconds(bucket: &TimeBucket) -> Result<u64, BucketError> {
    let (value, multiplier) = match *bucket {
        TimeBucket::Seconds(value) => (value, 1),
        TimeBucket::Minutes(value) => (value, SECONDS_PER_MINUTE),
        TimeBucket::Hours(value) => (value, SECONDS_PER_HOUR),
        TimeBucket::Days(value) => (value, SECONDS_PER_DAY),
    };
    if value == 0 {
        return Err(BucketError::ZeroTimeBucket);
    }
    Ok(u64::from(value) * multiplier)
}

fn signed_bucket_id(ordinal: i64) -> Bucket {
    (ordinal as u64) ^ SIGN_BIT
}

/// Map seconds since the Unix epoch to a timestamp bucket identity.
///
/// This lower-level helper is shared by the timestamp Parquet coverage path.
pub fn bucket_id_from_epoch_secs(bucket: &TimeBucket, seconds: i64) -> Result<Bucket, BucketError> {
    let width = i128::from(time_bucket_width_seconds(bucket)?);
    let ordinal = i128::from(seconds).div_euclid(width) as i64;
    Ok(signed_bucket_id(ordinal))
}

fn timestamp_bucket_id(bucket: &TimeBucket, value: DateTime<Utc>) -> Result<Bucket, BucketError> {
    bucket_id_from_epoch_secs(bucket, value.timestamp())
}

fn int64_bucket_id(value: i64, bucket_width: u64) -> Bucket {
    let ordinal = i128::from(value).div_euclid(i128::from(bucket_width)) as i64;
    signed_bucket_id(ordinal)
}

/// Map an ordered-index value to its canonical coverage bucket identity.
pub fn bucket_id(kind: &IndexKind, value: &IndexValue) -> Result<Bucket, BucketError> {
    value
        .validate_kind(kind)
        .map_err(|source| BucketError::IndexValue { source })?;

    match (kind, value) {
        (IndexKind::Timestamp { bucket, .. }, IndexValue::Timestamp(value)) => {
            timestamp_bucket_id(bucket, *value)
        }
        (IndexKind::Int64 { bucket_width }, IndexValue::Int64(value)) => {
            Ok(int64_bucket_id(*value, bucket_width.get()))
        }
        (IndexKind::UInt64 { bucket_width }, IndexValue::UInt64(value)) => {
            Ok(*value / bucket_width.get())
        }
        _ => unreachable!("value domain was validated above"),
    }
}

/// Decode one internal coverage bucket into its logical ordered-index interval.
pub fn logical_bucket_range(
    kind: &IndexKind,
    bucket: Bucket,
) -> Result<LogicalBucketRange, BucketError> {
    let outside_domain = || BucketError::BucketOutsideDomain {
        kind: kind.name(),
        bucket,
    };

    match kind {
        IndexKind::Timestamp {
            bucket: time_bucket,
            ..
        } => {
            let ordinal = i128::from((bucket ^ SIGN_BIT) as i64);
            let width = i128::from(time_bucket_width_seconds(time_bucket)?);
            let domain_start = i128::from(DateTime::<Utc>::MIN_UTC.timestamp());
            let domain_end = i128::from(DateTime::<Utc>::MAX_UTC.timestamp()) + 1;
            let start = (ordinal * width).max(domain_start);
            let end = ((ordinal + 1) * width).min(domain_end);
            if start >= end {
                return Err(outside_domain());
            }

            let start = Utc
                .timestamp_opt(start as i64, 0)
                .single()
                .ok_or_else(&outside_domain)?;
            let end_inclusive = end == domain_end;
            let end = if end_inclusive {
                DateTime::<Utc>::MAX_UTC
            } else {
                Utc.timestamp_opt(end as i64, 0)
                    .single()
                    .ok_or_else(&outside_domain)?
            };
            Ok(LogicalBucketRange::new(
                start.into(),
                end.into(),
                end_inclusive,
            ))
        }
        IndexKind::Int64 { bucket_width } => {
            let ordinal = i128::from((bucket ^ SIGN_BIT) as i64);
            let width = i128::from(bucket_width.get());
            let domain_start = i128::from(i64::MIN);
            let domain_end = i128::from(i64::MAX) + 1;
            let start = (ordinal * width).max(domain_start);
            let end = ((ordinal + 1) * width).min(domain_end);
            if start >= end {
                return Err(outside_domain());
            }

            let end_inclusive = end == domain_end;
            Ok(LogicalBucketRange::new(
                IndexValue::Int64(start as i64),
                IndexValue::Int64(if end_inclusive { i64::MAX } else { end as i64 }),
                end_inclusive,
            ))
        }
        IndexKind::UInt64 { bucket_width } => {
            let width = u128::from(bucket_width.get());
            let domain_end = u128::from(u64::MAX) + 1;
            let start = u128::from(bucket) * width;
            let end = ((u128::from(bucket) + 1) * width).min(domain_end);
            if start >= end {
                return Err(outside_domain());
            }

            let end_inclusive = end == domain_end;
            Ok(LogicalBucketRange::new(
                IndexValue::UInt64(start as u64),
                IndexValue::UInt64(if end_inclusive { u64::MAX } else { end as u64 }),
                end_inclusive,
            ))
        }
    }
}

/// Return the first and last buckets intersecting a half-open range `[start, end)`.
pub fn bucket_range(
    kind: &IndexKind,
    start: &IndexValue,
    end: &IndexValue,
) -> Result<RangeInclusive<Bucket>, BucketError> {
    validate_index_range(kind, start, end).map_err(|source| BucketError::IndexValue { source })?;

    let first = bucket_id(kind, start)?;
    Ok(first..=bucket_id(kind, &value_before(end)?)?)
}

/// Return the bucket containing the final value before an exclusive endpoint.
pub fn bucket_for_exclusive_end(kind: &IndexKind, end: &IndexValue) -> Result<Bucket, BucketError> {
    end.validate_kind(kind)
        .map_err(|source| BucketError::IndexValue { source })?;
    bucket_id(kind, &value_before(end)?)
}

fn value_before(end: &IndexValue) -> Result<IndexValue, BucketError> {
    Ok(match end {
        IndexValue::Timestamp(end) => {
            IndexValue::Timestamp(end.checked_sub_signed(Duration::nanoseconds(1)).ok_or(
                BucketError::RangeEndUnderflow {
                    end: IndexValue::Timestamp(*end),
                },
            )?)
        }
        IndexValue::Int64(end) => IndexValue::Int64(end.checked_sub(1).ok_or({
            BucketError::RangeEndUnderflow {
                end: IndexValue::Int64(*end),
            }
        })?),
        IndexValue::UInt64(end) => IndexValue::UInt64(end.checked_sub(1).ok_or({
            BucketError::RangeEndUnderflow {
                end: IndexValue::UInt64(*end),
            }
        })?),
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use chrono::TimeZone;

    use super::*;

    fn timestamp_kind(bucket: TimeBucket) -> IndexKind {
        IndexKind::Timestamp {
            bucket,
            timezone: None,
        }
    }

    #[test]
    fn timestamp_mapping_is_ordered_across_epoch() {
        let kind = timestamp_kind(TimeBucket::Seconds(1));
        let before = Utc.timestamp_opt(-1, 0).single().unwrap().into();
        let epoch = Utc.timestamp_opt(0, 0).single().unwrap().into();
        let after = Utc.timestamp_opt(1, 0).single().unwrap().into();

        assert_eq!(bucket_id(&kind, &before).unwrap(), SIGN_BIT - 1);
        assert_eq!(bucket_id(&kind, &epoch).unwrap(), SIGN_BIT);
        assert_eq!(bucket_id(&kind, &after).unwrap(), SIGN_BIT + 1);
    }

    #[test]
    fn timestamp_mapping_uses_euclidean_buckets_before_epoch() {
        let bucket = TimeBucket::Minutes(1);
        assert_eq!(
            bucket_id_from_epoch_secs(&bucket, -61).unwrap(),
            SIGN_BIT - 2
        );
        assert_eq!(
            bucket_id_from_epoch_secs(&bucket, -60).unwrap(),
            SIGN_BIT - 1
        );
        assert_eq!(
            bucket_id_from_epoch_secs(&bucket, -1).unwrap(),
            SIGN_BIT - 1
        );
        assert_eq!(bucket_id_from_epoch_secs(&bucket, 0).unwrap(), SIGN_BIT);
    }

    #[test]
    fn int64_mapping_handles_zero_and_extremes() {
        for width in [1, 3, u64::MAX] {
            let kind = IndexKind::Int64 {
                bucket_width: NonZeroU64::new(width).unwrap(),
            };
            let values = [i64::MIN, -1, 0, 1, i64::MAX];
            let buckets: Vec<_> = values
                .into_iter()
                .map(|value| bucket_id(&kind, &value.into()).unwrap())
                .collect();
            assert!(buckets.windows(2).all(|pair| pair[0] <= pair[1]));
        }

        let unit = IndexKind::Int64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        };
        assert_eq!(bucket_id(&unit, &i64::MIN.into()).unwrap(), 0);
        assert_eq!(bucket_id(&unit, &0i64.into()).unwrap(), SIGN_BIT);
        assert_eq!(bucket_id(&unit, &i64::MAX.into()).unwrap(), u64::MAX);
    }

    #[test]
    fn uint64_mapping_is_exact_through_max() {
        let unit = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        };
        for value in [0, i64::MAX as u64 + 1, u64::MAX] {
            assert_eq!(bucket_id(&unit, &value.into()).unwrap(), value);
        }

        let width = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        };
        assert_eq!(bucket_id(&width, &u64::MAX.into()).unwrap(), u64::MAX / 10);
    }

    #[test]
    fn logical_bucket_ranges_use_configured_index_units() {
        let signed_unit = IndexKind::Int64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        };
        let signed_unit_bucket = bucket_id(&signed_unit, &50_464i64.into()).unwrap();
        assert_eq!(
            logical_bucket_range(&signed_unit, signed_unit_bucket)
                .unwrap()
                .to_string(),
            "[50464, 50465)"
        );

        let signed = IndexKind::Int64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        };
        let signed_bucket = bucket_id(&signed, &(-11i64).into()).unwrap();
        assert_eq!(
            logical_bucket_range(&signed, signed_bucket)
                .unwrap()
                .to_string(),
            "[-20, -10)"
        );

        let unsigned = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        };
        let unsigned_bucket = bucket_id(&unsigned, &50_464u64.into()).unwrap();
        assert_eq!(
            logical_bucket_range(&unsigned, unsigned_bucket)
                .unwrap()
                .to_string(),
            "[50460, 50470)"
        );

        let timestamp = timestamp_kind(TimeBucket::Hours(1));
        let epoch = Utc.timestamp_opt(0, 0).single().unwrap();
        let timestamp_bucket = bucket_id(&timestamp, &epoch.into()).unwrap();
        assert_eq!(
            logical_bucket_range(&timestamp, timestamp_bucket)
                .unwrap()
                .to_string(),
            "[1970-01-01T00:00:00Z, 1970-01-01T01:00:00Z)"
        );

        let before_epoch = Utc.timestamp_opt(-1, 0).single().unwrap();
        let before_epoch_bucket = bucket_id(&timestamp, &before_epoch.into()).unwrap();
        assert_eq!(
            logical_bucket_range(&timestamp, before_epoch_bucket)
                .unwrap()
                .to_string(),
            "[1969-12-31T23:00:00Z, 1970-01-01T00:00:00Z)"
        );
    }

    #[test]
    fn logical_bucket_ranges_clip_at_domain_maximum() -> Result<(), BucketError> {
        let signed = IndexKind::Int64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        };
        let signed_range =
            logical_bucket_range(&signed, bucket_id(&signed, &i64::MAX.into()).unwrap())?;
        assert_eq!(signed_range.end(), &IndexValue::Int64(i64::MAX));
        assert!(signed_range.end_inclusive());
        let signed_min_range =
            logical_bucket_range(&signed, bucket_id(&signed, &i64::MIN.into()).unwrap())?;
        assert_eq!(signed_min_range.start(), &IndexValue::Int64(i64::MIN));
        assert!(!signed_min_range.end_inclusive());

        let unsigned = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        };
        let unsigned_range =
            logical_bucket_range(&unsigned, bucket_id(&unsigned, &u64::MAX.into()).unwrap())?;
        assert_eq!(unsigned_range.end(), &IndexValue::UInt64(u64::MAX));
        assert!(unsigned_range.end_inclusive());

        let timestamp = timestamp_kind(TimeBucket::Days(u32::MAX));
        let timestamp_range = logical_bucket_range(
            &timestamp,
            bucket_id(&timestamp, &IndexValue::Timestamp(DateTime::<Utc>::MAX_UTC))?,
        )?;
        assert_eq!(
            timestamp_range.end(),
            &IndexValue::Timestamp(DateTime::<Utc>::MAX_UTC)
        );
        assert!(timestamp_range.end_inclusive());
        let timestamp_min_range = logical_bucket_range(
            &timestamp,
            bucket_id(&timestamp, &IndexValue::Timestamp(DateTime::<Utc>::MIN_UTC))?,
        )?;
        assert_eq!(
            timestamp_min_range.start(),
            &IndexValue::Timestamp(DateTime::<Utc>::MIN_UTC)
        );

        Ok(())
    }

    #[test]
    fn logical_bucket_range_rejects_unreachable_bucket() {
        let kind = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(2).unwrap(),
        };
        assert!(matches!(
            logical_bucket_range(&kind, u64::MAX),
            Err(BucketError::BucketOutsideDomain { .. })
        ));
    }

    #[test]
    fn half_open_integer_ranges_do_not_cross_end_boundary() {
        let signed = IndexKind::Int64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        };
        let range = bucket_range(&signed, &0i64.into(), &20i64.into()).unwrap();
        assert_eq!(range, SIGN_BIT..=SIGN_BIT + 1);
        assert_eq!(
            bucket_for_exclusive_end(&signed, &20i64.into()).unwrap(),
            SIGN_BIT + 1
        );

        let unsigned = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        };
        assert_eq!(
            bucket_range(&unsigned, &0u64.into(), &20u64.into()).unwrap(),
            0..=1
        );
        assert_eq!(
            bucket_range(&unsigned, &0u64.into(), &1u64.into()).unwrap(),
            0..=0
        );
    }

    #[test]
    fn half_open_timestamp_range_preserves_nanoseconds() {
        let kind = timestamp_kind(TimeBucket::Seconds(1));
        let start = Utc.timestamp_opt(0, 0).single().unwrap();
        let boundary = Utc.timestamp_opt(2, 0).single().unwrap();

        assert_eq!(
            bucket_range(&kind, &start.into(), &boundary.into()).unwrap(),
            SIGN_BIT..=SIGN_BIT + 1
        );
        assert_eq!(
            bucket_range(
                &kind,
                &start.into(),
                &(boundary + Duration::nanoseconds(1)).into(),
            )
            .unwrap(),
            SIGN_BIT..=SIGN_BIT + 2
        );
    }

    #[test]
    fn invalid_domains_ranges_and_zero_time_buckets_are_errors() {
        let kind = timestamp_kind(TimeBucket::Seconds(0));
        let epoch = Utc.timestamp_opt(0, 0).single().unwrap();
        assert_eq!(
            bucket_id(&kind, &epoch.into()),
            Err(BucketError::ZeroTimeBucket)
        );

        let unsigned = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        };
        assert!(matches!(
            bucket_range(&unsigned, &0i64.into(), &1i64.into()),
            Err(BucketError::IndexValue { .. })
        ));
        assert!(matches!(
            bucket_range(&unsigned, &1u64.into(), &1u64.into()),
            Err(BucketError::IndexValue { .. })
        ));
        assert!(matches!(
            bucket_for_exclusive_end(&unsigned, &0u64.into()),
            Err(BucketError::RangeEndUnderflow { .. })
        ));
    }
}
