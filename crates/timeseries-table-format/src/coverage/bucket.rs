//! Stable order-preserving mappings from ordered-index values to 64-bit buckets.

use std::ops::RangeInclusive;

use chrono::{DateTime, Duration, Utc};
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
