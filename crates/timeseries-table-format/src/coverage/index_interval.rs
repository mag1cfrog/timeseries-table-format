//! Stable order-preserving mappings from ordered-index values to index interval IDs.

use std::{fmt, ops::RangeInclusive};

use chrono::{DateTime, Duration, SecondsFormat, TimeZone, Utc};
use snafu::Snafu;

use crate::{
    coverage::IndexIntervalId,
    metadata::table_metadata::{
        IndexKind, IndexValue, IndexValueError, TimeIndexGranularity, validate_index_range,
    },
};

const SIGN_BIT: u64 = 0x8000_0000_0000_0000;
const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * 60;
const SECONDS_PER_DAY: u64 = 24 * 60 * 60;

/// Errors produced while mapping ordered values to index interval IDs.
#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum IndexIntervalMappingError {
    /// The value or range does not match the registered index domain.
    #[snafu(display("Invalid ordered index value: {source}"))]
    IndexValue {
        /// Domain or range validation error.
        source: IndexValueError,
    },
    /// A directly constructed timestamp index granularity has a zero width.
    #[snafu(display("Timestamp index granularity must be nonzero"))]
    ZeroTimeIndexGranularity,
    /// A validated range end could not be adjusted to the final included value.
    #[snafu(display("Ordered range end cannot be adjusted to its predecessor: {end}"))]
    RangeEndUnderflow {
        /// Exclusive range end.
        end: IndexValue,
    },
    /// An index interval ID cannot occur in the configured logical index domain.
    #[snafu(display(
        "Index interval ID {index_interval_id} is outside the logical {kind} index domain"
    ))]
    IntervalIdOutsideDomain {
        /// Registered ordered-index domain.
        kind: &'static str,
        /// Internal index interval ID.
        index_interval_id: IndexIntervalId,
    },
}

/// Logical ordered-index interval represented by one index interval ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInterval {
    start: IndexValue,
    end: IndexValue,
    end_inclusive: bool,
}

impl IndexInterval {
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

    /// Whether the end is included because the interval reaches the domain maximum.
    pub fn end_inclusive(&self) -> bool {
        self.end_inclusive
    }
}

impl fmt::Display for IndexInterval {
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
            _ => unreachable!("index interval endpoints share one index domain"),
        }
    }
}

fn time_index_granularity_seconds(
    index_granularity: &TimeIndexGranularity,
) -> Result<u64, IndexIntervalMappingError> {
    let (value, multiplier) = match *index_granularity {
        TimeIndexGranularity::Seconds(value) => (value, 1),
        TimeIndexGranularity::Minutes(value) => (value, SECONDS_PER_MINUTE),
        TimeIndexGranularity::Hours(value) => (value, SECONDS_PER_HOUR),
        TimeIndexGranularity::Days(value) => (value, SECONDS_PER_DAY),
    };
    if value == 0 {
        return Err(IndexIntervalMappingError::ZeroTimeIndexGranularity);
    }
    Ok(u64::from(value) * multiplier)
}

fn signed_index_interval_id(ordinal: i64) -> IndexIntervalId {
    (ordinal as u64) ^ SIGN_BIT
}

/// Map seconds since the Unix epoch to a timestamp index interval ID.
///
/// This lower-level helper is shared by the timestamp Parquet coverage path.
pub fn index_interval_id_from_epoch_secs(
    index_granularity: &TimeIndexGranularity,
    seconds: i64,
) -> Result<IndexIntervalId, IndexIntervalMappingError> {
    let width = i128::from(time_index_granularity_seconds(index_granularity)?);
    let ordinal = i128::from(seconds).div_euclid(width) as i64;
    Ok(signed_index_interval_id(ordinal))
}

fn timestamp_index_interval_id(
    index_granularity: &TimeIndexGranularity,
    value: DateTime<Utc>,
) -> Result<IndexIntervalId, IndexIntervalMappingError> {
    index_interval_id_from_epoch_secs(index_granularity, value.timestamp())
}

fn int64_index_interval_id(value: i64, index_granularity: u64) -> IndexIntervalId {
    let ordinal = i128::from(value).div_euclid(i128::from(index_granularity)) as i64;
    signed_index_interval_id(ordinal)
}

/// Map an ordered-index value to its canonical index interval ID.
pub fn index_interval_id_for_value(
    kind: &IndexKind,
    value: &IndexValue,
) -> Result<IndexIntervalId, IndexIntervalMappingError> {
    value
        .validate_kind(kind)
        .map_err(|source| IndexIntervalMappingError::IndexValue { source })?;

    match (kind, value) {
        (
            IndexKind::Timestamp {
                index_granularity, ..
            },
            IndexValue::Timestamp(value),
        ) => timestamp_index_interval_id(index_granularity, *value),
        (IndexKind::Int64 { index_granularity }, IndexValue::Int64(value)) => {
            Ok(int64_index_interval_id(*value, index_granularity.get()))
        }
        (IndexKind::UInt64 { index_granularity }, IndexValue::UInt64(value)) => {
            Ok(*value / index_granularity.get())
        }
        _ => unreachable!("value domain was validated above"),
    }
}

/// Decode one index interval ID into its logical ordered-index interval.
pub fn index_interval_for_id(
    kind: &IndexKind,
    index_interval_id: IndexIntervalId,
) -> Result<IndexInterval, IndexIntervalMappingError> {
    let outside_domain = || IndexIntervalMappingError::IntervalIdOutsideDomain {
        kind: kind.name(),
        index_interval_id,
    };

    match kind {
        IndexKind::Timestamp {
            index_granularity, ..
        } => {
            let ordinal = i128::from((index_interval_id ^ SIGN_BIT) as i64);
            let width = i128::from(time_index_granularity_seconds(index_granularity)?);
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
            Ok(IndexInterval::new(start.into(), end.into(), end_inclusive))
        }
        IndexKind::Int64 { index_granularity } => {
            let ordinal = i128::from((index_interval_id ^ SIGN_BIT) as i64);
            let width = i128::from(index_granularity.get());
            let domain_start = i128::from(i64::MIN);
            let domain_end = i128::from(i64::MAX) + 1;
            let start = (ordinal * width).max(domain_start);
            let end = ((ordinal + 1) * width).min(domain_end);
            if start >= end {
                return Err(outside_domain());
            }

            let end_inclusive = end == domain_end;
            Ok(IndexInterval::new(
                IndexValue::Int64(start as i64),
                IndexValue::Int64(if end_inclusive { i64::MAX } else { end as i64 }),
                end_inclusive,
            ))
        }
        IndexKind::UInt64 { index_granularity } => {
            let width = u128::from(index_granularity.get());
            let domain_end = u128::from(u64::MAX) + 1;
            let start = u128::from(index_interval_id) * width;
            let end = ((u128::from(index_interval_id) + 1) * width).min(domain_end);
            if start >= end {
                return Err(outside_domain());
            }

            let end_inclusive = end == domain_end;
            Ok(IndexInterval::new(
                IndexValue::UInt64(start as u64),
                IndexValue::UInt64(if end_inclusive { u64::MAX } else { end as u64 }),
                end_inclusive,
            ))
        }
    }
}

/// Return the first and last index interval IDs intersecting `[start, end)`.
pub fn index_interval_id_range(
    kind: &IndexKind,
    start: &IndexValue,
    end: &IndexValue,
) -> Result<RangeInclusive<IndexIntervalId>, IndexIntervalMappingError> {
    validate_index_range(kind, start, end)
        .map_err(|source| IndexIntervalMappingError::IndexValue { source })?;

    let first = index_interval_id_for_value(kind, start)?;
    Ok(first..=index_interval_id_for_value(kind, &value_before(end)?)?)
}

/// Return the index interval ID containing the value before an exclusive end.
pub fn index_interval_id_for_exclusive_end(
    kind: &IndexKind,
    end: &IndexValue,
) -> Result<IndexIntervalId, IndexIntervalMappingError> {
    end.validate_kind(kind)
        .map_err(|source| IndexIntervalMappingError::IndexValue { source })?;
    index_interval_id_for_value(kind, &value_before(end)?)
}

fn value_before(end: &IndexValue) -> Result<IndexValue, IndexIntervalMappingError> {
    Ok(match end {
        IndexValue::Timestamp(end) => {
            IndexValue::Timestamp(end.checked_sub_signed(Duration::nanoseconds(1)).ok_or(
                IndexIntervalMappingError::RangeEndUnderflow {
                    end: IndexValue::Timestamp(*end),
                },
            )?)
        }
        IndexValue::Int64(end) => IndexValue::Int64(end.checked_sub(1).ok_or({
            IndexIntervalMappingError::RangeEndUnderflow {
                end: IndexValue::Int64(*end),
            }
        })?),
        IndexValue::UInt64(end) => IndexValue::UInt64(end.checked_sub(1).ok_or({
            IndexIntervalMappingError::RangeEndUnderflow {
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

    fn timestamp_kind(index_granularity: TimeIndexGranularity) -> IndexKind {
        IndexKind::Timestamp {
            index_granularity,
            timezone: None,
        }
    }

    #[test]
    fn timestamp_mapping_is_ordered_across_epoch() {
        let kind = timestamp_kind(TimeIndexGranularity::Seconds(1));
        let before = Utc.timestamp_opt(-1, 0).single().unwrap().into();
        let epoch = Utc.timestamp_opt(0, 0).single().unwrap().into();
        let after = Utc.timestamp_opt(1, 0).single().unwrap().into();

        assert_eq!(
            index_interval_id_for_value(&kind, &before).unwrap(),
            SIGN_BIT - 1
        );
        assert_eq!(
            index_interval_id_for_value(&kind, &epoch).unwrap(),
            SIGN_BIT
        );
        assert_eq!(
            index_interval_id_for_value(&kind, &after).unwrap(),
            SIGN_BIT + 1
        );
    }

    #[test]
    fn timestamp_mapping_uses_euclidean_intervals_before_epoch() {
        let index_granularity = TimeIndexGranularity::Minutes(1);
        assert_eq!(
            index_interval_id_from_epoch_secs(&index_granularity, -61).unwrap(),
            SIGN_BIT - 2
        );
        assert_eq!(
            index_interval_id_from_epoch_secs(&index_granularity, -60).unwrap(),
            SIGN_BIT - 1
        );
        assert_eq!(
            index_interval_id_from_epoch_secs(&index_granularity, -1).unwrap(),
            SIGN_BIT - 1
        );
        assert_eq!(
            index_interval_id_from_epoch_secs(&index_granularity, 0).unwrap(),
            SIGN_BIT
        );
    }

    #[test]
    fn int64_mapping_handles_zero_and_extremes() {
        for width in [1, 3, u64::MAX] {
            let kind = IndexKind::Int64 {
                index_granularity: NonZeroU64::new(width).unwrap(),
            };
            let values = [i64::MIN, -1, 0, 1, i64::MAX];
            let interval_ids: Vec<_> = values
                .into_iter()
                .map(|value| index_interval_id_for_value(&kind, &value.into()).unwrap())
                .collect();
            assert!(interval_ids.windows(2).all(|pair| pair[0] <= pair[1]));
        }

        let unit = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        assert_eq!(
            index_interval_id_for_value(&unit, &i64::MIN.into()).unwrap(),
            0
        );
        assert_eq!(
            index_interval_id_for_value(&unit, &0i64.into()).unwrap(),
            SIGN_BIT
        );
        assert_eq!(
            index_interval_id_for_value(&unit, &i64::MAX.into()).unwrap(),
            u64::MAX
        );
    }

    #[test]
    fn uint64_mapping_is_exact_through_max() {
        let unit_granularity_kind = IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        for value in [0, i64::MAX as u64 + 1, u64::MAX] {
            assert_eq!(
                index_interval_id_for_value(&unit_granularity_kind, &value.into()).unwrap(),
                value
            );
        }

        let ten_value_granularity_kind = IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(10).unwrap(),
        };
        assert_eq!(
            index_interval_id_for_value(&ten_value_granularity_kind, &u64::MAX.into()).unwrap(),
            u64::MAX / 10
        );
    }

    #[test]
    fn index_intervals_use_configured_index_units() {
        let signed_unit = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        let signed_unit_interval_id =
            index_interval_id_for_value(&signed_unit, &50_464i64.into()).unwrap();
        assert_eq!(
            index_interval_for_id(&signed_unit, signed_unit_interval_id)
                .unwrap()
                .to_string(),
            "[50464, 50465)"
        );

        let signed = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(10).unwrap(),
        };
        let signed_interval_id = index_interval_id_for_value(&signed, &(-11i64).into()).unwrap();
        assert_eq!(
            index_interval_for_id(&signed, signed_interval_id)
                .unwrap()
                .to_string(),
            "[-20, -10)"
        );

        let unsigned = IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(10).unwrap(),
        };
        let unsigned_interval_id =
            index_interval_id_for_value(&unsigned, &50_464u64.into()).unwrap();
        assert_eq!(
            index_interval_for_id(&unsigned, unsigned_interval_id)
                .unwrap()
                .to_string(),
            "[50460, 50470)"
        );

        let timestamp = timestamp_kind(TimeIndexGranularity::Hours(1));
        let epoch = Utc.timestamp_opt(0, 0).single().unwrap();
        let timestamp_interval_id = index_interval_id_for_value(&timestamp, &epoch.into()).unwrap();
        assert_eq!(
            index_interval_for_id(&timestamp, timestamp_interval_id)
                .unwrap()
                .to_string(),
            "[1970-01-01T00:00:00Z, 1970-01-01T01:00:00Z)"
        );

        let before_epoch = Utc.timestamp_opt(-1, 0).single().unwrap();
        let before_epoch_interval_id =
            index_interval_id_for_value(&timestamp, &before_epoch.into()).unwrap();
        assert_eq!(
            index_interval_for_id(&timestamp, before_epoch_interval_id)
                .unwrap()
                .to_string(),
            "[1969-12-31T23:00:00Z, 1970-01-01T00:00:00Z)"
        );
    }

    #[test]
    fn index_intervals_clip_at_domain_maximum() -> Result<(), IndexIntervalMappingError> {
        let signed = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(10).unwrap(),
        };
        let signed_range = index_interval_for_id(
            &signed,
            index_interval_id_for_value(&signed, &i64::MAX.into()).unwrap(),
        )?;
        assert_eq!(signed_range.end(), &IndexValue::Int64(i64::MAX));
        assert!(signed_range.end_inclusive());
        let signed_min_range = index_interval_for_id(
            &signed,
            index_interval_id_for_value(&signed, &i64::MIN.into()).unwrap(),
        )?;
        assert_eq!(signed_min_range.start(), &IndexValue::Int64(i64::MIN));
        assert!(!signed_min_range.end_inclusive());

        let unsigned = IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(10).unwrap(),
        };
        let unsigned_range = index_interval_for_id(
            &unsigned,
            index_interval_id_for_value(&unsigned, &u64::MAX.into()).unwrap(),
        )?;
        assert_eq!(unsigned_range.end(), &IndexValue::UInt64(u64::MAX));
        assert!(unsigned_range.end_inclusive());

        let timestamp = timestamp_kind(TimeIndexGranularity::Days(u32::MAX));
        let timestamp_range = index_interval_for_id(
            &timestamp,
            index_interval_id_for_value(
                &timestamp,
                &IndexValue::Timestamp(DateTime::<Utc>::MAX_UTC),
            )?,
        )?;
        assert_eq!(
            timestamp_range.end(),
            &IndexValue::Timestamp(DateTime::<Utc>::MAX_UTC)
        );
        assert!(timestamp_range.end_inclusive());
        let timestamp_min_range = index_interval_for_id(
            &timestamp,
            index_interval_id_for_value(
                &timestamp,
                &IndexValue::Timestamp(DateTime::<Utc>::MIN_UTC),
            )?,
        )?;
        assert_eq!(
            timestamp_min_range.start(),
            &IndexValue::Timestamp(DateTime::<Utc>::MIN_UTC)
        );

        Ok(())
    }

    #[test]
    fn index_interval_rejects_unreachable_interval_id() {
        let kind = IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(2).unwrap(),
        };
        assert!(matches!(
            index_interval_for_id(&kind, u64::MAX),
            Err(IndexIntervalMappingError::IntervalIdOutsideDomain { .. })
        ));
    }

    #[test]
    fn half_open_integer_ranges_do_not_cross_end_boundary() {
        let signed = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(10).unwrap(),
        };
        let range = index_interval_id_range(&signed, &0i64.into(), &20i64.into()).unwrap();
        assert_eq!(range, SIGN_BIT..=SIGN_BIT + 1);
        assert_eq!(
            index_interval_id_for_exclusive_end(&signed, &20i64.into()).unwrap(),
            SIGN_BIT + 1
        );

        let unsigned = IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(10).unwrap(),
        };
        assert_eq!(
            index_interval_id_range(&unsigned, &0u64.into(), &20u64.into()).unwrap(),
            0..=1
        );
        assert_eq!(
            index_interval_id_range(&unsigned, &0u64.into(), &1u64.into()).unwrap(),
            0..=0
        );
    }

    #[test]
    fn half_open_timestamp_range_preserves_nanoseconds() {
        let kind = timestamp_kind(TimeIndexGranularity::Seconds(1));
        let start = Utc.timestamp_opt(0, 0).single().unwrap();
        let boundary = Utc.timestamp_opt(2, 0).single().unwrap();

        assert_eq!(
            index_interval_id_range(&kind, &start.into(), &boundary.into()).unwrap(),
            SIGN_BIT..=SIGN_BIT + 1
        );
        assert_eq!(
            index_interval_id_range(
                &kind,
                &start.into(),
                &(boundary + Duration::nanoseconds(1)).into(),
            )
            .unwrap(),
            SIGN_BIT..=SIGN_BIT + 2
        );
    }

    #[test]
    fn invalid_domains_ranges_and_zero_time_granularities_are_errors() {
        let kind = timestamp_kind(TimeIndexGranularity::Seconds(0));
        let epoch = Utc.timestamp_opt(0, 0).single().unwrap();
        assert_eq!(
            index_interval_id_for_value(&kind, &epoch.into()),
            Err(IndexIntervalMappingError::ZeroTimeIndexGranularity)
        );

        let unsigned = IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        assert!(matches!(
            index_interval_id_range(&unsigned, &0i64.into(), &1i64.into()),
            Err(IndexIntervalMappingError::IndexValue { .. })
        ));
        assert!(matches!(
            index_interval_id_range(&unsigned, &1u64.into(), &1u64.into()),
            Err(IndexIntervalMappingError::IndexValue { .. })
        ));
        assert!(matches!(
            index_interval_id_for_exclusive_end(&unsigned, &0u64.into()),
            Err(IndexIntervalMappingError::RangeEndUnderflow { .. })
        ));
    }
}
