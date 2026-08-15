use chrono::{DateTime, Duration, Months, Utc};
use datafusion::scalar::ScalarValue;

#[derive(Debug, Clone, Copy)]
pub(crate) struct UnifiedInterval {
    pub(crate) months: i32,
    pub(crate) days: i32,
    pub(crate) nanos: i64,
}

impl UnifiedInterval {
    pub(super) fn zero() -> Self {
        Self {
            months: 0,
            days: 0,
            nanos: 0,
        }
    }

    pub(super) fn checked_add(self, rhs: Self, sign: i32) -> Option<Self> {
        Some(Self {
            months: self.months.checked_add(rhs.months.checked_mul(sign)?)?,
            days: self.days.checked_add(rhs.days.checked_mul(sign)?)?,
            nanos: self
                .nanos
                .checked_add(rhs.nanos.checked_mul(sign as i64)?)?,
        })
    }
}

pub(super) fn interval_from_scalar(value: &ScalarValue) -> Option<UnifiedInterval> {
    match value {
        ScalarValue::IntervalMonthDayNano(Some(value)) => Some(UnifiedInterval {
            months: value.months,
            days: value.days,
            nanos: value.nanoseconds,
        }),
        ScalarValue::IntervalDayTime(Some(value)) => Some(UnifiedInterval {
            months: 0,
            days: value.days,
            nanos: i64::from(value.milliseconds).checked_mul(1_000_000)?,
        }),
        ScalarValue::IntervalYearMonth(Some(value)) => Some(UnifiedInterval {
            months: *value,
            days: 0,
            nanos: 0,
        }),
        _ => None,
    }
}

pub(crate) fn add_interval(
    datetime: DateTime<Utc>,
    interval: UnifiedInterval,
    sign: i32,
) -> Option<DateTime<Utc>> {
    let mut result = datetime;
    let months = interval.months.checked_mul(sign)?;
    if months > 0 {
        result = result.checked_add_months(Months::new(months as u32))?;
    } else if months < 0 {
        result = result.checked_sub_months(Months::new(months.unsigned_abs()))?;
    }

    let days = interval.days.checked_mul(sign)?;
    if days != 0 {
        result = result.checked_add_signed(Duration::days(i64::from(days)))?;
    }

    let nanos = interval.nanos.checked_mul(i64::from(sign))?;
    if nanos != 0 {
        result = result.checked_add_signed(Duration::nanoseconds(nanos))?;
    }

    Some(result)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn rejects_interval_accumulation_overflow() {
        let maximum = UnifiedInterval {
            months: i32::MAX,
            days: 0,
            nanos: 0,
        };
        let one_month = UnifiedInterval {
            months: 1,
            days: 0,
            nanos: 0,
        };

        assert!(maximum.checked_add(one_month, 1).is_none());
    }

    #[test]
    fn rejects_interval_sign_overflow() {
        let datetime = Utc.timestamp_opt(0, 0).unwrap();
        let interval = UnifiedInterval {
            months: i32::MIN,
            days: 0,
            nanos: 0,
        };

        assert!(add_interval(datetime, interval, -1).is_none());
    }
}
