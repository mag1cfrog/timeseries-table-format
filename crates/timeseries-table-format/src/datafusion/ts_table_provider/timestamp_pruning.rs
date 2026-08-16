use std::sync::Arc;

use arrow::array::timezone::Tz as ArrowTz;
use arrow::datatypes::{DataType, TimeUnit};
use chrono::offset::LocalResult;
use chrono::{
    DateTime, Datelike, Days, Duration, Months, NaiveDate, NaiveDateTime, Offset, TimeZone,
    Timelike, Utc,
};
use chrono_tz::Tz;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result as DFResult;
use datafusion::functions::datetime::{
    date_bin::DateBinFunc, date_trunc::DateTruncFunc, to_date::ToDateFunc,
    to_timestamp::ToTimestampFunc, to_unixtime::ToUnixtimeFunc,
};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

use super::segment_pruning::timestamp_scalar;
use super::{ParsedTz, parse_tz};

#[derive(Debug, Clone, Copy)]
pub(crate) struct UnifiedInterval {
    pub(crate) months: i32,
    pub(crate) days: i32,
    pub(crate) nanos: i64,
}

#[derive(Debug, Clone, Copy)]
enum TruncUnit {
    Second,
    Minute,
    Hour,
    Day,
}

#[derive(Debug, Clone, Copy)]
enum BinStride {
    FixedNanos(i64),
    Months(i64),
}

#[derive(Debug, Clone, Copy, Default)]
struct IntervalShape {
    month_terms: usize,
    has_days: bool,
    has_nanos: bool,
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
    apply_interval(datetime, interval, sign)
}

fn apply_interval<Tz: TimeZone>(
    datetime: DateTime<Tz>,
    interval: UnifiedInterval,
    sign: i32,
) -> Option<DateTime<Tz>> {
    let mut result = datetime;
    let months = interval.months.checked_mul(sign)?;
    if months > 0 {
        result = result.checked_add_months(Months::new(months as u32))?;
    } else if months < 0 {
        result = result.checked_sub_months(Months::new(months.unsigned_abs()))?;
    }

    let days = interval.days.checked_mul(sign)?;
    if days > 0 {
        result = result.checked_add_days(Days::new(days as u64))?;
    } else if days < 0 {
        result = result.checked_sub_days(Days::new(u64::from(days.unsigned_abs())))?;
    }

    let nanos = interval.nanos.checked_mul(i64::from(sign))?;
    if nanos != 0 {
        result = result.checked_add_signed(Duration::nanoseconds(nanos))?;
    }

    Some(result)
}

pub(super) fn normalize_timestamp_predicate(
    predicate: Expr,
    index_column: &str,
    index_type: &DataType,
) -> DFResult<Expr> {
    predicate
        .transform_up(|expr| {
            if let Some(normalized) = normalize_comparison(&expr, index_column, index_type) {
                Ok(Transformed::yes(normalized))
            } else {
                Ok(Transformed::no(expr))
            }
        })
        .data()
}

fn normalize_comparison(expr: &Expr, index_column: &str, index_type: &DataType) -> Option<Expr> {
    let Expr::BinaryExpr(comparison) = expr else {
        return None;
    };
    if !is_comparison(comparison.op) {
        return None;
    }

    normalize_interval_comparison(comparison, index_column, index_type)
        .or_else(|| normalize_to_unixtime_comparison(comparison, index_column, index_type))
        .or_else(|| normalize_to_date_comparison(comparison, index_column, index_type))
        .or_else(|| normalize_date_trunc_comparison(comparison, index_column, index_type))
        .or_else(|| normalize_date_bin_comparison(comparison, index_column, index_type))
}

fn normalize_interval_comparison(
    comparison: &BinaryExpr,
    index_column: &str,
    index_type: &DataType,
) -> Option<Expr> {
    let left_shape = interval_shape(&comparison.left);
    if is_interval_arithmetic(&comparison.left)
        && let Some((column, interval)) =
            extract_index_with_interval(&comparison.left, index_column)
        && let Some(datetime) = timestamp_literal(&comparison.right)
    {
        return shifted_comparison(
            column,
            comparison.op,
            datetime,
            interval,
            left_shape,
            index_type,
        );
    }

    if !is_interval_arithmetic(&comparison.right) {
        return None;
    }
    let (column, interval) = extract_index_with_interval(&comparison.right, index_column)?;
    let datetime = timestamp_literal(&comparison.left)?;
    shifted_comparison(
        column,
        flip_comparison(comparison.op)?,
        datetime,
        interval,
        interval_shape(&comparison.right),
        index_type,
    )
}

fn is_interval_arithmetic(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::Plus | Operator::Minus)
    )
}

fn interval_shape(expr: &Expr) -> IntervalShape {
    match expr {
        Expr::Literal(value, _) => {
            interval_from_scalar(value).map_or_else(IntervalShape::default, |interval| {
                IntervalShape {
                    month_terms: usize::from(interval.months != 0),
                    has_days: interval.days != 0,
                    has_nanos: interval.nanos != 0,
                }
            })
        }
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::Plus | Operator::Minus) => {
            let left = interval_shape(&binary.left);
            let right = interval_shape(&binary.right);
            IntervalShape {
                month_terms: left.month_terms.saturating_add(right.month_terms),
                has_days: left.has_days || right.has_days,
                has_nanos: left.has_nanos || right.has_nanos,
            }
        }
        _ => IntervalShape::default(),
    }
}

fn shifted_comparison(
    column: Expr,
    operator: Operator,
    datetime: DateTime<Utc>,
    interval: UnifiedInterval,
    shape: IntervalShape,
    index_type: &DataType,
) -> Option<Expr> {
    let DataType::Timestamp(unit, timezone) = index_type else {
        return None;
    };
    if shape.has_days && shape.has_nanos {
        return None;
    }
    let shifted = apply_interval_in_index_timezone(datetime, interval, -1, timezone.as_deref())?;
    if shape.month_terms != 0
        && (shape.month_terms != 1
            || shape.has_days
            || shape.has_nanos
            || !month_shift_is_safe_to_invert(shifted, datetime, interval, timezone.as_deref()))
    {
        return None;
    }
    let literal = timestamp_scalar(shifted, unit, timezone.clone())?;
    Some(binary(column, operator, Expr::Literal(literal, None)))
}

fn apply_interval_in_index_timezone(
    datetime: DateTime<Utc>,
    interval: UnifiedInterval,
    sign: i32,
    timezone: Option<&str>,
) -> Option<DateTime<Utc>> {
    let timezone = timezone.unwrap_or("+00:00").parse::<ArrowTz>().ok()?;
    apply_interval(datetime.with_timezone(&timezone), interval, sign)
        .map(|value| value.with_timezone(&Utc))
}

fn month_shift_is_safe_to_invert(
    source: DateTime<Utc>,
    target: DateTime<Utc>,
    interval: UnifiedInterval,
    timezone: Option<&str>,
) -> bool {
    let Ok(timezone) = timezone.unwrap_or("+00:00").parse::<ArrowTz>() else {
        return false;
    };
    let source = source.with_timezone(&timezone);
    let target = target.with_timezone(&timezone);
    if !apply_interval(source, interval, 1).is_some_and(|value| value == target) {
        return false;
    }
    last_day_of_month(&target).is_some_and(|last_day| target.day() != last_day)
}

fn last_day_of_month<Tz: TimeZone>(datetime: &DateTime<Tz>) -> Option<u32> {
    datetime
        .date_naive()
        .with_day(1)
        .and_then(|date| date.checked_add_months(Months::new(1)))
        .and_then(|date| date.checked_sub_signed(Duration::days(1)))
        .map(|date| date.day())
}

fn extract_index_with_interval(expr: &Expr, index_column: &str) -> Option<(Expr, UnifiedInterval)> {
    fn walk(expr: &Expr, index_column: &str) -> Option<(Option<Expr>, UnifiedInterval)> {
        if let Expr::Column(column) = expr
            && column.name == index_column
        {
            return Some((Some(expr.clone()), UnifiedInterval::zero()));
        }

        if let Expr::Literal(value, _) = expr {
            return Some((None, interval_from_scalar(value)?));
        }

        let Expr::BinaryExpr(binary) = expr else {
            return None;
        };
        if !matches!(binary.op, Operator::Plus | Operator::Minus) {
            return None;
        }

        let (left_column, left_interval) = walk(&binary.left, index_column)?;
        let (right_column, right_interval) = walk(&binary.right, index_column)?;
        if left_column.is_some() == right_column.is_some() {
            return None;
        }

        if binary.op == Operator::Plus {
            return match (left_column, right_column) {
                (Some(column), None) => {
                    Some((Some(column), left_interval.checked_add(right_interval, 1)?))
                }
                (None, Some(column)) => {
                    Some((Some(column), right_interval.checked_add(left_interval, 1)?))
                }
                _ => None,
            };
        }

        match (left_column, right_column) {
            (Some(column), None) => {
                Some((Some(column), left_interval.checked_add(right_interval, -1)?))
            }
            _ => None,
        }
    }

    let (column, interval) = walk(expr, index_column)?;
    Some((column?, interval))
}

fn timestamp_literal(expr: &Expr) -> Option<DateTime<Utc>> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value {
        ScalarValue::TimestampSecond(Some(value), _) => Utc.timestamp_opt(*value, 0).single(),
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            Utc.timestamp_millis_opt(*value).single()
        }
        ScalarValue::TimestampMicrosecond(Some(value), _) => Utc
            .timestamp_opt(
                value.div_euclid(1_000_000),
                value.rem_euclid(1_000_000) as u32 * 1_000,
            )
            .single(),
        ScalarValue::TimestampNanosecond(Some(value), _) => Utc
            .timestamp_opt(
                value.div_euclid(1_000_000_000),
                value.rem_euclid(1_000_000_000) as u32,
            )
            .single(),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|value| value.with_timezone(&Utc)),
        _ => None,
    }
}

fn normalize_to_unixtime_comparison(
    comparison: &BinaryExpr,
    index_column: &str,
    index_type: &DataType,
) -> Option<Expr> {
    if let Some(column) = to_unixtime_index(&comparison.left, index_column) {
        return unix_second_comparison(
            column,
            comparison.op,
            integer_literal(&comparison.right)?,
            index_type,
        );
    }

    let column = to_unixtime_index(&comparison.right, index_column)?;
    unix_second_comparison(
        column,
        flip_comparison(comparison.op)?,
        integer_literal(&comparison.left)?,
        index_type,
    )
}

fn to_unixtime_index(expr: &Expr, index_column: &str) -> Option<Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if !is_builtin_function::<ToUnixtimeFunc>(function) || function.args.len() != 1 {
        return None;
    }
    let Expr::Column(column) = &function.args[0] else {
        return None;
    };
    (column.name == index_column).then(|| function.args[0].clone())
}

fn integer_literal(expr: &Expr) -> Option<i64> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value {
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value),
        ScalarValue::UInt8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn unix_second_comparison(
    column: Expr,
    operator: Operator,
    second: i64,
    index_type: &DataType,
) -> Option<Expr> {
    let DataType::Timestamp(unit, timezone) = index_type else {
        return None;
    };
    let (lower, upper) = unix_second_bounds(second, unit)?;
    let lower = timestamp_raw_literal(lower, unit, timezone.clone());
    let upper = timestamp_raw_literal(upper, unit, timezone.clone());

    match operator {
        Operator::Eq => Some(binary(
            binary(column.clone(), Operator::GtEq, lower),
            Operator::And,
            binary(column, Operator::LtEq, upper),
        )),
        Operator::NotEq => Some(binary(
            binary(column.clone(), Operator::Lt, lower),
            Operator::Or,
            binary(column, Operator::Gt, upper),
        )),
        Operator::Lt => Some(binary(column, Operator::Lt, lower)),
        Operator::LtEq => Some(binary(column, Operator::LtEq, upper)),
        Operator::Gt => Some(binary(column, Operator::Gt, upper)),
        Operator::GtEq => Some(binary(column, Operator::GtEq, lower)),
        _ => None,
    }
}

fn unix_second_bounds(second: i64, unit: &TimeUnit) -> Option<(i64, i64)> {
    let units_per_second = match unit {
        TimeUnit::Second => return Some((second, second)),
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    };

    // Arrow narrows timestamps with signed integer division, which truncates
    // toward zero. The zero-second bucket therefore spans both sides of epoch.
    if second > 0 {
        Some((
            second.checked_mul(units_per_second)?,
            second
                .checked_add(1)?
                .checked_mul(units_per_second)?
                .checked_sub(1)?,
        ))
    } else if second < 0 {
        Some((
            second
                .checked_sub(1)?
                .checked_mul(units_per_second)?
                .checked_add(1)?,
            second.checked_mul(units_per_second)?,
        ))
    } else {
        let edge = units_per_second - 1;
        Some((-edge, edge))
    }
}

fn timestamp_raw_literal(value: i64, unit: &TimeUnit, timezone: Option<Arc<str>>) -> Expr {
    let value = match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), timezone),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), timezone),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), timezone),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), timezone),
    };
    Expr::Literal(value, None)
}

fn normalize_to_date_comparison(
    comparison: &BinaryExpr,
    index_column: &str,
    index_type: &DataType,
) -> Option<Expr> {
    if let Some(column) = to_date_index(&comparison.left, index_column) {
        return date_comparison(
            column,
            comparison.op,
            date_literal(&comparison.right)?,
            index_type,
        );
    }

    let column = to_date_index(&comparison.right, index_column)?;
    date_comparison(
        column,
        flip_comparison(comparison.op)?,
        date_literal(&comparison.left)?,
        index_type,
    )
}

fn to_date_index(expr: &Expr, index_column: &str) -> Option<Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if !is_builtin_function::<ToDateFunc>(function) || function.args.len() != 1 {
        return None;
    }

    let Expr::Cast(cast) = &function.args[0] else {
        return None;
    };
    if !matches!(
        cast.data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return None;
    }
    let Expr::Column(column) = cast.expr.as_ref() else {
        return None;
    };
    (column.name == index_column).then(|| cast.expr.as_ref().clone())
}

fn date_literal(expr: &Expr) -> Option<NaiveDate> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value {
        ScalarValue::Date32(Some(days)) => NaiveDate::from_ymd_opt(1970, 1, 1)?
            .checked_add_signed(Duration::days(i64::from(*days))),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => NaiveDate::parse_from_str(value, "%Y-%m-%d").ok(),
        _ => None,
    }
}

fn date_comparison(
    column: Expr,
    operator: Operator,
    date: NaiveDate,
    index_type: &DataType,
) -> Option<Expr> {
    let (start, end) = date_bounds(date, index_type)?;
    bucket_comparison(column, operator, start, end, true)
}

fn bucket_comparison(
    column: Expr,
    operator: Operator,
    start: Expr,
    end: Expr,
    aligned: bool,
) -> Option<Expr> {
    match operator {
        Operator::Eq | Operator::NotEq if !aligned => None,
        Operator::Eq => Some(binary(
            binary(column.clone(), Operator::GtEq, start),
            Operator::And,
            binary(column, Operator::Lt, end),
        )),
        Operator::NotEq => Some(binary(
            binary(column.clone(), Operator::Lt, start),
            Operator::Or,
            binary(column, Operator::GtEq, end),
        )),
        Operator::Lt => Some(binary(
            column,
            Operator::Lt,
            if aligned { start } else { end },
        )),
        Operator::LtEq => Some(binary(column, Operator::Lt, end)),
        Operator::Gt => Some(binary(column, Operator::GtEq, end)),
        Operator::GtEq => Some(binary(
            column,
            Operator::GtEq,
            if aligned { start } else { end },
        )),
        _ => None,
    }
}

fn date_bounds(date: NaiveDate, index_type: &DataType) -> Option<(Expr, Expr)> {
    let DataType::Timestamp(unit, Some(timezone)) = index_type else {
        return None;
    };
    let timezone_name = timezone.clone();
    let timezone = parse_tz(timezone)?;
    let start = date.and_hms_opt(0, 0, 0)?;
    let end = date.succ_opt()?.and_hms_opt(0, 0, 0)?;
    let (start, end) = match timezone {
        ParsedTz::Utc => (Utc.from_utc_datetime(&start), Utc.from_utc_datetime(&end)),
        ParsedTz::Fixed(offset) => (
            offset
                .from_local_datetime(&start)
                .single()?
                .with_timezone(&Utc),
            offset
                .from_local_datetime(&end)
                .single()?
                .with_timezone(&Utc),
        ),
        ParsedTz::Olson(timezone) => (
            timezone
                .from_local_datetime(&start)
                .single()?
                .with_timezone(&Utc),
            timezone
                .from_local_datetime(&end)
                .single()?
                .with_timezone(&Utc),
        ),
    };

    Some((
        Expr::Literal(
            timestamp_scalar(start, unit, Some(timezone_name.clone()))?,
            None,
        ),
        Expr::Literal(timestamp_scalar(end, unit, Some(timezone_name))?, None),
    ))
}

fn normalize_date_trunc_comparison(
    comparison: &BinaryExpr,
    index_column: &str,
    index_type: &DataType,
) -> Option<Expr> {
    if let Some((column, unit)) = date_trunc_index(&comparison.left, index_column, index_type) {
        return date_trunc_comparison(
            column,
            unit,
            comparison.op,
            timestamp_literal(&comparison.right)?,
            index_type,
        );
    }

    let (column, unit) = date_trunc_index(&comparison.right, index_column, index_type)?;
    date_trunc_comparison(
        column,
        unit,
        flip_comparison(comparison.op)?,
        timestamp_literal(&comparison.left)?,
        index_type,
    )
}

fn date_trunc_index(
    expr: &Expr,
    index_column: &str,
    index_type: &DataType,
) -> Option<(Expr, TruncUnit)> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if !is_builtin_function::<DateTruncFunc>(function) || function.args.len() != 2 {
        return None;
    }
    let unit = match &function.args[0] {
        Expr::Literal(ScalarValue::Utf8(Some(unit)) | ScalarValue::Utf8View(Some(unit)), _) => {
            match unit.to_ascii_lowercase().as_str() {
                "second" => TruncUnit::Second,
                "minute" => TruncUnit::Minute,
                "hour" => TruncUnit::Hour,
                "day" => TruncUnit::Day,
                _ => return None,
            }
        }
        _ => return None,
    };
    Some((
        timestamp_index_column(&function.args[1], index_column, index_type)?,
        unit,
    ))
}

fn timestamp_index_column(expr: &Expr, index_column: &str, index_type: &DataType) -> Option<Expr> {
    if let Expr::Column(column) = expr {
        return (column.name == index_column).then(|| expr.clone());
    }

    let Expr::Cast(cast) = expr else {
        return None;
    };
    let Expr::Column(column) = cast.expr.as_ref() else {
        return None;
    };
    let DataType::Timestamp(index_unit, index_timezone) = index_type else {
        return None;
    };
    let DataType::Timestamp(cast_unit, cast_timezone) = &cast.data_type else {
        return None;
    };
    (column.name == index_column
        && index_timezone == cast_timezone
        && timestamp_unit_rank(cast_unit) >= timestamp_unit_rank(index_unit))
    .then(|| cast.expr.as_ref().clone())
}

fn timestamp_unit_rank(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

fn date_trunc_comparison(
    column: Expr,
    unit: TruncUnit,
    operator: Operator,
    literal: DateTime<Utc>,
    index_type: &DataType,
) -> Option<Expr> {
    let DataType::Timestamp(timestamp_unit, timezone) = index_type else {
        return None;
    };
    timestamp_scalar(literal, timestamp_unit, timezone.clone())?;
    let (start, end) = truncation_bucket(literal, unit, timezone.as_deref())?;
    let aligned = start == literal;
    let start = Expr::Literal(
        timestamp_scalar(start, timestamp_unit, timezone.clone())?,
        None,
    );
    let end = Expr::Literal(
        timestamp_scalar(end, timestamp_unit, timezone.clone())?,
        None,
    );
    bucket_comparison(column, operator, start, end, aligned)
}

fn truncation_bucket(
    literal: DateTime<Utc>,
    unit: TruncUnit,
    timezone: Option<&str>,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let Some(timezone) = timezone else {
        return utc_truncation_bucket(literal, unit);
    };
    match parse_tz(timezone)? {
        ParsedTz::Utc => utc_truncation_bucket(literal, unit),
        ParsedTz::Olson(_) if matches!(unit, TruncUnit::Second | TruncUnit::Minute) => {
            utc_truncation_bucket(literal, unit)
        }
        ParsedTz::Olson(timezone) => local_truncation_bucket(literal, unit, timezone),
        ParsedTz::Fixed(_) => None,
    }
}

fn utc_truncation_bucket(
    literal: DateTime<Utc>,
    unit: TruncUnit,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let seconds = match unit {
        TruncUnit::Second => 1,
        TruncUnit::Minute => 60,
        TruncUnit::Hour => 3_600,
        TruncUnit::Day => 86_400,
    };
    let floor_seconds = literal
        .timestamp()
        .div_euclid(seconds)
        .checked_mul(seconds)?;
    let start = Utc.timestamp_opt(floor_seconds, 0).single()?;
    let end = start.checked_add_signed(Duration::seconds(seconds))?;
    Some((start, end))
}

fn local_truncation_bucket(
    literal: DateTime<Utc>,
    unit: TruncUnit,
    timezone: Tz,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let local = literal.with_timezone(&timezone);
    let floor_local = truncate_local(local.naive_local(), unit)?;
    // Match DataFusion 51: an ambiguous floor keeps the input's UTC offset.
    // The second occurrence of that local boundary ends the first bucket.
    let (start, repeated_end) = match floor_local.and_local_timezone(timezone) {
        LocalResult::Single(start) => (start, None),
        LocalResult::Ambiguous(first, second) => {
            let start = if first.offset().fix() == local.offset().fix() {
                first
            } else {
                second
            };
            let (earlier, later) = if first <= second {
                (first, second)
            } else {
                (second, first)
            };
            (start, (start == earlier).then_some(later))
        }
        LocalResult::None => (resolve_nonexistent(timezone, floor_local)?, None),
    };

    let end = if let Some(end) = repeated_end {
        end
    } else {
        resolve_boundary(timezone, next_local_boundary(floor_local, unit)?)?
    };
    Some((start.with_timezone(&Utc), end.with_timezone(&Utc)))
}

fn truncate_local(value: NaiveDateTime, unit: TruncUnit) -> Option<NaiveDateTime> {
    match unit {
        TruncUnit::Second => value.with_nanosecond(0),
        TruncUnit::Minute => value.with_nanosecond(0)?.with_second(0),
        TruncUnit::Hour => value.with_nanosecond(0)?.with_second(0)?.with_minute(0),
        TruncUnit::Day => value
            .with_nanosecond(0)?
            .with_second(0)?
            .with_minute(0)?
            .with_hour(0),
    }
}

fn next_local_boundary(value: NaiveDateTime, unit: TruncUnit) -> Option<NaiveDateTime> {
    match unit {
        TruncUnit::Second => value.checked_add_signed(Duration::seconds(1)),
        TruncUnit::Minute => value.checked_add_signed(Duration::minutes(1)),
        TruncUnit::Hour => value.checked_add_signed(Duration::hours(1)),
        TruncUnit::Day => value.date().succ_opt()?.and_hms_opt(0, 0, 0),
    }
}

fn resolve_boundary(timezone: Tz, value: NaiveDateTime) -> Option<DateTime<Tz>> {
    match value.and_local_timezone(timezone) {
        LocalResult::Single(value) => Some(value),
        LocalResult::Ambiguous(first, second) => Some(first.min(second)),
        LocalResult::None => resolve_nonexistent(timezone, value),
    }
}

fn resolve_nonexistent(timezone: Tz, value: NaiveDateTime) -> Option<DateTime<Tz>> {
    // DataFusion resolves DST gaps by moving to a valid time and back.
    let adjustment = Duration::hours(3);
    value
        .checked_sub_signed(adjustment)?
        .and_local_timezone(timezone)
        .single()?
        .checked_add_signed(adjustment)
}

fn normalize_date_bin_comparison(
    comparison: &BinaryExpr,
    index_column: &str,
    index_type: &DataType,
) -> Option<Expr> {
    if let Some((column, stride, origin)) =
        date_bin_index(&comparison.left, index_column, index_type)
    {
        return date_bin_comparison(
            column,
            stride,
            origin,
            comparison.op,
            timestamp_literal(&comparison.right)?,
            index_type,
        );
    }

    let (column, stride, origin) = date_bin_index(&comparison.right, index_column, index_type)?;
    date_bin_comparison(
        column,
        stride,
        origin,
        flip_comparison(comparison.op)?,
        timestamp_literal(&comparison.left)?,
        index_type,
    )
}

fn date_bin_index(
    expr: &Expr,
    index_column: &str,
    index_type: &DataType,
) -> Option<(Expr, BinStride, DateTime<Utc>)> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if !is_builtin_function::<DateBinFunc>(function) || !matches!(function.args.len(), 2 | 3) {
        return None;
    }
    let stride = date_bin_stride(&function.args[0])?;
    let column = timestamp_index_column(&function.args[1], index_column, index_type)?;
    let origin = if function.args.len() == 3 {
        date_bin_origin(&function.args[2])?
    } else {
        Utc.timestamp_opt(0, 0).single()?
    };
    Some((column, stride, origin))
}

fn date_bin_stride(expr: &Expr) -> Option<BinStride> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    let interval = match value {
        ScalarValue::IntervalDayTime(Some(_)) | ScalarValue::IntervalMonthDayNano(Some(_)) => {
            interval_from_scalar(value)?
        }
        _ => return None,
    };

    if interval.months != 0 {
        if interval.months < 0 || interval.days != 0 || interval.nanos != 0 {
            return None;
        }
        return Some(BinStride::Months(i64::from(interval.months)));
    }

    let nanos = i64::from(interval.days)
        .checked_mul(86_400_000_000_000)?
        .checked_add(interval.nanos)?;
    (nanos > 0).then_some(BinStride::FixedNanos(nanos))
}

fn date_bin_origin(expr: &Expr) -> Option<DateTime<Utc>> {
    if let Some(origin) = timestamp_literal(expr) {
        return Some(origin);
    }
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if !is_builtin_function::<ToTimestampFunc>(function) || function.args.len() != 1 {
        return None;
    }
    timestamp_literal(&function.args[0])
}

fn date_bin_comparison(
    column: Expr,
    stride: BinStride,
    origin: DateTime<Utc>,
    operator: Operator,
    literal: DateTime<Utc>,
    index_type: &DataType,
) -> Option<Expr> {
    let DataType::Timestamp(unit, timezone) = index_type else {
        return None;
    };
    timestamp_scalar(literal, unit, timezone.clone())?;
    let (start, end) = date_bin_bucket(literal, stride, origin)?;
    let aligned = start == literal;
    let start = Expr::Literal(timestamp_scalar(start, unit, timezone.clone())?, None);
    let end = Expr::Literal(timestamp_scalar(end, unit, timezone.clone())?, None);
    bucket_comparison(column, operator, start, end, aligned)
}

fn date_bin_bucket(
    literal: DateTime<Utc>,
    stride: BinStride,
    origin: DateTime<Utc>,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    match stride {
        BinStride::FixedNanos(stride) => {
            let literal = literal.timestamp_nanos_opt()?;
            let origin = origin.timestamp_nanos_opt()?;
            let distance = literal.checked_sub(origin)?.div_euclid(stride);
            let start = origin.checked_add(distance.checked_mul(stride)?)?;
            let end = start.checked_add(stride)?;
            Some((datetime_from_nanos(start)?, datetime_from_nanos(end)?))
        }
        BinStride::Months(stride) => month_bin_bucket(literal, origin, stride),
    }
}

fn month_bin_bucket(
    literal: DateTime<Utc>,
    origin: DateTime<Utc>,
    stride: i64,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let year_months = i64::from(literal.year())
        .checked_sub(i64::from(origin.year()))?
        .checked_mul(12)?;
    let month_diff = year_months
        .checked_add(i64::from(literal.month()))?
        .checked_sub(i64::from(origin.month()))?;
    let mut month_delta = month_diff.div_euclid(stride).checked_mul(stride)?;
    let mut start = add_months(origin, month_delta)?;
    if start > literal {
        month_delta = month_delta.checked_sub(stride)?;
        start = add_months(origin, month_delta)?;
    }
    let end = add_months(origin, month_delta.checked_add(stride)?)?;
    Some((start, end))
}

fn add_months(value: DateTime<Utc>, months: i64) -> Option<DateTime<Utc>> {
    if months < 0 {
        value.checked_sub_months(Months::new(u32::try_from(months.unsigned_abs()).ok()?))
    } else {
        value.checked_add_months(Months::new(u32::try_from(months).ok()?))
    }
}

fn datetime_from_nanos(value: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_opt(
        value.div_euclid(1_000_000_000),
        value.rem_euclid(1_000_000_000) as u32,
    )
    .single()
}

fn is_comparison(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
}

fn flip_comparison(operator: Operator) -> Option<Operator> {
    match operator {
        Operator::Eq | Operator::NotEq => Some(operator),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        _ => None,
    }
}

fn binary(left: Expr, operator: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), operator, Box::new(right)))
}

fn is_builtin_function<T: 'static>(function: &ScalarFunction) -> bool {
    function.func.inner().as_any().is::<T>()
}

#[cfg(test)]
mod tests {
    use arrow::array::types::IntervalMonthDayNano;
    use chrono::TimeZone;
    use datafusion::common::Column;
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::logical_expr::expr::Cast;

    use super::*;

    fn column(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    fn timestamp_millis(value: i64, timezone: Option<Arc<str>>) -> Expr {
        Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(value), timezone),
            None,
        )
    }

    fn interval(months: i32, days: i32, nanoseconds: i64) -> Expr {
        Expr::Literal(
            ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            })),
            None,
        )
    }

    fn timestamp_type(timezone: Option<Arc<str>>) -> DataType {
        DataType::Timestamp(TimeUnit::Millisecond, timezone)
    }

    fn scalar_function(udf: Arc<ScalarUDF>, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(udf, args))
    }

    fn to_unixtime(args: Vec<Expr>) -> Expr {
        scalar_function(datafusion::functions::datetime::to_unixtime(), args)
    }

    fn to_date(args: Vec<Expr>) -> Expr {
        scalar_function(datafusion::functions::datetime::to_date(), args)
    }

    fn date_trunc(unit: Expr, index: Expr) -> Expr {
        scalar_function(
            datafusion::functions::datetime::date_trunc(),
            vec![unit, index],
        )
    }

    fn date_bin(stride: Expr, index: Expr, origin: Option<Expr>) -> Expr {
        let mut args = vec![stride, index];
        args.extend(origin);
        scalar_function(datafusion::functions::datetime::date_bin(), args)
    }

    fn to_timestamp(value: Expr) -> Expr {
        scalar_function(datafusion::functions::datetime::to_timestamp(), vec![value])
    }

    fn string_cast(expr: Expr) -> Expr {
        Expr::Cast(Cast {
            expr: Box::new(expr),
            data_type: DataType::Utf8View,
        })
    }

    fn string_literal(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
    }

    fn timestamp_range(start: &str, end: &str, timezone: Arc<str>) -> Expr {
        let millis = |value: &str| {
            DateTime::parse_from_rfc3339(value)
                .unwrap()
                .timestamp_millis()
        };
        binary(
            binary(
                column("ts"),
                Operator::GtEq,
                timestamp_millis(millis(start), Some(timezone.clone())),
            ),
            Operator::And,
            binary(
                column("ts"),
                Operator::Lt,
                timestamp_millis(millis(end), Some(timezone)),
            ),
        )
    }

    #[test]
    fn normalizes_interval_arithmetic_and_reversed_comparisons() {
        let cases = [
            (
                binary(
                    binary(column("ts"), Operator::Plus, interval(0, 0, 60_000_000_000)),
                    Operator::Eq,
                    timestamp_millis(120_000, None),
                ),
                binary(column("ts"), Operator::Eq, timestamp_millis(60_000, None)),
            ),
            (
                binary(
                    timestamp_millis(120_000, None),
                    Operator::LtEq,
                    binary(column("ts"), Operator::Plus, interval(0, 0, 60_000_000_000)),
                ),
                binary(column("ts"), Operator::GtEq, timestamp_millis(60_000, None)),
            ),
            (
                binary(
                    binary(column("ts"), Operator::Plus, interval(1, 0, 0)),
                    Operator::Eq,
                    timestamp_millis(2_678_460_000, None),
                ),
                binary(column("ts"), Operator::Eq, timestamp_millis(60_000, None)),
            ),
        ];

        for (predicate, expected) in cases {
            assert_eq!(
                normalize_timestamp_predicate(predicate, "ts", &timestamp_type(None)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn leaves_ambiguous_or_composed_month_arithmetic_unchanged() {
        let timestamp = |value: &str| {
            timestamp_millis(
                DateTime::parse_from_rfc3339(value)
                    .unwrap()
                    .timestamp_millis(),
                None,
            )
        };
        let predicates = [
            binary(
                binary(column("ts"), Operator::Plus, interval(1, 0, 0)),
                Operator::Eq,
                timestamp("2024-02-29T00:00:00Z"),
            ),
            binary(
                binary(
                    binary(column("ts"), Operator::Plus, interval(1, 0, 0)),
                    Operator::Plus,
                    interval(1, 0, 0),
                ),
                Operator::Eq,
                timestamp("2024-03-29T00:00:00Z"),
            ),
            binary(
                binary(column("ts"), Operator::Plus, interval(1, 1, 0)),
                Operator::Eq,
                timestamp("2024-03-29T00:00:00Z"),
            ),
            binary(
                binary(
                    binary(column("ts"), Operator::Plus, interval(1, 0, 0)),
                    Operator::Minus,
                    interval(1, 0, 0),
                ),
                Operator::Eq,
                timestamp("2024-01-31T00:00:00Z"),
            ),
        ];

        for predicate in predicates {
            assert_eq!(
                normalize_timestamp_predicate(predicate.clone(), "ts", &timestamp_type(None))
                    .unwrap(),
                predicate
            );
        }
    }

    #[test]
    fn leaves_mixed_calendar_day_and_fixed_duration_arithmetic_unchanged() {
        let timezone: Arc<str> = "America/New_York".into();
        let target = || {
            timestamp_millis(
                DateTime::parse_from_rfc3339("2024-03-10T03:30:00-04:00")
                    .unwrap()
                    .timestamp_millis(),
                Some(timezone.clone()),
            )
        };
        let predicates = [
            binary(
                binary(
                    column("ts"),
                    Operator::Plus,
                    interval(0, 1, 3_600_000_000_000),
                ),
                Operator::Eq,
                target(),
            ),
            binary(
                binary(
                    binary(column("ts"), Operator::Plus, interval(0, 1, 0)),
                    Operator::Plus,
                    interval(0, 0, 3_600_000_000_000),
                ),
                Operator::Eq,
                target(),
            ),
            binary(
                binary(
                    binary(
                        binary(column("ts"), Operator::Plus, interval(0, 1, 0)),
                        Operator::Plus,
                        interval(0, 0, 3_600_000_000_000),
                    ),
                    Operator::Minus,
                    interval(0, 1, 0),
                ),
                Operator::Eq,
                target(),
            ),
        ];
        let index_type = timestamp_type(Some(timezone));

        for predicate in predicates {
            assert_eq!(
                normalize_timestamp_predicate(predicate.clone(), "ts", &index_type).unwrap(),
                predicate
            );
        }
    }

    #[test]
    fn preserves_boolean_structure_around_exact_rewrites() {
        let predicate = Expr::Not(Box::new(binary(
            binary(column("ts"), Operator::Plus, interval(0, 0, 1_000_000)),
            Operator::Lt,
            timestamp_millis(2, None),
        )));
        let expected = Expr::Not(Box::new(binary(
            column("ts"),
            Operator::Lt,
            timestamp_millis(1, None),
        )));

        assert_eq!(
            normalize_timestamp_predicate(predicate, "ts", &timestamp_type(None)).unwrap(),
            expected
        );
    }

    #[test]
    fn leaves_unsupported_and_overflowing_arithmetic_unchanged() {
        let direct = binary(column("ts"), Operator::Lt, timestamp_millis(1_000, None));
        let unsupported = binary(
            binary(column("ts"), Operator::Minus, column("ts")),
            Operator::Lt,
            interval(0, 0, 1_000_000_000),
        );
        let overflow = binary(
            binary(
                column("ts"),
                Operator::Plus,
                Expr::Literal(ScalarValue::IntervalYearMonth(Some(i32::MIN)), None),
            ),
            Operator::Eq,
            timestamp_millis(0, None),
        );

        for predicate in [direct, unsupported, overflow] {
            assert_eq!(
                normalize_timestamp_predicate(predicate.clone(), "ts", &timestamp_type(None),)
                    .unwrap(),
                predicate
            );
        }
    }

    #[test]
    fn normalizes_to_unixtime_with_exact_truncation_boundaries() {
        let cases = [
            (
                binary(
                    to_unixtime(vec![column("ts")]),
                    Operator::Eq,
                    Expr::Literal(ScalarValue::Int64(Some(60)), None),
                ),
                binary(
                    binary(column("ts"), Operator::GtEq, timestamp_millis(60_000, None)),
                    Operator::And,
                    binary(column("ts"), Operator::LtEq, timestamp_millis(60_999, None)),
                ),
            ),
            (
                binary(
                    to_unixtime(vec![column("ts")]),
                    Operator::Eq,
                    Expr::Literal(ScalarValue::Int64(Some(0)), None),
                ),
                binary(
                    binary(column("ts"), Operator::GtEq, timestamp_millis(-999, None)),
                    Operator::And,
                    binary(column("ts"), Operator::LtEq, timestamp_millis(999, None)),
                ),
            ),
            (
                binary(
                    to_unixtime(vec![column("ts")]),
                    Operator::NotEq,
                    Expr::Literal(ScalarValue::Int64(Some(-1)), None),
                ),
                binary(
                    binary(column("ts"), Operator::Lt, timestamp_millis(-1_999, None)),
                    Operator::Or,
                    binary(column("ts"), Operator::Gt, timestamp_millis(-1_000, None)),
                ),
            ),
            (
                binary(
                    Expr::Literal(ScalarValue::Int64(Some(60)), None),
                    Operator::Lt,
                    to_unixtime(vec![column("ts")]),
                ),
                binary(column("ts"), Operator::Gt, timestamp_millis(60_999, None)),
            ),
        ];

        for (predicate, expected) in cases {
            assert_eq!(
                normalize_timestamp_predicate(predicate, "ts", &timestamp_type(None)).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn unix_second_bounds_match_arrow_truncation_for_every_timestamp_unit() {
        let units = [
            (TimeUnit::Second, 1),
            (TimeUnit::Millisecond, 1_000),
            (TimeUnit::Microsecond, 1_000_000),
            (TimeUnit::Nanosecond, 1_000_000_000),
        ];

        for (unit, units_per_second) in units {
            for second in -3..=3 {
                let (lower, upper) = unix_second_bounds(second, &unit).unwrap();
                for raw in [lower - 1, lower, upper, upper + 1] {
                    assert_eq!(
                        raw / units_per_second == second,
                        (lower..=upper).contains(&raw)
                    );
                }
            }
        }
    }

    #[test]
    fn leaves_unsupported_to_unixtime_forms_unchanged() {
        let predicates = [
            binary(
                to_unixtime(vec![column("ts"), column("other")]),
                Operator::Lt,
                Expr::Literal(ScalarValue::Int64(Some(60)), None),
            ),
            binary(
                to_unixtime(vec![column("other")]),
                Operator::Lt,
                Expr::Literal(ScalarValue::Int64(Some(60)), None),
            ),
            binary(
                to_unixtime(vec![column("ts")]),
                Operator::Lt,
                Expr::Literal(ScalarValue::Float64(Some(60.0)), None),
            ),
            binary(
                to_unixtime(vec![column("ts")]),
                Operator::Lt,
                Expr::Literal(ScalarValue::Utf8(Some("60".to_string())), None),
            ),
            binary(
                to_unixtime(vec![column("ts")]),
                Operator::Lt,
                Expr::Literal(ScalarValue::Int64(Some(i64::MAX)), None),
            ),
        ];

        for predicate in predicates {
            assert_eq!(
                normalize_timestamp_predicate(predicate.clone(), "ts", &timestamp_type(None))
                    .unwrap(),
                predicate
            );
        }
    }

    #[test]
    fn normalizes_to_date_ranges_and_reversed_comparisons() {
        let utc: Arc<str> = "UTC".into();
        let equality = binary(
            to_date(vec![string_cast(column("ts"))]),
            Operator::Eq,
            Expr::Literal(ScalarValue::Utf8(Some("2024-01-08".to_string())), None),
        );
        let expected = binary(
            binary(
                column("ts"),
                Operator::GtEq,
                timestamp_millis(1_704_672_000_000, Some(utc.clone())),
            ),
            Operator::And,
            binary(
                column("ts"),
                Operator::Lt,
                timestamp_millis(1_704_758_400_000, Some(utc.clone())),
            ),
        );
        assert_eq!(
            normalize_timestamp_predicate(equality, "ts", &timestamp_type(Some(utc))).unwrap(),
            expected
        );

        let new_york: Arc<str> = "America/New_York".into();
        let reversed = binary(
            Expr::Literal(ScalarValue::Utf8(Some("2024-03-10".to_string())), None),
            Operator::Lt,
            to_date(vec![string_cast(column("ts"))]),
        );
        let expected = binary(
            column("ts"),
            Operator::GtEq,
            timestamp_millis(1_710_129_600_000, Some(new_york.clone())),
        );
        assert_eq!(
            normalize_timestamp_predicate(reversed, "ts", &timestamp_type(Some(new_york)),)
                .unwrap(),
            expected
        );
    }

    #[test]
    fn leaves_unsupported_to_date_forms_unchanged() {
        let date = || Expr::Literal(ScalarValue::Utf8(Some("2024-01-08".to_string())), None);
        let predicates = [
            binary(to_date(vec![column("ts")]), Operator::Eq, date()),
            binary(
                to_date(vec![string_cast(column("ts")), date()]),
                Operator::Eq,
                date(),
            ),
            binary(
                to_date(vec![string_cast(column("other"))]),
                Operator::Eq,
                date(),
            ),
            binary(
                to_date(vec![string_cast(column("ts"))]),
                Operator::Eq,
                Expr::Literal(ScalarValue::Utf8(Some("not-a-date".to_string())), None),
            ),
        ];
        let index_type = timestamp_type(Some("UTC".into()));

        for predicate in predicates {
            assert_eq!(
                normalize_timestamp_predicate(predicate.clone(), "ts", &index_type).unwrap(),
                predicate
            );
        }

        let missing_timezone = binary(
            to_date(vec![string_cast(column("ts"))]),
            Operator::Eq,
            date(),
        );
        assert_eq!(
            normalize_timestamp_predicate(missing_timezone.clone(), "ts", &timestamp_type(None),)
                .unwrap(),
            missing_timezone
        );
    }

    #[test]
    fn normalizes_date_trunc_buckets_across_epoch_and_dst_boundaries() {
        let cases = [
            (
                "minute",
                "1970-01-01T00:01:00Z",
                "1970-01-01T00:01:00Z",
                "1970-01-01T00:02:00Z",
                "UTC",
            ),
            (
                "hour",
                "1969-12-31T23:00:00Z",
                "1969-12-31T23:00:00Z",
                "1970-01-01T00:00:00Z",
                "UTC",
            ),
            (
                "day",
                "2024-03-10T00:00:00-05:00",
                "2024-03-10T05:00:00Z",
                "2024-03-11T04:00:00Z",
                "America/New_York",
            ),
            (
                "hour",
                "2024-03-10T01:00:00-05:00",
                "2024-03-10T06:00:00Z",
                "2024-03-10T07:00:00Z",
                "America/New_York",
            ),
            (
                "hour",
                "2024-11-03T01:00:00-04:00",
                "2024-11-03T05:00:00Z",
                "2024-11-03T06:00:00Z",
                "America/New_York",
            ),
            (
                "hour",
                "2024-11-03T01:00:00-05:00",
                "2024-11-03T06:00:00Z",
                "2024-11-03T07:00:00Z",
                "America/New_York",
            ),
        ];

        for (unit, literal, start, end, timezone) in cases {
            let timezone: Arc<str> = timezone.into();
            let predicate = binary(
                date_trunc(string_literal(unit), column("ts")),
                Operator::Eq,
                string_literal(literal),
            );
            assert_eq!(
                normalize_timestamp_predicate(
                    predicate,
                    "ts",
                    &timestamp_type(Some(timezone.clone())),
                )
                .unwrap(),
                timestamp_range(start, end, timezone),
                "wrong bucket for {unit} at {literal}"
            );
        }
    }

    #[test]
    fn normalizes_date_trunc_operand_reversal() {
        let timezone: Arc<str> = "UTC".into();
        let reversed = binary(
            string_literal("1970-01-01T00:01:00Z"),
            Operator::Lt,
            date_trunc(string_literal("minute"), column("ts")),
        );
        assert_eq!(
            normalize_timestamp_predicate(reversed, "ts", &timestamp_type(Some(timezone.clone())),)
                .unwrap(),
            binary(
                column("ts"),
                Operator::GtEq,
                timestamp_millis(120_000, Some(timezone)),
            )
        );
    }

    #[test]
    fn leaves_unsupported_date_trunc_forms_unchanged() {
        let predicates = [
            binary(
                scalar_function(
                    datafusion::functions::datetime::date_trunc(),
                    vec![string_literal("minute")],
                ),
                Operator::Eq,
                string_literal("1970-01-01T00:01:00Z"),
            ),
            binary(
                date_trunc(column("unit"), column("ts")),
                Operator::Eq,
                string_literal("1970-01-01T00:01:00Z"),
            ),
            binary(
                date_trunc(string_literal("week"), column("ts")),
                Operator::Eq,
                string_literal("1970-01-01T00:00:00Z"),
            ),
            binary(
                date_trunc(string_literal("minute"), column("other")),
                Operator::Eq,
                string_literal("1970-01-01T00:01:00Z"),
            ),
            binary(
                date_trunc(string_literal("minute"), column("ts")),
                Operator::Eq,
                string_literal("not-a-timestamp"),
            ),
        ];
        let index_type = timestamp_type(Some("UTC".into()));

        for predicate in predicates {
            assert_eq!(
                normalize_timestamp_predicate(predicate.clone(), "ts", &index_type).unwrap(),
                predicate
            );
        }

        let timezone: Arc<str> = "UTC".into();
        let overflow = binary(
            date_trunc(string_literal("hour"), column("ts")),
            Operator::Eq,
            Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(i64::MAX), Some(timezone.clone())),
                None,
            ),
        );
        let nanosecond_type = DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone));
        assert_eq!(
            normalize_timestamp_predicate(overflow.clone(), "ts", &nanosecond_type).unwrap(),
            overflow
        );

        let fixed_offset = binary(
            date_trunc(string_literal("hour"), column("ts")),
            Operator::Eq,
            string_literal("1970-01-01T00:00:00+07:00"),
        );
        assert_eq!(
            normalize_timestamp_predicate(
                fixed_offset.clone(),
                "ts",
                &timestamp_type(Some("+07:00".into())),
            )
            .unwrap(),
            fixed_offset
        );
    }

    #[test]
    fn normalizes_fixed_date_bin_buckets_with_origins_and_timezones() {
        let cases = [
            (
                interval(0, 0, 60_000_000_000),
                None,
                "1970-01-01T00:01:00Z",
                "1970-01-01T00:01:00Z",
                "1970-01-01T00:02:00Z",
                "UTC",
            ),
            (
                interval(0, 0, 60_000_000_000),
                None,
                "1969-12-31T23:59:00Z",
                "1969-12-31T23:59:00Z",
                "1970-01-01T00:00:00Z",
                "UTC",
            ),
            (
                interval(0, 0, 60_000_000_000),
                Some(to_timestamp(string_literal("1970-01-01T00:00:30Z"))),
                "1970-01-01T00:01:30Z",
                "1970-01-01T00:01:30Z",
                "1970-01-01T00:02:30Z",
                "UTC",
            ),
            (
                interval(0, 0, 7_200_000_000_000),
                None,
                "2024-03-10T01:00:00-05:00",
                "2024-03-10T06:00:00Z",
                "2024-03-10T08:00:00Z",
                "America/New_York",
            ),
        ];

        for (stride, origin, literal, start, end, timezone) in cases {
            let timezone: Arc<str> = timezone.into();
            let predicate = binary(
                date_bin(stride, column("ts"), origin),
                Operator::Eq,
                string_literal(literal),
            );
            assert_eq!(
                normalize_timestamp_predicate(
                    predicate,
                    "ts",
                    &timestamp_type(Some(timezone.clone())),
                )
                .unwrap(),
                timestamp_range(start, end, timezone),
                "wrong date_bin bucket at {literal}"
            );
        }
    }

    #[test]
    fn normalizes_month_date_bin_from_the_original_anchor() {
        let cases = [
            (
                3,
                "2020-01-01T00:00:00Z",
                "2020-04-01T00:00:00Z",
                "2020-04-01T00:00:00Z",
                "2020-07-01T00:00:00Z",
            ),
            (
                1,
                "2024-01-31T00:00:00Z",
                "2024-02-29T00:00:00Z",
                "2024-02-29T00:00:00Z",
                "2024-03-31T00:00:00Z",
            ),
        ];

        for (months, origin, literal, start, end) in cases {
            let timezone: Arc<str> = "UTC".into();
            let predicate = binary(
                date_bin(
                    interval(months, 0, 0),
                    column("ts"),
                    Some(to_timestamp(string_literal(origin))),
                ),
                Operator::Eq,
                string_literal(literal),
            );
            assert_eq!(
                normalize_timestamp_predicate(
                    predicate,
                    "ts",
                    &timestamp_type(Some(timezone.clone())),
                )
                .unwrap(),
                timestamp_range(start, end, timezone),
                "wrong month bucket from {origin}"
            );
        }
    }

    #[test]
    fn normalizes_date_bin_operand_reversal() {
        let timezone: Arc<str> = "UTC".into();
        let reversed = binary(
            string_literal("1970-01-01T00:01:00Z"),
            Operator::Lt,
            date_bin(interval(0, 0, 60_000_000_000), column("ts"), None),
        );
        assert_eq!(
            normalize_timestamp_predicate(reversed, "ts", &timestamp_type(Some(timezone.clone())),)
                .unwrap(),
            binary(
                column("ts"),
                Operator::GtEq,
                timestamp_millis(120_000, Some(timezone)),
            )
        );
    }

    #[test]
    fn leaves_non_aligned_bucket_comparisons_unchanged() {
        let predicates = [
            Expr::IsNull(Box::new(binary(
                date_trunc(string_literal("minute"), column("ts")),
                Operator::Eq,
                string_literal("1970-01-01T00:01:30Z"),
            ))),
            Expr::IsNull(Box::new(binary(
                date_bin(interval(0, 0, 60_000_000_000), column("ts"), None),
                Operator::NotEq,
                string_literal("1970-01-01T00:01:30Z"),
            ))),
        ];
        let index_type = timestamp_type(Some("UTC".into()));

        for predicate in predicates {
            assert_eq!(
                normalize_timestamp_predicate(predicate.clone(), "ts", &index_type).unwrap(),
                predicate
            );
        }
    }

    #[test]
    fn leaves_unsupported_date_bin_forms_unchanged() {
        let minute = || interval(0, 0, 60_000_000_000);
        let literal = || string_literal("1970-01-01T00:01:00Z");
        let predicates = [
            binary(
                scalar_function(datafusion::functions::datetime::date_bin(), vec![minute()]),
                Operator::Eq,
                literal(),
            ),
            binary(
                date_bin(column("stride"), column("ts"), None),
                Operator::Eq,
                literal(),
            ),
            binary(
                date_bin(interval(0, 0, 0), column("ts"), None),
                Operator::Eq,
                literal(),
            ),
            binary(
                date_bin(interval(0, 0, -1), column("ts"), None),
                Operator::Eq,
                literal(),
            ),
            binary(
                date_bin(interval(1, 1, 0), column("ts"), None),
                Operator::Eq,
                literal(),
            ),
            binary(
                date_bin(minute(), column("other"), None),
                Operator::Eq,
                literal(),
            ),
            binary(
                date_bin(minute(), column("ts"), Some(column("origin"))),
                Operator::Eq,
                literal(),
            ),
            binary(
                date_bin(minute(), column("ts"), None),
                Operator::Eq,
                string_literal("not-a-timestamp"),
            ),
        ];
        let index_type = timestamp_type(Some("UTC".into()));

        for predicate in predicates {
            assert_eq!(
                normalize_timestamp_predicate(predicate.clone(), "ts", &index_type).unwrap(),
                predicate
            );
        }
    }

    #[test]
    fn leaves_overflowing_date_bin_boundaries_unchanged() {
        let timezone: Arc<str> = "UTC".into();
        let timestamp = |value| {
            Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(value), Some(timezone.clone())),
                None,
            )
        };
        let cases = [
            binary(
                date_bin(interval(0, 0, 1_000_000_000), column("ts"), None),
                Operator::Eq,
                timestamp(i64::MAX),
            ),
            binary(
                date_bin(
                    interval(0, 0, 1_000_000_000),
                    column("ts"),
                    Some(timestamp(i64::MAX)),
                ),
                Operator::Eq,
                timestamp(i64::MIN),
            ),
        ];
        let index_type = DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone));

        for predicate in cases {
            assert_eq!(
                normalize_timestamp_predicate(predicate.clone(), "ts", &index_type).unwrap(),
                predicate
            );
        }
    }

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
