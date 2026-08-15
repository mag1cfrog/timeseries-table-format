use arrow::datatypes::DataType;
use chrono::{DateTime, Duration, Months, TimeZone, Utc};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

use super::segment_pruning::timestamp_scalar;

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
}

fn normalize_interval_comparison(
    comparison: &BinaryExpr,
    index_column: &str,
    index_type: &DataType,
) -> Option<Expr> {
    if is_interval_arithmetic(&comparison.left)
        && let Some((column, interval)) =
            extract_index_with_interval(&comparison.left, index_column)
        && let Some(datetime) = timestamp_literal(&comparison.right)
    {
        return shifted_comparison(column, comparison.op, datetime, interval, index_type);
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
        index_type,
    )
}

fn is_interval_arithmetic(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::Plus | Operator::Minus)
    )
}

fn shifted_comparison(
    column: Expr,
    operator: Operator,
    datetime: DateTime<Utc>,
    interval: UnifiedInterval,
    index_type: &DataType,
) -> Option<Expr> {
    let datetime = add_interval(datetime, interval, -1)?;
    let DataType::Timestamp(unit, timezone) = index_type else {
        return None;
    };
    let literal = timestamp_scalar(datetime, unit, timezone.clone())?;
    Some(binary(column, operator, Expr::Literal(literal, None)))
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
        _ => None,
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::types::IntervalMonthDayNano;
    use arrow::datatypes::TimeUnit;
    use chrono::TimeZone;
    use datafusion::common::Column;
    use datafusion::logical_expr::Expr;

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
