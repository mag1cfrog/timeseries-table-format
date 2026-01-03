use super::*;
use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use chrono_tz::Tz;
use datafusion::common::Result as DFResult;
use datafusion::common::{Column, TableReference};
use datafusion::logical_expr::Between;
use datafusion::logical_expr::BinaryExpr;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::Volatility;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::expr::{Alias, Cast, ScalarFunction};
use datafusion::logical_expr::expr_fn::create_udf;
use datafusion::logical_expr_common::columnar_value::ColumnarValue;
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

fn dt(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("valid rfc3339")
        .with_timezone(&Utc)
}

fn olson_tz(name: &str) -> ParsedTz {
    ParsedTz::Olson(name.parse::<Tz>().expect("valid tz"))
}

fn unix_seconds_to_datetime_test(secs: f64) -> Option<DateTime<Utc>> {
    if !secs.is_finite() {
        return None;
    }
    let whole = secs.trunc() as i64;
    let frac = secs - (whole as f64);
    let nanos = (frac * 1e9).round() as i64;
    let (adj_secs, adj_nanos) = if nanos > 1_000_000_000 {
        (whole + 1, nanos - 1_000_000_000)
    } else if nanos < 0 {
        (whole - 1, nanos + 1_000_000_000)
    } else {
        (whole, nanos)
    };

    Utc.timestamp_opt(adj_secs, adj_nanos as u32).single()
}

fn col(name: &str) -> Expr {
    Expr::Column(Column::from_name(name))
}

fn lit_str(value: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
}

fn lit_i64(value: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(Some(value)), None)
}

fn lit_f64(value: f64) -> Expr {
    Expr::Literal(ScalarValue::Float64(Some(value)), None)
}

fn lit_interval_mdn(months: i32, days: i32, nanos: i64) -> Expr {
    Expr::Literal(
        ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
            months,
            days,
            nanoseconds: nanos,
        })),
        None,
    )
}

fn lit_interval_day_time(days: i32, millis: i32) -> Expr {
    Expr::Literal(
        ScalarValue::IntervalDayTime(Some(IntervalDayTime {
            days,
            milliseconds: millis,
        })),
        None,
    )
}

fn lit_interval_year_month(months: i32) -> Expr {
    Expr::Literal(ScalarValue::IntervalYearMonth(Some(months)), None)
}

fn scalar_fn(name: &str, args: Vec<Expr>) -> Expr {
    let udf = create_udf(
        name,
        vec![],
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        Volatility::Immutable,
        Arc::new(|_: &[ColumnarValue]| -> DFResult<ColumnarValue> {
            unreachable!("udf should not execute in tests")
        }),
    );
    Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(udf), args))
}

fn cast(expr: Expr, data_type: DataType) -> Expr {
    Expr::Cast(Cast {
        expr: Box::new(expr),
        data_type,
    })
}

fn alias(expr: Expr, name: &str) -> Expr {
    Expr::Alias(Alias::new(expr, None::<TableReference>, name))
}

fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn assert_cmp(expr: Expr, expected_op: Operator, expected_ts: &str) {
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, expected_op);
            assert_eq!(ts, dt(expected_ts));
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

fn assert_cmp_dt(expr: Expr, expected_op: Operator, expected_ts: DateTime<Utc>) {
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, expected_op);
            assert_eq!(ts, expected_ts);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

fn assert_unknown(expr: Expr) {
    let pred = compile_time_pred(&expr, "ts", None);
    assert!(matches!(pred, TimePred::Unknown));
}

fn between(expr: Expr, low: Expr, high: Expr, negated: bool) -> Expr {
    Expr::Between(Between {
        expr: Box::new(expr),
        negated,
        low: Box::new(low),
        high: Box::new(high),
    })
}

fn in_list(expr: Expr, list: Vec<Expr>, negated: bool) -> Expr {
    Expr::InList(InList {
        expr: Box::new(expr),
        list,
        negated,
    })
}

#[test]
fn eval_cmp_lt() {
    let seg_min = dt("2024-01-08T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::Lt, lit, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );

    let seg_min = dt("2024-01-05T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::Lt, lit, seg_min, seg_max),
        IntervalTruth::MaybeTrue
    );

    let seg_min = dt("2024-01-05T00:00:00Z");
    let seg_max = dt("2024-01-07T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::Lt, lit, seg_min, seg_max),
        IntervalTruth::AlwaysTrue
    );
}

#[test]
fn eval_cmp_lte() {
    let seg_min = dt("2024-01-09T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::LtEq, lit, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );

    let seg_min = dt("2024-01-07T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::LtEq, lit, seg_min, seg_max),
        IntervalTruth::MaybeTrue
    );

    let seg_min = dt("2024-01-05T00:00:00Z");
    let seg_max = dt("2024-01-08T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::LtEq, lit, seg_min, seg_max),
        IntervalTruth::AlwaysTrue
    );
}

#[test]
fn eval_cmp_gt() {
    let seg_min = dt("2024-01-05T00:00:00Z");
    let seg_max = dt("2024-01-08T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::Gt, lit, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );

    let seg_min = dt("2024-01-05T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::Gt, lit, seg_min, seg_max),
        IntervalTruth::MaybeTrue
    );

    let seg_min = dt("2024-01-09T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::Gt, lit, seg_min, seg_max),
        IntervalTruth::AlwaysTrue
    );
}

#[test]
fn eval_cmp_gte() {
    let seg_min = dt("2024-01-05T00:00:00Z");
    let seg_max = dt("2024-01-07T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::GtEq, lit, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );

    let seg_min = dt("2024-01-05T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::GtEq, lit, seg_min, seg_max),
        IntervalTruth::MaybeTrue
    );

    let seg_min = dt("2024-01-08T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::GtEq, lit, seg_min, seg_max),
        IntervalTruth::AlwaysTrue
    );
}

#[test]
fn eval_cmp_eq() {
    let seg_min = dt("2024-01-08T00:00:00Z");
    let seg_max = dt("2024-01-08T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::Eq, lit, seg_min, seg_max),
        IntervalTruth::AlwaysTrue
    );

    let seg_min = dt("2024-01-09T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::Eq, lit, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );

    let seg_min = dt("2024-01-07T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::Eq, lit, seg_min, seg_max),
        IntervalTruth::MaybeTrue
    );
}

#[test]
fn eval_cmp_neq() {
    let seg_min = dt("2024-01-08T00:00:00Z");
    let seg_max = dt("2024-01-08T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::NotEq, lit, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );

    let seg_min = dt("2024-01-09T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::NotEq, lit, seg_min, seg_max),
        IntervalTruth::AlwaysTrue
    );

    let seg_min = dt("2024-01-07T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    let lit = dt("2024-01-08T00:00:00Z");
    assert_eq!(
        eval_cmp_on_interval(Operator::NotEq, lit, seg_min, seg_max),
        IntervalTruth::MaybeTrue
    );
}

#[test]
fn compile_time_pred_and_preserves_ts_constraint() {
    let expr = binary(
        binary(col("symbol"), Operator::Eq, lit_str("AAPL")),
        Operator::And,
        binary(col("ts"), Operator::GtEq, lit_str("2024-01-08T00:00:00Z")),
    );

    let pred = compile_time_pred(&expr, "ts", None);

    // Segment fully before the literal: should be prunable if AND preserves the ts constraint.
    let seg_min = dt("2024-01-01T00:00:00Z");
    let seg_max = dt("2024-01-02T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_time_pred_or_disables_pruning() {
    let expr = binary(
        binary(col("symbol"), Operator::Eq, lit_str("AAPL")),
        Operator::Or,
        binary(col("ts"), Operator::GtEq, lit_str("2024-01-08T00:00:00Z")),
    );

    let pred = compile_time_pred(&expr, "ts", None);

    let seg_min = dt("2024-01-01T00:00:00Z");
    let seg_max = dt("2024-01-02T00:00:00Z");
    assert_ne!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );

    let seg_min = dt("2024-01-10T00:00:00Z");
    let seg_max = dt("2024-01-11T00:00:00Z");
    assert_ne!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_between_prunes_outside_range() {
    let expr = between(
        col("ts"),
        lit_str("2024-01-08T00:00:00Z"),
        lit_str("2024-01-10T00:00:00Z"),
        false,
    );
    let pred = compile_time_pred(&expr, "ts", None);

    let seg_min = dt("2024-01-05T00:00:00Z");
    let seg_max = dt("2024-01-07T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_between_keeps_inside_range() {
    let expr = between(
        col("ts"),
        lit_str("2024-01-08T00:00:00Z"),
        lit_str("2024-01-10T00:00:00Z"),
        false,
    );
    let pred = compile_time_pred(&expr, "ts", None);

    let seg_min = dt("2024-01-08T00:00:00Z");
    let seg_max = dt("2024-01-09T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysTrue
    );
}

#[test]
fn compile_not_between_prunes_inside_range() {
    let expr = between(
        col("ts"),
        lit_str("2024-01-08T00:00:00Z"),
        lit_str("2024-01-10T00:00:00Z"),
        true,
    );
    let pred = compile_time_pred(&expr, "ts", None);

    let seg_min = dt("2024-01-08T00:00:00Z");
    let seg_max = dt("2024-01-09T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_not_between_keeps_outside_range() {
    let expr = between(
        col("ts"),
        lit_str("2024-01-08T00:00:00Z"),
        lit_str("2024-01-10T00:00:00Z"),
        true,
    );
    let pred = compile_time_pred(&expr, "ts", None);

    let seg_min = dt("2024-01-05T00:00:00Z");
    let seg_max = dt("2024-01-07T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysTrue
    );
}

#[test]
fn compile_in_list_prunes_outside_values() {
    let expr = in_list(
        col("ts"),
        vec![
            lit_str("2024-01-08T00:00:00Z"),
            lit_str("2024-01-10T00:00:00Z"),
        ],
        false,
    );
    let pred = compile_time_pred(&expr, "ts", None);

    let seg_min = dt("2024-01-05T00:00:00Z");
    let seg_max = dt("2024-01-07T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_in_list_keeps_overlap() {
    let expr = in_list(
        col("ts"),
        vec![
            lit_str("2024-01-08T00:00:00Z"),
            lit_str("2024-01-10T00:00:00Z"),
        ],
        false,
    );
    let pred = compile_time_pred(&expr, "ts", None);

    let seg_min = dt("2024-01-07T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::MaybeTrue
    );
}

#[test]
fn compile_not_in_list_keeps_segments() {
    let expr = in_list(col("ts"), vec![lit_str("2024-01-08T00:00:00Z")], true);
    let pred = compile_time_pred(&expr, "ts", None);

    let seg_min = dt("2024-01-08T00:00:00Z");
    let seg_max = dt("2024-01-10T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::MaybeTrue
    );
}

#[test]
fn compile_unknown_and_cmp_keeps_cmp_for_pruning() {
    let unknown = binary(col("ts"), Operator::Plus, lit_i64(1));
    let cmp = binary(col("ts"), Operator::GtEq, lit_str("2024-01-08T00:00:00Z"));
    let expr = binary(unknown, Operator::And, cmp);

    let pred = compile_time_pred(&expr, "ts", None);

    // Segment fully before the literal: should be prunable if AND keeps the time constraint.
    let seg_min = dt("2024-01-01T00:00:00Z");
    let seg_max = dt("2024-01-02T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_time_leaf_rejects_unsupported_op_with_ts_literal() {
    let expr = binary(col("ts"), Operator::Plus, lit_str("2024-01-08T00:00:00Z"));
    let pred = compile_time_pred(&expr, "ts", None);
    assert!(matches!(pred, TimePred::Unknown));
}

#[test]
fn compile_time_pred_simple_ts_lt_literal() {
    let expr = binary(col("ts"), Operator::Lt, lit_str("2024-01-08T00:00:00Z"));
    assert_cmp(expr, Operator::Lt, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_flipped_literal_gte_ts() {
    let expr = binary(lit_str("2024-01-08T00:00:00Z"), Operator::GtEq, col("ts"));
    assert_cmp(expr, Operator::LtEq, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_ts_plus_day_lt_literal() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, lit_interval_day_time(1, 0)),
        Operator::Lt,
        lit_str("2024-01-08T00:00:00Z"),
    );
    let expected = add_interval(
        dt("2024-01-08T00:00:00Z"),
        UnifiedInterval {
            months: 0,
            days: 1,
            nanos: 0,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_ts_minus_hours_lte_literal() {
    let expr = binary(
        binary(
            col("ts"),
            Operator::Minus,
            lit_interval_mdn(0, 0, 2 * 3_600_000_000_000),
        ),
        Operator::LtEq,
        lit_str("2024-01-08T00:00:00Z"),
    );
    let expected = add_interval(
        dt("2024-01-08T00:00:00Z"),
        UnifiedInterval {
            months: 0,
            days: 0,
            nanos: -2 * 3_600_000_000_000,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::LtEq);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_literal_gt_ts_plus_minutes() {
    let expr = binary(
        lit_str("2024-01-08T00:00:00Z"),
        Operator::Gt,
        binary(
            col("ts"),
            Operator::Plus,
            lit_interval_mdn(0, 0, 30 * 60_000_000_000),
        ),
    );
    let expected = add_interval(
        dt("2024-01-08T00:00:00Z"),
        UnifiedInterval {
            months: 0,
            days: 0,
            nanos: 30 * 60_000_000_000,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_literal_lte_ts_minus_hour() {
    let expr = binary(
        lit_str("2024-01-08T00:00:00Z"),
        Operator::LtEq,
        binary(
            col("ts"),
            Operator::Minus,
            lit_interval_mdn(0, 0, 3_600_000_000_000),
        ),
    );
    let expected = add_interval(
        dt("2024-01-08T00:00:00Z"),
        UnifiedInterval {
            months: 0,
            days: 0,
            nanos: -3_600_000_000_000,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::GtEq);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_ts_plus_day_plus_hour_lt_literal() {
    let expr = binary(
        binary(
            binary(col("ts"), Operator::Plus, lit_interval_day_time(1, 0)),
            Operator::Plus,
            lit_interval_mdn(0, 0, 3_600_000_000_000),
        ),
        Operator::Lt,
        lit_str("2024-01-08T12:00:00Z"),
    );
    let expected = add_interval(
        dt("2024-01-08T12:00:00Z"),
        UnifiedInterval {
            months: 0,
            days: 1,
            nanos: 3_600_000_000_000,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_ts_minus_day_minus_hour_lt_literal() {
    let expr = binary(
        binary(
            binary(col("ts"), Operator::Minus, lit_interval_day_time(1, 0)),
            Operator::Minus,
            lit_interval_mdn(0, 0, 3_600_000_000_000),
        ),
        Operator::Lt,
        lit_str("2024-01-08T12:00:00Z"),
    );
    let expected = add_interval(
        dt("2024-01-08T12:00:00Z"),
        UnifiedInterval {
            months: 0,
            days: -1,
            nanos: -3_600_000_000_000,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_literal_gt_interval_plus_ts_plus_interval() {
    let expr = binary(
        lit_str("2024-01-08T12:00:00Z"),
        Operator::Gt,
        binary(
            binary(lit_interval_day_time(1, 0), Operator::Plus, col("ts")),
            Operator::Plus,
            lit_interval_mdn(0, 0, 3_600_000_000_000),
        ),
    );
    let expected = add_interval(
        dt("2024-01-08T12:00:00Z"),
        UnifiedInterval {
            months: 0,
            days: 1,
            nanos: 3_600_000_000_000,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_mixed_intervals_month_day_hour() {
    let expr = binary(
        binary(
            binary(col("ts"), Operator::Plus, lit_interval_year_month(1)),
            Operator::Plus,
            lit_interval_day_time(2, 0),
        ),
        Operator::Lt,
        lit_str("2024-02-10T00:00:00Z"),
    );
    let expected = add_interval(
        dt("2024-02-10T00:00:00Z"),
        UnifiedInterval {
            months: 1,
            days: 2,
            nanos: 0,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_mixed_intervals_with_minus() {
    let expr = binary(
        binary(
            binary(col("ts"), Operator::Plus, lit_interval_year_month(2)),
            Operator::Minus,
            lit_interval_day_time(1, 0),
        ),
        Operator::Lt,
        lit_str("2024-04-10T00:00:00Z"),
    );
    let expected = add_interval(
        dt("2024-04-10T00:00:00Z"),
        UnifiedInterval {
            months: 2,
            days: -1,
            nanos: 0,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_interval_plus_ts_lt_literal() {
    let expr = binary(
        binary(lit_interval_day_time(1, 0), Operator::Plus, col("ts")),
        Operator::Lt,
        lit_str("2024-01-08T00:00:00Z"),
    );
    let expected = add_interval(
        dt("2024-01-08T00:00:00Z"),
        UnifiedInterval {
            months: 0,
            days: 1,
            nanos: 0,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_interval_minus_ts_is_unknown() {
    let expr = binary(
        binary(lit_interval_day_time(1, 0), Operator::Minus, col("ts")),
        Operator::Lt,
        lit_str("2024-01-08T00:00:00Z"),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_ts_plus_numeric_is_unknown() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, lit_i64(1)),
        Operator::Lt,
        lit_str("2024-01-08T00:00:00Z"),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_ts_plus_col_is_unknown() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, col("other")),
        Operator::Lt,
        lit_str("2024-01-08T00:00:00Z"),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_ts_plus_interval_plus_col_is_unknown() {
    let expr = binary(
        binary(
            binary(col("ts"), Operator::Plus, lit_interval_day_time(1, 0)),
            Operator::Plus,
            col("other"),
        ),
        Operator::Lt,
        lit_str("2024-01-08T00:00:00Z"),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_ts_plus_interval_lt_ts_plus_interval_is_unknown() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, lit_interval_day_time(1, 0)),
        Operator::Lt,
        binary(col("ts"), Operator::Plus, lit_interval_day_time(2, 0)),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_ts_appears_on_both_sides_is_unknown() {
    let expr = binary(
        binary(
            binary(col("ts"), Operator::Plus, lit_interval_day_time(1, 0)),
            Operator::Plus,
            col("ts"),
        ),
        Operator::Lt,
        lit_str("2024-01-08T00:00:00Z"),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_interval_year_month_literal() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, lit_interval_year_month(2)),
        Operator::Lt,
        lit_str("2024-03-10T00:00:00Z"),
    );
    let expected = add_interval(
        dt("2024-03-10T00:00:00Z"),
        UnifiedInterval {
            months: 2,
            days: 0,
            nanos: 0,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_interval_day_time_literal() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, lit_interval_day_time(1, 0)),
        Operator::Lt,
        lit_str("2024-01-10T00:00:00Z"),
    );
    let expected = add_interval(
        dt("2024-01-10T00:00:00Z"),
        UnifiedInterval {
            months: 0,
            days: 1,
            nanos: 0,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_interval_month_day_nano_literal() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, lit_interval_mdn(0, 0, 5)),
        Operator::Lt,
        lit_str("2024-01-10T00:00:00.000000005Z"),
    );
    let expected = add_interval(
        dt("2024-01-10T00:00:00.000000005Z"),
        UnifiedInterval {
            months: 0,
            days: 0,
            nanos: 5,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_interval_microsecond_precision() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, lit_interval_mdn(0, 0, 1_000)),
        Operator::Lt,
        lit_str("2024-01-10T00:00:00.000001Z"),
    );
    let expected = add_interval(
        dt("2024-01-10T00:00:00.000001Z"),
        UnifiedInterval {
            months: 0,
            days: 0,
            nanos: 1_000,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_month_rollover_boundary() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, lit_interval_year_month(1)),
        Operator::Lt,
        lit_str("2024-03-31T00:00:00Z"),
    );
    let expected = add_interval(
        dt("2024-03-31T00:00:00Z"),
        UnifiedInterval {
            months: 1,
            days: 0,
            nanos: 0,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_overflow_returns_unknown() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, lit_interval_year_month(i32::MAX)),
        Operator::Lt,
        lit_str("2024-01-10T00:00:00Z"),
    );
    let pred = compile_time_pred(&expr, "ts", None);
    assert!(matches!(pred, TimePred::Unknown));
}

#[test]
fn compile_time_pred_and_preserves_interval_constraint() {
    let expr = binary(
        binary(col("symbol"), Operator::Eq, lit_str("AAPL")),
        Operator::And,
        binary(
            binary(col("ts"), Operator::Plus, lit_interval_day_time(1, 0)),
            Operator::Lt,
            lit_str("2024-01-10T00:00:00Z"),
        ),
    );
    let pred = compile_time_pred(&expr, "ts", None);

    // Segment entirely after threshold should be pruned.
    let seg_min = dt("2024-01-10T00:00:00Z");
    let seg_max = dt("2024-01-11T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_time_pred_or_disables_interval_pruning() {
    let expr = binary(
        binary(col("symbol"), Operator::Eq, lit_str("AAPL")),
        Operator::Or,
        binary(
            binary(col("ts"), Operator::Plus, lit_interval_day_time(1, 0)),
            Operator::Lt,
            lit_str("2024-01-10T00:00:00Z"),
        ),
    );
    let pred = compile_time_pred(&expr, "ts", None);

    let seg_min = dt("2024-01-10T00:00:00Z");
    let seg_max = dt("2024-01-11T00:00:00Z");
    assert_ne!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_time_pred_ts_gte_to_timestamp_string() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp", vec![lit_str("2024-01-08T00:00:00Z")]),
    );
    assert_cmp(expr, Operator::GtEq, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_ts_lt_to_timestamp_seconds_string() {
    let expr = binary(
        col("ts"),
        Operator::Lt,
        scalar_fn(
            "to_timestamp_seconds",
            vec![lit_str("2024-01-08T00:00:00Z")],
        ),
    );
    assert_cmp(expr, Operator::Lt, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_ts_eq_to_timestamp_millis_string() {
    let expr = binary(
        col("ts"),
        Operator::Eq,
        scalar_fn(
            "to_timestamp_millis",
            vec![lit_str("2024-01-08T00:00:00.123Z")],
        ),
    );
    assert_cmp(expr, Operator::Eq, "2024-01-08T00:00:00.123Z");
}

#[test]
fn compile_time_pred_ts_gt_to_timestamp_micros_string() {
    let expr = binary(
        col("ts"),
        Operator::Gt,
        scalar_fn(
            "to_timestamp_micros",
            vec![lit_str("2024-01-08T00:00:00.123456Z")],
        ),
    );
    assert_cmp(expr, Operator::Gt, "2024-01-08T00:00:00.123456Z");
}

#[test]
fn compile_time_pred_ts_lte_to_timestamp_nanos_string() {
    let expr = binary(
        col("ts"),
        Operator::LtEq,
        scalar_fn(
            "to_timestamp_nanos",
            vec![lit_str("2024-01-08T00:00:00.123456789Z")],
        ),
    );
    assert_cmp(expr, Operator::LtEq, "2024-01-08T00:00:00.123456789Z");
}

#[test]
fn compile_time_pred_ts_gte_to_timestamp_seconds_numeric() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp", vec![lit_i64(1_704_672_000)]),
    );
    assert_cmp(expr, Operator::GtEq, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_ts_gte_to_timestamp_seconds_alias_numeric() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp_seconds", vec![lit_i64(1_704_672_000)]),
    );
    assert_cmp(expr, Operator::GtEq, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_ts_gte_to_timestamp_millis_numeric() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp_millis", vec![lit_i64(1_704_672_000_123)]),
    );
    let expected = unix_seconds_to_datetime_test(1_704_672_000_123f64 / 1_000.0).expect("dt");
    assert_cmp_dt(expr, Operator::GtEq, expected);
}

#[test]
fn compile_time_pred_ts_gte_to_timestamp_micros_numeric() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp_micros", vec![lit_i64(1_704_672_000_123_456)]),
    );
    let expected =
        unix_seconds_to_datetime_test(1_704_672_000_123_456f64 / 1_000_000.0).expect("dt");
    assert_cmp_dt(expr, Operator::GtEq, expected);
}

#[test]
fn compile_time_pred_ts_gte_to_timestamp_nanos_numeric() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn(
            "to_timestamp_nanos",
            vec![lit_i64(1_704_672_000_123_456_789)],
        ),
    );
    let expected =
        unix_seconds_to_datetime_test(1_704_672_000_123_456_789f64 / 1_000_000_000.0).expect("dt");
    assert_cmp_dt(expr, Operator::GtEq, expected);
}

#[test]
fn compile_time_pred_literal_to_timestamp_lte_ts() {
    let expr = binary(
        scalar_fn("to_timestamp", vec![lit_i64(1_704_672_000)]),
        Operator::LtEq,
        col("ts"),
    );
    assert_cmp(expr, Operator::GtEq, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_literal_to_timestamp_millis_gt_ts() {
    let expr = binary(
        scalar_fn("to_timestamp_millis", vec![lit_i64(1_704_672_000_123)]),
        Operator::Gt,
        col("ts"),
    );
    let expected = unix_seconds_to_datetime_test(1_704_672_000_123f64 / 1_000.0).expect("dt");
    assert_cmp_dt(expr, Operator::Lt, expected);
}

#[test]
fn compile_time_pred_to_timestamp_no_args_is_unknown() {
    let expr = binary(col("ts"), Operator::GtEq, scalar_fn("to_timestamp", vec![]));
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_to_timestamp_too_many_args_is_unknown() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp", vec![lit_i64(1), lit_i64(2)]),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_to_timestamp_non_literal_is_unknown() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp", vec![col("other")]),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_to_timestamp_millis_invalid_string_is_unknown() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp_millis", vec![lit_str("not a timestamp")]),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_to_timestamp_micros_bad_date_is_unknown() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp_micros", vec![lit_str("2024-13-99T00:00:00Z")]),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_to_timestamp_nan_is_unknown() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp", vec![lit_f64(f64::NAN)]),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_ts_gte_casted_to_timestamp() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        cast(
            scalar_fn("to_timestamp", vec![lit_i64(1_704_672_000)]),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ),
    );
    assert_cmp(expr, Operator::GtEq, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_ts_gte_aliased_to_timestamp() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        alias(
            scalar_fn("to_timestamp", vec![lit_i64(1_704_672_000)]),
            "ts_alias",
        ),
    );
    assert_cmp(expr, Operator::GtEq, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_to_timestamp_seconds_fractional() {
    let expr = binary(
        col("ts"),
        Operator::GtEq,
        scalar_fn("to_timestamp_seconds", vec![lit_f64(1_704_672_000.5)]),
    );
    assert_cmp(expr, Operator::GtEq, "2024-01-08T00:00:00.500000000Z");
}

#[test]
fn compile_time_pred_interval_lt_to_timestamp_seconds() {
    let expr = binary(
        binary(col("ts"), Operator::Plus, lit_interval_day_time(1, 0)),
        Operator::Lt,
        scalar_fn("to_timestamp", vec![lit_i64(1_704_672_000)]),
    );
    let expected = add_interval(
        dt("2024-01-08T00:00:00Z"),
        UnifiedInterval {
            months: 0,
            days: 1,
            nanos: 0,
        },
        -1,
    )
    .expect("shifted");
    let pred = compile_time_pred(&expr, "ts", None);
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, Operator::Lt);
            assert_eq!(ts, expected);
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

#[test]
fn compile_time_pred_literal_to_timestamp_gt_ts_plus_interval() {
    let expr = binary(
        scalar_fn("to_timestamp_millis", vec![lit_i64(1_704_672_000_123)]),
        Operator::Gt,
        binary(
            col("ts"),
            Operator::Plus,
            lit_interval_mdn(0, 0, 3_600_000_000_000),
        ),
    );
    let base = unix_seconds_to_datetime_test(1_704_672_000_123f64 / 1_000.0).expect("dt");
    let expected = add_interval(
        base,
        UnifiedInterval {
            months: 0,
            days: 0,
            nanos: 3_600_000_000_000,
        },
        -1,
    )
    .expect("shifted");
    assert_cmp_dt(expr, Operator::Lt, expected);
}

#[test]
fn compile_time_pred_to_unixtime_ts_gte_numeric() {
    let expr = binary(
        scalar_fn("to_unixtime", vec![col("ts")]),
        Operator::GtEq,
        lit_i64(1_704_672_000),
    );
    assert_cmp(expr, Operator::GtEq, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_numeric_lt_to_unixtime_ts() {
    let expr = binary(
        lit_i64(1_704_672_000),
        Operator::Lt,
        scalar_fn("to_unixtime", vec![col("ts")]),
    );
    assert_cmp(expr, Operator::Gt, "2024-01-08T00:00:00Z");
}

#[test]
fn compile_time_pred_to_unixtime_invalid_arity_is_unknown() {
    let expr = binary(
        scalar_fn("to_unixtime", vec![col("ts"), lit_i64(1)]),
        Operator::GtEq,
        lit_i64(1_704_672_000),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_to_unixtime_non_numeric_literal_is_unknown() {
    let expr = binary(
        scalar_fn("to_unixtime", vec![col("ts")]),
        Operator::Lt,
        lit_str("1704672000"),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_to_date_eq_literal_range() {
    let expr = binary(
        scalar_fn("to_date", vec![col("ts")]),
        Operator::Eq,
        lit_str("2024-01-01"),
    );
    let pred = compile_time_pred(&expr, "ts", Some(&ParsedTz::Utc));
    let start = dt("2024-01-01T00:00:00Z");
    let end = dt("2024-01-02T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, start, start),
        IntervalTruth::AlwaysTrue
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, end, end),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_time_pred_to_date_lt_literal() {
    let expr = binary(
        scalar_fn("to_date", vec![col("ts")]),
        Operator::Lt,
        lit_str("2024-01-01"),
    );
    let pred = compile_time_pred(&expr, "ts", Some(&ParsedTz::Utc));
    let before = dt("2023-12-31T23:59:59Z");
    let at_start = dt("2024-01-01T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, before, before),
        IntervalTruth::AlwaysTrue
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, at_start, at_start),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_time_pred_to_date_gte_literal() {
    let expr = binary(
        scalar_fn("to_date", vec![col("ts")]),
        Operator::GtEq,
        lit_str("2024-01-01"),
    );
    let pred = compile_time_pred(&expr, "ts", Some(&ParsedTz::Utc));
    let before = dt("2023-12-31T23:59:59Z");
    let at_start = dt("2024-01-01T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, before, before),
        IntervalTruth::AlwaysFalse
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, at_start, at_start),
        IntervalTruth::AlwaysTrue
    );
}

#[test]
fn compile_time_pred_to_date_invalid_arity_is_unknown() {
    let expr = binary(
        scalar_fn("to_date", vec![col("ts"), lit_str("2024-01-01")]),
        Operator::Eq,
        lit_str("2024-01-01"),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_to_date_non_date_literal_is_unknown() {
    let expr = binary(
        scalar_fn("to_date", vec![col("ts")]),
        Operator::Eq,
        lit_str("not-a-date"),
    );
    assert_unknown(expr);
}

#[test]
fn compile_time_pred_date_trunc_gt_non_aligned_hour() {
    let expr = binary(
        scalar_fn("date_trunc", vec![lit_str("hour"), col("ts")]),
        Operator::Gt,
        lit_str("2020-01-01T10:30:00Z"),
    );
    let pred = compile_time_pred(&expr, "ts", None);
    let before = dt("2020-01-01T10:59:59Z");
    let at_next = dt("2020-01-01T11:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, before, before),
        IntervalTruth::AlwaysFalse
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, at_next, at_next),
        IntervalTruth::AlwaysTrue
    );
}

#[test]
fn compile_time_pred_date_trunc_hour_negative_timestamp_floor() {
    let expr = binary(
        scalar_fn("date_trunc", vec![lit_str("hour"), col("ts")]),
        Operator::Eq,
        lit_str("1969-12-31T23:00:00Z"),
    );
    let pred = compile_time_pred(&expr, "ts", None);
    let inside = dt("1969-12-31T23:30:00Z");
    let outside = dt("1970-01-01T00:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, inside, inside),
        IntervalTruth::AlwaysTrue
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, outside, outside),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_time_pred_date_trunc_day_olson_dst_boundary() {
    let expr = binary(
        scalar_fn("date_trunc", vec![lit_str("day"), col("ts")]),
        Operator::Eq,
        lit_str("2024-03-10T00:00:00-05:00"),
    );
    let tz = olson_tz("America/New_York");
    let pred = compile_time_pred(&expr, "ts", Some(&tz));
    let start = dt("2024-03-10T05:00:00Z");
    let before_end = dt("2024-03-11T03:59:59Z");
    let at_end = dt("2024-03-11T04:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, start, start),
        IntervalTruth::AlwaysTrue
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, before_end, before_end),
        IntervalTruth::AlwaysTrue
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, at_end, at_end),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_time_pred_date_trunc_hour_olson_dst_spring_forward() {
    let expr = binary(
        scalar_fn("date_trunc", vec![lit_str("hour"), col("ts")]),
        Operator::Eq,
        lit_str("2024-03-10T03:00:00-04:00"),
    );
    let tz = olson_tz("America/New_York");
    let pred = compile_time_pred(&expr, "ts", Some(&tz));
    let start = dt("2024-03-10T07:00:00Z");
    let before_end = dt("2024-03-10T07:59:59Z");
    let at_end = dt("2024-03-10T08:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, start, start),
        IntervalTruth::AlwaysTrue
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, before_end, before_end),
        IntervalTruth::AlwaysTrue
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, at_end, at_end),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_time_pred_date_trunc_minute_olson_ignores_tz() {
    let expr = binary(
        scalar_fn("date_trunc", vec![lit_str("minute"), col("ts")]),
        Operator::Eq,
        lit_str("2024-03-10T07:30:00Z"),
    );
    let tz = olson_tz("America/New_York");
    let pred = compile_time_pred(&expr, "ts", Some(&tz));
    let inside = dt("2024-03-10T07:30:30Z");
    let outside = dt("2024-03-10T07:31:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, inside, inside),
        IntervalTruth::AlwaysTrue
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, outside, outside),
        IntervalTruth::AlwaysFalse
    );
}

#[test]
fn compile_time_pred_date_trunc_lt_aligned_hour() {
    let expr = binary(
        scalar_fn("date_trunc", vec![lit_str("hour"), col("ts")]),
        Operator::Lt,
        lit_str("2024-01-01T10:00:00Z"),
    );
    let pred = compile_time_pred(&expr, "ts", None);
    let before = dt("2024-01-01T09:59:59Z");
    let at_floor = dt("2024-01-01T10:00:00Z");
    assert_eq!(
        eval_time_pred_on_segment(&pred, before, before),
        IntervalTruth::AlwaysTrue
    );
    assert_eq!(
        eval_time_pred_on_segment(&pred, at_floor, at_floor),
        IntervalTruth::AlwaysFalse
    );
}
