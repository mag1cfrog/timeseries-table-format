use super::*;
use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use datafusion::common::Column;
use datafusion::logical_expr::Between;
use datafusion::logical_expr::BinaryExpr;
use datafusion::logical_expr::expr::InList;

fn dt(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("valid rfc3339")
        .with_timezone(&Utc)
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

fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn assert_cmp(expr: Expr, expected_op: Operator, expected_ts: &str) {
    let pred = compile_time_pred(&expr, "ts");
    match pred {
        TimePred::Cmp { op, ts } => {
            assert_eq!(op, expected_op);
            assert_eq!(ts, dt(expected_ts));
        }
        other => panic!("expected TimePred::Cmp, got {other:?}"),
    }
}

fn assert_unknown(expr: Expr) {
    let pred = compile_time_pred(&expr, "ts");
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

    let pred = compile_time_pred(&expr, "ts");

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

    let pred = compile_time_pred(&expr, "ts");

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
    let pred = compile_time_pred(&expr, "ts");

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
    let pred = compile_time_pred(&expr, "ts");

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
    let pred = compile_time_pred(&expr, "ts");

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
    let pred = compile_time_pred(&expr, "ts");

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
    let pred = compile_time_pred(&expr, "ts");

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
    let pred = compile_time_pred(&expr, "ts");

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
    let pred = compile_time_pred(&expr, "ts");

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

    let pred = compile_time_pred(&expr, "ts");

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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");
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
    let pred = compile_time_pred(&expr, "ts");

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
    let pred = compile_time_pred(&expr, "ts");

    let seg_min = dt("2024-01-10T00:00:00Z");
    let seg_max = dt("2024-01-11T00:00:00Z");
    assert_ne!(
        eval_time_pred_on_segment(&pred, seg_min, seg_max),
        IntervalTruth::AlwaysFalse
    );
}
