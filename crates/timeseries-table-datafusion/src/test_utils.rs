use chrono::{DateTime, Utc};
use datafusion::logical_expr::{Expr, Operator};

use crate::ts_table_provider::{IntervalTruth, ParsedTz, TimePred, UnifiedInterval};

/// Simplified, public view of a compiled time predicate for tests.
#[derive(Debug, PartialEq)]
pub enum CompiledTimePred {
    /// Predicate always true.
    True,
    /// Predicate always false.
    False,
    /// Predicate does not reference the timestamp column.
    NonTime,
    /// Predicate references ts but cannot be evaluated for pruning.
    Unknown,
    /// A direct comparison against a timestamp literal.
    Cmp {
        /// Comparison operator.
        op: Operator,
        /// Literal timestamp in UTC.
        ts: DateTime<Utc>,
    },
    /// Any other form (e.g., And/Or/Not trees).
    Other,
}

/// Result of evaluating a compiled time predicate against a segment.
#[derive(Debug, PartialEq, Eq)]
pub enum CompiledIntervalTruth {
    /// Predicate always true over the segment interval.
    AlwaysTrue,
    /// Predicate always false over the segment interval.
    AlwaysFalse,
    /// Predicate may be true for some values in the segment interval.
    MaybeTrue,
}

/// Compile a time predicate and map it into `CompiledTimePred`.
pub fn compile_time_pred_for_tests(expr: &Expr, ts_col: &str) -> CompiledTimePred {
    compile_time_pred_for_tests_with_tz(expr, ts_col, None)
}

/// Compile a time predicate with an explicit timezone.
pub(crate) fn compile_time_pred_for_tests_with_tz(
    expr: &Expr,
    ts_col: &str,
    tz: Option<&ParsedTz>,
) -> CompiledTimePred {
    match crate::ts_table_provider::compile_time_pred(expr, ts_col, tz) {
        TimePred::True => CompiledTimePred::True,
        TimePred::False => CompiledTimePred::False,
        TimePred::NonTime => CompiledTimePred::NonTime,
        TimePred::Unknown => CompiledTimePred::Unknown,
        TimePred::Cmp { op, ts } => CompiledTimePred::Cmp { op, ts },
        _ => CompiledTimePred::Other,
    }
}

/// Evaluate a time predicate against a segment interval using production logic.
pub fn eval_time_pred_on_segment_for_tests(
    expr: &Expr,
    ts_col: &str,
    seg_min: DateTime<Utc>,
    seg_max: DateTime<Utc>,
) -> CompiledIntervalTruth {
    eval_time_pred_on_segment_for_tests_with_tz(expr, ts_col, seg_min, seg_max, None)
}

/// Evaluate a time predicate against a segment interval with an explicit timezone.
pub(crate) fn eval_time_pred_on_segment_for_tests_with_tz(
    expr: &Expr,
    ts_col: &str,
    seg_min: DateTime<Utc>,
    seg_max: DateTime<Utc>,
    tz: Option<&ParsedTz>,
) -> CompiledIntervalTruth {
    let pred = crate::ts_table_provider::compile_time_pred(expr, ts_col, tz);
    match crate::ts_table_provider::eval_time_pred_on_segment(&pred, seg_min, seg_max) {
        IntervalTruth::AlwaysTrue => CompiledIntervalTruth::AlwaysTrue,
        IntervalTruth::AlwaysFalse => CompiledIntervalTruth::AlwaysFalse,
        IntervalTruth::MaybeTrue => CompiledIntervalTruth::MaybeTrue,
    }
}

/// Evaluate a time predicate against a segment interval using UTC boundaries.
pub fn eval_time_pred_on_segment_for_tests_utc(
    expr: &Expr,
    ts_col: &str,
    seg_min: DateTime<Utc>,
    seg_max: DateTime<Utc>,
) -> CompiledIntervalTruth {
    eval_time_pred_on_segment_for_tests_with_tz(
        expr,
        ts_col,
        seg_min,
        seg_max,
        Some(&ParsedTz::Utc),
    )
}

/// Interval value for tests, split into calendar and sub-day components.
pub struct TestInterval {
    /// Calendar months.
    pub months: i32,
    /// Calendar days.
    pub days: i32,
    /// Sub-day nanos.
    pub nanos: i64,
}

/// Apply a test interval to a timestamp using the production logic.
pub fn add_interval_for_tests(
    dt: DateTime<Utc>,
    interval: TestInterval,
    sign: i32,
) -> Option<DateTime<Utc>> {
    crate::ts_table_provider::add_interval(
        dt,
        UnifiedInterval {
            months: interval.months,
            days: interval.days,
            nanos: interval.nanos,
        },
        sign,
    )
}
