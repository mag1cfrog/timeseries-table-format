use chrono::{DateTime, Utc};
use datafusion::logical_expr::{Expr, Operator};

use crate::ts_table_provider::{TimePred, UnifiedInterval};

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

/// Compile a time predicate and map it into `CompiledTimePred`.
pub fn compile_time_pred_for_tests(expr: &Expr, ts_col: &str) -> CompiledTimePred {
    match crate::ts_table_provider::compile_time_pred(expr, ts_col) {
        TimePred::True => CompiledTimePred::True,
        TimePred::False => CompiledTimePred::False,
        TimePred::NonTime => CompiledTimePred::NonTime,
        TimePred::Unknown => CompiledTimePred::Unknown,
        TimePred::Cmp { op, ts } => CompiledTimePred::Cmp { op, ts },
        _ => CompiledTimePred::Other,
    }
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
