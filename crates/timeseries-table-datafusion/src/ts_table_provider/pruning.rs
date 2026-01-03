use chrono::{DateTime, Utc};
use datafusion::logical_expr::Operator;

use crate::ts_table_provider::{IntervalTruth, TimePred};

pub(crate) fn eval_cmp_on_interval(
    op: Operator,
    dt: DateTime<Utc>,
    seg_min: DateTime<Utc>,
    seg_max: DateTime<Utc>,
) -> IntervalTruth {
    use IntervalTruth::*;
    match op {
        Operator::Lt => {
            if seg_max < dt {
                AlwaysTrue
            } else if seg_min >= dt {
                AlwaysFalse
            } else {
                MaybeTrue
            }
        }
        Operator::LtEq => {
            if seg_max <= dt {
                AlwaysTrue
            } else if seg_min > dt {
                AlwaysFalse
            } else {
                MaybeTrue
            }
        }
        Operator::Gt => {
            if seg_min > dt {
                AlwaysTrue
            } else if seg_max <= dt {
                AlwaysFalse
            } else {
                MaybeTrue
            }
        }
        Operator::GtEq => {
            if seg_min >= dt {
                AlwaysTrue
            } else if seg_max < dt {
                AlwaysFalse
            } else {
                MaybeTrue
            }
        }
        Operator::Eq => {
            if dt < seg_min || dt > seg_max {
                AlwaysFalse
            } else if seg_min == seg_max && seg_min == dt {
                AlwaysTrue
            } else {
                MaybeTrue
            }
        }
        Operator::NotEq => {
            // Only definitely false if segment is exactly one timestamp equal to dt.
            if seg_min == seg_max && seg_min == dt {
                AlwaysFalse
            } else if dt < seg_min || dt > seg_max {
                AlwaysTrue
            } else {
                MaybeTrue
            }
        }
        _ => MaybeTrue,
    }
}

/// Result of evaluating a time predicate against an entire segment time interval
/// `[ts_min, ts_max]` (both inclusive).
///
/// IMPORTANT: this is **universal over the interval**, not "does the segment match".
///
/// - `AlwaysFalse`: the predicate cannot be true for any timestamp in the interval.
///   This segment is safe to PRUNE (skip the file).
/// - `MaybeTrue`: the predicate may be true for some timestamps in the interval.
///   Must KEEP the segment.
/// - `AlwaysTrue`: the predicate is true for all timestamps in the interval.
///   Still KEEP the segment (we are deciding pruning only; execution still runs).
pub(crate) fn eval_time_pred_on_segment(
    pred: &TimePred,
    seg_min: DateTime<Utc>,
    seg_max: DateTime<Utc>,
) -> IntervalTruth {
    use IntervalTruth::*;
    use TimePred::*;
    match pred {
        True => AlwaysTrue,
        False => AlwaysFalse,
        Unknown => MaybeTrue,
        NonTime => MaybeTrue,
        Cmp { op, ts } => eval_cmp_on_interval(*op, *ts, seg_min, seg_max),
        And(a, b) => eval_time_pred_on_segment(a, seg_min, seg_max)
            .and(eval_time_pred_on_segment(b, seg_min, seg_max)),
        Or(a, b) => eval_time_pred_on_segment(a, seg_min, seg_max)
            .or(eval_time_pred_on_segment(b, seg_min, seg_max)),
        Not(x) => eval_time_pred_on_segment(x, seg_min, seg_max).not(),
    }
}
