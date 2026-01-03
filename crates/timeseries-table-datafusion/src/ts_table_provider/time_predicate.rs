use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Between, Operator};
use datafusion::scalar::ScalarValue;

use crate::ts_table_provider::ParsedTz;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TimePred {
    True,
    False,
    Unknown, // mentions ts but we can't reason about it; in AND it is neutral for pruning
    NonTime, // does NOT mention ts at all (time-independent predicate)
    Cmp { op: Operator, ts: DateTime<Utc> }, // ts_col OP literal
    And(Box<TimePred>, Box<TimePred>),
    Or(Box<TimePred>, Box<TimePred>),
    Not(Box<TimePred>),
}

impl TimePred {
    pub(crate) fn and(a: TimePred, b: TimePred) -> TimePred {
        use TimePred::*;
        match (a, b) {
            (False, _) | (_, False) => False,
            (True, x) | (x, True) => x,

            (NonTime, x) | (x, NonTime) => x,

            // don't let Unknown erase usable constraints in AND.
            (Unknown, x) | (x, Unknown) => x,
            (x, y) => And(Box::new(x), Box::new(y)),
        }
    }

    pub(crate) fn or(a: TimePred, b: TimePred) -> TimePred {
        use TimePred::*;
        match (a, b) {
            (True, _) | (_, True) => True,
            (False, x) | (x, False) => x,

            (NonTime, _) | (_, NonTime) => Unknown,

            (Unknown, x) | (x, Unknown) => match x {
                True => True,
                _ => Unknown,
            },
            (x, y) => Or(Box::new(x), Box::new(y)),
        }
    }

    pub(crate) fn not(x: TimePred) -> TimePred {
        use TimePred::*;
        match x {
            True => False,
            False => True,
            NonTime => Unknown,
            Unknown => Unknown,
            Not(inner) => *inner,
            other => Not(Box::new(other)),
        }
    }
}

// Unified interval representation (calendar-aware months + days + nanos).
#[derive(Debug, Clone, Copy)]
pub(crate) struct UnifiedInterval {
    pub(crate) months: i32,
    pub(crate) days: i32,
    pub(crate) nanos: i64,
}

impl UnifiedInterval {
    fn zero() -> Self {
        Self {
            months: 0,
            days: 0,
            nanos: 0,
        }
    }

    fn add(self, rhs: Self, sign: i32) -> Self {
        Self {
            months: self.months.saturating_add(rhs.months.saturating_mul(sign)),
            days: self.days.saturating_add(rhs.days.saturating_mul(sign)),
            nanos: self
                .nanos
                .saturating_add(rhs.nanos.saturating_mul(sign as i64)),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum IntervalTruth {
    AlwaysTrue,
    AlwaysFalse,
    MaybeTrue,
}

impl IntervalTruth {
    pub(crate) fn and(self, rhs: IntervalTruth) -> IntervalTruth {
        use IntervalTruth::*;
        match (self, rhs) {
            (AlwaysFalse, _) | (_, AlwaysFalse) => AlwaysFalse,
            (AlwaysTrue, x) | (x, AlwaysTrue) => x,
            _ => MaybeTrue,
        }
    }

    pub(crate) fn or(self, rhs: IntervalTruth) -> IntervalTruth {
        use IntervalTruth::*;
        match (self, rhs) {
            (AlwaysTrue, _) | (_, AlwaysTrue) => AlwaysTrue,
            (AlwaysFalse, x) | (x, AlwaysFalse) => x,
            _ => MaybeTrue,
        }
    }

    pub(crate) fn not(self) -> IntervalTruth {
        use IntervalTruth::*;
        match self {
            AlwaysTrue => AlwaysFalse,
            AlwaysFalse => AlwaysTrue,
            MaybeTrue => MaybeTrue,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TsTransform {
    ToUnixtime,
    ToDate,
}

// --------------------------- helpers -----------------------

fn unwrap_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(a) => unwrap_expr(&a.expr),
        Expr::Cast(c) => unwrap_expr(&c.expr),
        Expr::TryCast(c) => unwrap_expr(&c.expr),
        other => other,
    }
}

fn expr_to_numeric(expr: &Expr) -> Option<f64> {
    match unwrap_expr(expr) {
        Expr::Literal(v, _) => match v {
            ScalarValue::Int64(Some(x)) => Some(*x as f64),
            ScalarValue::Int32(Some(x)) => Some(*x as f64),
            ScalarValue::UInt64(Some(x)) => Some(*x as f64),
            ScalarValue::UInt32(Some(x)) => Some(*x as f64),
            ScalarValue::Float64(Some(x)) => Some(*x),
            ScalarValue::Float32(Some(x)) => Some(*x as f64),
            _ => None,
        },
        _ => None,
    }
}

/// Convert a scalar literal into a UTC DateTime, if supported.
fn scalar_to_utc_datetime(v: &ScalarValue) -> Option<DateTime<Utc>> {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            let dt = DateTime::parse_from_rfc3339(s).ok()?;
            Some(dt.with_timezone(&Utc))
        }

        // Timestamp scalars (units vary by DF version)
        ScalarValue::TimestampSecond(Some(x), _) => Some(Utc.timestamp_opt(*x, 0).single()?),
        ScalarValue::TimestampMillisecond(Some(x), _) => {
            Some(Utc.timestamp_millis_opt(*x).single()?)
        }
        ScalarValue::TimestampMicrosecond(Some(x), _) => {
            let secs = x.div_euclid(1_000_000);
            let micros = x.rem_euclid(1_000_000) as u32;
            Some(Utc.timestamp_opt(secs, micros * 1000).single()?)
        }
        ScalarValue::TimestampNanosecond(Some(x), _) => {
            let secs = x.div_euclid(1_000_000_000);
            let nanos = x.rem_euclid(1_000_000_000) as u32;
            Some(Utc.timestamp_opt(secs, nanos).single()?)
        }

        _ => None,
    }
}

/// Returns true if the expression is the timestamp column reference.
fn expr_is_ts(expr: &Expr, ts_col: &str) -> bool {
    match unwrap_expr(expr) {
        Expr::Column(c) => c.name == ts_col,
        _ => false,
    }
}

/// Returns true if the expression tree mentions the timestamp column anywhere.
/// This is broader than `expr_is_ts`, which only matches a direct reference
/// (optionally wrapped by alias/cast).
pub(crate) fn expr_mentions_ts(expr: &Expr, ts_col: &str) -> bool {
    let e = unwrap_expr(expr);

    match e {
        Expr::Column(c) => c.name == ts_col,

        Expr::BinaryExpr(be) => {
            expr_mentions_ts(&be.left, ts_col) || expr_mentions_ts(&be.right, ts_col)
        }

        Expr::Not(e) => expr_mentions_ts(e, ts_col),

        Expr::Between(b) => {
            expr_mentions_ts(&b.expr, ts_col)
                || expr_mentions_ts(&b.low, ts_col)
                || expr_mentions_ts(&b.high, ts_col)
        }

        Expr::InList(il) => {
            expr_mentions_ts(&il.expr, ts_col)
                || il.list.iter().any(|e| expr_mentions_ts(e, ts_col))
        }

        Expr::ScalarFunction(sf) => sf.args.iter().any(|x| expr_mentions_ts(x, ts_col)),

        _ => false,
    }
}

fn parse_date_str(s: &str) -> Option<DateTime<Utc>> {
    // Accept YYYY-MM-DD as midnight UTC
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    Some(Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0)?))
}

fn unix_seconds_to_datetime(secs: f64) -> Option<DateTime<Utc>> {
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

/// Extract a DateTime literal from expressions like aliases/casts.
fn parse_ts_literal(expr: &Expr) -> Option<DateTime<Utc>> {
    match unwrap_expr(expr) {
        Expr::Literal(v, _) => scalar_to_utc_datetime(v).or_else(|| {
            // allow date-only strings as midnight UTC
            if let ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) = v {
                parse_date_str(s)
            } else {
                None
            }
        }),

        Expr::ScalarFunction(sf) => {
            let name = sf.name().to_ascii_lowercase();
            let args = &sf.args;

            // Support: to_timestamp*(literal)
            if name == "to_timestamp"
                || name == "to_timestamp_seconds"
                || name == "to_timestamp_millis"
                || name == "to_timestamp_micros"
                || name == "to_timestamp_nanos"
            {
                if args.len() != 1 {
                    return None;
                }

                // numeric seconds/millis/micros/nanos OR RFC3339 string
                if let Some(dt) = parse_ts_literal(&args[0]) {
                    return Some(dt);
                }

                let n = expr_to_numeric(&args[0])?;
                return match name.as_str() {
                    "to_timestamp" | "to_timestamp_seconds" => unix_seconds_to_datetime(n),
                    "to_timestamp_millis" => unix_seconds_to_datetime(n / 1_000.0),
                    "to_timestamp_micros" => unix_seconds_to_datetime(n / 1_000_000.0),
                    "to_timestamp_nanos" => unix_seconds_to_datetime(n / 1_000_000_000.0),
                    _ => None,
                };
            }

            None
        }

        _ => None,
    }
}

fn scalar_to_bool(v: &ScalarValue) -> Option<bool> {
    match v {
        ScalarValue::Boolean(Some(b)) => Some(*b),
        _ => None,
    }
}

fn interval_from_scalar(v: &ScalarValue) -> Option<UnifiedInterval> {
    match v {
        ScalarValue::IntervalMonthDayNano(Some(v)) => Some(UnifiedInterval {
            months: v.months,
            days: v.days,
            nanos: v.nanoseconds,
        }),
        ScalarValue::IntervalDayTime(Some(v)) => Some(UnifiedInterval {
            months: 0,
            days: v.days,
            nanos: (v.milliseconds as i64) * 1_000_000,
        }),
        ScalarValue::IntervalYearMonth(Some(v)) => Some(UnifiedInterval {
            months: *v,
            days: 0,
            nanos: 0,
        }),
        _ => None,
    }
}

// Flatten an expression of +/- into a net interval applied to ts.
// Supports: ts, ts + iv, iv + ts, ts + iv + iv, iv + ts - iv, etc.
fn extract_ts_with_interval(expr: &Expr, ts_col: &str) -> Option<UnifiedInterval> {
    // Return net interval to be ADDED to ts, puls whether subtree contains ts.
    fn walk(expr: &Expr, ts_col: &str) -> Option<(UnifiedInterval, bool)> {
        if expr_is_ts(expr, ts_col) {
            return Some((UnifiedInterval::zero(), true));
        }

        if let Expr::BinaryExpr(be) = expr
            && matches!(be.op, Operator::Plus | Operator::Minus)
        {
            let (left_iv, left_has_ts) = walk(&be.left, ts_col)?;
            let (right_iv, right_has_ts) = walk(&be.right, ts_col)?;

            // Reject if ts appears on both sides or neither side.
            if left_has_ts == right_has_ts {
                return None;
            }

            if be.op == Operator::Plus {
                // commutative: L + (ts + R) => ts + (R + L)
                if left_has_ts {
                    return Some((left_iv.add(right_iv, 1), true));
                } else {
                    return Some((right_iv.add(left_iv, 1), true));
                }
            }

            // Minus: only allow ts on the left (ts - R)
            if left_has_ts {
                return Some((left_iv.add(right_iv, -1), true));
            }
            return None;
        }

        // Not ts, not +/: if it's an interval literal, return it (no ts).
        if let Expr::Literal(v, _) = expr
            && let Some(iv) = interval_from_scalar(v)
        {
            return Some((iv, false));
        }
        None
    }

    let (net, has_ts) = walk(expr, ts_col)?;
    if has_ts { Some(net) } else { None }
}

pub(crate) fn add_interval(
    dt: DateTime<Utc>,
    iv: UnifiedInterval,
    sign: i32,
) -> Option<DateTime<Utc>> {
    use chrono::{Duration, Months};

    let mut out = dt;
    let months = iv.months.saturating_mul(sign);
    if months != 0 {
        if months > 0 {
            out = out.checked_add_months(Months::new(months as u32))?;
        } else {
            out = out.checked_sub_months(Months::new((-months) as u32))?;
        }
    }

    let days = iv.days.saturating_mul(sign);
    if days != 0 {
        out = out.checked_add_signed(Duration::days(days as i64))?;
    }

    let nanos = iv.nanos.saturating_mul(sign as i64);
    if nanos != 0 {
        out = out.checked_add_signed(Duration::nanoseconds(nanos))?;
    }

    Some(out)
}

/// Flip comparison direction when operands are swapped.
fn flip_op(op: Operator) -> Option<Operator> {
    match op {
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Eq => Some(Operator::Eq),
        Operator::NotEq => Some(Operator::NotEq),
        _ => None,
    }
}

/// ScalarFunction handle
fn match_ts_transform(expr: &Expr, ts_col: &str) -> Option<TsTransform> {
    // Unwrap wrappers around scalar functions
    let e = unwrap_expr(expr);

    if let Expr::ScalarFunction(sf) = e {
        let name = sf.name();

        if name.eq_ignore_ascii_case("to_unixtime")
            && sf.args.len() == 1
            && expr_is_ts(&sf.args[0], ts_col)
        {
            return Some(TsTransform::ToUnixtime);
        }

        if name.eq_ignore_ascii_case("to_date")
            && sf.args.len() == 1
            && expr_is_ts(&sf.args[0], ts_col)
        {
            return Some(TsTransform::ToDate);
        }
    }

    None
}

fn parse_date_literal(expr: &Expr) -> Option<NaiveDate> {
    match unwrap_expr(expr) {
        Expr::Literal(v, _) => match v {
            ScalarValue::Date32(Some(days)) => {
                // Date32 is days since epoch
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
                Some(epoch.checked_add_signed(Duration::days(*days as i64))?)
            }
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
            }
            _ => scalar_to_utc_datetime(v).map(|dt| dt.date_naive()),
        },
        _ => None,
    }
}

fn date_bounds_utc(
    date: NaiveDate,
    tz: Option<&ParsedTz>,
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let tz = tz?;
    let start_naive = date.and_hms_opt(0, 0, 0)?;
    let end_naive = (date + Duration::days(1)).and_hms_opt(0, 0, 0)?;

    match tz {
        ParsedTz::Utc => {
            let s = Utc.from_utc_datetime(&start_naive);
            let e = Utc.from_utc_datetime(&end_naive);
            Some((s, e))
        }
        ParsedTz::Fixed(off) => {
            let s = off.from_local_datetime(&start_naive).single()?;
            let e = off.from_local_datetime(&end_naive).single()?;
            Some((s.with_timezone(&Utc), e.with_timezone(&Utc)))
        }
        ParsedTz::Olson(tz) => {
            let start = tz.from_local_datetime(&start_naive);
            let end = tz.from_local_datetime(&end_naive);

            let pick = |res| match res {
                chrono::LocalResult::Single(x) => Some(vec![x]),
                chrono::LocalResult::Ambiguous(a, b) => Some(vec![a, b]),
                chrono::LocalResult::None => None,
            };

            let starts = pick(start)?;
            let ends = pick(end)?;

            let min_start = starts.iter().min().map(|dt| dt.with_timezone(&Utc))?;
            let max_end = ends.iter().max().map(|dt| dt.with_timezone(&Utc))?;

            Some((min_start, max_end))
        }
    }
}

// --------------------------- compile ---------------------------------------

fn compile_between(b: &Between, ts_col: &str) -> TimePred {
    if !expr_is_ts(&b.expr, ts_col) {
        return TimePred::Unknown;
    }

    let low = match parse_ts_literal(&b.low) {
        Some(dt) => dt,
        None => return TimePred::Unknown,
    };

    let high = match parse_ts_literal(&b.high) {
        Some(dt) => dt,
        None => return TimePred::Unknown,
    };

    let inner = TimePred::and(
        TimePred::Cmp {
            op: Operator::GtEq,
            ts: low,
        },
        TimePred::Cmp {
            op: Operator::LtEq,
            ts: high,
        },
    );

    if b.negated {
        TimePred::not(inner)
    } else {
        inner
    }
}

fn compile_in_list(il: &InList, ts_col: &str) -> TimePred {
    if !expr_is_ts(&il.expr, ts_col) {
        return TimePred::Unknown;
    }

    // Only handle literal datetime values. Otherwise: Unknown(safe)
    let mut dts = Vec::with_capacity(il.list.len());
    for e in &il.list {
        match parse_ts_literal(e) {
            Some(dt) => dts.push(dt),
            None => return TimePred::Unknown,
        }
    }

    // IN () edge-case:
    // - expr IN () is always false
    // - expr NOT IN () is always true (no constraint)
    if dts.is_empty() {
        return if il.negated {
            TimePred::Unknown
        } else {
            TimePred::False
        };
    }

    // Build OR chian of Eq comparisons
    let mut p = TimePred::Cmp {
        op: Operator::Eq,
        ts: dts[0],
    };
    for dt in dts.into_iter().skip(1) {
        p = TimePred::or(
            p,
            TimePred::Cmp {
                op: Operator::Eq,
                ts: dt,
            },
        );
    }

    if il.negated { TimePred::not(p) } else { p }
}

fn compile_transform_cmp(
    tx: TsTransform,
    op: Operator,
    other: &Expr,
    tz: Option<&ParsedTz>,
) -> Option<TimePred> {
    match tx {
        TsTransform::ToUnixtime => {
            let secs = expr_to_numeric(other)?;
            let dt = unix_seconds_to_datetime(secs)?;
            Some(TimePred::Cmp { op, ts: dt })
        }

        TsTransform::ToDate => {
            let day0 = parse_date_literal(other)?;
            let (start, end) = date_bounds_utc(day0, tz)?;

            Some(match op {
                Operator::Eq => TimePred::and(
                    TimePred::Cmp {
                        op: Operator::GtEq,
                        ts: start,
                    },
                    TimePred::Cmp {
                        op: Operator::Lt,
                        ts: end,
                    },
                ),

                Operator::NotEq => TimePred::or(
                    TimePred::Cmp {
                        op: Operator::Lt,
                        ts: start,
                    },
                    TimePred::Cmp {
                        op: Operator::GtEq,
                        ts: end,
                    },
                ),

                Operator::Lt => TimePred::Cmp {
                    op: Operator::Lt,
                    ts: start,
                },

                Operator::LtEq => TimePred::Cmp {
                    op: Operator::Lt,
                    ts: end,
                },

                Operator::Gt => TimePred::Cmp {
                    op: Operator::GtEq,
                    ts: end,
                },

                Operator::GtEq => TimePred::Cmp {
                    op: Operator::GtEq,
                    ts: start,
                },

                _ => return None,
            })
        }
    }
}

fn compile_time_leaf_from_binary(
    left: &Expr,
    op: Operator,
    right: &Expr,
    ts_col: &str,
    tz: Option<&ParsedTz>,
) -> TimePred {
    // Only support comparison ops we can reason about at compile time.
    if !matches!(
        op,
        Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Eq
            | Operator::NotEq
    ) {
        return TimePred::Unknown;
    }

    // 1) ts OP literal_timestamp (or literal-producing scalar fn)
    if expr_is_ts(left, ts_col)
        && let Some(dt) = parse_ts_literal(right)
    {
        return TimePred::Cmp { op, ts: dt };
    }

    // 2) literal_timestamp OP ts (flip)
    if expr_is_ts(right, ts_col)
        && let Some(dt) = parse_ts_literal(left)
        && let Some(flop) = flip_op(op)
    {
        return TimePred::Cmp { op: flop, ts: dt };
    }

    // 3) (ts +/- interval +/- interval ...) OP literal_ts
    if let Some(net_iv) = extract_ts_with_interval(left, ts_col)
        && let Some(dt) = parse_ts_literal(right)
    {
        // ts + net_iv OP dt => ts OP (dt - net_iv)
        if let Some(shifted) = add_interval(dt, net_iv, -1) {
            return TimePred::Cmp { op, ts: shifted };
        }
    }

    // 4) literal_ts OP (ts +/- interval +/- interval ...) (flip)
    if let Some(net_iv) = extract_ts_with_interval(right, ts_col)
        && let Some(dt) = parse_ts_literal(left)
        && let Some(flop) = flip_op(op)
        && let Some(shifted) = add_interval(dt, net_iv, -1)
    {
        return TimePred::Cmp {
            op: flop,
            ts: shifted,
        };
    }

    // 5) scalar functions handle
    if let Some(tx) = match_ts_transform(left, ts_col)
        && let Some(tp) = compile_transform_cmp(tx, op, right, tz)
    {
        return tp;
    }
    if let Some(tx) = match_ts_transform(right, ts_col)
        && let Some(flop) = flip_op(op)
        && let Some(tp) = compile_transform_cmp(tx, flop, left, tz)
    {
        return tp;
    }

    // If it mentions ts but we don't understand it, keep Unknown (do not prune).
    TimePred::Unknown
}

pub(crate) fn compile_time_pred(expr: &Expr, ts_col: &str, tz: Option<&ParsedTz>) -> TimePred {
    if !expr_mentions_ts(expr, ts_col) {
        return TimePred::NonTime;
    }

    match expr {
        Expr::BinaryExpr(be) => {
            if be.op == Operator::And {
                return TimePred::and(
                    compile_time_pred(&be.left, ts_col, tz),
                    compile_time_pred(&be.right, ts_col, tz),
                );
            }
            if be.op == Operator::Or {
                return TimePred::or(
                    compile_time_pred(&be.left, ts_col, tz),
                    compile_time_pred(&be.right, ts_col, tz),
                );
            }

            // Leaf (comparisons, eq/ne, etc)
            compile_time_leaf_from_binary(&be.left, be.op, &be.right, ts_col, tz)
        }

        // DF Not variant
        Expr::Not(e) => TimePred::not(compile_time_pred(e, ts_col, tz)),

        // Literal bool: foldable
        Expr::Literal(v, _) => match scalar_to_bool(v) {
            Some(true) => TimePred::True,
            Some(false) => TimePred::False,
            None => TimePred::Unknown,
        },

        // Wrappers
        Expr::Alias(a) => compile_time_pred(&a.expr, ts_col, tz),
        Expr::Cast(c) => compile_time_pred(&c.expr, ts_col, tz),
        Expr::TryCast(c) => compile_time_pred(&c.expr, ts_col, tz),

        Expr::Between(b) => compile_between(b, ts_col),

        Expr::InList(il) => compile_in_list(il, ts_col),

        _ => TimePred::Unknown,
    }
}
