use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use chrono::DateTime;

use chrono::NaiveDate;
use chrono::TimeZone;
use chrono::Utc;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::common::DFSchema;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfigBuilder;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Between;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::expressions::lit;
use timeseries_table_core::storage::TableLocation;
use timeseries_table_core::time_series_table::TimeSeriesTable;
use timeseries_table_core::transaction_log::SegmentMeta;
use timeseries_table_core::transaction_log::TableState;
use tokio::sync::RwLock;

/// DataFusion table provider for a timeseries table schema.
///
/// The schema is captured when the provider is constructed. If the table schema
/// evolves, re-register a new provider to pick up the updated schema.
#[derive(Debug)]
pub struct TsTableProvider {
    table: Arc<TimeSeriesTable>,
    schema: SchemaRef,
    cache: RwLock<Cache>,

    // Baseline: local filesystem only
    object_store_url: ObjectStoreUrl,
}

#[derive(Debug)]
struct Cache {
    version: Option<u64>,
    state: Option<TableState>,
}

/// Wrap a generic error for DataFusion APIs.
fn df_external<E>(e: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(e))
}

/// Build a DataFusion execution error from a message.
fn df_exec(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(msg.into())
}

fn unwrap_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(a) => unwrap_expr(&a.expr),
        Expr::Cast(c) => unwrap_expr(&c.expr),
        Expr::TryCast(c) => unwrap_expr(&c.expr),
        other => other,
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
fn expr_mentions_ts(expr: &Expr, ts_col: &str) -> bool {
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

        _ => false,
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

fn parse_date_str(s: &str) -> Option<DateTime<Utc>> {
    // Accept YYYY-MM-DD as midnight UTC
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    Some(Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0)?))
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

/// If the expression is `ts OP literal`, return the operator and timestamp.
fn match_time_comparison(
    col_expr: &Expr,
    op: Operator,
    lit_expr: &Expr,
    ts_col: &str,
) -> Option<(Operator, DateTime<Utc>)> {
    if !expr_is_ts(col_expr, ts_col) {
        return None;
    }

    if !matches!(
        op,
        Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Eq
            | Operator::NotEq
    ) {
        return None;
    }

    let dt = parse_ts_literal(lit_expr)?;
    Some((op, dt))
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum TimePred {
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
    fn and(a: TimePred, b: TimePred) -> TimePred {
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

    fn or(a: TimePred, b: TimePred) -> TimePred {
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

    fn not(x: TimePred) -> TimePred {
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

fn scalar_to_bool(v: &ScalarValue) -> Option<bool> {
    match v {
        ScalarValue::Boolean(Some(b)) => Some(*b),
        _ => None,
    }
}

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

fn compile_time_leaf_from_binary(
    left: &Expr,
    op: Operator,
    right: &Expr,
    ts_col: &str,
) -> TimePred {
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

    // If it mentions ts but we don't understand it, keep Unknown (do not prune).
    TimePred::Unknown
}

fn compile_time_pred(expr: &Expr, ts_col: &str) -> TimePred {
    if !expr_mentions_ts(expr, ts_col) {
        return TimePred::NonTime;
    }

    match expr {
        Expr::BinaryExpr(be) => {
            if be.op == Operator::And {
                return TimePred::and(
                    compile_time_pred(&be.left, ts_col),
                    compile_time_pred(&be.right, ts_col),
                );
            }
            if be.op == Operator::Or {
                return TimePred::or(
                    compile_time_pred(&be.left, ts_col),
                    compile_time_pred(&be.right, ts_col),
                );
            }

            // Leaf (comparisons, eq/ne, etc)
            compile_time_leaf_from_binary(&be.left, be.op, &be.right, ts_col)
        }

        // DF Not variant
        Expr::Not(e) => TimePred::not(compile_time_pred(e, ts_col)),

        // Literal bool: foldable
        Expr::Literal(v, _) => match scalar_to_bool(v) {
            Some(true) => TimePred::True,
            Some(false) => TimePred::False,
            None => TimePred::Unknown,
        },

        // Wrappers
        Expr::Alias(a) => compile_time_pred(&a.expr, ts_col),
        Expr::Cast(c) => compile_time_pred(&c.expr, ts_col),
        Expr::TryCast(c) => compile_time_pred(&c.expr, ts_col),

        Expr::Between(b) => compile_between(b, ts_col),

        Expr::InList(il) => compile_in_list(il, ts_col),

        _ => TimePred::Unknown,
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum IntervalTruth {
    AlwaysTrue,
    AlwaysFalse,
    MaybeTrue,
}

impl IntervalTruth {
    fn and(self, rhs: IntervalTruth) -> IntervalTruth {
        use IntervalTruth::*;
        match (self, rhs) {
            (AlwaysFalse, _) | (_, AlwaysFalse) => AlwaysFalse,
            (AlwaysTrue, x) | (x, AlwaysTrue) => x,
            _ => MaybeTrue,
        }
    }

    fn or(self, rhs: IntervalTruth) -> IntervalTruth {
        use IntervalTruth::*;
        match (self, rhs) {
            (AlwaysTrue, _) | (_, AlwaysTrue) => AlwaysTrue,
            (AlwaysFalse, x) | (x, AlwaysFalse) => x,
            _ => MaybeTrue,
        }
    }

    fn not(self) -> IntervalTruth {
        use IntervalTruth::*;
        match self {
            AlwaysTrue => AlwaysFalse,
            AlwaysFalse => AlwaysTrue,
            MaybeTrue => MaybeTrue,
        }
    }
}

fn eval_cmp_on_interval(
    op: Operator,
    dt: DateTime<Utc>,
    seg_min: DateTime<Utc>,
    seg_max: DateTime<Utc>,
) -> IntervalTruth {
    match op {
        Operator::Lt => {
            if seg_max < dt {
                IntervalTruth::AlwaysTrue
            } else if seg_min >= dt {
                IntervalTruth::AlwaysFalse
            } else {
                IntervalTruth::MaybeTrue
            }
        }
        Operator::LtEq => {
            if seg_max <= dt {
                IntervalTruth::AlwaysTrue
            } else if seg_min > dt {
                IntervalTruth::AlwaysFalse
            } else {
                IntervalTruth::MaybeTrue
            }
        }
        Operator::Gt => {
            if seg_min > dt {
                IntervalTruth::AlwaysTrue
            } else if seg_max <= dt {
                IntervalTruth::AlwaysFalse
            } else {
                IntervalTruth::MaybeTrue
            }
        }
        Operator::GtEq => {
            if seg_min >= dt {
                IntervalTruth::AlwaysTrue
            } else if seg_max < dt {
                IntervalTruth::AlwaysFalse
            } else {
                IntervalTruth::MaybeTrue
            }
        }
        Operator::Eq => {
            if dt < seg_min || dt > seg_max {
                IntervalTruth::AlwaysFalse
            } else if seg_min == seg_max && seg_min == dt {
                IntervalTruth::AlwaysTrue
            } else {
                IntervalTruth::MaybeTrue
            }
        }
        Operator::NotEq => {
            // Only definitely false if segment is exactly one timestamp equal to dt.
            if seg_min == seg_max && seg_min == dt {
                IntervalTruth::AlwaysFalse
            } else if dt < seg_min || dt > seg_max {
                IntervalTruth::AlwaysTrue
            } else {
                IntervalTruth::MaybeTrue
            }
        }
        _ => IntervalTruth::MaybeTrue,
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
fn eval_time_pred_on_segment(
    pred: &TimePred,
    seg_min: DateTime<Utc>,
    seg_max: DateTime<Utc>,
) -> IntervalTruth {
    match pred {
        TimePred::True => IntervalTruth::AlwaysTrue,
        TimePred::False => IntervalTruth::AlwaysFalse,
        TimePred::Unknown => IntervalTruth::MaybeTrue,
        TimePred::NonTime => IntervalTruth::MaybeTrue,
        TimePred::Cmp { op, ts } => eval_cmp_on_interval(*op, *ts, seg_min, seg_max),
        TimePred::And(a, b) => eval_time_pred_on_segment(a, seg_min, seg_max)
            .and(eval_time_pred_on_segment(b, seg_min, seg_max)),
        TimePred::Or(a, b) => eval_time_pred_on_segment(a, seg_min, seg_max)
            .or(eval_time_pred_on_segment(b, seg_min, seg_max)),
        TimePred::Not(x) => eval_time_pred_on_segment(x, seg_min, seg_max).not(),
    }
}

impl TsTableProvider {
    /// Creates a new provider backed by the given `TimeSeriesTable`.
    pub fn try_new(table: Arc<TimeSeriesTable>) -> DFResult<Self> {
        // Use the table's current in-memory snapshot to get schema.
        // (No schema evolution in v0.1, so this is stable.)
        let schema = table
            .state()
            .table_meta
            .arrow_schema_ref()
            .map_err(df_external)?;

        let object_store_url = ObjectStoreUrl::parse("file://").map_err(df_external)?; // baseline: local FS

        Ok(Self {
            table,
            schema,
            cache: RwLock::new(Cache {
                version: None,
                state: None,
            }),
            object_store_url,
        })
    }

    async fn latest_state(&self) -> DFResult<TableState> {
        let current_version = self.table.current_version().await.map_err(df_external)?;

        // Fast path: cache hit
        {
            let cache = self.cache.read().await;
            if cache.version == Some(current_version)
                && let Some(st) = cache.state.clone()
            {
                return Ok(st);
            }
        }

        // Refresh from log
        let state = self.table.load_latest_state().await.map_err(df_external)?;
        let mut cache = self.cache.write().await;
        cache.version = Some(state.version);
        cache.state = Some(state.clone());
        Ok(state)
    }

    fn segment_abs_path(&self, seg: &SegmentMeta) -> DFResult<PathBuf> {
        match self.table.location() {
            TableLocation::Local(root) => Ok(root.join(&seg.path)),
        }
    }

    async fn segment_file_size(&self, seg: &SegmentMeta) -> datafusion::error::Result<u64> {
        if let Some(sz) = seg.file_size {
            return Ok(sz);
        }

        // For baseline local FS, fallback to stat if missing (keeps provider usable for older tables).
        match self.table.location() {
            TableLocation::Local(root) => {
                let abs = root.join(&seg.path);
                let meta = tokio::fs::metadata(&abs).await.map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "missing SegmentMeta.file_size and failed to stat file: {} ({})",
                        abs.display(),
                        e
                    ))
                })?;
                Ok(meta.len())
            }
        }
    }

    /// Return the time column name from the table's index spec.
    fn time_column_name(&self) -> &str {
        self.table.index_spec().timestamp_column.as_str()
    }

    fn prune_segments_by_time<'a>(
        &self,
        segments: Vec<&'a SegmentMeta>,
        filters: &[Expr],
    ) -> Vec<&'a SegmentMeta> {
        let ts_col = self.time_column_name();

        let mut saw_any_ts = false;
        let mut compiled = TimePred::True;

        for f in filters {
            if expr_mentions_ts(f, ts_col) {
                saw_any_ts = true;
                compiled = TimePred::and(compiled, compile_time_pred(f, ts_col))
            }
        }

        if !saw_any_ts {
            return segments;
        }

        // Prune only if definitely false for that segment.
        segments
            .into_iter()
            .filter(|seg| {
                eval_time_pred_on_segment(&compiled, seg.ts_min, seg.ts_max)
                    != IntervalTruth::AlwaysFalse
            })
            .collect()
    }
}

#[async_trait]
impl TableProvider for TsTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Inexact: we may prune files, and Parquet may prune row groups/pages,
        // but DataFusion will still apply the filter for correctness.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr], // may include all WHERE predicates
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // 1) Get a snapshot (TableState) from core table
        let snapshot = self.latest_state().await?;

        let segments = snapshot.segments_sorted_by_time();

        let df_schema = DFSchema::try_from(self.schema().as_ref().clone())?;
        let predicate = conjunction(filters.to_vec());
        let predicate = predicate
            .map(|p| state.create_physical_expr(p, &df_schema))
            .transpose()?
            .unwrap_or_else(|| lit(true));

        // Build Parquet scan plan (DataSourceExec + ParquetSource)
        let parquet_source = Arc::new(ParquetSource::default().with_predicate(predicate));

        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.clone(),
            self.schema.clone(),
            parquet_source,
        )
        .with_projection_indices(projection.cloned())
        .with_limit(limit);

        let selected = self.prune_segments_by_time(segments, filters);
        for seg in selected {
            let abs = self.segment_abs_path(seg)?;
            let abs = tokio::fs::canonicalize(&abs).await.map_err(|e| {
                df_exec(format!(
                    "failed to canonicalize segment path {}: {}",
                    abs.display(),
                    e
                ))
            })?;

            let file_size = self.segment_file_size(seg).await?;

            // PartitionedFile takes a "location" string. For local filesystem Parquet scans, passing
            // an absolute path is fine (DataFusion's local filesystem object store can open it).
            let pf = PartitionedFile::new(abs.display().to_string(), file_size);

            builder = builder.with_file(pf);
        }

        // Produce the execution plan
        let plan = DataSourceExec::from_data_source(builder.build());
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
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
}
