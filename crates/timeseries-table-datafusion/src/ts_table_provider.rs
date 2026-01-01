use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Duration;
use chrono::TimeZone;
use chrono::Utc;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfigBuilder;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
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

fn df_external<E>(e: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(e))
}

fn df_exec(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(msg.into())
}

#[derive(Debug, Clone, Default)]
struct TimeRange {
    // [start, end)
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
}

impl TimeRange {
    fn intersect(&mut self, other: &TimeRange) {
        self.start = match (self.start, other.start) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (None, x) => x,
            (x, None) => x,
        };

        self.end = match (self.end, other.end) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (None, x) => x,
            (x, None) => x,
        };
    }

    fn is_empty(&self) -> bool {
        match (self.start, self.end) {
            (Some(s), Some(e)) => s >= e,
            _ => false,
        }
    }

    /// Segment bounds are inclusive [ts_min, ts_max]. Keep segment if it overlaps [start, end).
    fn overlap_inclusive(&self, ts_min: DateTime<Utc>, ts_max: DateTime<Utc>) -> bool {
        if self.is_empty() {
            return false;
        }

        // keep if ts_max >= start AND ts_min < end
        if let Some(start) = self.start
            && ts_max < start
        {
            return false;
        }

        if let Some(end) = self.end
            && ts_min >= end
        {
            return false;
        }

        true
    }
}

fn is_ts_column(expr: &Expr, ts_col: &str) -> bool {
    match expr {
        Expr::Column(c) => c.name == ts_col,
        Expr::Alias(a) => is_ts_column(&a.expr, ts_col),
        Expr::Cast(c) => is_ts_column(&c.expr, ts_col),
        Expr::TryCast(c) => is_ts_column(&c.expr, ts_col),
        _ => false,
    }
}

fn scalar_to_datetime(v: &ScalarValue) -> Option<DateTime<Utc>> {
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
            let secs = *x / 1_000_000;
            let micros = (*x % 1_000_000) as u32;
            Some(Utc.timestamp_opt(secs, micros * 1000).single()?)
        }
        ScalarValue::TimestampNanosecond(Some(x), _) => {
            let secs = *x / 1_000_000_000;
            let nanos = (*x % 1_000_000_000) as u32;
            Some(Utc.timestamp_opt(secs, nanos).single()?)
        }

        _ => None,
    }
}

fn expr_to_datetime(expr: &Expr) -> Option<DateTime<Utc>> {
    match expr {
        Expr::Literal(v, _) => scalar_to_datetime(v),
        Expr::Alias(a) => expr_to_datetime(&a.expr),
        Expr::Cast(c) => expr_to_datetime(&c.expr),
        Expr::TryCast(c) => expr_to_datetime(&c.expr),
        _ => None,
    }
}

fn match_time_comparison(
    col_expr: &Expr,
    op: Operator,
    lit_expr: &Expr,
    ts_col: &str,
) -> Option<(Operator, DateTime<Utc>)> {
    if !is_ts_column(col_expr, ts_col) {
        return None;
    }
    let dt = expr_to_datetime(lit_expr)?;
    Some((op, dt))
}

fn flip_op(op: Operator) -> Option<Operator> {
    match op {
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        _ => None,
    }
}

fn apply_constraint(range: &mut TimeRange, op: Operator, dt: DateTime<Utc>) {
    match op {
        Operator::GtEq => range.start = Some(range.start.map_or(dt, |s| s.max(dt))),
        Operator::Gt => {
            let dt2 = dt + Duration::nanoseconds(1);
            range.start = Some(range.start.map_or(dt2, |s| s.max(dt2)));
        }
        Operator::Lt => {
            range.end = Some(range.end.map_or(dt, |e| e.min(dt)));
        }
        Operator::LtEq => {
            let dt2 = dt + Duration::nanoseconds(1);
            range.end = Some(range.end.map_or(dt2, |e| e.min(dt2)));
        }
        _ => {}
    }
}

fn collect_time_constraints(expr: &Expr, ts_col: &str, range: &mut TimeRange) -> bool {
    match expr {
        Expr::BinaryExpr(be) => {
            if be.op == Operator::And {
                let a = collect_time_constraints(&be.left, ts_col, range);
                let b = collect_time_constraints(&be.right, ts_col, range);
                return a || b;
            }

            // Comparison: try to interpret (ts OP literal) or (literal OP ts)
            if let Some((op, lit_dt)) = match_time_comparison(&be.left, be.op, &be.right, ts_col)
                .or_else(|| {
                    match_time_comparison(&be.right, be.op, &be.left, ts_col)
                        .and_then(|(op, dt)| flip_op(op).map(|op| (op, dt)))
                })
            {
                apply_constraint(range, op, lit_dt);
                return true;
            }

            false
        }

        // DataFusion may wrap comparisons in aliases/casts; handle those by drilling down.
        Expr::Alias(a) => collect_time_constraints(&a.expr, ts_col, range),
        Expr::Cast(c) => collect_time_constraints(&c.expr, ts_col, range),
        Expr::TryCast(c) => collect_time_constraints(&c.expr, ts_col, range),

        _ => false,
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

        // Build an effective [start, end) range by intersecting constraints from filters.
        let mut combined = TimeRange::default();
        let mut saw_any_time_predicate = false;

        for f in filters {
            let mut r = TimeRange::default();
            if collect_time_constraints(f, ts_col, &mut r) {
                combined.intersect(&r);
                saw_any_time_predicate = true;
            }
        }

        if !saw_any_time_predicate {
            return segments;
        }

        // If range is contradictory, return no segments.
        if combined.is_empty() {
            return vec![];
        }

        segments
            .into_iter()
            .filter(|seg| combined.overlap_inclusive(seg.ts_min, seg.ts_max))
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
        let ts_col = self.time_column_name();

        let v = filters
            .iter()
            .map(|f| {
                let mut r = TimeRange::default();
                if collect_time_constraints(f, ts_col, &mut r) {
                    // Inexact: we only use it for file pruning; DataFusion still applies the filter.
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        Ok(v)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr], // will be empty until implement supports_filters_pushdown
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // 1) Get a snapshot (TableState) from core table
        let snapshot = self.latest_state().await?;

        let segments = snapshot.segments_sorted_by_time();

        let selected = self.prune_segments_by_time(segments, filters);

        // Build Parquet scan plan (DataSourceExec + ParquetSource)
        let parquet_source = Arc::new(ParquetSource::default());

        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.clone(),
            self.schema.clone(),
            parquet_source,
        )
        .with_projection_indices(projection.cloned())
        .with_limit(limit);

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
