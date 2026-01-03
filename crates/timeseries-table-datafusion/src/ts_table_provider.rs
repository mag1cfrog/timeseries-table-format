#[cfg(test)]
mod tests;
mod time_predicate;
use arrow::datatypes::DataType;

use chrono::FixedOffset;

use chrono_tz::Tz;
pub(crate) use time_predicate::*;

mod pruning;
pub(crate) use pruning::*;

use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::common::DFSchema;

use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfigBuilder;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreUrl;

use datafusion::logical_expr::Expr;

use datafusion::logical_expr::TableProviderFilterPushDown;

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

#[derive(Debug, Clone, Copy)]
pub(crate) enum ParsedTz {
    Utc,
    Fixed(FixedOffset),
    Olson(Tz),
}

fn parse_tz(tz: &str) -> Option<ParsedTz> {
    if tz.eq_ignore_ascii_case("utc") {
        return Some(ParsedTz::Utc);
    }
    if let Ok(offset) = tz.parse::<FixedOffset>() {
        return Some(ParsedTz::Fixed(offset));
    }
    if let Ok(tz) = tz.parse::<Tz>() {
        return Some(ParsedTz::Olson(tz));
    }
    None
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

    fn ts_timezone(&self) -> Option<String> {
        let ts_col = self.time_column_name();
        let field = self.schema.field_with_name(ts_col).ok()?;
        match field.data_type() {
            DataType::Timestamp(_, Some(tz)) => Some(tz.to_string()),
            _ => None,
        }
    }

    fn prune_segments_by_time<'a>(
        &self,
        segments: Vec<&'a SegmentMeta>,
        filters: &[Expr],
    ) -> Vec<&'a SegmentMeta> {
        let ts_col = self.time_column_name();
        let tz_opt = self.ts_timezone();
        let parsed_tz = tz_opt.as_deref().and_then(parse_tz);

        let mut saw_any_ts = false;
        let mut compiled = TimePred::True;

        for f in filters {
            if expr_mentions_ts(f, ts_col) {
                saw_any_ts = true;
                compiled = TimePred::and(compiled, compile_time_pred(f, ts_col, parsed_tz.as_ref()))
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
