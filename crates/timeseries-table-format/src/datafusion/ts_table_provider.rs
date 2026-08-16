mod segment_pruning;
#[cfg(test)]
mod tests;
mod timestamp_pruning;

use crate::storage::file_size;

use std::collections::HashSet;
use std::path::Path;
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

use crate::metadata::table_metadata::IndexKind;
use crate::table::TimeSeriesTable;
use crate::transaction_log::SegmentMeta;
use crate::transaction_log::TableState;
use datafusion::logical_expr::utils::{conjunction, expr_to_columns};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::expressions::lit;
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

        let object_store_url =
            ObjectStoreUrl::parse(table.location().object_store_url()).map_err(df_external)?;
        let state = table.state().clone();

        Ok(Self {
            table,
            schema,
            cache: RwLock::new(Cache {
                version: Some(state.version),
                state: Some(state),
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

    async fn segment_file_size(&self, seg: &SegmentMeta) -> datafusion::error::Result<u64> {
        if let Some(sz) = seg.file_size {
            return Ok(sz);
        }

        let sz = file_size(self.table.location().storage(), Path::new(&seg.path))
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "missing Segment.file_size and failed to stat file: {} ({})",
                    seg.path, e
                ))
            })?;
        Ok(sz)
    }

    /// Return the ordered-index column name from the table's index spec.
    fn index_column_name(&self) -> &str {
        self.table.index_spec().column.as_str()
    }

    fn prune_segments_by_index<'a>(
        &self,
        segments: Vec<&'a SegmentMeta>,
        filters: &[Expr],
        pruning_predicate: &Arc<dyn PhysicalExpr>,
    ) -> DFResult<Vec<&'a SegmentMeta>> {
        let mut columns = HashSet::new();
        for filter in filters {
            expr_to_columns(filter, &mut columns)?;
        }
        if !columns
            .iter()
            .any(|column| column.name == self.index_column_name())
        {
            return Ok(segments);
        }

        segment_pruning::prune_segments(
            &self.schema,
            self.table.index_spec(),
            segments,
            pruning_predicate,
        )
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

        for segment in snapshot.segments.values() {
            segment
                .validate_bounds(&self.table.index_spec().kind)
                .map_err(df_external)?;
        }

        let segments = snapshot.segments_sorted_by_index().map_err(df_external)?;

        let df_schema = DFSchema::try_from(self.schema().as_ref().clone())?;
        let exact_predicate = conjunction(filters.to_vec())
            .map(|p| state.create_physical_expr(p, &df_schema))
            .transpose()?
            .unwrap_or_else(|| lit(true));

        let pruning_predicate =
            if matches!(self.table.index_spec().kind, IndexKind::Timestamp { .. }) {
                let index_column = self.index_column_name();
                let index_type = self.schema.field_with_name(index_column)?.data_type();
                let normalized_filters = filters
                    .iter()
                    .cloned()
                    .map(|filter| {
                        timestamp_pruning::normalize_timestamp_predicate(
                            filter,
                            index_column,
                            index_type,
                        )
                    })
                    .collect::<DFResult<Vec<_>>>()?;

                if normalized_filters.as_slice() == filters {
                    Arc::clone(&exact_predicate)
                } else {
                    conjunction(normalized_filters)
                        .map(|p| state.create_physical_expr(p, &df_schema))
                        .transpose()?
                        .unwrap_or_else(|| lit(true))
                }
            } else {
                Arc::clone(&exact_predicate)
            };

        // Build Parquet scan plan (DataSourceExec + ParquetSource)
        let parquet_source =
            Arc::new(ParquetSource::default().with_predicate(Arc::clone(&exact_predicate)));

        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.clone(),
            self.schema.clone(),
            parquet_source,
        )
        .with_projection_indices(projection.cloned())
        .with_limit(limit);

        let selected = self.prune_segments_by_index(segments, filters, &pruning_predicate)?;
        for seg in selected {
            let file_size = self.segment_file_size(seg).await?;
            let location = self
                .table
                .location()
                .object_store_path(Path::new(&seg.path))
                .map_err(df_external)?;
            let pf = PartitionedFile::new(location.as_ref(), file_size);

            builder = builder.with_file(pf);
        }

        // Produce the execution plan
        let plan = DataSourceExec::from_data_source(builder.build());
        Ok(plan)
    }
}
