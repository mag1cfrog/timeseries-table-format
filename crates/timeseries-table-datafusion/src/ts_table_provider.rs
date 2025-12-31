use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use timeseries_table_core::time_series_table::TimeSeriesTable;
use timeseries_table_core::transaction_log::TableState;
use tokio::sync::RwLock;

/// DataFusion table provider for a timeseries table schema.
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
    state: Option<TableState>
}

fn df_external<E>(e: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(e))
}

impl TsTableProvider {
    /// Creates a new provider backed by the given Arrow schema.
    pub fn try_new(table: Arc<TimeSeriesTable>) -> DFResult<Self> {
        // Use the table's current in-memory snapshot to get schema.
        // (No schema evolution in v0.1, so this is stable.)
        let schema = table.state().table_meta.arrow_schema_ref().map_err(df_external)?;

        let object_store_url = ObjectStoreUrl::parse("file://").map_err(df_external)?; // baseline: local FS

        Ok(Self { table, schema, cache: RwLock::new(Cache { version: None, state: None }), object_store_url })

    }

    async fn latest_state(&self) -> DFResult<TableState> {
        let current_version = self.table.current_version().await.map_err(df_external)?;

        // Fast path: cache hit
        {
            let cache = self.cache.read().await;
            if cache.version == Some(current_version) {
                if let Some(st) = cache.state.clone() {
                    return Ok(st);
                }
            }
        }

        // Refresh from log
        let state = self.table.load_latest_state().await.map_err(df_external)?;
        let mut cache = self.cache.write().await;
        cache.version = Some(state.version);
        cache.state = Some(state.clone());
        Ok(state)
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

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr], // will be empty until implement supports_filters_pushdown
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        
        // 1) Get a snapshot (TableState) from core table
        let snapshot = self
            .table
            .state().
    }
}
