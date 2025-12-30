use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use timeseries_table_core::time_series_table::TimeSeriesTable;

/// DataFusion table provider for a timeseries table schema.
#[derive(Debug)]
pub struct TsTableProvider {
    table: Arc<TimeSeriesTable>,
    schema: SchemaRef,
}

impl TsTableProvider {
    /// Creates a new provider backed by the given Arrow schema.
    pub fn new(table: Arc<TimeSeriesTable>, schema: SchemaRef) -> Self {
        Self { table, schema }
    }

    fn normalize_local_path(p: &Path) -> Result<String, DataFusionError> {
        let s = p
            .to_str()
            .ok_or_else(|| DataFusionError::External("non-utf8 path".into()))?;

        Ok(s.strip_prefix('/').unwrap_or(s).to_string())
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
