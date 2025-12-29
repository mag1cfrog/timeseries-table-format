use std::sync::Arc;

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

/// DataFusion table provider for a timeseries table schema.
#[derive(Debug)]
pub struct TsTableProvider {
    schema: Arc<Schema>,
}

impl TsTableProvider {
    /// Creates a new provider backed by the given Arrow schema.
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
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
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "TsTableProvider::scan is implemented in sub-issue #52".to_string(),
        ))
    }
}
