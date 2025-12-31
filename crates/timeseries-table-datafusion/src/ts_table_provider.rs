use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfigBuilder;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr], // will be empty until implement supports_filters_pushdown
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // 1) Get a snapshot (TableState) from core table
        let snapshot = self.latest_state().await?;

        let segments = snapshot.segments_sorted_by_time();

        // Build Parquet scan plan (DataSourceExec + ParquetSource)
        let parquet_source = Arc::new(ParquetSource::default());

        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.clone(),
            self.schema.clone(),
            parquet_source,
        )
        .with_projection_indices(projection.cloned())
        .with_limit(limit);

        for seg in segments {
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
