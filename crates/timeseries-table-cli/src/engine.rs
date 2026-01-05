use std::path::{Path, PathBuf};

use crate::{
    error::{CliError, CliResult},
    query::{self, QueryOpts, QueryResult, default_table_name},
};

#[allow(dead_code)]
#[async_trait::async_trait]
/// Minimal engine trait.
pub trait Engine: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn execute(&self, sql: &str, opts: &QueryOpts) -> Result<QueryResult, Self::Error>;

    fn table_name(&self) -> Option<&str> {
        None
    }
}

pub struct DataFusionEngine {
    table_root: PathBuf,
    table_name: String,
}

impl DataFusionEngine {
    pub fn new(table_root: impl AsRef<Path>) -> Self {
        let table_root = table_root.as_ref().to_path_buf();
        let table_name = default_table_name(&table_root);

        Self {
            table_root,
            table_name,
        }
    }

    pub async fn prepare_session(&self) -> CliResult<query::QuerySession> {
        query::prepare_session(&self.table_root).await
    }
}

#[async_trait::async_trait]
impl Engine for DataFusionEngine {
    type Error = CliError;

    async fn execute(&self, sql: &str, opts: &QueryOpts) -> Result<QueryResult, Self::Error> {
        let session = self.prepare_session().await?;
        let res = query::run_query(&session, sql, opts).await?;
        Ok(res)
    }

    fn table_name(&self) -> Option<&str> {
        Some(self.table_name.as_str())
    }
}
