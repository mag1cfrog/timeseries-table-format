//! Query the NVDA sample table with a 5-period moving average using DataFusion.

use std::{path::PathBuf, sync::Arc};

use datafusion::prelude::*;
use timeseries_table_core::{storage::TableLocation, table::TimeSeriesTable};
use timeseries_table_datafusion::{TsTableProvider, pretty::pretty_format_batches_compact_floats};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Resolve the table path produced by the ingestion example.
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()?;
    let table_root = workspace_root.join("examples/nvda_table");

    let table = TimeSeriesTable::open(TableLocation::local(&table_root)).await?;
    let provider = TsTableProvider::try_new(Arc::new(table))?;

    let ctx = SessionContext::new();
    ctx.register_table("nvda_1h", Arc::new(provider))?;

    let sql = r#"
        SELECT
            ts,
            close,
            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY ts
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS ma_5
        FROM nvda_1h
        WHERE ts >= '2024-06-01T00:00:00Z'
          AND ts <  '2024-06-04T00:00:00Z'
        ORDER BY ts
        LIMIT 15
    "#;

    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    let rendered = pretty_format_batches_compact_floats(&batches)?;
    println!("{rendered}");

    Ok(())
}
