//! Ingest NVDA 1h sample data into a timeseries-table-format table using the core API.

use std::{fs::File, path::PathBuf, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_csv::ReaderBuilder;
use arrow_csv::reader::Format;
use timeseries_table_format::{
    metadata::table_metadata::{IndexKind, IndexSpec, TableMeta, TimeIndexGranularity},
    storage::TableLocation,
    table::TimeSeriesTable,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Resolve paths relative to the workspace root.
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()?;
    let csv_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/assets/nvda_1h_sample.csv");
    let table_root = workspace_root.join("examples/nvda_table");

    // Start clean so the example is repeatable.
    if tokio::fs::try_exists(&table_root).await? {
        tokio::fs::remove_dir_all(&table_root).await?;
    }

    // 1) Open the CSV as an Arrow batch reader.
    let schema = Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Int64, false),
    ]);

    let format = Format::default().with_header(true);
    let reader = ReaderBuilder::new(Arc::new(schema))
        .with_format(format)
        .build(File::open(&csv_path)?)?;

    // 2) Create a time-series table.
    let index = IndexSpec {
        column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        kind: IndexKind::Timestamp {
            index_granularity: TimeIndexGranularity::Hours(1),
            timezone: None,
        },
    };
    let meta = TableMeta::new_time_series(index);
    let location = TableLocation::local(&table_root);
    let mut table = TimeSeriesTable::create(location, meta).await?;

    // 3) Stream the CSV batches into a table-managed Parquet segment.
    let version = table.append(reader).await?;

    println!("Table root     : {}", table_root.display());
    println!("Committed ver. : {}", version);

    Ok(())
}
