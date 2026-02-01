//! Ingest NVDA 1h sample data into a timeseries-table-format table using the core API.

use std::{fs::File, path::PathBuf, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_csv::ReaderBuilder;
use arrow_csv::reader::Format;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use timeseries_table_core::{
    metadata::table_metadata::{TableMeta, TimeBucket, TimeIndexSpec},
    storage::TableLocation,
    table::TimeSeriesTable,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Resolve paths relative to the workspace root.
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..");
    let csv_path = workspace_root.join("examples/data/nvda_1h_sample.csv");
    let table_root = workspace_root.join("examples/nvda_table");
    let parquet_path = table_root.join("data/nvda_1h.parquet");

    // Start clean so the example is repeatable.
    if tokio::fs::try_exists(&table_root).await? {
        tokio::fs::remove_dir_all(&table_root).await?;
    }
    let data_dir = parquet_path
        .parent()
        .ok_or("parquet path missing parent directory")?;
    tokio::fs::create_dir_all(data_dir).await?;

    // 1) Load CSV into Arrow batches.
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
    let reader = ReaderBuilder::new(Arc::new(schema.clone()))
        .with_format(format)
        .build(File::open(&csv_path)?)?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }

    // 2) Write a single Parquet segment.
    let props = WriterProperties::builder().build();
    let parquet_file = File::create(&parquet_path)?;
    let mut writer = ArrowWriter::try_new(parquet_file, schema.clone(), Some(props))?;
    for batch in &batches {
        writer.write(batch)?;
    }
    writer.close()?;

    // 3) Create a time-series table.
    let index = TimeIndexSpec {
        timestamp_column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        bucket: TimeBucket::Hours(1),
        timezone: None,
    };
    let meta = TableMeta::new_time_series(index);
    let location = TableLocation::local(&table_root);
    let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

    // 4) Append the segment via the transaction log (OCC).
    let relative_path = location.ensure_parquet_under_root(&parquet_path).await?;
    let relative_str = relative_path
        .to_str()
        .ok_or("relative Parquet path is not valid UTF-8")?;
    let version = table.append_parquet_segment(relative_str, "ts").await?;

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("Table root     : {}", table_root.display());
    println!("Committed ver. : {}", version);
    println!("Rows ingested  : {}", rows);

    Ok(())
}
