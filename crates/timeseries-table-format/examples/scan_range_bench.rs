//! Prepare and scan deterministic tables for the core scan RSS benchmark.

use std::fs::File;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BinaryBuilder, RecordBatch, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::Duration;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde_json::json;
use timeseries_table_format::{
    metadata::table_metadata::{TableMeta, TimeBucket, TimeIndexSpec},
    storage::TableLocation,
    table::TimeSeriesTable,
};

const SEGMENT_PATH: &str = "data/segment.parquet";
const TIME_COLUMN: &str = "ts";
const SCAN_BATCH_SIZE: usize = 8_192;

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Debug, Subcommand)]
enum Mode {
    /// Generate and append one deterministic Parquet segment.
    Prepare {
        #[arg(long)]
        table: PathBuf,
        #[arg(long)]
        row_groups: NonZeroUsize,
        #[arg(long)]
        rows_per_group: NonZeroUsize,
        #[arg(long)]
        payload_bytes: NonZeroUsize,
    },
    /// Scan the complete prepared segment and report incremental delivery.
    Scan {
        #[arg(long)]
        table: PathBuf,
    },
}

fn benchmark_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            TIME_COLUMN,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("payload", DataType::Binary, false),
    ]))
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn write_segment(
    path: &Path,
    row_groups: usize,
    rows_per_group: usize,
    payload_bytes: usize,
) -> Result<(u64, usize), Box<dyn std::error::Error>> {
    let total_rows = row_groups
        .checked_mul(rows_per_group)
        .ok_or_else(|| invalid_data("total row count overflow"))?;
    i64::try_from(total_rows).map_err(|_| invalid_data("timestamps exceed i64"))?;

    let schema = benchmark_schema();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_max_row_group_size(rows_per_group)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(properties))?;
    let payload = vec![0xA5; payload_bytes];
    let payload_capacity = rows_per_group
        .checked_mul(payload_bytes)
        .ok_or_else(|| invalid_data("row-group payload size overflow"))?;
    let mut max_batch_memory_bytes = 0;

    for group in 0..row_groups {
        let first = group
            .checked_mul(rows_per_group)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| invalid_data("timestamp overflow"))?;
        let end = first
            .checked_add(i64::try_from(rows_per_group)?)
            .ok_or_else(|| invalid_data("timestamp overflow"))?;
        let timestamps = TimestampMillisecondArray::from_iter_values(first..end);
        let mut payloads = BinaryBuilder::with_capacity(rows_per_group, payload_capacity);
        for _ in 0..rows_per_group {
            payloads.append_value(&payload);
        }
        let columns: Vec<ArrayRef> = vec![Arc::new(timestamps), Arc::new(payloads.finish())];
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        max_batch_memory_bytes = max_batch_memory_bytes.max(batch.get_array_memory_size());
        writer.write(&batch)?;
        writer.flush()?;
    }

    let metadata = writer.close()?;
    if metadata.num_row_groups() != row_groups {
        return Err(invalid_data(format!(
            "expected {row_groups} row groups, wrote {}",
            metadata.num_row_groups()
        ))
        .into());
    }

    Ok((u64::try_from(total_rows)?, max_batch_memory_bytes))
}

async fn prepare(
    table_root: PathBuf,
    row_groups: usize,
    rows_per_group: usize,
    payload_bytes: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if table_root.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("table path already exists: {}", table_root.display()),
        )
        .into());
    }

    let segment_path = table_root.join(SEGMENT_PATH);
    std::fs::create_dir_all(
        segment_path
            .parent()
            .ok_or_else(|| invalid_data("segment path has no parent"))?,
    )?;
    let (total_rows, max_generated_batch_memory_bytes) =
        write_segment(&segment_path, row_groups, rows_per_group, payload_bytes)?;
    let segment_file_bytes = std::fs::metadata(&segment_path)?.len();

    let index = TimeIndexSpec {
        timestamp_column: TIME_COLUMN.to_string(),
        entity_columns: Vec::new(),
        bucket: TimeBucket::Seconds(1),
        timezone: None,
    };
    let location = TableLocation::local(&table_root);
    let mut table = TimeSeriesTable::create(location, TableMeta::new_time_series(index)).await?;
    table
        .append_parquet_segment(SEGMENT_PATH, TIME_COLUMN)
        .await?;

    println!(
        "{}",
        serde_json::to_string(&json!({
            "mode": "prepare",
            "table_path": table_root,
            "segment_path": SEGMENT_PATH,
            "segment_file_bytes": segment_file_bytes,
            "row_group_count": row_groups,
            "rows_per_row_group": rows_per_group,
            "total_rows": total_rows,
            "payload_bytes_per_row": payload_bytes,
            "max_generated_batch_memory_bytes": max_generated_batch_memory_bytes,
        }))?
    );
    Ok(())
}

async fn scan(table_root: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let table = TimeSeriesTable::open(TableLocation::local(&table_root)).await?;
    if table.state().segments.len() != 1 {
        return Err(invalid_data(format!(
            "expected one segment, found {}",
            table.state().segments.len()
        ))
        .into());
    }
    let segment = table
        .state()
        .segments
        .values()
        .next()
        .ok_or_else(|| invalid_data("prepared table has no segment"))?;
    let segment_path = segment.path.clone();
    let expected_rows = segment.row_count;
    let start = segment.ts_min;
    let end = segment
        .ts_max
        .checked_add_signed(Duration::milliseconds(1))
        .ok_or_else(|| invalid_data("scan end timestamp overflow"))?;

    let started = Instant::now();
    let mut batches = table.scan_range(start, end).await?;
    let mut first_batch_ns = None;
    let mut batch_count = 0_u64;
    let mut row_count = 0_u64;
    let mut max_batch_memory_bytes = 0_usize;

    while let Some(batch) = batches.next().await.transpose()? {
        first_batch_ns.get_or_insert_with(|| started.elapsed().as_nanos());
        batch_count += 1;
        row_count = row_count
            .checked_add(u64::try_from(batch.num_rows())?)
            .ok_or_else(|| invalid_data("returned row count overflow"))?;
        max_batch_memory_bytes = max_batch_memory_bytes.max(batch.get_array_memory_size());
        drop(batch);
    }

    let total_elapsed_ns = started.elapsed().as_nanos();
    let first_batch_ns = first_batch_ns.ok_or_else(|| invalid_data("scan returned no batches"))?;
    if row_count != expected_rows {
        return Err(invalid_data(format!(
            "expected {expected_rows} rows, scanned {row_count}"
        ))
        .into());
    }
    if first_batch_ns >= total_elapsed_ns {
        return Err(invalid_data("first batch was not returned before scan completion").into());
    }

    println!(
        "{}",
        serde_json::to_string(&json!({
            "mode": "scan",
            "table_path": table_root,
            "segment_path": segment_path,
            "scan_batch_size": SCAN_BATCH_SIZE,
            "returned_batch_count": batch_count,
            "returned_row_count": row_count,
            "max_returned_batch_memory_bytes": max_batch_memory_bytes,
            "time_to_first_batch_ns": first_batch_ns,
            "total_elapsed_ns": total_elapsed_ns,
        }))?
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Args::parse().mode {
        Mode::Prepare {
            table,
            row_groups,
            rows_per_group,
            payload_bytes,
        } => {
            prepare(
                table,
                row_groups.get(),
                rows_per_group.get(),
                payload_bytes.get(),
            )
            .await
        }
        Mode::Scan { table } => scan(table).await,
    }
}
