//! Prepare and scan deterministic tables for the core scan RSS benchmark.

use std::cell::Cell;
use std::fs::File;
use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, BinaryBuilder, RecordBatch, RecordBatchIterator, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use chrono::Duration;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use timeseries_table_format::{
    AppendRequest,
    metadata::table_metadata::{IndexKind, IndexSpec, IndexValue, TableMeta, TimeBucket},
    storage::TableLocation,
    table::TimeSeriesTable,
};

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

fn generated_batch(
    schema: &Arc<Schema>,
    group: usize,
    rows_per_group: usize,
    payload_bytes: usize,
) -> Result<RecordBatch, ArrowError> {
    let first = group
        .checked_mul(rows_per_group)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| ArrowError::ComputeError("timestamp overflow".to_string()))?;
    let end = first
        .checked_add(
            i64::try_from(rows_per_group)
                .map_err(|_| ArrowError::ComputeError("rows per group exceed i64".to_string()))?,
        )
        .ok_or_else(|| ArrowError::ComputeError("timestamp overflow".to_string()))?;
    let payload_capacity = rows_per_group
        .checked_mul(payload_bytes)
        .ok_or_else(|| ArrowError::ComputeError("row-group payload size overflow".to_string()))?;

    let timestamps =
        TimestampMillisecondArray::from_iter_values((first..end).map(|value| value * 1_000));
    let mut payloads = BinaryBuilder::with_capacity(rows_per_group, payload_capacity);
    let mut payload = vec![0xA5; payload_bytes];
    for row in first..end {
        let row = row.to_le_bytes();
        let prefix_len = payload.len().min(row.len());
        payload[..prefix_len].copy_from_slice(&row[..prefix_len]);
        payloads.append_value(&payload);
    }
    let columns: Vec<ArrayRef> = vec![Arc::new(timestamps), Arc::new(payloads.finish())];
    RecordBatch::try_new(schema.clone(), columns)
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

    let total_rows = row_groups
        .checked_mul(rows_per_group)
        .ok_or_else(|| invalid_data("total row count overflow"))?;
    total_rows
        .checked_mul(1_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| invalid_data("timestamps exceed i64"))?;
    let expected_rows_per_group = i64::try_from(rows_per_group)?;

    let index = IndexSpec {
        column: TIME_COLUMN.to_string(),
        entity_columns: Vec::new(),
        kind: IndexKind::Timestamp {
            bucket: TimeBucket::Seconds(1),
            timezone: None,
        },
    };
    let location = TableLocation::local(&table_root);
    let mut table = TimeSeriesTable::create(location, TableMeta::new_time_series(index)).await?;
    let schema = benchmark_schema();
    let max_generated_batch_memory_bytes = Cell::new(0);
    {
        let batches = (0..row_groups).map(|group| {
            let batch = generated_batch(&schema, group, rows_per_group, payload_bytes)?;
            max_generated_batch_memory_bytes.set(
                max_generated_batch_memory_bytes
                    .get()
                    .max(batch.get_array_memory_size()),
            );
            Ok(batch)
        });
        let reader = RecordBatchIterator::new(batches, schema.clone());
        table
            .append(AppendRequest::new(reader).max_rows_per_row_group(rows_per_group))
            .await?;
    }

    let segment = table
        .state()
        .segments
        .values()
        .next()
        .ok_or_else(|| invalid_data("prepared table has no segment"))?;
    if table.state().segments.len() != 1 || segment.row_count != u64::try_from(total_rows)? {
        return Err(invalid_data("prepared table has unexpected segment metadata").into());
    }
    let segment_path = segment.path.clone();
    let segment_file = table_root.join(&segment_path);
    let segment_file_bytes = std::fs::metadata(&segment_file)?.len();
    let metadata = ParquetRecordBatchReaderBuilder::try_new(File::open(&segment_file)?)?;
    if metadata.metadata().num_row_groups() != row_groups
        || metadata
            .metadata()
            .row_groups()
            .iter()
            .any(|row_group| row_group.num_rows() != expected_rows_per_group)
    {
        return Err(invalid_data("configured row-group shape was not preserved").into());
    }
    let max_row_group_bytes = metadata
        .metadata()
        .row_groups()
        .iter()
        .map(|row_group| u64::try_from(row_group.total_byte_size()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0);

    println!(
        "{}",
        serde_json::to_string(&json!({
            "mode": "prepare",
            "table_path": table_root,
            "segment_path": segment_path,
            "segment_file_bytes": segment_file_bytes,
            "row_group_count": row_groups,
            "rows_per_row_group": rows_per_group,
            "total_rows": total_rows,
            "payload_bytes_per_row": payload_bytes,
            "max_generated_batch_memory_bytes": max_generated_batch_memory_bytes.get(),
            "max_row_group_bytes": max_row_group_bytes,
            "process_id": std::process::id(),
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
    let (start, max) = match (&segment.index_min, &segment.index_max) {
        (IndexValue::Timestamp(start), IndexValue::Timestamp(max)) => (*start, *max),
        _ => return Err(invalid_data("expected timestamp segment bounds").into()),
    };
    let end = max
        .checked_add_signed(Duration::milliseconds(1))
        .ok_or_else(|| invalid_data("scan end timestamp overflow"))?;

    let started = Instant::now();
    let mut batches = table.scan_range(start, end).await?;
    let mut first_batch_ns = None;
    let mut batch_count = 0_u64;
    let mut row_count = 0_u64;
    let mut max_batch_memory_bytes = 0_usize;

    while let Some(batch) = batches.next().await.transpose()? {
        if batch.num_rows() == 0 {
            return Err(invalid_data("scan returned an empty batch").into());
        }
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
            "process_id": std::process::id(),
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
