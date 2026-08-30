//! Compare streaming append with diagnostics disabled and enabled.

use std::{io, sync::Arc, time::Duration, time::Instant};

use arrow::{
    array::TimestampMillisecondArray,
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use tempfile::TempDir;
use timeseries_table_format::{
    metadata::{
        index::{IndexKind, IndexSpec, TimeIndexGranularity},
        logical_schema::{LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit},
        table::TableMeta,
    },
    storage::TableLocation,
    table::TimeSeriesTable,
};
use tracing::{Dispatch, instrument::WithSubscriber};
use tracing_subscriber::filter::LevelFilter;

const ITERATIONS: usize = 21;
const WARMUP_ITERATIONS: usize = 3;
const ROWS: usize = 10_000;
const BATCH_ROWS: usize = 1_000;

fn table_meta() -> Result<TableMeta, Box<dyn std::error::Error>> {
    let index = IndexSpec {
        column: "ts".to_string(),
        entity_columns: Vec::new(),
        kind: IndexKind::Timestamp {
            index_granularity: TimeIndexGranularity::Seconds(1),
            timezone: None,
        },
    };
    let schema = LogicalSchema::new(vec![LogicalField {
        name: "ts".to_string(),
        data_type: LogicalDataType::Timestamp {
            unit: LogicalTimestampUnit::Millis,
            timezone: None,
        },
        nullable: false,
    }])?;
    Ok(TableMeta::new_time_series_with_schema(index, schema))
}

fn batches() -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    )]));
    (0..ROWS)
        .step_by(BATCH_ROWS)
        .map(|start| {
            let end = (start + BATCH_ROWS).min(ROWS);
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(TimestampMillisecondArray::from_iter_values(
                    (start..end).map(|row| row as i64 * 1_000),
                ))],
            )
        })
        .collect()
}

fn dispatch(level: LevelFilter) -> Dispatch {
    Dispatch::new(
        tracing_subscriber::fmt()
            .with_max_level(level)
            .with_ansi(false)
            .without_time()
            .with_writer(io::sink)
            .finish(),
    )
}

async fn append_once(
    batches: &[RecordBatch],
    dispatch: &Dispatch,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()?).await?;
    let batches = batches.to_vec();
    let started = Instant::now();
    table
        .append(batches)
        .with_subscriber(dispatch.clone())
        .await?;
    Ok(started.elapsed())
}

async fn measure(
    batches: &[RecordBatch],
    disabled_dispatch: &Dispatch,
    enabled_dispatch: &Dispatch,
) -> Result<(Duration, Duration), Box<dyn std::error::Error>> {
    let mut disabled_samples = Vec::with_capacity(ITERATIONS);
    let mut enabled_samples = Vec::with_capacity(ITERATIONS);
    for iteration in 0..WARMUP_ITERATIONS + ITERATIONS {
        let (disabled, enabled) = if iteration.is_multiple_of(2) {
            (
                append_once(batches, disabled_dispatch).await?,
                append_once(batches, enabled_dispatch).await?,
            )
        } else {
            let enabled = append_once(batches, enabled_dispatch).await?;
            let disabled = append_once(batches, disabled_dispatch).await?;
            (disabled, enabled)
        };
        if iteration >= WARMUP_ITERATIONS {
            disabled_samples.push(disabled);
            enabled_samples.push(enabled);
        }
    }
    disabled_samples.sort_unstable();
    enabled_samples.sort_unstable();
    Ok((
        disabled_samples[ITERATIONS / 2],
        enabled_samples[ITERATIONS / 2],
    ))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let batches = batches()?;
    let disabled_dispatch = dispatch(LevelFilter::OFF);
    let enabled_dispatch = dispatch(LevelFilter::DEBUG);
    let (disabled, enabled) = measure(&batches, &disabled_dispatch, &enabled_dispatch).await?;
    let disabled_ms = disabled.as_secs_f64() * 1_000.0;
    let enabled_ms = enabled.as_secs_f64() * 1_000.0;

    println!("mode,iterations,rows,batches,median_ms");
    println!(
        "disabled,{ITERATIONS},{ROWS},{},{disabled_ms:.3}",
        batches.len()
    );
    println!(
        "enabled,{ITERATIONS},{ROWS},{},{enabled_ms:.3}",
        batches.len()
    );
    println!(
        "enabled_overhead_percent,{:.3}",
        (enabled_ms / disabled_ms - 1.0) * 100.0
    );
    Ok(())
}
