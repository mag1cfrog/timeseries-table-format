//! example doc

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use snafu::ResultExt;
use timeseries_table_core::{
    storage::TableLocation,
    time_series_table::TimeSeriesTable,
    transaction_log::{TableMeta, TimeBucket, TimeIndexSpec},
};

use crate::error::{
    AppendSegmentSnafu, CliResult, CreateTableSnafu, InvalidBucketSnafu, OpenTableSnafu,
    StorageSnafu,
};

mod engine;
mod error;

#[derive(Debug, Subcommand)]
enum Command {
    /// Create an empty time-series table (schema adopted from first append)
    Create {
        #[arg(long)]
        table: PathBuf,

        #[arg(long = "time-column")]
        time_column: String,

        /// e.g. 1h, 15m, 1d
        #[arg(long)]
        bucket: String,

        /// Optional IANA timezone string
        #[arg(long)]
        timezone: Option<String>,

        /// Repeatable entity column names
        #[arg(long = "entity")]
        entity: Vec<String>,
    },

    /// Append an existing Parquet file as a new segment
    Append {
        #[arg(long)]
        table: PathBuf,

        #[arg(long)]
        parquet: PathBuf,

        /// Override timestamp column name (default: from table metadata)
        #[arg(long = "time-column")]
        time_column: Option<String>,
    },
}

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

fn parse_time_bucket(spec: &str) -> CliResult<TimeBucket> {
    spec.parse::<TimeBucket>().context(InvalidBucketSnafu {
        spec: spec.to_string(),
    })
}

async fn create_table(table_root: &Path, meta: TableMeta) -> CliResult<()> {
    let location =
        TableLocation::parse(table_root.to_string_lossy().as_ref()).context(StorageSnafu)?;

    TimeSeriesTable::create(location, meta)
        .await
        .context(CreateTableSnafu {
            table: table_root.display().to_string(),
        })?;

    Ok(())
}

async fn cmd_create(
    table: &Path,
    time_column: String,
    bucket: String,
    timezone: Option<String>,
    entity_columns: Vec<String>,
) -> CliResult<()> {
    let bucket = parse_time_bucket(&bucket)?;

    let index = TimeIndexSpec {
        timestamp_column: time_column,
        bucket,
        timezone,
        entity_columns,
    };

    let meta = TableMeta::new_time_series(index);
    create_table(table, meta).await?;

    println!("Created table at {}", table.display());
    Ok(())
}

async fn open_table(location: TableLocation, table_root: &Path) -> CliResult<TimeSeriesTable> {
    TimeSeriesTable::open(location)
        .await
        .context(OpenTableSnafu {
            table: table_root.display().to_string(),
        })
}

async fn cmd_append(table: &Path, parquet: &Path, time_column: Option<String>) -> CliResult<()> {
    let location = TableLocation::parse(table.to_string_lossy().as_ref()).context(StorageSnafu)?;
    // Open first so we can read metadata for default ts column.
    let mut t = open_table(location.clone(), table).await?;

    let ts_col = match time_column {
        Some(c) => c,
        None => t.index_spec().timestamp_column.clone(),
    };

    let rel = location
        .ensure_parquet_under_root(parquet)
        .await
        .context(StorageSnafu)?;

    let rel_str = if cfg!(windows) {
        rel.to_string_lossy().replace('\\', "/")
    } else {
        rel.to_string_lossy().to_string()
    };

    t.append_parquet_segment(&rel_str, &ts_col)
        .await
        .context(AppendSegmentSnafu {
            table: table.display().to_string(),
        })?;

    println!("Appended segment: {rel_str}");
    Ok(())
}

async fn run() -> CliResult<()> {
    let cli = Cli::parse();

    match cli.cmd {
        Command::Create {
            table,
            time_column,
            bucket,
            timezone,
            entity,
        } => cmd_create(&table, time_column, bucket, timezone, entity).await,

        Command::Append {
            table,
            parquet,
            time_column,
        } => cmd_append(&table, &parquet, time_column).await,
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
