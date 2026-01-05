//! CLI tool for managing time-series tables.

mod engine;
mod error;
mod query;
mod shell;

use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, Subcommand, ValueEnum};
use snafu::ResultExt;
use timeseries_table_core::{
    storage::TableLocation,
    time_series_table::TimeSeriesTable,
    transaction_log::{TableMeta, TimeBucket, TimeIndexSpec},
};

use crate::{
    error::{
        AppendSegmentSnafu, CliError, CliResult, CreateTableSnafu, InvalidBucketSnafu,
        OpenTableSnafu, StorageSnafu,
    },
    query::{QueryOpts, page_output, print_query_result, write_query_result},
    shell::cmd_shell,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormatArg {
    Csv,
    Jsonl,
}

impl From<OutputFormatArg> for crate::query::OutputFormat {
    fn from(v: OutputFormatArg) -> Self {
        match v {
            OutputFormatArg::Csv => crate::query::OutputFormat::Csv,
            OutputFormatArg::Jsonl => crate::query::OutputFormat::Jsonl,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum BackendKind {
    DataFusion,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BackendArg {
    DataFusion,
}

impl From<BackendArg> for BackendKind {
    fn from(value: BackendArg) -> Self {
        match value {
            BackendArg::DataFusion => BackendKind::DataFusion,
        }
    }
}

fn make_engine(
    backend: BackendKind,
    table: &Path,
) -> Box<dyn engine::Engine<Error = error::CliError>> {
    match backend {
        BackendKind::DataFusion => Box::new(engine::DataFusionEngine::new(table)),
    }
}

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

        /// Print elapsed time for the append
        #[arg(long, default_value_t = false)]
        timing: bool,
    },

    /// Execute a SQL query via DataFusion against the table
    Query {
        #[arg(long)]
        table: PathBuf,

        #[arg(long)]
        sql: String,

        #[arg(long, default_value_t = false)]
        explain: bool,

        #[arg(long, default_value_t = false)]
        timing: bool,

        /// Page output through `less -S` (no truncation; horizontal scroll)
        #[arg(long, default_value_t = false)]
        pager: bool,

        #[arg(long, default_value_t = 10)]
        max_rows: usize,

        #[arg(long)]
        output: Option<PathBuf>,

        #[arg(long, value_enum, default_value_t = OutputFormatArg::Csv)]
        format: OutputFormatArg,

        /// Query backend (default: datafusion)
        #[arg(long, value_enum, default_value_t = BackendArg::DataFusion)]
        backend: BackendArg,
    },

    /// Interactive shell (keeps a live table handle; supports refresh/append/query)
    Shell {
        #[arg(long)]
        table: PathBuf,

        /// Optional history file path
        #[arg(long)]
        history: Option<PathBuf>,

        /// Backend (default: datafusion)
        #[arg(long, value_enum, default_value_t = BackendArg::DataFusion)]
        backend: BackendArg,
    },
}

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

struct QueryArgs {
    table: PathBuf,
    sql: String,
    explain: bool,
    timing: bool,
    pager: bool,
    max_rows: usize,
    output: Option<PathBuf>,
    format: OutputFormatArg,
    backend: BackendArg,
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

async fn cmd_append(
    table: &Path,
    parquet: &Path,
    time_column: Option<String>,
    timing: bool,
) -> CliResult<()> {
    let start = Instant::now();
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

    if timing {
        println!(
            "Appended segment: {rel_str} (elapsed_ms: {})",
            start.elapsed().as_millis()
        );
    } else {
        println!("Appended segment: {rel_str}");
    }
    Ok(())
}

async fn cmd_query_with_engine(
    engine: &dyn engine::Engine<Error = CliError>,
    sql: String,
    opts: QueryOpts,
    pager: bool,
) -> CliResult<()> {
    let session = engine.prepare_session().await?;
    if let Some(name) = session.table_name() {
        let quoted = query::quote_identifier(name);
        eprintln!("Registered table as '{}' (quoted: {quoted})", name);
    }

    let res = session.run_query(&sql, &opts).await?;
    if pager {
        let mut buf = Vec::new();
        write_query_result(&res, &opts, &mut buf)?;
        let rendered = String::from_utf8_lossy(&buf);
        page_output(&rendered)?;
    } else {
        print_query_result(&res, &opts)?;
    }
    Ok(())
}

async fn cmd_query(args: QueryArgs) -> CliResult<()> {
    let opts = query::QueryOpts {
        explain: args.explain,
        timing: args.timing,
        max_rows: args.max_rows,
        output: args.output,
        format: args.format.into(),
    };

    let engine = make_engine(args.backend.into(), &args.table);
    cmd_query_with_engine(engine.as_ref(), args.sql, opts, args.pager).await
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
            timing,
        } => cmd_append(&table, &parquet, time_column, timing).await,

        Command::Query {
            table,
            sql,
            explain,
            timing,
            pager,
            max_rows,
            output,
            format,
            backend,
        } => {
            cmd_query(QueryArgs {
                table,
                sql,
                explain,
                timing,
                pager,
                max_rows,
                output,
                format,
                backend,
            })
            .await
        }

        Command::Shell {
            table,
            history,
            backend,
        } => cmd_shell(table, history, backend).await,
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
