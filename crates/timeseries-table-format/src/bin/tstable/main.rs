//! CLI tool for managing time-series tables.

mod engine;
mod error;
mod observability;
mod query;
mod shell;

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, Subcommand, ValueEnum};
use snafu::ResultExt;
use timeseries_table_format::{
    metadata::table_metadata::{IndexKind, IndexSpec, TableMeta, TimeBucket},
    storage::TableLocation,
    table::{OptimizeReport, TimeSeriesTable},
};

use crate::{
    error::{
        AppendSegmentSnafu, CliError, CliResult, CreateTableSnafu, InvalidBucketSnafu,
        OpenTableSnafu, OptimizeTableSnafu, StorageSnafu,
    },
    query::{
        QueryOpts, page_output, preview_message, print_query_result, render_preview,
        write_query_summary,
    },
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum IndexTypeArg {
    Timestamp,
    Int64,
    #[value(name = "uint64")]
    UInt64,
}

impl IndexTypeArg {
    fn name(self) -> &'static str {
        match self {
            Self::Timestamp => "timestamp",
            Self::Int64 => "int64",
            Self::UInt64 => "uint64",
        }
    }
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

        /// Ascending ordered-index column
        #[arg(long = "index-column")]
        index_column: String,

        /// Ordered-index value domain
        #[arg(long = "index-type", value_enum)]
        index_type: IndexTypeArg,

        /// Timestamp coverage bucket, e.g. 1h, 15m, 1d
        #[arg(long)]
        bucket: Option<String>,

        /// Optional timestamp IANA timezone
        #[arg(long)]
        timezone: Option<String>,

        /// Positive integer coverage bucket width in index-value units
        #[arg(long = "bucket-width", allow_hyphen_values = true)]
        bucket_width: Option<String>,

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

        /// Print elapsed time for the append
        #[arg(long, default_value_t = false)]
        timing: bool,
    },

    /// Rewrite mixed-entity segments into single-entity segments
    Optimize {
        #[arg(long)]
        table: PathBuf,
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
        table: Option<PathBuf>,

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

fn parse_bucket_width(spec: &str, index_type: IndexTypeArg) -> CliResult<NonZeroU64> {
    spec.parse::<NonZeroU64>()
        .map_err(|_| CliError::InvalidIndexOption {
            option: "--bucket-width",
            index_type: index_type.name(),
            reason: format!("'{spec}' is not an integer in 1..={}", u64::MAX),
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
    index_column: String,
    index_type: IndexTypeArg,
    bucket: Option<String>,
    bucket_width: Option<String>,
    timezone: Option<String>,
    entity_columns: Vec<String>,
) -> CliResult<()> {
    let invalid = |option, reason: &str| CliError::InvalidIndexOption {
        option,
        index_type: index_type.name(),
        reason: reason.to_string(),
    };

    let kind = match index_type {
        IndexTypeArg::Timestamp => {
            if bucket_width.is_some() {
                return Err(invalid(
                    "--bucket-width",
                    "use --bucket for timestamp indexes",
                ));
            }
            let bucket = bucket
                .as_deref()
                .ok_or_else(|| invalid("--bucket", "is required"))?;
            IndexKind::Timestamp {
                bucket: parse_time_bucket(bucket)?,
                timezone,
            }
        }
        IndexTypeArg::Int64 | IndexTypeArg::UInt64 => {
            if bucket.is_some() {
                return Err(invalid("--bucket", "is only valid for timestamp indexes"));
            }
            if timezone.is_some() {
                return Err(invalid("--timezone", "is only valid for timestamp indexes"));
            }
            let value = bucket_width
                .as_deref()
                .ok_or_else(|| invalid("--bucket-width", "is required"))?;
            let bucket_width = parse_bucket_width(value, index_type)?;

            match index_type {
                IndexTypeArg::Int64 => IndexKind::Int64 { bucket_width },
                IndexTypeArg::UInt64 => IndexKind::UInt64 { bucket_width },
                IndexTypeArg::Timestamp => unreachable!(),
            }
        }
    };

    let index = IndexSpec {
        column: index_column,
        entity_columns,
        kind,
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

async fn cmd_append(table: &Path, parquet: &Path, timing: bool) -> CliResult<()> {
    let start = Instant::now();
    let location = TableLocation::parse(table.to_string_lossy().as_ref()).context(StorageSnafu)?;
    let mut t = open_table(location, table).await?;

    let (_, rel_str) = t
        .append_parquet_from_path(parquet)
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

fn print_optimize_report(report: &OptimizeReport) {
    println!("starting_version: {}", report.starting_version);
    println!("committed_version: {}", report.committed_version);
    println!(
        "candidate_source_segments: {}",
        report.candidate_source_segments
    );
    println!(
        "source_segments_replaced: {}",
        report.source_segments_replaced
    );
    println!(
        "replacement_segments_written: {}",
        report.replacement_segments_written
    );
    println!(
        "distinct_identities_materialized: {}",
        report.distinct_identities_materialized
    );
    println!("rows_read: {}", report.rows_read);
    println!("rows_written: {}", report.rows_written);
    println!("no_op: {}", report.no_op);
}

async fn cmd_optimize(table: &Path) -> CliResult<()> {
    let location = TableLocation::parse(table.to_string_lossy().as_ref()).context(StorageSnafu)?;
    let mut table_handle = open_table(location, table).await?;
    let report = table_handle.optimize().await.context(OptimizeTableSnafu {
        table: table.display().to_string(),
    })?;
    print_optimize_report(&report);
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
        if let Some(rendered) = render_preview(&res, &opts) {
            page_output(&rendered)?;
        }
        if let Some(message) = preview_message(&res, &opts) {
            println!("{message}");
        }
        write_query_summary(&res, &opts, &mut std::io::stdout())?;
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
    observability::init().map_err(|reason| CliError::DiagnosticsInitialization { reason })?;

    match cli.cmd {
        Command::Create {
            table,
            index_column,
            index_type,
            bucket,
            timezone,
            bucket_width,
            entity,
        } => {
            cmd_create(
                &table,
                index_column,
                index_type,
                bucket,
                bucket_width,
                timezone,
                entity,
            )
            .await
        }

        Command::Append {
            table,
            parquet,
            timing,
        } => cmd_append(&table, &parquet, timing).await,

        Command::Optimize { table } => cmd_optimize(&table).await,

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
