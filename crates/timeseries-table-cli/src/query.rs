use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::StreamExt;

use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use datafusion::prelude::{SessionConfig, SessionContext};
use snafu::ResultExt;
use timeseries_table_core::{
    storage::{OutputLocation, OutputSink, TableLocation, open_output_sink},
    time_series_table::TimeSeriesTable,
};
use timeseries_table_datafusion::TsTableProvider;

use crate::error::{ArrowSnafu, CliResult, DataFusionSnafu, OpenTableSnafu, StorageSnafu};

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Csv,
    Jsonl,
}

#[derive(Debug, Clone)]
pub struct QueryOpts {
    pub explain: bool,
    pub timing: bool,
    pub max_rows: usize,
    pub output: Option<PathBuf>,
    pub format: OutputFormat,
}

pub struct QueryResult {
    pub table_name: String,
    pub total_rows: u64,
    pub preview_batches: Vec<RecordBatch>,
    pub elapsed: Option<Duration>,
}

pub struct QuerySession {
    pub ctx: SessionContext,
    pub table_name: String,
}

struct SinkWriter {
    sink: OutputSink,
}

impl Write for SinkWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.sink.writer().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.sink.writer().flush()
    }
}

impl SinkWriter {
    async fn finish(self) -> CliResult<()> {
        self.sink.finish().await.context(StorageSnafu)
    }
}

enum OutputWriter {
    Csv(Box<arrow_csv::Writer<SinkWriter>>),
    Jsonl(Box<arrow_json::LineDelimitedWriter<SinkWriter>>),
}

impl OutputWriter {
    async fn create_output_writer(path: &Path, format: OutputFormat) -> CliResult<Self> {
        let spec = path.to_string_lossy();
        let out = OutputLocation::parse(&spec).context(StorageSnafu)?;
        let sink = open_output_sink(&out.storage, &out.rel_path)
            .await
            .context(StorageSnafu)?;

        let writer = SinkWriter { sink };

        match format {
            OutputFormat::Csv => {
                // Arrow CSV writer wirtes RecordBatch => CSV.
                // it does NOT support ListArray / StructArray.
                let writer = arrow_csv::WriterBuilder::new().build(writer);
                Ok(OutputWriter::Csv(Box::new(writer)))
            }
            OutputFormat::Jsonl => {
                // Line delimited JSON writer.
                Ok(OutputWriter::Jsonl(Box::new(
                    arrow_json::LineDelimitedWriter::new(writer),
                )))
            }
        }
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> CliResult<()> {
        match self {
            OutputWriter::Csv(w) => w.write(batch).context(ArrowSnafu),

            OutputWriter::Jsonl(w) => w.write_batches(&[batch]).context(ArrowSnafu),
        }
    }

    async fn finish(self) -> CliResult<()> {
        match self {
            OutputWriter::Csv(w) => {
                let sink = w.into_inner();
                sink.finish().await
            }

            OutputWriter::Jsonl(mut w) => {
                w.finish().context(ArrowSnafu)?;
                let sink = w.into_inner();
                sink.finish().await
            }
        }
    }
}

/// Pick a stable, user-friendly SQL table name from the table root path.
/// Fallback is "t".
fn default_table_name(table_root: &Path) -> String {
    table_root
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "t".to_string())
}

pub async fn prepare_session(table_root: &Path) -> CliResult<QuerySession> {
    let location =
        TableLocation::parse(table_root.to_string_lossy().as_ref()).context(StorageSnafu)?;

    // Open core table handle
    let table = TimeSeriesTable::open(location)
        .await
        .context(OpenTableSnafu {
            table: table_root.display().to_string(),
        })?;

    let table = Arc::new(table);

    // Build provider
    let provider = TsTableProvider::try_new(table).context(DataFusionSnafu)?;

    // SessionContext
    let cfg = SessionConfig::new();
    let ctx = SessionContext::new_with_config(cfg);

    // Register under derived name
    let table_name = default_table_name(table_root);
    ctx.register_table(table_name.as_str(), Arc::new(provider))
        .context(DataFusionSnafu)?;

    Ok(QuerySession { ctx, table_name })
}

pub async fn run_query(
    session: &QuerySession,
    sql: &str,
    opts: &QueryOpts,
) -> CliResult<QueryResult> {
    // 1) EXPLAIN (optional)
    if opts.explain {
        let explain_sql = format!("EXPLAIN {sql}");
        let df = session
            .ctx
            .sql(&explain_sql)
            .await
            .context(DataFusionSnafu)?;

        let batches = df.collect().await.context(DataFusionSnafu)?;

        let rendered = pretty_format_batches(&batches).context(ArrowSnafu)?;

        println!("{rendered}");
    }

    // 2) Execute main SQL fully, but only keep first N rows for display.
    let start = Instant::now();

    let df = session.ctx.sql(sql).await.context(DataFusionSnafu)?;

    let mut stream = df.execute_stream().await.context(DataFusionSnafu)?;

    let mut total_rows: u64 = 0;
    let mut preview_rows_left = opts.max_rows;
    let mut preview_batches: Vec<RecordBatch> = Vec::new();

    let mut out = if let Some(path) = &opts.output {
        Some(OutputWriter::create_output_writer(path, opts.format).await?)
    } else {
        None
    };

    while let Some(item) = stream.next().await {
        let batch = item.context(DataFusionSnafu)?;
        total_rows += batch.num_rows() as u64;

        // write full output if requested
        if let Some(w) = out.as_mut() {
            w.write_batch(&batch)?;
        }

        // keep preview
        if preview_rows_left > 0 {
            if batch.num_rows() <= preview_rows_left {
                preview_rows_left -= batch.num_rows();
                preview_batches.push(batch);
            } else {
                // slice to remaning rows
                let sliced = batch.slice(0, preview_rows_left);
                preview_rows_left = 0;
                preview_batches.push(sliced);
            }
        }
    }

    if let Some(w) = out {
        w.finish().await?;
    }

    let elapsed = opts.timing.then(|| start.elapsed());

    Ok(QueryResult {
        table_name: session.table_name.clone(),
        total_rows,
        preview_batches,
        elapsed,
    })
}

pub fn print_query_result(res: &QueryResult, opts: &QueryOpts) -> CliResult<()> {
    // Pretty print preview
    if !res.preview_batches.is_empty() {
        let rendered = pretty_format_batches(&res.preview_batches).context(ArrowSnafu)?;
        println!("{rendered}");
    } else {
        println!("(no rows)");
    }

    println!("table: {}", res.table_name);
    println!("total_rows: {}", res.total_rows);

    if let Some(d) = res.elapsed {
        println!("elapsed_ms: {}", d.as_millis());
    }

    if let Some(path) = &opts.output {
        println!("wrote: {} ({:?})", path.display(), opts.format);
    }

    Ok(())
}
