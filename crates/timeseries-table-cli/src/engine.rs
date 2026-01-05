use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow::{
    error::ArrowError,
    util::{
        display::{ArrayFormatter, FormatOptions},
        pretty::pretty_format_batches,
    },
};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures_util::StreamExt;
use snafu::ResultExt;
use timeseries_table_core::{
    storage::{OutputLocation, OutputSink, TableLocation, open_output_sink},
    time_series_table::TimeSeriesTable,
};
use timeseries_table_datafusion::TsTableProvider;

use crate::{
    error::{ArrowSnafu, CliError, CliResult, DataFusionSnafu, OpenTableSnafu, StorageSnafu},
    query::{OutputFormat, QueryOpts, QueryResult, default_table_name},
};

/// Minimal engine trait.
#[async_trait::async_trait]
pub trait Engine: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn execute(&self, sql: &str, opts: &QueryOpts) -> Result<QueryResult, Self::Error>;

    fn table_name(&self) -> Option<&str> {
        None
    }
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
                // Arrow CSV writer writes RecordBatch => CSV.
                // it does NOT support ListArray / StructArray.
                let writer = arrow_csv::WriterBuilder::new().build(writer);
                Ok(OutputWriter::Csv(Box::new(writer)))
            }
            OutputFormat::Jsonl => Ok(OutputWriter::Jsonl(Box::new(
                arrow_json::LineDelimitedWriter::new(writer),
            ))),
        }
    }

    fn write_batch(&mut self, batch: &arrow::array::RecordBatch) -> CliResult<()> {
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

pub struct DataFusionEngine {
    table_root: PathBuf,
    table_name: String,
}

impl DataFusionEngine {
    pub fn new(table_root: impl AsRef<Path>) -> Self {
        let table_root = table_root.as_ref().to_path_buf();
        let table_name = default_table_name(&table_root);

        Self {
            table_root,
            table_name,
        }
    }

    async fn prepare_session(&self) -> CliResult<SessionContext> {
        let location = TableLocation::parse(self.table_root.to_string_lossy().as_ref())
            .context(StorageSnafu)?;

        let table = TimeSeriesTable::open(location)
            .await
            .context(OpenTableSnafu {
                table: self.table_root.display().to_string(),
            })?;

        let table = Arc::new(table);
        let provider = TsTableProvider::try_new(table).context(DataFusionSnafu)?;

        let cfg = SessionConfig::new();
        let ctx = SessionContext::new_with_config(cfg);

        ctx.register_table(self.table_name.as_str(), Arc::new(provider))
            .context(DataFusionSnafu)?;

        Ok(ctx)
    }
}

#[async_trait::async_trait]
impl Engine for DataFusionEngine {
    type Error = CliError;

    async fn execute(&self, sql: &str, opts: &QueryOpts) -> Result<QueryResult, Self::Error> {
        let ctx = self.prepare_session().await?;

        if opts.explain {
            let explain_sql = format!("EXPLAIN {sql}");
            let df = ctx.sql(&explain_sql).await.context(DataFusionSnafu)?;
            let batches = df.collect().await.context(DataFusionSnafu)?;
            let rendered = pretty_format_batches(&batches).context(ArrowSnafu)?;
            println!("{rendered}");
        }

        let start = Instant::now();

        let df = ctx.sql(sql).await.context(DataFusionSnafu)?;
        let mut stream = df.execute_stream().await.context(DataFusionSnafu)?;

        let mut total_rows: u64 = 0;
        let mut preview_rows_left = opts.max_rows;
        let mut columns: Vec<String> = Vec::new();
        let mut preview_rows: Vec<Vec<String>> = Vec::new();

        let mut out = if let Some(path) = &opts.output {
            Some(OutputWriter::create_output_writer(path, opts.format).await?)
        } else {
            None
        };

        while let Some(item) = stream.next().await {
            let batch = item.context(DataFusionSnafu)?;
            total_rows += batch.num_rows() as u64;

            if columns.is_empty() {
                columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();
            }

            if let Some(w) = out.as_mut() {
                w.write_batch(&batch)?;
            }

            if preview_rows_left > 0 {
                let options = FormatOptions::default();
                let formatters = batch
                    .columns()
                    .iter()
                    .map(|col| ArrayFormatter::try_new(col.as_ref(), &options))
                    .collect::<Result<Vec<_>, ArrowError>>()
                    .context(ArrowSnafu)?;

                let rows_to_take = preview_rows_left.min(batch.num_rows());
                for row_idx in 0..rows_to_take {
                    let mut row = Vec::with_capacity(formatters.len());
                    for formatter in &formatters {
                        row.push(
                            formatter
                                .value(row_idx)
                                .try_to_string()
                                .context(ArrowSnafu)?,
                        );
                    }
                    preview_rows.push(row);
                }

                preview_rows_left -= rows_to_take;
            }
        }

        if let Some(w) = out {
            w.finish().await?;
        }

        let elapsed = opts.timing.then(|| start.elapsed());

        Ok(QueryResult {
            columns,
            preview_rows,
            total_rows,
            elapsed,
        })
    }

    fn table_name(&self) -> Option<&str> {
        Some(self.table_name.as_str())
    }
}
