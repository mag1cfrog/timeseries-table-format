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

#[async_trait::async_trait]
pub trait QuerySession: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn run_query(&self, sql: &str, opts: &QueryOpts) -> Result<QueryResult, Self::Error>;

    #[allow(dead_code)]
    fn table_name(&self) -> Option<&str> {
        None
    }
}

/// Minimal engine trait.
#[async_trait::async_trait]
pub trait Engine: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn execute(&self, sql: &str, opts: &QueryOpts) -> Result<QueryResult, Self::Error>;

    #[allow(dead_code)]
    async fn prepare_session(&self) -> Result<Box<dyn QuerySession<Error = Self::Error>>, Self::Error>;

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

pub struct DataFusionSession {
    ctx: SessionContext,
    #[allow(dead_code)]
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

    async fn prepare_session_internal(&self) -> CliResult<DataFusionSession> {
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

        Ok(DataFusionSession {
            ctx,
            table_name: self.table_name.clone(),
        })
    }
}

#[async_trait::async_trait]
impl QuerySession for DataFusionSession {
    type Error = CliError;

    async fn run_query(&self, sql: &str, opts: &QueryOpts) -> Result<QueryResult, Self::Error> {
        if opts.explain {
            let explain_sql = format!("EXPLAIN {sql}");
            let df = self.ctx.sql(&explain_sql).await.context(DataFusionSnafu)?;
            let batches = df.collect().await.context(DataFusionSnafu)?;
            let rendered = pretty_format_batches(&batches).context(ArrowSnafu)?;
            println!("{rendered}");
        }

        let start = Instant::now();

        let df = self.ctx.sql(sql).await.context(DataFusionSnafu)?;
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

#[async_trait::async_trait]
impl Engine for DataFusionEngine {
    type Error = CliError;

    async fn execute(&self, sql: &str, opts: &QueryOpts) -> Result<QueryResult, Self::Error> {
        let session = self.prepare_session_internal().await?;
        session.run_query(sql, opts).await
    }

    async fn prepare_session(
        &self,
    ) -> Result<Box<dyn QuerySession<Error = Self::Error>>, Self::Error> {
        Ok(Box::new(self.prepare_session_internal().await?))
    }

    fn table_name(&self) -> Option<&str> {
        Some(self.table_name.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMillisecondBuilder,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;
    use timeseries_table_core::{
        storage::TableLocation,
        time_series_table::TimeSeriesTable,
        transaction_log::{
            TableMeta, TimeBucket, TimeIndexSpec,
            logical_schema::{LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit},
        },
    };

    use crate::query::{OutputFormat, QueryOpts, default_table_name, print_query_result};

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn make_table_meta() -> TestResult<TableMeta> {
        let index = TimeIndexSpec {
            timestamp_column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string()],
            bucket: TimeBucket::Minutes(1),
            timezone: None,
        };

        let logical_schema = LogicalSchema::new(vec![
            LogicalField {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Millis,
                    timezone: None,
                },
                nullable: false,
            },
            LogicalField {
                name: "symbol".to_string(),
                data_type: LogicalDataType::Utf8,
                nullable: false,
            },
            LogicalField {
                name: "price".to_string(),
                data_type: LogicalDataType::Float64,
                nullable: false,
            },
            LogicalField {
                name: "volume".to_string(),
                data_type: LogicalDataType::Int64,
                nullable: false,
            },
            LogicalField {
                name: "is_trade".to_string(),
                data_type: LogicalDataType::Bool,
                nullable: false,
            },
            LogicalField {
                name: "venue".to_string(),
                data_type: LogicalDataType::Utf8,
                nullable: true,
            },
            LogicalField {
                name: "payload".to_string(),
                data_type: LogicalDataType::Binary,
                nullable: false,
            },
        ])?;

        Ok(TableMeta::new_time_series_with_schema(
            index,
            logical_schema,
        ))
    }

    fn lcg_next(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        *seed
    }

    fn write_parquet_rows(path: &std::path::Path, rows: usize) -> TestResult {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut ts_builder = TimestampMillisecondBuilder::with_capacity(rows);
        let mut sym_builder = StringBuilder::new();
        let mut price_builder = Float64Builder::with_capacity(rows);
        let mut volume_builder = Int64Builder::with_capacity(rows);
        let mut trade_builder = BooleanBuilder::with_capacity(rows);
        let mut venue_builder = StringBuilder::new();
        let mut payload_builder = BinaryBuilder::new();

        let mut seed = 0xBAD_5EED_u64;
        let base_ts = 1_700_000_000_000i64;
        for i in 0..rows {
            let rnd = lcg_next(&mut seed);
            let ts = base_ts + (i as i64) * 1_000;
            let symbol = "SYM1".to_string();
            let price = 100.0 + (rnd % 10_000) as f64 / 100.0;
            let volume = 1_000 + (rnd % 5_000) as i64;
            let is_trade = i % 2 == 0;
            let venue = if i % 5 == 0 {
                None
            } else {
                Some(format!("X{}", (rnd % 7) + 1))
            };
            let payload = vec![i as u8, (i.wrapping_mul(3)) as u8];

            ts_builder.append_value(ts);
            sym_builder.append_value(symbol);
            price_builder.append_value(price);
            volume_builder.append_value(volume);
            trade_builder.append_value(is_trade);
            match venue {
                Some(v) => venue_builder.append_value(v),
                None => venue_builder.append_null(),
            }
            payload_builder.append_value(payload);
        }

        let schema = Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
            Field::new("is_trade", DataType::Boolean, false),
            Field::new("venue", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(ts_builder.finish()) as _,
                Arc::new(sym_builder.finish()),
                Arc::new(price_builder.finish()),
                Arc::new(volume_builder.finish()),
                Arc::new(trade_builder.finish()),
                Arc::new(venue_builder.finish()),
                Arc::new(payload_builder.finish()),
            ],
        )?;

        let file = std::fs::File::create(path)?;
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }

    async fn build_table_with_rows(rows: usize) -> TestResult<(TempDir, usize)> {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_table_meta()?).await?;

        let rel = "data/segment.parquet";
        write_parquet_rows(&tmp.path().join(rel), rows)?;
        table.append_parquet_segment(rel, "ts").await?;

        Ok((tmp, rows))
    }

    #[tokio::test]
    async fn query_caps_preview_rows() -> TestResult<()> {
        use crate::engine::Engine;
        let (tmp, total) = build_table_with_rows(15).await?;
        let engine = super::DataFusionEngine::new(tmp.path());
        let table_name = default_table_name(tmp.path());
        let table_ident = format!("\"{}\"", table_name);

        let opts = QueryOpts {
            explain: false,
            timing: false,
            max_rows: 5,
            output: None,
            format: OutputFormat::Csv,
        };

        let sql = format!("SELECT * FROM {} ORDER BY ts", table_ident);
        let res = engine.execute(&sql, &opts).await?;

        assert_eq!(res.total_rows, total as u64);
        assert_eq!(res.preview_rows.len(), 5);
        assert_eq!(res.columns.len(), 7);

        Ok(())
    }

    #[tokio::test]
    async fn query_writes_csv_and_jsonl() -> TestResult<()> {
        use crate::engine::Engine;
        let (tmp, total) = build_table_with_rows(12).await?;
        let engine = super::DataFusionEngine::new(tmp.path());
        let table_name = default_table_name(tmp.path());
        let table_ident = format!("\"{}\"", table_name);
        let sql = format!("SELECT * FROM {} ORDER BY ts", table_ident);

        let csv_path = tmp.path().join("out.csv");
        let opts_csv = QueryOpts {
            explain: false,
            timing: false,
            max_rows: 3,
            output: Some(csv_path.clone()),
            format: OutputFormat::Csv,
        };
        let _ = engine.execute(&sql, &opts_csv).await?;

        let csv_contents = std::fs::read_to_string(&csv_path)?;
        let csv_lines: Vec<&str> = csv_contents.lines().collect();
        assert!(!csv_lines.is_empty());
        assert!(csv_lines.len() > total);

        let jsonl_path = tmp.path().join("out.jsonl");
        let opts_jsonl = QueryOpts {
            explain: false,
            timing: false,
            max_rows: 2,
            output: Some(jsonl_path.clone()),
            format: OutputFormat::Jsonl,
        };
        let _ = engine.execute(&sql, &opts_jsonl).await?;

        let jsonl_contents = std::fs::read_to_string(&jsonl_path)?;
        let jsonl_lines: Vec<&str> = jsonl_contents.lines().collect();
        assert_eq!(jsonl_lines.len(), total);

        Ok(())
    }

    #[tokio::test]
    async fn query_real_world_preview_prints() -> TestResult<()> {
        use crate::engine::Engine;
        let (tmp, total) = build_table_with_rows(25).await?;
        let engine = super::DataFusionEngine::new(tmp.path());
        let table_name = default_table_name(tmp.path());
        let table_ident = format!("\"{}\"", table_name);

        let opts = QueryOpts {
            explain: false,
            timing: false,
            max_rows: 12,
            output: None,
            format: OutputFormat::Csv,
        };

        let sql = format!(
            "SELECT ts, symbol, price, volume, is_trade, venue, payload \
             FROM {} ORDER BY ts",
            table_ident
        );

        let res = engine.execute(&sql, &opts).await?;
        assert_eq!(res.total_rows, total as u64);
        assert_eq!(res.preview_rows.len(), 12);

        print_query_result(&res, &opts)?;

        Ok(())
    }

    #[tokio::test]
    async fn query_explain_runs() -> TestResult<()> {
        use crate::engine::Engine;
        let (tmp, _total) = build_table_with_rows(5).await?;
        let engine = super::DataFusionEngine::new(tmp.path());
        let table_name = default_table_name(tmp.path());
        let table_ident = format!("\"{}\"", table_name);

        let opts = QueryOpts {
            explain: true,
            timing: false,
            max_rows: 3,
            output: None,
            format: OutputFormat::Csv,
        };

        let sql = format!("SELECT * FROM {} ORDER BY ts", table_ident);
        let res = engine.execute(&sql, &opts).await?;

        assert_eq!(res.columns.len(), 7);
        Ok(())
    }

    #[tokio::test]
    async fn query_timing_sets_elapsed() -> TestResult<()> {
        use crate::engine::Engine;
        let (tmp, _total) = build_table_with_rows(10).await?;
        let engine = super::DataFusionEngine::new(tmp.path());
        let table_name = default_table_name(tmp.path());
        let table_ident = format!("\"{}\"", table_name);

        let opts = QueryOpts {
            explain: false,
            timing: true,
            max_rows: 4,
            output: None,
            format: OutputFormat::Csv,
        };

        let sql = format!("SELECT * FROM {} ORDER BY ts", table_ident);
        let res = engine.execute(&sql, &opts).await?;

        assert!(res.elapsed.is_some());
        Ok(())
    }
}
