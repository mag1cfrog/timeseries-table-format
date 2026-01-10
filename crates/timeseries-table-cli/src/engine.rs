use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow::{
    datatypes::DataType,
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
    error::{
        ArrowSnafu, CliError, CliResult, CsvUnsupportedTypeSnafu, DataFusionSnafu, OpenTableSnafu,
        StorageSnafu,
    },
    query::{OutputFormat, QueryOpts, QueryResult, default_table_name},
};

/// Backend-agnostic query session for reuse by future shells or services.
#[async_trait::async_trait]
pub trait QuerySession: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn run_query(&self, sql: &str, opts: &QueryOpts) -> Result<QueryResult, Self::Error>;

    /// Optional table identifier registered in the session.
    fn table_name(&self) -> Option<&str> {
        None
    }
}

/// Query execution backend abstraction for CLI and non-interactive usage.
#[async_trait::async_trait]
pub trait Engine: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Prepare a reusable session (e.g., for interactive shells).
    async fn prepare_session(
        &self,
    ) -> Result<Box<dyn QuerySession<Error = Self::Error>>, Self::Error>;

    /// Prepare a session using an existing table snapshot, avoiding log replay.
    async fn prepare_session_from_table(
        &self,
        table: &TimeSeriesTable,
    ) -> Result<Box<dyn QuerySession<Error = Self::Error>>, Self::Error> {
        let _ = table;
        self.prepare_session().await
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

fn ensure_csv_supported(schema: &arrow::datatypes::Schema) -> CliResult<()> {
    for field in schema.fields() {
        let dt = field.data_type();
        let unsupported = matches!(
            dt,
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Struct(_)
                | DataType::Map(_, _)
                | DataType::Union(_, _)
        );

        if unsupported {
            return CsvUnsupportedTypeSnafu {
                field: field.name().to_string(),
                data_type: format!("{dt:?}"),
            }
            .fail();
        }
    }

    Ok(())
}

pub struct DataFusionEngine {
    table_root: PathBuf,
    table_name: String,
}

pub struct DataFusionSession {
    ctx: SessionContext,
    /// Retained for future session reuse and user messaging.
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

    async fn prepare_session_from_table_internal(
        &self,
        table: &TimeSeriesTable,
    ) -> CliResult<DataFusionSession> {
        let state = table.state().clone();
        let location = table.location().clone();
        let table = TimeSeriesTable::from_state(location, state).context(OpenTableSnafu {
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
        let mut csv_checked = false;

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
                if !csv_checked && opts.format == OutputFormat::Csv {
                    ensure_csv_supported(batch.schema().as_ref())?;
                    csv_checked = true;
                }
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

    async fn prepare_session(
        &self,
    ) -> Result<Box<dyn QuerySession<Error = Self::Error>>, Self::Error> {
        Ok(Box::new(self.prepare_session_internal().await?))
    }

    async fn prepare_session_from_table(
        &self,
        table: &TimeSeriesTable,
    ) -> Result<Box<dyn QuerySession<Error = Self::Error>>, Self::Error> {
        Ok(Box::new(
            self.prepare_session_from_table_internal(table).await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
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

    mod test_common {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/common/mod.rs"));
    }

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

    async fn build_table_with_rows(rows: usize) -> TestResult<(TempDir, usize)> {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_table_meta()?).await?;

        let rel = "data/segment.parquet";
        test_common::write_parquet_rows(&tmp.path().join(rel), rows)?;
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
        let session = engine.prepare_session().await?;
        let res = session.run_query(&sql, &opts).await?;

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
        let session = engine.prepare_session().await?;
        let _ = session.run_query(&sql, &opts_csv).await?;

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
        let session = engine.prepare_session().await?;
        let _ = session.run_query(&sql, &opts_jsonl).await?;

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

        let session = engine.prepare_session().await?;
        let res = session.run_query(&sql, &opts).await?;
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
        let session = engine.prepare_session().await?;
        let res = session.run_query(&sql, &opts).await?;

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
        let session = engine.prepare_session().await?;
        let res = session.run_query(&sql, &opts).await?;

        assert!(res.elapsed.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn prepare_session_exposes_table_name() -> TestResult<()> {
        use crate::engine::Engine;
        let (tmp, _total) = build_table_with_rows(2).await?;
        let engine = super::DataFusionEngine::new(tmp.path());
        let table_name = default_table_name(tmp.path());

        let session = engine.prepare_session().await?;
        assert_eq!(session.table_name(), Some(table_name.as_str()));

        Ok(())
    }

    #[tokio::test]
    async fn prepare_session_from_path_works() -> TestResult<()> {
        use crate::engine::Engine;

        let (tmp, _total) = build_table_with_rows(2).await?;

        let engine = super::DataFusionEngine::new(tmp.path());
        let session = engine.prepare_session().await?;

        // Should be able to query after prepare_session
        assert!(session.table_name().is_some());
        Ok(())
    }

    #[tokio::test]
    async fn prepare_session_from_table_works() -> TestResult<()> {
        use crate::engine::Engine;

        let (tmp, _total) = build_table_with_rows(2).await?;
        let location = TableLocation::local(tmp.path());
        let table = TimeSeriesTable::open(location).await?;

        let engine = super::DataFusionEngine::new(tmp.path());
        let session = engine.prepare_session_from_table(&table).await?;

        // Should be able to query after prepare_session_from_table
        assert!(session.table_name().is_some());
        Ok(())
    }

    #[test]
    fn csv_schema_validation_rejects_nested_types() -> TestResult<()> {
        let schema = Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new(
                "items",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]);

        let err = super::ensure_csv_supported(&schema).unwrap_err();
        match err {
            crate::error::CliError::CsvUnsupportedType { field, .. } => {
                assert_eq!(field, "items");
            }
            other => return Err(format!("unexpected error: {other:?}").into()),
        }

        Ok(())
    }

    #[test]
    fn csv_schema_validation_accepts_flat_types() -> TestResult<()> {
        let schema = Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]);

        super::ensure_csv_supported(&schema)?;
        Ok(())
    }
}
