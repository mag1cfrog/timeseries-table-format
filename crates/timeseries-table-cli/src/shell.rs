use std::{path::PathBuf, time::Instant};

use rustyline::{DefaultEditor, error::ReadlineError};
use snafu::ResultExt;
use timeseries_table_core::storage::TableLocation;
use tokio::runtime::Handle;

use crate::{
    BackendArg,
    engine::{Engine, QuerySession},
    error::{CliError, CliResult, OpenTableSnafu, StorageSnafu},
    make_engine, open_table,
    query::{OutputFormat, QueryOpts, default_table_name, print_query_result, quote_identifier},
};

type BoxedEngine = Box<dyn Engine<Error = CliError>>;
type BoxedSession = Box<dyn QuerySession<Error = CliError>>;

#[allow(dead_code)]
struct TokenSpan {
    value: String,
    start: usize,
    end: usize,
}

enum CommandAction {
    Continue,
    Break,
}

#[allow(dead_code)]
struct CommandResult {
    action: CommandAction,
    query_result: Option<crate::query::QueryResult>,
}

struct ShellContext {
    table_root: PathBuf,
    location: TableLocation,
    engine: BoxedEngine,
    table: timeseries_table_core::time_series_table::TimeSeriesTable,
    session: BoxedSession,
    timing: bool,
}

fn print_help() {
    println!(
        r#"commands:
  refresh
  append <parquet_path>
  query [--max-rows N] [--format csv|jsonl] [--output PATH] [--timing] [--explain] [--] <sql>
  explain [--max-rows N] [--format csv|jsonl] [--output PATH] [--timing] [--] <sql>
  \timing           toggle per-command elapsed time
  help
  exit | quit
notes:
  - use `--` to separate flags from SQL (e.g. SQL with leading `--`)
"#
    );
}

async fn build_context(
    table_root: PathBuf,
    backend: BackendArg,
) -> CliResult<(ShellContext, String)> {
    // Parse location once.
    let location =
        TableLocation::parse(table_root.to_string_lossy().as_ref()).context(StorageSnafu)?;

    // Cached mutable table handle for refresh/append.
    let table = open_table(location.clone(), table_root.as_path()).await?;

    // Cached query session built from the same in-memory table snapshot.
    let engine = make_engine(backend.into(), table_root.as_path());
    let session = engine.prepare_session_from_table(&table).await?;

    let table_name = session
        .table_name()
        .map(|s| s.to_string())
        .unwrap_or_else(|| default_table_name(&table_root));

    Ok((
        ShellContext {
            table_root,
            location,
            engine,
            table,
            session,
            timing: false,
        },
        table_name,
    ))
}

fn lex_with_spans(input: &str) -> Result<Vec<TokenSpan>, String> {
    let bytes = input.as_bytes();
    let mut i = 0;
    let mut tokens = Vec::new();

    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }

        let start = i;
        let mut value = String::new();
        let quote = if bytes[i] == b'"' || bytes[i] == b'\'' {
            let q = bytes[i];
            i += 1;
            Some(q)
        } else {
            None
        };

        if let Some(q) = quote {
            while i < bytes.len() {
                let b = bytes[i];
                if b == q {
                    i += 1;
                    break;
                }
                if b == b'\\' {
                    i += 1;
                    if i >= bytes.len() {
                        return Err("unterminated escape in quoted token".to_string());
                    }
                    value.push(bytes[i] as char);
                    i += 1;
                    continue;
                }
                value.push(b as char);
                i += 1;
            }
            if i <= bytes.len() && bytes.get(i.wrapping_sub(1)) != Some(&q) {
                return Err("unterminated quoted token".to_string());
            }
        } else {
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                let b = bytes[i];
                if b == b'\\' {
                    i += 1;
                    if i >= bytes.len() {
                        return Err("unterminated escape in token".to_string());
                    }
                    value.push(bytes[i] as char);
                    i += 1;
                    continue;
                }
                value.push(b as char);
                i += 1;
            }
        }

        let end = i;
        tokens.push(TokenSpan { value, start, end });
    }

    Ok(tokens)
}

fn parse_format(raw: &str) -> Result<OutputFormat, String> {
    match raw.to_ascii_lowercase().as_str() {
        "csv" => Ok(OutputFormat::Csv),
        "jsonl" => Ok(OutputFormat::Jsonl),
        other => Err(format!("unknown format: {other}")),
    }
}

fn parse_sql_command(
    rest: &str,
    default_max_rows: usize,
    default_timing: bool,
    allow_explain_flag: bool,
) -> Result<(String, QueryOpts), String> {
    let tokens = lex_with_spans(rest)?;
    if tokens.is_empty() {
        return Err("missing SQL".to_string());
    }

    let mut opts = QueryOpts {
        explain: false,
        timing: default_timing,
        max_rows: default_max_rows,
        output: None,
        format: OutputFormat::Csv,
    };

    let mut i = 0;
    let mut sql_start: Option<usize> = None;
    while i < tokens.len() {
        let token = &tokens[i].value;
        if token == "--" {
            if i + 1 >= tokens.len() {
                return Err("missing SQL after `--`".to_string());
            }
            sql_start = Some(tokens[i + 1].start);
            break;
        }

        if token.starts_with("--") {
            if let Some(v) = token.strip_prefix("--max-rows=") {
                opts.max_rows = v
                    .parse::<usize>()
                    .map_err(|_| "invalid --max-rows value".to_string())?;
                i += 1;
                continue;
            }
            if let Some(v) = token.strip_prefix("--format=") {
                opts.format = parse_format(v)?;
                i += 1;
                continue;
            }
            if let Some(v) = token.strip_prefix("--output=") {
                opts.output = Some(PathBuf::from(v));
                i += 1;
                continue;
            }
            if token == "--max-rows" {
                if i + 1 >= tokens.len() {
                    return Err("missing value for --max-rows".to_string());
                }
                opts.max_rows = tokens[i + 1]
                    .value
                    .parse::<usize>()
                    .map_err(|_| "invalid --max-rows value".to_string())?;
                i += 2;
                continue;
            }
            if token == "--format" {
                if i + 1 >= tokens.len() {
                    return Err("missing value for --format".to_string());
                }
                opts.format = parse_format(&tokens[i + 1].value)?;
                i += 2;
                continue;
            }
            if token == "--output" {
                if i + 1 >= tokens.len() {
                    return Err("missing value for --output".to_string());
                }
                opts.output = Some(PathBuf::from(tokens[i + 1].value.as_str()));
                i += 2;
                continue;
            }
            if token == "--timing" {
                opts.timing = true;
                i += 1;
                continue;
            }
            if token == "--explain" && allow_explain_flag {
                opts.explain = true;
                i += 1;
                continue;
            }

            return Err(format!("unknown flag: {token}"));
        }

        sql_start = Some(tokens[i].start);
        break;
    }

    let sql_start = sql_start.ok_or_else(|| "missing SQL".to_string())?;
    let sql = rest[sql_start..].trim();
    if sql.is_empty() {
        return Err("missing SQL".to_string());
    }

    Ok((sql.to_string(), opts))
}

async fn process_command(ctx: &mut ShellContext, trimmed: &str) -> CliResult<CommandResult> {
    if trimmed == "exit" || trimmed == "quit" {
        return Ok(CommandResult {
            action: CommandAction::Break,
            query_result: None,
        });
    }

    if trimmed == "help" {
        print_help();
        return Ok(CommandResult {
            action: CommandAction::Continue,
            query_result: None,
        });
    }

    if trimmed == r"\timing" || trimmed == r"\\timing" {
        ctx.timing = !ctx.timing;
        println!("timing: {}", if ctx.timing { "on" } else { "off" });
        return Ok(CommandResult {
            action: CommandAction::Continue,
            query_result: None,
        });
    }

    if trimmed == "refresh" {
        let start = Instant::now();
        let changed = ctx.table.refresh().await.map_err(|e| CliError::OpenTable {
            table: ctx.table_root.display().to_string(),
            source: Box::new(e),
        });

        match changed {
            Ok(changed) => {
                if ctx.timing {
                    println!(
                        "refreshed: {} (elapsed_ms: {})",
                        changed,
                        start.elapsed().as_millis()
                    );
                } else {
                    println!("refreshed: {changed}");
                }

                // Rebuild query session only when the table actually changed.
                if changed {
                    ctx.session = ctx.engine.prepare_session_from_table(&ctx.table).await?;
                }
            }
            Err(e) => println!("{e}"),
        }

        return Ok(CommandResult {
            action: CommandAction::Continue,
            query_result: None,
        });
    }

    if let Some(rest) = trimmed.strip_prefix("append ") {
        let start = Instant::now();
        // 1) refresh first
        if let Err(e) = ctx.table.refresh().await.context(OpenTableSnafu {
            table: ctx.table_root.display().to_string(),
        }) {
            println!("{e}");
            return Ok(CommandResult {
                action: CommandAction::Continue,
                query_result: None,
            });
        }

        // 2) ensure parquet under root
        let parquet_path = PathBuf::from(rest.trim());
        let rel = match ctx.location.ensure_parquet_under_root(&parquet_path).await {
            Ok(r) => r,
            Err(e) => {
                println!("{}", CliError::Storage { source: e });
                return Ok(CommandResult {
                    action: CommandAction::Continue,
                    query_result: None,
                });
            }
        };

        let rel_str = if cfg!(windows) {
            rel.to_string_lossy().replace('\\', "/")
        } else {
            rel.to_string_lossy().to_string()
        };

        // 3) append (uses cached handle)
        let ts_col = ctx.table.index_spec().timestamp_column.clone();
        match ctx.table.append_parquet_segment(&rel_str, &ts_col).await {
            Ok(s) => {
                if ctx.timing {
                    println!(
                        "appended: {rel_str}, size: {s}. (elapsed_ms: {})",
                        start.elapsed().as_millis()
                    );
                } else {
                    println!("appended: {rel_str}, size: {s}.");
                }
                // Rebuild query session from the refreshed in-memory table snapshot.
                ctx.session = ctx.engine.prepare_session_from_table(&ctx.table).await?;
            }

            Err(e) => {
                println!(
                    "{}",
                    CliError::AppendSegment {
                        table: ctx.table_root.display().to_string(),
                        source: Box::new(e),
                    }
                );
            }
        }

        return Ok(CommandResult {
            action: CommandAction::Continue,
            query_result: None,
        });
    }

    if let Some(rest) = trimmed.strip_prefix("query") {
        if !rest.is_empty() && !rest.starts_with(' ') {
            println!("unknown command. type 'help'.");
            return Ok(CommandResult {
                action: CommandAction::Continue,
                query_result: None,
            });
        }

        let rest = rest.trim();
        let (sql, opts) = match parse_sql_command(rest, 10, ctx.timing, true) {
            Ok(res) => res,
            Err(e) => {
                println!("{e}");
                return Ok(CommandResult {
                    action: CommandAction::Continue,
                    query_result: None,
                });
            }
        };

        // Refresh before queries so results track new commits.
        match ctx.table.refresh().await {
            Ok(changed) => {
                if changed {
                    ctx.session = ctx.engine.prepare_session_from_table(&ctx.table).await?;
                }
            }
            Err(e) => {
                println!(
                    "{}",
                    CliError::OpenTable {
                        table: ctx.table_root.display().to_string(),
                        source: Box::new(e),
                    }
                );
                return Ok(CommandResult {
                    action: CommandAction::Continue,
                    query_result: None,
                });
            }
        }

        let res = match ctx.session.run_query(sql.trim(), &opts).await {
            Ok(res) => {
                let _ = print_query_result(&res, &opts);
                Some(res)
            }
            Err(e) => {
                println!("{e}");
                None
            }
        };

        return Ok(CommandResult {
            action: CommandAction::Continue,
            query_result: res,
        });
    }

    if let Some(rest) = trimmed.strip_prefix("explain") {
        if !rest.is_empty() && !rest.starts_with(' ') {
            println!("unknown command. type 'help'.");
            return Ok(CommandResult {
                action: CommandAction::Continue,
                query_result: None,
            });
        }

        let rest = rest.trim();
        let (sql, mut opts) = match parse_sql_command(rest, 1000, ctx.timing, false) {
            Ok(res) => res,
            Err(e) => {
                println!("{e}");
                return Ok(CommandResult {
                    action: CommandAction::Continue,
                    query_result: None,
                });
            }
        };

        // Refresh before queries so results track new commits.
        match ctx.table.refresh().await {
            Ok(changed) => {
                if changed {
                    ctx.session = ctx.engine.prepare_session_from_table(&ctx.table).await?;
                }
            }
            Err(e) => {
                println!(
                    "{}",
                    CliError::OpenTable {
                        table: ctx.table_root.display().to_string(),
                        source: Box::new(e),
                    }
                );
                return Ok(CommandResult {
                    action: CommandAction::Continue,
                    query_result: None,
                });
            }
        }

        // plan-only: just run an EXPLAIN statement through the same session
        let explain_sql = format!("EXPLAIN {}", sql.trim());
        opts.explain = false; // because we are explicitly running EXPLAIN

        let res = match ctx.session.run_query(&explain_sql, &opts).await {
            Ok(res) => {
                let _ = print_query_result(&res, &opts);
                Some(res)
            }
            Err(e) => {
                println!("{e}");
                None
            }
        };

        return Ok(CommandResult {
            action: CommandAction::Continue,
            query_result: res,
        });
    }

    println!("unknown command. type 'help'.");
    Ok(CommandResult {
        action: CommandAction::Continue,
        query_result: None,
    })
}

fn shell_blocking(
    handle: Handle,
    table_root: PathBuf,
    history: Option<PathBuf>,
    backend: BackendArg,
) -> CliResult<()> {
    let (mut ctx, table_name) = handle.block_on(build_context(table_root, backend))?;

    let history_path = history.unwrap_or_else(|| ctx.table_root.join(".ts_table_history"));

    // rustyline editor
    let mut rl = DefaultEditor::new().map_err(|e| CliError::PathInvariantNoSource {
        message: format!("failed to initialize readline: {e}"),
        path: None,
    })?;

    // history best-effort
    {
        let _ = rl.load_history(&history_path);
    }

    println!("ts-table shell");
    println!("table: {}", ctx.table_root.display());
    println!(
        "registered as: {} (quoted: {})",
        table_name,
        quote_identifier(&table_name)
    );
    println!("type 'help' for commands\n");

    loop {
        let prompt = if ctx.timing {
            format!("ts-table[{table_name}](timing)> ")
        } else {
            format!("ts-table[{table_name}]> ")
        };

        let line = match rl.readline(&prompt) {
            Ok(l) => l,
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D
                println!("^D");
                break;
            }
            Err(e) => {
                println!("readline error: {e}");
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Save to history best-effort
        let _ = rl.add_history_entry(trimmed);

        let result = handle.block_on(process_command(&mut ctx, trimmed))?;
        if matches!(result.action, CommandAction::Break) {
            break;
        }
    }

    {
        let _ = rl.save_history(&history_path);
    }

    Ok(())
}

/// Run an interactive shell in a blocking thread (rustyline is blocking).
pub async fn cmd_shell(
    table_root: PathBuf,
    history: Option<PathBuf>,
    backend: BackendArg,
) -> CliResult<()> {
    let handle = Handle::current();

    // Move everything into the blocking closure.
    tokio::task::spawn_blocking(move || shell_blocking(handle, table_root, history, backend))
        .await
        .map_err(|e| {
            // JoinError -> treat as internal error
            CliError::PathInvariantNoSource {
                message: format!("shell thread failed: {e}"),
                path: None,
            }
        })??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use timeseries_table_core::{
        storage::TableLocation,
        time_series_table::TimeSeriesTable,
        transaction_log::{
            TableMeta, TimeBucket, TimeIndexSpec,
            logical_schema::{LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit},
        },
    };

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

    async fn build_table_with_rows(rows: usize) -> TestResult<TempDir> {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_table_meta()?).await?;

        let rel = "data/segment.parquet";
        test_common::write_parquet_rows(&tmp.path().join(rel), rows)?;
        table.append_parquet_segment(rel, "ts").await?;

        Ok(tmp)
    }

    fn write_parquet_rows_with_base(
        path: &std::path::Path,
        rows: usize,
        base_ts: i64,
    ) -> TestResult {
        use arrow::array::{
            BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
            TimestampMillisecondBuilder,
        };
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

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
        for i in 0..rows {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let rnd = seed;
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

    fn query_sql(table_name: &str) -> String {
        format!(
            "query SELECT * FROM {} ORDER BY ts",
            quote_identifier(table_name)
        )
    }

    fn explain_sql(table_name: &str) -> String {
        format!(
            "explain SELECT * FROM {} ORDER BY ts",
            quote_identifier(table_name)
        )
    }

    #[tokio::test]
    async fn shell_query_auto_refreshes_after_external_append() -> TestResult<()> {
        let tmp = build_table_with_rows(4).await?;
        let (mut ctx, table_name) =
            build_context(tmp.path().to_path_buf(), BackendArg::DataFusion).await?;

        let res = process_command(&mut ctx, &query_sql(&table_name))
            .await?
            .query_result
            .expect("query result");
        assert_eq!(res.total_rows, 4);

        let rel = "data/segment2.parquet";
        write_parquet_rows_with_base(&tmp.path().join(rel), 3, 1_700_000_100_000)?;
        let location = TableLocation::local(tmp.path());
        let mut other = TimeSeriesTable::open(location).await?;
        other.append_parquet_segment(rel, "ts").await?;

        let res = process_command(&mut ctx, &query_sql(&table_name))
            .await?
            .query_result
            .expect("query result");
        assert_eq!(res.total_rows, 7);

        Ok(())
    }

    #[tokio::test]
    async fn shell_append_then_query_sees_new_rows() -> TestResult<()> {
        let tmp = build_table_with_rows(5).await?;
        let (mut ctx, table_name) =
            build_context(tmp.path().to_path_buf(), BackendArg::DataFusion).await?;

        let rel = tmp.path().join("data/segment2.parquet");
        write_parquet_rows_with_base(&rel, 6, 1_700_000_100_000)?;

        let append_cmd = format!("append {}", rel.display());
        process_command(&mut ctx, &append_cmd).await?;

        let res = process_command(&mut ctx, &query_sql(&table_name))
            .await?
            .query_result
            .expect("query result");
        assert_eq!(res.total_rows, 11);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shell_explain_auto_refreshes() -> TestResult<()> {
        use timeseries_table_core::transaction_log::table_state::reset_rebuild_table_state_count;

        let tmp = build_table_with_rows(2).await?;
        let (mut ctx, table_name) =
            build_context(tmp.path().to_path_buf(), BackendArg::DataFusion).await?;

        let rel = "data/segment2.parquet";
        write_parquet_rows_with_base(&tmp.path().join(rel), 3, 1_700_000_100_000)?;
        let location = TableLocation::local(tmp.path());
        let mut other = TimeSeriesTable::open(location.clone()).await?;
        other.append_parquet_segment(rel, "ts").await?;

        let current_version = other.current_version().await?;
        assert!(current_version > ctx.table.state().version);

        reset_rebuild_table_state_count();
        let res = process_command(&mut ctx, &explain_sql(&table_name))
            .await?
            .query_result
            .expect("explain result");
        assert!(!res.columns.is_empty());
        assert_eq!(ctx.table.state().version, current_version);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shell_refresh_noop_does_not_rebuild() -> TestResult<()> {
        use timeseries_table_core::transaction_log::table_state::{
            rebuild_table_state_count, reset_rebuild_table_state_count,
        };

        let tmp = build_table_with_rows(1).await?;
        let (mut ctx, _table_name) =
            build_context(tmp.path().to_path_buf(), BackendArg::DataFusion).await?;

        reset_rebuild_table_state_count();
        process_command(&mut ctx, "refresh").await?;
        assert_eq!(rebuild_table_state_count(), 0);

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shell_timing_toggle_sets_query_elapsed() -> TestResult<()> {
        let tmp = build_table_with_rows(3).await?;
        let (mut ctx, table_name) =
            build_context(tmp.path().to_path_buf(), BackendArg::DataFusion).await?;

        process_command(&mut ctx, r"\timing").await?;
        let res = process_command(&mut ctx, &query_sql(&table_name))
            .await?
            .query_result
            .expect("query result");

        assert!(res.elapsed.is_some());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shell_exit_and_quit_break() -> TestResult<()> {
        let tmp = build_table_with_rows(1).await?;
        let (mut ctx, _table_name) =
            build_context(tmp.path().to_path_buf(), BackendArg::DataFusion).await?;

        let res = process_command(&mut ctx, "exit").await?;
        assert!(matches!(res.action, CommandAction::Break));

        let res = process_command(&mut ctx, "quit").await?;
        assert!(matches!(res.action, CommandAction::Break));

        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shell_help_and_unknown_continue() -> TestResult<()> {
        let tmp = build_table_with_rows(1).await?;
        let (mut ctx, _table_name) =
            build_context(tmp.path().to_path_buf(), BackendArg::DataFusion).await?;

        let res = process_command(&mut ctx, "help").await?;
        assert!(matches!(res.action, CommandAction::Continue));

        let res = process_command(&mut ctx, "not-a-command").await?;
        assert!(matches!(res.action, CommandAction::Continue));

        Ok(())
    }

    #[test]
    fn shell_query_parses_flags_and_sql() -> TestResult<()> {
        let (sql, opts) = parse_sql_command(
            "--max-rows 0 --format jsonl --output \"out.jsonl\" --timing SELECT \"my table\" FROM t",
            10,
            false,
            true,
        )?;

        assert_eq!(opts.max_rows, 0);
        assert!(matches!(opts.format, OutputFormat::Jsonl));
        assert_eq!(opts.output, Some(PathBuf::from("out.jsonl")));
        assert!(opts.timing);
        assert!(!opts.explain);
        assert_eq!(sql, "SELECT \"my table\" FROM t");

        Ok(())
    }

    #[test]
    fn shell_query_supports_double_dash_separator() -> TestResult<()> {
        let (sql, _opts) = parse_sql_command(
            "--max-rows=1 -- SELECT --not-a-flag FROM t",
            10,
            false,
            true,
        )?;
        assert_eq!(sql, "SELECT --not-a-flag FROM t");
        Ok(())
    }

    #[test]
    fn shell_query_accepts_explain_flag() -> TestResult<()> {
        let (sql, opts) = parse_sql_command("--explain SELECT 1", 10, false, true)?;
        assert!(opts.explain);
        assert_eq!(sql, "SELECT 1");
        Ok(())
    }

    #[test]
    fn shell_explain_rejects_explain_flag() -> TestResult<()> {
        let err = parse_sql_command("--explain SELECT 1", 1000, false, false).unwrap_err();
        assert!(err.contains("unknown flag"));
        Ok(())
    }
}
