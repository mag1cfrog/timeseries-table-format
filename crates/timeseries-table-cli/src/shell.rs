use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use chrono::Local;
use rustyline::{DefaultEditor, error::ReadlineError};
use snafu::ResultExt;
use terminal_size::terminal_size;
use timeseries_table_core::{
    metadata::table_metadata::{TableMeta, TimeBucket, TimeIndexSpec},
    storage::TableLocation,
    table::TimeSeriesTable,
};
use tokio::runtime::Handle;

use crate::{
    BackendArg,
    engine::{Engine, QuerySession},
    error::{
        CliError, CliResult, CreateTableSnafu, InvalidBucketSnafu, OpenTableSnafu, StorageSnafu,
    },
    make_engine, open_table,
    query::{
        OutputFormat, QueryOpts, default_table_name, page_output, preview_message,
        print_query_result, quote_identifier, render_preview, write_query_summary,
    },
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
    table: timeseries_table_core::table::TimeSeriesTable,
    session: BoxedSession,
    timing: bool,
    pager: bool,
    table_name: String,
    alias: Option<String>,
    last_status: Option<StatusInfo>,
}

struct StatusInfo {
    label: String,
    elapsed: Option<Duration>,
    timestamp: chrono::DateTime<chrono::Local>,
}

fn print_help() {
    println!(
        r#"commands:
  refresh
  append <parquet_path>
  query [--max-rows N] [--format csv|jsonl] [--output PATH] [--timing] [--explain] [--] <sql>
  explain [--max-rows N] [--format csv|jsonl] [--output PATH] [--timing] [--] <sql>
  \timing           toggle per-command elapsed time
  \pager            toggle pager output (less -S)
  clear | cls
  alias <name>      set prompt alias (SQL alias rewrite too)
  alias             show current alias
  alias --clear     reset alias to default
  help
  exit | quit
notes:
  - use `--` to separate flags from SQL (e.g. SQL with leading `--`)
"#
    );
}

fn clear_screen() {
    // ANSI clear screen + cursor home; best-effort.
    print!("\x1b[2J\x1b[H");
    let _ = std::io::stdout().flush();
}

fn use_color() -> bool {
    std::env::var_os("NO_COLOR").is_none() && std::env::var("TERM").ok().as_deref() != Some("dumb")
}

fn strip_ansi_len(s: &str) -> usize {
    let mut len = 0;
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && matches!(chars.peek(), Some('[')) {
            chars.next();
            for c in chars.by_ref() {
                if c == 'm' {
                    break;
                }
            }
            continue;
        }
        len += 1;
    }
    len
}

fn format_elapsed(elapsed: Duration) -> String {
    if elapsed.as_millis() < 1000 {
        format!("{}ms", elapsed.as_millis())
    } else {
        format!("{:.2}s", elapsed.as_secs_f64())
    }
}

fn render_status_line(ctx: &ShellContext, width: usize, color: bool) -> String {
    let display_name = ctx.alias.as_deref().unwrap_or(&ctx.table_name);

    let mut flags = Vec::new();
    if ctx.timing {
        flags.push("timing");
    }
    if ctx.pager {
        flags.push("pager");
    }
    let flag_text = if flags.is_empty() {
        String::new()
    } else {
        format!(" [{}]", flags.join(","))
    };

    let (accent, dim, bold, reset) = if color {
        ("\x1b[38;5;45m", "\x1b[2m", "\x1b[1m", "\x1b[0m")
    } else {
        ("", "", "", "")
    };

    let left = format!("{accent}+-{reset} {bold}{display_name}{reset}{dim}{flag_text}{reset}");

    let right = ctx.last_status.as_ref().map(|status| {
        let time_str = status.timestamp.format("%I:%M:%S %p").to_string();
        let mut parts = Vec::new();
        parts.push(status.label.clone());
        if let Some(elapsed) = status.elapsed {
            parts.push(format_elapsed(elapsed));
        }
        parts.push(time_str);
        let payload = parts.join(" | ");
        format!("{dim}[{payload}]{reset}")
    });

    let mut line = left;
    if let Some(right) = right {
        let left_len = strip_ansi_len(&line);
        let right_len = strip_ansi_len(&right);
        if width > left_len + right_len + 1 {
            line.push_str(&" ".repeat(width - left_len - right_len));
            line.push_str(&right);
        } else {
            line.push(' ');
            line.push_str(&right);
        }
    }

    line
}

fn format_status_line(ctx: &ShellContext) -> Option<String> {
    let (width, _) = terminal_size().map(|(w, h)| (w.0 as usize, h.0 as usize))?;
    Some(render_status_line(ctx, width, use_color()))
}

fn format_prompt() -> String {
    if use_color() {
        "\x1b[38;5;39m>>\x1b[0m ".to_string()
    } else {
        ">> ".to_string()
    }
}

fn set_status(ctx: &mut ShellContext, label: &str, elapsed: Option<Duration>) {
    ctx.last_status = Some(StatusInfo {
        label: label.to_string(),
        elapsed,
        timestamp: Local::now(),
    });
}

fn query_elapsed(
    run_result: &Result<crate::query::QueryResult, CliError>,
    run_start: Instant,
) -> Duration {
    match run_result {
        Ok(res) => res.elapsed.unwrap_or_else(|| run_start.elapsed()),
        Err(_) => run_start.elapsed(),
    }
}

fn table_log_exists(root: &Path) -> bool {
    root.join("_timeseries_log").exists()
}

fn prompt_line(prompt: &str) -> CliResult<String> {
    print!("{prompt}");
    io::stdout()
        .flush()
        .map_err(|e| CliError::PathInvariantNoSource {
            message: format!("failed to flush stdout: {e}"),
            path: None,
        })?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| CliError::PathInvariantNoSource {
            message: format!("failed to read input: {e}"),
            path: None,
        })?;
    Ok(input.trim().to_string())
}

fn prompt_non_empty(prompt: &str) -> CliResult<String> {
    loop {
        let input = prompt_line(prompt)?;
        if !input.trim().is_empty() {
            return Ok(input);
        }
    }
}

fn prompt_optional(prompt: &str) -> CliResult<Option<String>> {
    let input = prompt_line(prompt)?;
    let trimmed = input.trim();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_string()))
    }
}

fn parse_time_bucket(spec: &str) -> CliResult<TimeBucket> {
    spec.parse::<TimeBucket>().context(InvalidBucketSnafu {
        spec: spec.to_string(),
    })
}

async fn create_table_interactive(table_root: &Path) -> CliResult<()> {
    println!("table not found; creating new table...");
    let time_column = prompt_non_empty("time column name: ")?;
    let bucket = loop {
        let spec = prompt_non_empty("time bucket (e.g. 1s, 1m, 1h, 1d): ")?;
        match parse_time_bucket(&spec) {
            Ok(b) => break b,
            Err(e) => println!("{e}"),
        }
    };
    let timezone = prompt_optional("timezone (optional, IANA TZ): ")?;
    let entities = prompt_optional("entity columns (comma-separated, optional): ")?
        .map(|s| {
            s.split(',')
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let index = TimeIndexSpec {
        timestamp_column: time_column,
        bucket,
        timezone,
        entity_columns: entities,
    };
    let meta = TableMeta::new_time_series(index);
    let location =
        TableLocation::parse(table_root.to_string_lossy().as_ref()).context(StorageSnafu)?;
    TimeSeriesTable::create(location, meta)
        .await
        .context(CreateTableSnafu {
            table: table_root.display().to_string(),
        })?;

    println!("created table at {}", table_root.display());
    Ok(())
}

async fn append_first_segment(
    table_root: &Path,
    location: &TableLocation,
    table: &mut TimeSeriesTable,
) -> CliResult<()> {
    loop {
        let parquet_path = prompt_non_empty("first segment parquet path: ")?;
        let parquet_path = PathBuf::from(parquet_path);

        if let Err(e) = table.refresh().await.context(OpenTableSnafu {
            table: table_root.display().to_string(),
        }) {
            println!("{e}");
            continue;
        }

        let rel = match location.ensure_parquet_under_root(&parquet_path).await {
            Ok(r) => r,
            Err(e) => {
                println!("{}", CliError::Storage { source: e });
                continue;
            }
        };

        let rel_str = if cfg!(windows) {
            rel.to_string_lossy().replace('\\', "/")
        } else {
            rel.to_string_lossy().to_string()
        };

        let ts_col = table.index_spec().timestamp_column.clone();
        match table.append_parquet_segment(&rel_str, &ts_col).await {
            Ok(s) => {
                println!("appended: {rel_str}, size: {s}.");
                break;
            }
            Err(e) => {
                println!(
                    "{}",
                    CliError::AppendSegment {
                        table: table_root.display().to_string(),
                        source: Box::new(e),
                    }
                );
            }
        }
    }

    Ok(())
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
            pager: false,
            table_name: table_name.clone(),
            alias: None,
            last_status: None,
        },
        table_name,
    ))
}

fn is_valid_alias(alias: &str) -> bool {
    let mut chars = alias.chars();
    let first = match chars.next() {
        Some(c) => c,
        None => return false,
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }

    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn rewrite_sql_alias(sql: &str, alias: &str, actual: &str) -> String {
    if alias == actual {
        return sql.to_string();
    }

    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_single = false;

    while let Some(ch) = chars.next() {
        if ch == '\'' {
            out.push(ch);
            if in_single {
                if matches!(chars.peek().copied(), Some('\'')) {
                    if let Some(next) = chars.next() {
                        out.push(next);
                    }
                } else {
                    in_single = false;
                }
            } else {
                in_single = true;
            }
            continue;
        }

        if in_single {
            out.push(ch);
            continue;
        }

        if ch == '"' {
            let mut buf = String::new();
            while let Some(n) = chars.next() {
                if n == '"' {
                    if let Some('"') = chars.peek().copied() {
                        // Escaped quote inside identifier.
                        chars.next();
                        buf.push('"');
                        continue;
                    }
                    break;
                }
                buf.push(n);
            }
            if buf == alias {
                out.push_str(&quote_identifier(actual));
            } else {
                out.push('"');
                out.push_str(&buf.replace('"', "\"\""));
                out.push('"');
            }
            continue;
        }

        if ch == '-' && matches!(chars.peek().copied(), Some('-')) {
            // Line comment: copy through end of line.
            out.push(ch);
            if let Some(next) = chars.next() {
                out.push(next);
            } else {
                continue;
            }

            for n in chars.by_ref() {
                out.push(n);
                if n == '\n' {
                    break;
                }
            }
            continue;
        }

        if ch == '/' && matches!(chars.peek().copied(), Some('*')) {
            // Block comment: copy through closing */
            out.push(ch);
            if let Some(next) = chars.next() {
                out.push(next);
            } else {
                continue;
            }
            while let Some(n) = chars.next() {
                out.push(n);
                if n == '*' && matches!(chars.peek().copied(), Some('/')) {
                    if let Some(close) = chars.next() {
                        out.push(close);
                    }
                    break;
                }
            }
            continue;
        }

        if ch.is_ascii_alphanumeric() || ch == '_' {
            let mut ident = String::new();
            ident.push(ch);
            while let Some(n) = chars.peek().copied() {
                if n.is_ascii_alphanumeric() || n == '_' {
                    ident.push(n);
                    chars.next();
                } else {
                    break;
                }
            }
            if ident == alias {
                out.push_str(actual);
            } else {
                out.push_str(&ident);
            }
            continue;
        }

        out.push(ch);
    }

    out
}

fn lex_with_spans(input: &str) -> Result<Vec<TokenSpan>, String> {
    let mut i = 0;
    let mut tokens = Vec::new();
    let len = input.len();

    while i < len {
        while i < len {
            let ch = match input[i..].chars().next() {
                Some(ch) => ch,
                None => break,
            };
            if ch.is_ascii_whitespace() {
                i += ch.len_utf8();
            } else {
                break;
            }
        }
        if i >= len {
            break;
        }

        let start = i;
        let mut value = String::new();
        let mut quote = None;
        if let Some(ch) = input[i..].chars().next()
            && (ch == '"' || ch == '\'')
        {
            quote = Some(ch);
            i += ch.len_utf8();
        }

        if let Some(q) = quote {
            let mut closed = false;
            while i < len {
                let ch = match input[i..].chars().next() {
                    Some(ch) => ch,
                    None => break,
                };
                if ch == q {
                    i += ch.len_utf8();
                    closed = true;
                    break;
                }
                if ch == '\\' {
                    i += ch.len_utf8();
                    if i >= len {
                        return Err("unterminated escape in quoted token".to_string());
                    }
                    let esc = input[i..]
                        .chars()
                        .next()
                        .ok_or_else(|| "unterminated escape in quoted token".to_string())?;
                    value.push(esc);
                    i += esc.len_utf8();
                    continue;
                }
                value.push(ch);
                i += ch.len_utf8();
            }
            if !closed {
                return Err("unterminated quoted token".to_string());
            }
        } else {
            while i < len {
                let ch = match input[i..].chars().next() {
                    Some(ch) => ch,
                    None => break,
                };
                if ch.is_ascii_whitespace() {
                    break;
                }
                if ch == '\\' {
                    i += ch.len_utf8();
                    if i >= len {
                        return Err("unterminated escape in token".to_string());
                    }
                    let esc = input[i..]
                        .chars()
                        .next()
                        .ok_or_else(|| "unterminated escape in token".to_string())?;
                    value.push(esc);
                    i += esc.len_utf8();
                    continue;
                }
                value.push(ch);
                i += ch.len_utf8();
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

    if trimmed == "clear" || trimmed == "cls" {
        clear_screen();
        return Ok(CommandResult {
            action: CommandAction::Continue,
            query_result: None,
        });
    }

    if trimmed == "alias" {
        match &ctx.alias {
            Some(name) => println!("alias: {name}"),
            None => println!("alias: (default)"),
        }
        return Ok(CommandResult {
            action: CommandAction::Continue,
            query_result: None,
        });
    }

    if trimmed == "unalias" || trimmed == "alias --clear" {
        ctx.alias = None;
        println!("alias: (default)");
        return Ok(CommandResult {
            action: CommandAction::Continue,
            query_result: None,
        });
    }

    if let Some(rest) = trimmed.strip_prefix("alias ") {
        let name = rest.trim();
        if name.is_empty() {
            println!("alias requires a name");
        } else if !is_valid_alias(name) {
            println!("alias must match [A-Za-z_][A-Za-z0-9_]*");
        } else {
            ctx.alias = Some(name.to_string());
            println!("alias: {name}");
        }
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

    if trimmed == r"\pager" || trimmed == r"\\pager" {
        ctx.pager = !ctx.pager;
        println!("pager: {}", if ctx.pager { "on" } else { "off" });
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

        set_status(ctx, "refresh", Some(start.elapsed()));
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

        set_status(ctx, "append", Some(start.elapsed()));
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
        let refresh_start = Instant::now();
        let refreshed = match ctx.table.refresh().await {
            Ok(changed) => {
                if changed {
                    ctx.session = ctx.engine.prepare_session_from_table(&ctx.table).await?;
                }
                changed
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
        };
        let refresh_elapsed = refresh_start.elapsed();

        let sql = if let Some(alias) = ctx.alias.as_deref() {
            rewrite_sql_alias(sql.trim(), alias, &ctx.table_name)
        } else {
            sql.trim().to_string()
        };

        let run_start = Instant::now();
        let run_result = ctx.session.run_query(sql.trim(), &opts).await;
        let query_elapsed = query_elapsed(&run_result, run_start);
        let (label, elapsed) = if refreshed {
            ("query+refresh", query_elapsed + refresh_elapsed)
        } else {
            ("query", query_elapsed)
        };
        set_status(ctx, label, Some(elapsed));

        let res = match run_result {
            Ok(res) => {
                if ctx.pager {
                    if let Some(rendered) = render_preview(&res, &opts) {
                        let _ = page_output(&rendered);
                    }
                    if let Some(message) = preview_message(&res, &opts) {
                        println!("{message}");
                    }
                    let _ = write_query_summary(&res, &opts, &mut std::io::stdout());
                } else {
                    let _ = print_query_result(&res, &opts);
                }
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
        let refresh_start = Instant::now();
        let refreshed = match ctx.table.refresh().await {
            Ok(changed) => {
                if changed {
                    ctx.session = ctx.engine.prepare_session_from_table(&ctx.table).await?;
                }
                changed
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
        };
        let refresh_elapsed = refresh_start.elapsed();

        // plan-only: just run an EXPLAIN statement through the same session
        let sql = if let Some(alias) = ctx.alias.as_deref() {
            rewrite_sql_alias(sql.trim(), alias, &ctx.table_name)
        } else {
            sql.trim().to_string()
        };

        let explain_sql = format!("EXPLAIN {}", sql.trim());
        opts.explain = false; // because we are explicitly running EXPLAIN

        let run_start = Instant::now();
        let run_result = ctx.session.run_query(&explain_sql, &opts).await;
        let query_elapsed = query_elapsed(&run_result, run_start);
        let (label, elapsed) = if refreshed {
            ("explain+refresh", query_elapsed + refresh_elapsed)
        } else {
            ("explain", query_elapsed)
        };
        set_status(ctx, label, Some(elapsed));

        let res = match run_result {
            Ok(res) => {
                if ctx.pager {
                    if let Some(rendered) = render_preview(&res, &opts) {
                        let _ = page_output(&rendered);
                    }
                    if let Some(message) = preview_message(&res, &opts) {
                        println!("{message}");
                    }
                    let _ = write_query_summary(&res, &opts, &mut std::io::stdout());
                } else {
                    let _ = print_query_result(&res, &opts);
                }
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
    table_root: Option<PathBuf>,
    history: Option<PathBuf>,
    backend: BackendArg,
) -> CliResult<()> {
    let table_root = match table_root {
        Some(path) => path,
        None => PathBuf::from(prompt_non_empty("table root path: ")?),
    };

    if !table_log_exists(&table_root) {
        handle.block_on(create_table_interactive(&table_root))?;
    }

    let location =
        TableLocation::parse(table_root.to_string_lossy().as_ref()).context(StorageSnafu)?;
    let mut table = handle.block_on(open_table(location.clone(), table_root.as_path()))?;

    if table.state().table_meta.logical_schema().is_none() {
        println!("table has no schema yet; please append the first segment.");
        handle.block_on(append_first_segment(&table_root, &location, &mut table))?;
    }

    let (mut ctx, table_name) = handle.block_on(build_context(table_root, backend))?;

    let history_path = history.unwrap_or_else(|| ctx.table_root.join(".ts_table_history"));

    // rustyline editor
    let mut rl = DefaultEditor::new().map_err(|e| CliError::PathInvariantNoSource {
        message: format!("failed to initialize readline: {e}"),
        path: None,
    })?;

    // history best-effort
    if let Err(e) = rl.load_history(&history_path) {
        eprintln!(
            "warning: failed to load history from {}: {e}",
            history_path.display()
        );
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
        if let Some(line) = format_status_line(&ctx) {
            println!("{line}");
        }
        let prompt = format_prompt();

        let line = match rl.readline(&prompt) {
            Ok(l) => l,
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C
                println!("^C");
                break;
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

    if let Err(e) = rl.save_history(&history_path) {
        eprintln!(
            "warning: failed to save history to {}: {e}",
            history_path.display()
        );
    }

    Ok(())
}

/// Run an interactive shell in a blocking thread (rustyline is blocking).
pub async fn cmd_shell(
    table_root: Option<PathBuf>,
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
    use snafu::ResultExt;
    use tempfile::TempDir;
    use timeseries_table_core::{
        metadata::logical_schema::{
            LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit,
        },
        metadata::table_metadata::{TableMeta, TimeBucket, TimeIndexSpec},
        storage::TableLocation,
        table::TimeSeriesTable,
    };

    mod test_common {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/common/mod.rs"));
    }

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn make_status_ctx() -> TestResult<(ShellContext, TempDir)> {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().to_path_buf();
        let location =
            TableLocation::parse(table_root.to_string_lossy().as_ref()).context(StorageSnafu)?;
        let meta = make_table_meta()?;

        let rt = tokio::runtime::Runtime::new()?;
        let (ctx, _) = rt.block_on(async {
            TimeSeriesTable::create(location.clone(), meta)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
            build_context(table_root.clone(), BackendArg::DataFusion)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        })?;

        Ok((ctx, tmp))
    }

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
        test_common::write_parquet_rows_with_base(&tmp.path().join(rel), 3, 1_700_000_100_000)?;
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
        test_common::write_parquet_rows_with_base(&rel, 6, 1_700_000_100_000)?;

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
        test_common::write_parquet_rows_with_base(&tmp.path().join(rel), 3, 1_700_000_100_000)?;
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
    fn alias_validation_rejects_invalid_names() {
        assert!(is_valid_alias("t"));
        assert!(is_valid_alias("_t2"));
        assert!(!is_valid_alias(""));
        assert!(!is_valid_alias("2bad"));
        assert!(!is_valid_alias("bad-name"));
        assert!(!is_valid_alias("bad name"));
        assert!(!is_valid_alias("bad;drop"));
    }

    #[test]
    fn alias_rewrite_replaces_identifiers_only() {
        let sql = "select * from t where note = 't' and tt = 1";
        let rewritten = rewrite_sql_alias(sql, "t", "nyc_hvfhv");
        assert_eq!(
            rewritten,
            "select * from nyc_hvfhv where note = 't' and tt = 1"
        );
    }

    #[test]
    fn alias_rewrite_handles_quoted_identifiers() {
        let sql = "select * from \"t\" join \"other\" on \"t\".id = other.id";
        let rewritten = rewrite_sql_alias(sql, "t", "nyc_hvfhv");
        assert_eq!(
            rewritten,
            "select * from \"nyc_hvfhv\" join \"other\" on \"nyc_hvfhv\".id = other.id"
        );
    }

    #[test]
    fn alias_rewrite_skips_line_comments() {
        let sql = "select * from t -- alias t\nwhere t.id = 1";
        let rewritten = rewrite_sql_alias(sql, "t", "nyc_hvfhv");
        assert_eq!(
            rewritten,
            "select * from nyc_hvfhv -- alias t\nwhere nyc_hvfhv.id = 1"
        );
    }

    #[test]
    fn alias_rewrite_skips_block_comments() {
        let sql = "select * from t /* alias t */ where t.id = 1";
        let rewritten = rewrite_sql_alias(sql, "t", "nyc_hvfhv");
        assert_eq!(
            rewritten,
            "select * from nyc_hvfhv /* alias t */ where nyc_hvfhv.id = 1"
        );
    }

    #[test]
    fn alias_rewrite_handles_comment_markers_in_strings() {
        let sql = "select '-- not comment' as note, '/* nope */' as note2 from t";
        let rewritten = rewrite_sql_alias(sql, "t", "nyc_hvfhv");
        assert_eq!(
            rewritten,
            "select '-- not comment' as note, '/* nope */' as note2 from nyc_hvfhv"
        );
    }

    #[test]
    fn alias_rewrite_handles_escaped_single_quotes() {
        let sql = "select 'it''s fine' as note from t";
        let rewritten = rewrite_sql_alias(sql, "t", "nyc_hvfhv");
        assert_eq!(rewritten, "select 'it''s fine' as note from nyc_hvfhv");
    }

    #[test]
    fn table_log_exists_detects_log_dir() -> TestResult {
        let tmp = TempDir::new()?;
        assert!(!table_log_exists(tmp.path()));
        std::fs::create_dir_all(tmp.path().join("_timeseries_log"))?;
        assert!(table_log_exists(tmp.path()));
        Ok(())
    }

    #[test]
    fn parse_time_bucket_accepts_and_rejects() {
        assert!(parse_time_bucket("1s").is_ok());
        assert!(parse_time_bucket("1m").is_ok());
        assert!(parse_time_bucket("1h").is_ok());
        assert!(parse_time_bucket("1d").is_ok());
        assert!(parse_time_bucket("1x").is_err());
        assert!(parse_time_bucket("bogus").is_err());
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

    #[test]
    fn status_line_right_aligns_when_space_allows() -> TestResult {
        let (mut ctx, _tmp) = make_status_ctx()?;
        ctx.alias = Some("alias".to_string());
        ctx.timing = true;
        ctx.pager = true;
        ctx.last_status = Some(StatusInfo {
            label: "query".to_string(),
            elapsed: Some(Duration::from_millis(12)),
            timestamp: Local::now(),
        });

        let line = render_status_line(&ctx, 80, false);
        assert!(line.contains("alias"));
        assert!(line.contains("[timing,pager]"));
        assert!(line.contains("[query | 12ms"));
        assert!(line.len() <= 80);
        assert!(line.ends_with(']'));
        Ok(())
    }

    #[test]
    fn status_line_omits_right_block_without_status() -> TestResult {
        let (mut ctx, _tmp) = make_status_ctx()?;
        ctx.alias = Some("alias".to_string());

        let line = render_status_line(&ctx, 40, false);
        assert!(line.contains("alias"));
        assert!(!line.contains('['));
        Ok(())
    }

    #[test]
    fn status_line_no_color_has_no_ansi() -> TestResult {
        let (mut ctx, _tmp) = make_status_ctx()?;
        ctx.last_status = Some(StatusInfo {
            label: "refresh".to_string(),
            elapsed: Some(Duration::from_millis(5)),
            timestamp: Local::now(),
        });

        let line = render_status_line(&ctx, 60, false);
        assert!(!line.contains('\u{1b}'));
        Ok(())
    }

    #[test]
    fn format_elapsed_formats_ms_and_seconds() {
        assert_eq!(format_elapsed(Duration::from_millis(950)), "950ms");
        assert_eq!(format_elapsed(Duration::from_millis(1500)), "1.50s");
    }

    #[test]
    fn query_elapsed_prefers_result_elapsed() {
        let result = Ok(crate::query::QueryResult {
            columns: vec![],
            preview_rows: vec![],
            total_rows: 0,
            elapsed: Some(Duration::from_millis(7)),
        });
        let elapsed = query_elapsed(&result, Instant::now());
        assert_eq!(elapsed, Duration::from_millis(7));
    }

    #[test]
    fn query_elapsed_uses_run_start_when_missing() {
        let result = Ok(crate::query::QueryResult {
            columns: vec![],
            preview_rows: vec![],
            total_rows: 0,
            elapsed: None,
        });
        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(5));
        let elapsed = query_elapsed(&result, start);
        assert!(elapsed >= Duration::from_millis(5));
    }
    #[test]
    fn shell_query_preserves_unicode_tokens() -> TestResult<()> {
        let (sql, opts) =
            parse_sql_command("--output café.jsonl SELECT café FROM t", 10, false, true)?;
        assert_eq!(opts.output, Some(PathBuf::from("café.jsonl")));
        assert_eq!(sql, "SELECT café FROM t");
        Ok(())
    }

    #[test]
    fn shell_query_preserves_unicode_in_quoted_tokens() -> TestResult<()> {
        let (sql, opts) = parse_sql_command(
            "--output \"mañana-Δ.jsonl\" SELECT 'naïve' AS note FROM \"tést\"",
            10,
            false,
            true,
        )?;
        assert_eq!(opts.output, Some(PathBuf::from("mañana-Δ.jsonl")));
        assert_eq!(sql, "SELECT 'naïve' AS note FROM \"tést\"");
        Ok(())
    }
}
