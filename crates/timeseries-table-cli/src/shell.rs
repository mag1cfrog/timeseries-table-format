use std::{path::PathBuf, time::Instant};

use rustyline::{DefaultEditor, error::ReadlineError};
use snafu::ResultExt;
use timeseries_table_core::storage::TableLocation;
use tokio::runtime::Handle;

use crate::{
    BackendArg,
    error::{CliError, CliResult, OpenTableSnafu, StorageSnafu},
    make_engine, open_table,
    query::{OutputFormat, QueryOpts, default_table_name, print_query_result, quote_identifier},
};

fn print_help() {
    println!(
        r#"commands:
  refresh
  append <parquet_path>
  query <sql>
  explain <sql>
  \timing           toggle per-command elapsed time
  exit | quit
"#
    );
}

fn shell_blocking(
    handle: Handle,
    table_root: PathBuf,
    history: Option<PathBuf>,
    backend: BackendArg,
) -> CliResult<()> {
    // Parse location once.
    let location =
        TableLocation::parse(table_root.to_string_lossy().as_ref()).context(StorageSnafu)?;

    // Cached mutable table handle for refresh/append.
    let mut table =
        handle.block_on(async { open_table(location.clone(), table_root.as_path()).await })?;

    // Cached query session
    let engine = make_engine(backend.into(), table_root.as_path());
    let mut session = handle.block_on(async { engine.prepare_session().await })?;

    let table_name = session
        .table_name()
        .map(|s| s.to_string())
        .unwrap_or_else(|| default_table_name(&table_root));

    let history_path = history.unwrap_or_else(|| table_root.join(".ts_table_history"));

    // rustyline editor
    let mut rl = DefaultEditor::new().map_err(|e| CliError::PathInvariantNoSource {
        message: format!("failed to initialize readline: {e}"),
        path: None,
    })?;

    // history best-effort
    {
        let _ = rl.load_history(&history_path);
    }

    let mut timing = false;

    println!("ts-table shell");
    println!("table: {}", table_root.display());
    println!(
        "registered as: {} (quoted: {})",
        table_name,
        quote_identifier(&table_name)
    );
    println!("type 'help' for commands\n");

    loop {
        let prompt = if timing {
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

        // Commands
        if trimmed == "exit" || trimmed == "quit" {
            break;
        }

        if trimmed == "help" {
            print_help();
            continue;
        }

        if trimmed == r"\timing" || trimmed == r"\\timing" {
            timing = !timing;
            println!("timing: {}", if timing { "on" } else { "off" });
            continue;
        }

        if trimmed == "refresh" {
            let start = Instant::now();
            let changed = handle
                .block_on(async { table.refresh().await })
                .map_err(|e| CliError::OpenTable {
                    table: table_root.display().to_string(),
                    source: Box::new(e),
                });

            match changed {
                Ok(changed) => {
                    if timing {
                        println!(
                            "refreshed: {} (elapsed_ms: {})",
                            changed,
                            start.elapsed().as_millis()
                        );
                    } else {
                        println!("refreshed: {changed}");
                    }

                    // Optional but simple: rebuild query session after refresh so schema/registration stays clean.
                    session = handle.block_on(async { engine.prepare_session().await })?;
                }
                Err(e) => println!("{e}"),
            }
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("append ") {
            let start = Instant::now();
            // 1) refresh first
            if let Err(e) = handle.block_on(async {
                table.refresh().await.context(OpenTableSnafu {
                    table: table_root.display().to_string(),
                })
            }) {
                println!("{e}");
                continue;
            }

            // 2) ensure parquet under root
            let parquet_path = PathBuf::from(rest.trim());
            let rel = match handle
                .block_on(async { location.ensure_parquet_under_root(&parquet_path).await })
            {
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

            // 3) append (uses cached handle)
            let ts_col = table.index_spec().timestamp_column.clone();
            match handle.block_on(async { table.append_parquet_segment(&rel_str, &ts_col).await }) {
                Ok(s) => {
                    if timing {
                        println!(
                            "appended: {rel_str}, size: {s}. (elapsed_ms: {})",
                            start.elapsed().as_millis()
                        );
                    } else {
                        println!("appended: {rel_str}, size: {s}.");
                    }
                    // Rebuild query session so it surely sees the latest snapshot/provider state.
                    session = handle.block_on(async { engine.prepare_session().await })?;
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
            continue;
        }

        if let Some(sql) = trimmed.strip_prefix("query ") {
            let opts = QueryOpts {
                explain: false,
                timing,
                max_rows: 10,
                output: None,
                format: OutputFormat::Csv,
            };

            match handle.block_on(async { session.run_query(sql.trim(), &opts).await }) {
                Ok(res) => {
                    let _ = print_query_result(&res, &opts);
                }
                Err(e) => println!("{e}"),
            }
            continue;
        }

        if let Some(sql) = trimmed.strip_prefix("explain ") {
            // plan-only: just run an EXPLAIN statement through the same session
            let explain_sql = format!("EXPLAIN {}", sql.trim());
            let opts = QueryOpts {
                explain: false, // because we are explicitly running EXPLAIN
                timing,
                max_rows: 1000,
                output: None,
                format: OutputFormat::Csv,
            };

            match handle.block_on(async { session.run_query(&explain_sql, &opts).await }) {
                Ok(res) => {
                    let _ = print_query_result(&res, &opts);
                }
                Err(e) => println!("{e}"),
            }
            continue;
        }

        println!("unknown command. type 'help'.");
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
