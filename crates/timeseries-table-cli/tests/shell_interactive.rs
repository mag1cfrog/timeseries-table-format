#![allow(missing_docs)]

use std::{process::Stdio, time::Duration};
use tempfile::TempDir;
use timeseries_table_core::{
    storage::TableLocation,
    time_series_table::TimeSeriesTable,
    transaction_log::{TableMeta, TimeBucket, TimeIndexSpec},
};
use tokio::io::AsyncWriteExt;

mod test_common {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/common/mod.rs"));
}

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn run_shell_with_input(args: &[&str], input: &str) -> TestResult<std::process::Output> {
    let bin = assert_cmd::cargo::cargo_bin!("timeseries-table-cli");
    let mut cmd = tokio::process::Command::new(bin);
    cmd.args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(input.as_bytes()).await?;
        drop(stdin);
    }

    let output = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .map_err(|_| "command timed out")??;

    Ok(output)
}

fn make_table_meta(
    time_column: &str,
    bucket: TimeBucket,
    entity_columns: Vec<String>,
) -> TableMeta {
    let index = TimeIndexSpec {
        timestamp_column: time_column.to_string(),
        entity_columns,
        bucket,
        timezone: None,
    };
    TableMeta::new_time_series(index)
}

async fn create_empty_table(path: &std::path::Path) -> TestResult<()> {
    let location = TableLocation::local(path);
    let meta = make_table_meta("ts", TimeBucket::Seconds(1), Vec::new());
    TimeSeriesTable::create(location, meta).await?;
    Ok(())
}

fn write_segment(path: &std::path::Path) -> TestResult<()> {
    test_common::write_parquet_rows(path, 5)?;
    Ok(())
}

fn shell_input_for_create(
    table_root: &std::path::Path,
    time_column: &str,
    bucket: &str,
    timezone: Option<&str>,
    entities: Option<&str>,
    first_segment: &std::path::Path,
) -> String {
    let mut input = String::new();
    input.push_str(&format!("{}\n", table_root.display()));
    input.push_str(&format!("{time_column}\n"));
    input.push_str(&format!("{bucket}\n"));
    if let Some(tz) = timezone {
        input.push_str(tz);
    }
    input.push('\n');
    if let Some(ent) = entities {
        input.push_str(ent);
    }
    input.push('\n');
    input.push_str(&format!("{}\n", first_segment.display()));
    input.push_str("exit\n");
    input
}

#[tokio::test]
async fn shell_interactive_create_and_append() -> TestResult<()> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    let seg_path = table_root.join("data/seg.parquet");
    write_segment(&seg_path)?;

    let input = shell_input_for_create(&table_root, "ts", "1s", None, None, &seg_path);

    let output = run_shell_with_input(&["shell"], &input).await?;

    assert!(output.status.success());
    assert!(table_root.join("_timeseries_log").exists());

    let table = TimeSeriesTable::open(TableLocation::local(&table_root)).await?;
    assert!(table.state().table_meta.logical_schema().is_some());

    Ok(())
}

#[tokio::test]
async fn shell_interactive_existing_table_skips_first_append_prompt() -> TestResult<()> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_empty_table(&table_root).await?;
    let seg_path = table_root.join("data/seg.parquet");
    write_segment(&seg_path)?;
    {
        let location = TableLocation::local(&table_root);
        let mut table = TimeSeriesTable::open(location).await?;
        table
            .append_parquet_segment("data/seg.parquet", "ts")
            .await?;
    }

    let input = format!("{}\nexit\n", table_root.display());
    let output = run_shell_with_input(&["shell"], &input).await?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("table has no schema yet"));
    assert!(!stdout.contains("first segment parquet path"));

    Ok(())
}

#[tokio::test]
async fn shell_with_table_prompts_for_first_segment() -> TestResult<()> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_empty_table(&table_root).await?;
    let seg_path = table_root.join("data/seg.parquet");
    write_segment(&seg_path)?;

    let input = format!("{}\nexit\n", seg_path.display());
    let table_root_str = table_root.to_string_lossy();
    let output =
        run_shell_with_input(&["shell", "--table", table_root_str.as_ref()], &input).await?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("table has no schema yet"));
    assert!(stdout.contains("first segment parquet path"));

    let table = TimeSeriesTable::open(TableLocation::local(&table_root)).await?;
    assert!(table.state().table_meta.logical_schema().is_some());

    Ok(())
}

#[tokio::test]
async fn shell_interactive_reprompts_invalid_bucket() -> TestResult<()> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    let seg_path = table_root.join("data/seg.parquet");
    write_segment(&seg_path)?;

    let mut input = String::new();
    input.push_str(&format!("{}\n", table_root.display()));
    input.push_str("ts\n");
    input.push_str("1x\n");
    input.push_str("1s\n");
    input.push('\n'); // timezone
    input.push('\n'); // entities
    input.push_str(&format!("{}\n", seg_path.display()));
    input.push_str("exit\n");

    let output = run_shell_with_input(&["shell"], &input).await?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Invalid --bucket '1x'"));

    let table = TimeSeriesTable::open(TableLocation::local(&table_root)).await?;
    assert!(table.state().table_meta.logical_schema().is_some());

    Ok(())
}

#[tokio::test]
async fn shell_interactive_parses_entity_columns() -> TestResult<()> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    let seg_path = table_root.join("data/seg.parquet");
    write_segment(&seg_path)?;

    let input = shell_input_for_create(&table_root, "ts", "1s", None, Some("symbol"), &seg_path);

    let output = run_shell_with_input(&["shell"], &input).await?;

    assert!(output.status.success());
    let table = TimeSeriesTable::open(TableLocation::local(&table_root)).await?;
    assert_eq!(
        table.index_spec().entity_columns,
        vec!["symbol".to_string()]
    );

    Ok(())
}
