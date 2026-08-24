#![allow(missing_docs)]

use std::{fs::File, num::NonZeroU64, process::Stdio, sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use tempfile::TempDir;
use timeseries_table_format::{
    metadata::{
        index::{IndexKind, IndexSpec, TimeIndexGranularity},
        table::TableMeta,
    },
    storage::TableLocation,
    table::TimeSeriesTable,
};
use tokio::io::AsyncWriteExt;

mod test_common {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/common/mod.rs"));
}

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn run_shell_with_input(args: &[&str], input: &str) -> TestResult<std::process::Output> {
    run_shell_with_input_and_filter(args, input, None).await
}

async fn run_shell_with_input_and_filter(
    args: &[&str],
    input: &str,
    rust_log: Option<&str>,
) -> TestResult<std::process::Output> {
    let bin = assert_cmd::cargo::cargo_bin!("tstable");
    let mut cmd = tokio::process::Command::new(bin);
    cmd.args(args)
        .env_remove("RUST_LOG")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(filter) = rust_log {
        cmd.env("RUST_LOG", filter);
    }

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
    index_column: &str,
    index_granularity: TimeIndexGranularity,
    entity_columns: Vec<String>,
) -> TableMeta {
    let index = IndexSpec {
        column: index_column.to_string(),
        entity_columns,
        kind: IndexKind::Timestamp {
            index_granularity,
            timezone: None,
        },
    };
    TableMeta::new_time_series(index)
}

async fn create_empty_table(path: &std::path::Path) -> TestResult<()> {
    let location = TableLocation::local(path);
    let meta = make_table_meta("ts", TimeIndexGranularity::Seconds(1), Vec::new());
    TimeSeriesTable::create(location, meta).await?;
    Ok(())
}

fn write_segment(path: &std::path::Path) -> TestResult<()> {
    test_common::write_parquet_rows(path, 5)?;
    Ok(())
}

fn write_numeric_segment(
    path: &std::path::Path,
    data_type: DataType,
    values: ArrayRef,
) -> TestResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let schema = Arc::new(Schema::new(vec![Field::new("idx", data_type, false)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values])?;
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn shell_input_for_create(
    table_root: &std::path::Path,
    index_column: &str,
    index_granularity: &str,
    timezone: Option<&str>,
    entities: Option<&str>,
    first_segment: &std::path::Path,
) -> String {
    let mut input = String::new();
    input.push_str(&format!("{}\n", table_root.display()));
    input.push_str(&format!("{index_column}\n"));
    input.push_str("timestamp\n");
    input.push_str(&format!("{index_granularity}\n"));
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
    assert!(String::from_utf8_lossy(&output.stdout).contains("index granularity (time interval"));
    assert!(table_root.join("_timeseries_log").exists());

    let table = TimeSeriesTable::open(TableLocation::local(&table_root)).await?;
    assert!(table.state().table_meta.logical_schema().is_some());

    Ok(())
}

#[tokio::test]
async fn shell_interactive_creates_integer_indexes() -> TestResult<()> {
    let tmp = TempDir::new()?;
    let cases: Vec<(&str, &str, DataType, ArrayRef, IndexKind)> = vec![
        (
            "int64",
            "4",
            DataType::Int64,
            Arc::new(Int64Array::from(vec![-4, 0, 4])),
            IndexKind::Int64 {
                index_granularity: NonZeroU64::new(4).unwrap(),
            },
        ),
        (
            "uint64",
            "8",
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![
                i64::MAX as u64 + 1,
                i64::MAX as u64 + 9,
            ])),
            IndexKind::UInt64 {
                index_granularity: NonZeroU64::new(8).unwrap(),
            },
        ),
    ];

    for (index_type, index_granularity, data_type, values, expected_kind) in cases {
        let table_root = tmp.path().join(index_type);
        let segment = table_root.join("data/segment.parquet");
        write_numeric_segment(&segment, data_type, values)?;
        let input = format!(
            "{}\nidx\n{index_type}\n{index_granularity}\n\n{}\nexit\n",
            table_root.display(),
            segment.display()
        );

        let output = run_shell_with_input(&["shell"], &input).await?;
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(!stdout.contains("timezone (optional"));
        assert!(stdout.contains("index granularity (positive integer"));

        let table = TimeSeriesTable::open(TableLocation::local(&table_root)).await?;
        assert_eq!(table.index_spec().column, "idx");
        assert_eq!(table.index_spec().kind, expected_kind);
    }

    Ok(())
}

#[tokio::test]
async fn shell_interactive_reprompts_invalid_index_type_and_granularity() -> TestResult<()> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    let segment = table_root.join("data/segment.parquet");
    write_numeric_segment(
        &segment,
        DataType::Int64,
        Arc::new(Int64Array::from(vec![-4, 0, 4])),
    )?;
    let input = format!(
        "{}\nidx\ninteger\nint64\n0\n-1\n4\n\n{}\nexit\n",
        table_root.display(),
        segment.display()
    );

    let output = run_shell_with_input(&["shell"], &input).await?;
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Invalid index type 'integer'"));
    assert!(stdout.contains("Invalid --index-granularity for --index-type int64"));

    let table = TimeSeriesTable::open(TableLocation::local(&table_root)).await?;
    assert_eq!(
        table.index_spec().kind,
        IndexKind::Int64 {
            index_granularity: NonZeroU64::new(4).unwrap(),
        }
    );

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
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&seg_path)?)?.build()?;
        table.append(reader).await?;
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
async fn shell_uses_one_subscriber_for_diagnostic_operations() -> TestResult<()> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_empty_table(&table_root).await?;
    let seg_path = table_root.join("data/seg.parquet");
    write_segment(&seg_path)?;

    let input = format!("{}\nexit\n", seg_path.display());
    let table_root_str = table_root.to_string_lossy();
    let output = run_shell_with_input_and_filter(
        &["shell", "--table", table_root_str.as_ref()],
        &input,
        Some("timeseries_table_format=debug"),
    )
    .await?;

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr.matches("Appended Parquet segment").count(),
        1,
        "stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("Failed to initialize CLI diagnostics"),
        "stderr:\n{stderr}"
    );
    Ok(())
}

#[tokio::test]
async fn shell_interactive_reprompts_invalid_index_granularity() -> TestResult<()> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    let seg_path = table_root.join("data/seg.parquet");
    write_segment(&seg_path)?;

    let mut input = String::new();
    input.push_str(&format!("{}\n", table_root.display()));
    input.push_str("ts\n");
    input.push_str("timestamp\n");
    input.push_str("1x\n");
    input.push_str("1s\n");
    input.push('\n'); // timezone
    input.push('\n'); // entities
    input.push_str(&format!("{}\n", seg_path.display()));
    input.push_str("exit\n");

    let output = run_shell_with_input(&["shell"], &input).await?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Invalid --index-granularity '1x'"));

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
