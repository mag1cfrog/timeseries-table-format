#![allow(missing_docs)]

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::{io, result::Result as StdResult};

use arrow::array::{Float64Builder, StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;
use timeseries_table_format::{
    metadata::table_metadata::{IndexKind, TimeBucket},
    storage::TableLocation,
    table::TimeSeriesTable,
};

fn cli_bin() -> &'static str {
    env!("CARGO_BIN_EXE_tstable")
}

fn run_cli(args: &[&str]) -> io::Result<Output> {
    Command::new(cli_bin()).args(args).output()
}

fn run_cli_strings(args: &[String]) -> io::Result<Output> {
    Command::new(cli_bin()).args(args).output()
}

fn assert_cli_success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn open_table_blocking(
    table_root: &Path,
) -> StdResult<TimeSeriesTable, Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    let location = TableLocation::local(table_root);
    let table = rt.block_on(TimeSeriesTable::open(location))?;
    Ok(table)
}

fn write_parquet_rows(
    path: &Path,
    rows: &[(i64, &str, f64)],
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut ts_builder = TimestampMillisecondBuilder::with_capacity(rows.len());
    let mut sym_builder =
        StringBuilder::with_capacity(rows.len(), rows.iter().map(|(_, s, _)| s.len()).sum());
    let mut price_builder = Float64Builder::with_capacity(rows.len());

    for (ts, sym, price) in rows {
        ts_builder.append_value(*ts);
        sym_builder.append_value(sym);
        price_builder.append_value(*price);
    }

    let schema = Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(ts_builder.finish()) as _,
            Arc::new(sym_builder.finish()),
            Arc::new(price_builder.finish()),
        ],
    )?;

    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn create_table_via_cli(
    table_root: &Path,
    bucket: &str,
    entity_columns: &[&str],
) -> StdResult<(), Box<dyn std::error::Error>> {
    let table_root_str = table_root.to_string_lossy().to_string();
    let mut args: Vec<String> = vec![
        "create".to_string(),
        "--table".to_string(),
        table_root_str,
        "--index-column".to_string(),
        "ts".to_string(),
        "--index-type".to_string(),
        "timestamp".to_string(),
        "--bucket".to_string(),
        bucket.to_string(),
        "--timezone".to_string(),
        "America/New_York".to_string(),
    ];
    for col in entity_columns {
        args.push("--entity".to_string());
        args.push((*col).to_string());
    }

    let output = run_cli_strings(&args)?;
    assert_cli_success(&output);
    Ok(())
}

#[test]
fn cli_create_creates_table() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "15m", &["symbol", "venue"])?;

    let table = open_table_blocking(&table_root)?;
    let index = table.index_spec();
    assert_eq!(index.column, "ts");
    assert_eq!(
        index.kind,
        IndexKind::Timestamp {
            bucket: TimeBucket::Minutes(15),
            timezone: Some("America/New_York".to_string())
        }
    );
    assert_eq!(
        index.entity_columns,
        vec!["symbol".to_string(), "venue".to_string()]
    );
    Ok(())
}

#[test]
fn cli_create_supports_integer_index_domains() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let cases = [
        (
            "int64",
            "4",
            IndexKind::Int64 {
                bucket_width: NonZeroU64::new(4).unwrap(),
            },
        ),
        (
            "uint64",
            "18446744073709551615",
            IndexKind::UInt64 {
                bucket_width: NonZeroU64::new(u64::MAX).unwrap(),
            },
        ),
    ];

    for (index_type, bucket_width, expected_kind) in cases {
        let table_root = tmp.path().join(index_type);
        let output = run_cli(&[
            "create",
            "--table",
            table_root.to_string_lossy().as_ref(),
            "--index-column",
            "idx",
            "--index-type",
            index_type,
            "--bucket-width",
            bucket_width,
        ])?;
        assert_cli_success(&output);

        let table = open_table_blocking(&table_root)?;
        assert_eq!(table.index_spec().column, "idx");
        assert_eq!(table.index_spec().kind, expected_kind);
    }

    Ok(())
}

#[test]
fn cli_create_rejects_invalid_index_option_combinations_before_io()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let cases: &[(&str, &[&str], &str)] = &[
        (
            "timestamp",
            &[],
            "Invalid --bucket for --index-type timestamp",
        ),
        (
            "timestamp",
            &["--bucket", "1m", "--bucket-width", "1"],
            "Invalid --bucket-width for --index-type timestamp",
        ),
        (
            "int64",
            &[],
            "Invalid --bucket-width for --index-type int64",
        ),
        (
            "int64",
            &["--bucket-width", "1", "--bucket", "1m"],
            "Invalid --bucket for --index-type int64",
        ),
        (
            "int64",
            &["--bucket-width", "1", "--timezone", "UTC"],
            "Invalid --timezone for --index-type int64",
        ),
        (
            "uint64",
            &["--bucket", "1m"],
            "Invalid --bucket for --index-type uint64",
        ),
        (
            "uint64",
            &["--bucket-width", "1", "--timezone", "UTC"],
            "Invalid --timezone for --index-type uint64",
        ),
    ];

    for (position, (index_type, options, expected_error)) in cases.iter().enumerate() {
        let table_root = tmp.path().join(format!("invalid-{position}"));
        let table_arg = table_root.to_string_lossy().into_owned();
        let mut args = vec![
            "create",
            "--table",
            table_arg.as_str(),
            "--index-column",
            "idx",
            "--index-type",
            index_type,
        ];
        args.extend_from_slice(options);

        let output = run_cli(&args)?;
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected_error),
            "unexpected stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!table_root.exists(), "invalid create performed storage I/O");
    }

    Ok(())
}

#[test]
fn cli_create_validates_integer_bucket_width_before_io() -> StdResult<(), Box<dyn std::error::Error>>
{
    let tmp = TempDir::new()?;
    for (position, value) in ["0", "-1", "18446744073709551616", "1.5", "1e3", ""]
        .into_iter()
        .enumerate()
    {
        let table_root = tmp.path().join(format!("invalid-width-{position}"));
        let output = run_cli(&[
            "create",
            "--table",
            table_root.to_string_lossy().as_ref(),
            "--index-column",
            "idx",
            "--index-type",
            "int64",
            "--bucket-width",
            value,
        ])?;

        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("Invalid --bucket-width for --index-type int64"),
            "unexpected stderr: {stderr}"
        );
        assert!(!table_root.exists(), "invalid create performed storage I/O");
    }

    Ok(())
}

#[test]
fn cli_create_help_uses_only_ordered_index_names() -> StdResult<(), Box<dyn std::error::Error>> {
    let output = run_cli(&["create", "--help"])?;
    assert_cli_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--index-column"));
    assert!(stdout.contains("--index-type <INDEX_TYPE>"));
    assert!(stdout.contains("timestamp, int64, uint64"));
    assert!(stdout.contains("--bucket-width"));
    assert!(!stdout.contains("--time-column"));

    let tmp = TempDir::new()?;
    let output = run_cli(&[
        "create",
        "--table",
        tmp.path().join("table").to_string_lossy().as_ref(),
        "--time-column",
        "ts",
        "--index-type",
        "timestamp",
        "--bucket",
        "1m",
    ])?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--time-column"));

    Ok(())
}

#[test]
fn cli_append_under_root_succeeds() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;

    let rel_path = PathBuf::from("data/seg-under-root.parquet");
    let parquet_path = table_root.join(&rel_path);
    write_parquet_rows(&parquet_path, &[(0, "A", 1.0)])?;

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        parquet_path.to_string_lossy().as_ref(),
    ])?;
    assert_cli_success(&output);

    let table = open_table_blocking(&table_root)?;
    assert_eq!(table.state().segments.len(), 1);
    let segment = table
        .state()
        .segments
        .values()
        .next()
        .ok_or_else(|| io::Error::other("segment missing"))?;
    assert_eq!(segment.path, rel_path.to_string_lossy());
    Ok(())
}

#[test]
fn cli_append_outside_root_copies_and_appends() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;

    let source_path = tmp.path().join("outside.parquet");
    write_parquet_rows(&source_path, &[(0, "A", 1.0)])?;

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source_path.to_string_lossy().as_ref(),
    ])?;
    assert_cli_success(&output);

    let expected_rel = PathBuf::from("data/outside.parquet");
    let expected_dst = table_root.join(&expected_rel);
    assert!(expected_dst.exists(), "expected copied parquet");

    let table = open_table_blocking(&table_root)?;
    assert_eq!(table.state().segments.len(), 1);
    let segment = table
        .state()
        .segments
        .values()
        .next()
        .ok_or_else(|| io::Error::other("segment missing"))?;
    assert_eq!(segment.path, expected_rel.to_string_lossy());
    Ok(())
}

#[test]
fn cli_failed_external_append_removes_its_copy() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    let output = run_cli(&[
        "create",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--index-column",
        "event_time",
        "--index-type",
        "timestamp",
        "--bucket",
        "1m",
    ])?;
    assert_cli_success(&output);

    let source_path = tmp.path().join("invalid-external.parquet");
    write_parquet_rows(&source_path, &[(0, "A", 1.0)])?;
    let source_before = std::fs::read(&source_path)?;
    let state_before = open_table_blocking(&table_root)?.state().clone();

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source_path.to_string_lossy().as_ref(),
    ])?;

    assert!(!output.status.success(), "append should fail");
    assert_eq!(std::fs::read(&source_path)?, source_before);
    assert!(!table_root.join("data/invalid-external.parquet").exists());
    assert!(!table_root.join("_coverage").exists());
    assert_eq!(open_table_blocking(&table_root)?.state(), &state_before);
    Ok(())
}

#[test]
fn cli_append_refuses_overwrite_existing_data_file() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;

    let existing_rel = PathBuf::from("data/seg.parquet");
    let existing_path = table_root.join(&existing_rel);
    write_parquet_rows(&existing_path, &[(0, "A", 1.0)])?;

    let source_path = tmp.path().join("seg.parquet");
    write_parquet_rows(&source_path, &[(1, "B", 2.0)])?;

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source_path.to_string_lossy().as_ref(),
    ])?;

    assert!(!output.status.success(), "append should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Path already exists"),
        "unexpected stderr: {stderr}"
    );
    Ok(())
}

#[test]
fn cli_append_uses_registered_time_column() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;

    let rel_path = PathBuf::from("data/seg-default-ts.parquet");
    let parquet_path = table_root.join(&rel_path);
    write_parquet_rows(&parquet_path, &[(0, "A", 1.0)])?;

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        parquet_path.to_string_lossy().as_ref(),
    ])?;
    assert_cli_success(&output);

    let table = open_table_blocking(&table_root)?;
    assert_eq!(table.state().segments.len(), 1);
    Ok(())
}

#[test]
fn cli_invalid_bucket_reports_user_friendly_error() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");

    let output = run_cli(&[
        "create",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--index-column",
        "ts",
        "--index-type",
        "timestamp",
        "--bucket",
        "1x",
    ])?;

    assert!(!output.status.success(), "create should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid --bucket"),
        "unexpected stderr: {stderr}"
    );
    Ok(())
}
