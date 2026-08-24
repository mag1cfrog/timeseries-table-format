#![allow(missing_docs)]

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::{
    io::{self, Seek, SeekFrom, Write},
    result::Result as StdResult,
};

use arrow::array::{
    ArrayRef, Float64Builder, Int64Array, StringArray, StringBuilder, TimestampMillisecondArray,
    TimestampMillisecondBuilder, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use tempfile::TempDir;
use timeseries_table_format::{
    metadata::{
        index::{IndexKind, TimeIndexGranularity},
        segments::SegmentEntityLayout,
    },
    storage::TableLocation,
    table::TimeSeriesTable,
};

fn cli_bin() -> &'static str {
    env!("CARGO_BIN_EXE_tstable")
}

fn run_cli(args: &[&str]) -> io::Result<Output> {
    Command::new(cli_bin())
        .env_remove("RUST_LOG")
        .args(args)
        .output()
}

fn run_cli_strings(args: &[String]) -> io::Result<Output> {
    Command::new(cli_bin())
        .env_remove("RUST_LOG")
        .args(args)
        .output()
}

fn create_command(table_root: &Path) -> Command {
    let mut command = Command::new(cli_bin());
    command.args([
        "create",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--index-column",
        "ts",
        "--index-type",
        "timestamp",
        "--index-granularity",
        "1h",
    ]);
    command
}

fn assert_cli_success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cli_diagnostics_default_to_warn_without_changing_stdout()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    let output = create_command(&table_root)
        .env_remove("RUST_LOG")
        .output()?;

    assert_cli_success(&output);
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("Created table at {}\n", table_root.display())
    );
    assert!(output.stderr.is_empty());
    Ok(())
}

#[test]
fn cli_debug_diagnostics_are_structured_stderr_only() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    let output = create_command(&table_root)
        .env("RUST_LOG", "timeseries_table_format=debug")
        .output()?;

    assert_cli_success(&output);
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("Created table at {}\n", table_root.display())
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("table.create"), "stderr:\n{stderr}");
    assert!(stderr.contains("committed_version=1"), "stderr:\n{stderr}");
    assert!(stderr.contains(" INFO "), "stderr:\n{stderr}");
    assert_eq!(stderr.matches("Created time-series table").count(), 1);
    assert!(!output.stderr.windows(2).any(|bytes| bytes == b"\x1b["));
    Ok(())
}

#[test]
fn cli_invalid_rust_log_warns_once_and_uses_default() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    let invalid_filter = "timeseries_table_format=verbose";
    let output = create_command(&table_root)
        .env("RUST_LOG", invalid_filter)
        .output()?;

    assert_cli_success(&output);
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("Created table at {}\n", table_root.display())
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(stderr.matches("invalid RUST_LOG filter").count(), 1);
    assert!(stderr.contains(invalid_filter), "stderr:\n{stderr}");
    assert!(
        stderr.contains("using warning-level default"),
        "stderr:\n{stderr}"
    );
    assert!(!stderr.contains("table.create"), "stderr:\n{stderr}");
    Ok(())
}

fn open_table_blocking(
    table_root: &Path,
) -> StdResult<TimeSeriesTable, Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    let location = TableLocation::local(table_root);
    let table = rt.block_on(TimeSeriesTable::open(location))?;
    Ok(table)
}

fn set_latest_writer_feature(
    table_root: &Path,
    feature: &str,
) -> StdResult<(), Box<dyn std::error::Error>> {
    let log_dir = table_root.join("_timeseries_log");
    let version: u64 = std::fs::read_to_string(log_dir.join("CURRENT"))?
        .trim()
        .parse()?;
    let commit_path = log_dir.join(format!("{version:010}.json"));
    let mut commit: serde_json::Value = serde_json::from_slice(&std::fs::read(&commit_path)?)?;
    let metadata = commit["actions"]
        .as_array_mut()
        .and_then(|actions| {
            actions
                .iter_mut()
                .find_map(|action| action.get_mut("UpdateTableMeta"))
        })
        .ok_or_else(|| io::Error::other("latest test commit must update table metadata"))?;
    metadata["required_writer_features"] = serde_json::json!([feature]);
    std::fs::write(commit_path, serde_json::to_vec(&commit)?)?;
    Ok(())
}

fn create_table_with_segment(
    tmp: &TempDir,
    table_name: &str,
) -> StdResult<PathBuf, Box<dyn std::error::Error>> {
    let table_root = tmp.path().join(table_name);
    create_table_via_cli(&table_root, "1m", &["symbol"])?;

    let source = tmp.path().join(format!("{table_name}.parquet"));
    write_parquet_rows(&source, &[(0, "A", 1.0), (60_000, "A", 2.0)])?;
    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source.to_string_lossy().as_ref(),
    ])?;
    assert_cli_success(&output);
    Ok(table_root)
}

#[test]
fn cli_protocol_allows_query_and_rejects_append_before_source_read()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = create_table_with_segment(&tmp, "protocol_table")?;
    set_latest_writer_feature(&table_root, "future_writer")?;

    let query = run_cli(&[
        "query",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--sql",
        "SELECT COUNT(*) FROM protocol_table",
    ])?;
    assert_cli_success(&query);

    let version_before = open_table_blocking(&table_root)?.state().version;
    let missing_source = tmp.path().join("missing.parquet");
    let append = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        missing_source.to_string_lossy().as_ref(),
    ])?;

    assert!(!append.status.success());
    assert!(append.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&append.stderr);
    assert!(
        stderr.contains("unsupported table writer features") && stderr.contains("future_writer"),
        "stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("Failed to read Parquet source"),
        "stderr:\n{stderr}"
    );
    assert_eq!(
        open_table_blocking(&table_root)?.state().version,
        version_before
    );
    Ok(())
}

#[test]
fn cli_diagnostics_stay_out_of_query_data_and_dependency_logs_are_not_duplicated()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = create_table_with_segment(&tmp, "diagnostic_table")?;
    let query_output = tmp.path().join("result.csv");

    let output = Command::new(cli_bin())
        .env(
            "RUST_LOG",
            "timeseries_table_format=debug,datafusion_optimizer=debug",
        )
        .args([
            "query",
            "--table",
            table_root.to_string_lossy().as_ref(),
            "--sql",
            "SELECT ts, symbol, price FROM diagnostic_table ORDER BY ts",
            "--output",
            query_output.to_string_lossy().as_ref(),
            "--format",
            "csv",
        ])
        .output()?;

    assert_cli_success(&output);
    let data = std::fs::read_to_string(query_output)?;
    assert!(data.starts_with("ts,symbol,price\n"), "data:\n{data}");
    for diagnostic in [
        "table.scan.plan",
        "timeseries_table_format",
        "datafusion_optimizer",
        "Analyzer took",
    ] {
        assert!(
            !data.contains(diagnostic),
            "data contains {diagnostic}:\n{data}"
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("table.scan.plan"), "stdout:\n{stdout}");
    assert!(!stdout.contains("Analyzer took"), "stdout:\n{stdout}");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr.matches("Analyzer took").count(),
        1,
        "stderr:\n{stderr}"
    );
    Ok(())
}

#[test]
fn cli_returned_error_is_printed_once_with_debug_enabled()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let missing_table = tmp.path().join("missing");
    let missing_parquet = tmp.path().join("missing.parquet");
    let output = Command::new(cli_bin())
        .env("RUST_LOG", "timeseries_table_format=debug")
        .args([
            "append",
            "--table",
            missing_table.to_string_lossy().as_ref(),
            "--parquet",
            missing_parquet.to_string_lossy().as_ref(),
        ])
        .output()?;

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr.matches("Failed to open v0.1 table").count(),
        1,
        "stderr:\n{stderr}"
    );
    assert!(!stderr.contains(" ERROR "), "stderr:\n{stderr}");
    Ok(())
}

fn write_parquet_rows(
    path: &Path,
    rows: &[(i64, &str, f64)],
) -> Result<(), Box<dyn std::error::Error>> {
    write_parquet_rows_with_properties(path, rows, None)
}

fn write_parquet_rows_with_properties(
    path: &Path,
    rows: &[(i64, &str, f64)],
    properties: Option<WriterProperties>,
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
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), properties)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn write_indexed_parquet(
    path: &Path,
    index_type: DataType,
    index_values: ArrayRef,
    tags: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("idx", index_type, false),
        Field::new("tag", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![index_values, Arc::new(StringArray::from(tags.to_vec()))],
    )?;
    let mut writer = ArrowWriter::try_new(std::fs::File::create(path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn create_table_via_cli(
    table_root: &Path,
    index_granularity: &str,
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
        "--index-granularity".to_string(),
        index_granularity.to_string(),
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
            index_granularity: TimeIndexGranularity::Minutes(15),
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
                index_granularity: NonZeroU64::new(4).unwrap(),
            },
        ),
        (
            "uint64",
            "18446744073709551615",
            IndexKind::UInt64 {
                index_granularity: NonZeroU64::new(u64::MAX).unwrap(),
            },
        ),
    ];

    for (index_type, index_granularity, expected_kind) in cases {
        let table_root = tmp.path().join(index_type);
        let output = run_cli(&[
            "create",
            "--table",
            table_root.to_string_lossy().as_ref(),
            "--index-column",
            "idx",
            "--index-type",
            index_type,
            "--index-granularity",
            index_granularity,
        ])?;
        assert_cli_success(&output);

        let table = open_table_blocking(&table_root)?;
        assert_eq!(table.index_spec().column, "idx");
        assert_eq!(table.index_spec().kind, expected_kind);
    }

    Ok(())
}

#[test]
fn cli_int64_create_append_query_and_wrong_domain_rollback()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("ordered_ints");
    let output = run_cli(&[
        "create",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--index-column",
        "idx",
        "--index-type",
        "int64",
        "--index-granularity",
        "10",
    ])?;
    assert_cli_success(&output);

    let state_before = open_table_blocking(&table_root)?.state().clone();
    let wrong_domains: Vec<(&str, DataType, ArrayRef)> = vec![
        (
            "timestamp.parquet",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Arc::new(TimestampMillisecondArray::from(vec![0, 1])),
        ),
        (
            "uint64.parquet",
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![0, 1])),
        ),
    ];
    for (filename, data_type, values) in wrong_domains {
        let source = tmp.path().join(filename);
        write_indexed_parquet(&source, data_type, values, &["wrong", "wrong"])?;
        let source_before = std::fs::read(&source)?;

        let output = run_cli(&[
            "append",
            "--table",
            table_root.to_string_lossy().as_ref(),
            "--parquet",
            source.to_string_lossy().as_ref(),
        ])?;

        assert!(!output.status.success(), "wrong index domain should fail");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("expected int64"),
            "unexpected stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(std::fs::read(&source)?, source_before);
        assert!(!table_root.join("data").join(filename).exists());
        assert!(!table_root.join("_coverage").exists());
        assert_eq!(open_table_blocking(&table_root)?.state(), &state_before);
    }

    let negative = tmp.path().join("negative.parquet");
    write_indexed_parquet(
        &negative,
        DataType::Int64,
        Arc::new(Int64Array::from(vec![-21, -11])),
        &["negative", "negative"],
    )?;
    let nonnegative = tmp.path().join("nonnegative.parquet");
    write_indexed_parquet(
        &nonnegative,
        DataType::Int64,
        Arc::new(Int64Array::from(vec![0, 10, 20])),
        &["nonnegative", "nonnegative", "nonnegative"],
    )?;
    for source in [&negative, &nonnegative] {
        let output = run_cli(&[
            "append",
            "--table",
            table_root.to_string_lossy().as_ref(),
            "--parquet",
            source.to_string_lossy().as_ref(),
        ])?;
        assert_cli_success(&output);
    }

    let query_output = tmp.path().join("result.csv");
    let output = run_cli(&[
        "query",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--sql",
        "SELECT idx, tag FROM ordered_ints WHERE idx >= -12 AND idx < 10 ORDER BY idx",
        "--output",
        query_output.to_string_lossy().as_ref(),
        "--format",
        "csv",
    ])?;
    assert_cli_success(&output);
    assert_eq!(
        std::fs::read_to_string(query_output)?,
        "idx,tag\n-11,negative\n0,nonnegative\n"
    );

    let table = open_table_blocking(&table_root)?;
    assert_eq!(table.state().segments.len(), 2);
    Ok(())
}

#[test]
fn cli_create_rejects_invalid_index_option_combinations_before_io()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let cases: &[(&str, &[&str], &str)] = &[
        ("timestamp", &[], "--index-granularity <INDEX_GRANULARITY>"),
        (
            "timestamp",
            &["--index-granularity", "1"],
            "Invalid --index-granularity '1' for --index-type timestamp",
        ),
        ("int64", &[], "--index-granularity <INDEX_GRANULARITY>"),
        (
            "int64",
            &["--index-granularity", "1m"],
            "Invalid --index-granularity for --index-type int64",
        ),
        (
            "int64",
            &["--index-granularity", "1", "--timezone", "UTC"],
            "Invalid --timezone for --index-type int64",
        ),
        (
            "uint64",
            &["--index-granularity", "1m"],
            "Invalid --index-granularity for --index-type uint64",
        ),
        (
            "uint64",
            &["--index-granularity", "1", "--timezone", "UTC"],
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
fn cli_create_validates_integer_index_granularity_before_io()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    for (position, value) in ["0", "-1", "18446744073709551616", "1.5", "1e3", ""]
        .into_iter()
        .enumerate()
    {
        let table_root = tmp.path().join(format!("invalid-granularity-{position}"));
        let output = run_cli(&[
            "create",
            "--table",
            table_root.to_string_lossy().as_ref(),
            "--index-column",
            "idx",
            "--index-type",
            "int64",
            "--index-granularity",
            value,
        ])?;

        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("Invalid --index-granularity for --index-type int64"),
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
    assert!(stdout.contains("--index-granularity <INDEX_GRANULARITY>"));
    assert!(stdout.contains("Time interval for timestamp"));
    assert!(stdout.contains("positive integer for int64 and uint64"));
    assert!(!stdout.contains("--bucket"));
    assert!(!stdout.contains("--bucket-width"));
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
        "--index-granularity",
        "1m",
    ])?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--time-column"));

    for removed_flag in ["--bucket", "--bucket-width"] {
        let table_root = tmp.path().join(removed_flag.trim_start_matches('-'));
        let output = run_cli(&[
            "create",
            "--table",
            table_root.to_string_lossy().as_ref(),
            "--index-column",
            "ts",
            "--index-type",
            "timestamp",
            "--index-granularity",
            "1m",
            removed_flag,
            "1m",
        ])?;
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(removed_flag));
        assert!(!table_root.exists());
    }

    Ok(())
}

#[test]
fn cli_optimize_help_exposes_only_the_table_argument() -> StdResult<(), Box<dyn std::error::Error>>
{
    let output = run_cli(&["optimize", "--help"])?;
    assert_cli_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--table <TABLE>"));
    assert!(!stdout.contains("--timing"));
    assert!(!stdout.contains("--output"));
    assert!(!stdout.contains("--strategy"));
    Ok(())
}

#[test]
fn cli_append_help_describes_streaming_parquet_import() -> StdResult<(), Box<dyn std::error::Error>>
{
    let output = run_cli(&["append", "--help"])?;
    assert_cli_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Stream a local Parquet file into a new table-managed segment"));
    assert!(stdout.contains("--table <TABLE>"));
    assert!(stdout.contains("--parquet <PARQUET>"));
    assert!(stdout.contains("--timing"));
    Ok(())
}

#[test]
fn cli_optimize_rewrites_mixed_segments_and_reports_repeated_no_op()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &["symbol"])?;

    let source = tmp.path().join("mixed.parquet");
    write_parquet_rows(&source, &[(0, "A", 1.0), (60_000, "B", 2.0)])?;
    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source.to_string_lossy().as_ref(),
    ])?;
    assert_cli_success(&output);

    let output = run_cli(&["optimize", "--table", table_root.to_string_lossy().as_ref()])?;
    assert_cli_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "starting_version: 2\n\
         committed_version: 3\n\
         candidate_source_segments: 1\n\
         source_segments_replaced: 1\n\
         replacement_segments_written: 2\n\
         distinct_identities_materialized: 2\n\
         rows_read: 2\n\
         rows_written: 2\n\
         no_op: false\n"
    );

    let table = open_table_blocking(&table_root)?;
    assert_eq!(table.state().version, 3);
    assert_eq!(table.state().segments.len(), 2);
    assert!(
        table
            .state()
            .segments
            .values()
            .all(|segment| matches!(segment.entity_layout, SegmentEntityLayout::Single(_)))
    );

    let output = run_cli(&["optimize", "--table", table_root.to_string_lossy().as_ref()])?;
    assert_cli_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "starting_version: 3\n\
         committed_version: 3\n\
         candidate_source_segments: 0\n\
         source_segments_replaced: 0\n\
         replacement_segments_written: 0\n\
         distinct_identities_materialized: 0\n\
         rows_read: 0\n\
         rows_written: 0\n\
         no_op: true\n"
    );
    assert_eq!(open_table_blocking(&table_root)?.state().version, 3);
    Ok(())
}

#[test]
fn cli_optimize_rejects_tables_without_entities_with_context()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;

    let output = run_cli(&["optimize", "--table", table_root.to_string_lossy().as_ref()])?;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(&table_root.display().to_string()));
    assert!(stderr.contains("no entity columns are configured"));
    Ok(())
}

#[test]
fn cli_optimize_preserves_failed_source_path_context() -> StdResult<(), Box<dyn std::error::Error>>
{
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &["symbol"])?;

    let source = tmp.path().join("missing-source.parquet");
    write_parquet_rows(&source, &[(0, "A", 1.0), (60_000, "B", 2.0)])?;
    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source.to_string_lossy().as_ref(),
    ])?;
    assert_cli_success(&output);

    let table = open_table_blocking(&table_root)?;
    let segment_path = table
        .state()
        .segments
        .values()
        .next()
        .ok_or_else(|| io::Error::other("segment missing"))?
        .path
        .clone();
    std::fs::remove_file(table_root.join(&segment_path))?;

    let output = run_cli(&["optimize", "--table", table_root.to_string_lossy().as_ref()])?;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(&table_root.display().to_string()));
    assert!(stderr.contains(&segment_path));
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
    let source_before = std::fs::read(&parquet_path)?;

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        parquet_path.to_string_lossy().as_ref(),
    ])?;
    assert_cli_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "Appended table version: 2\n"
    );
    assert_eq!(std::fs::read(&parquet_path)?, source_before);

    let table = open_table_blocking(&table_root)?;
    assert_eq!(table.state().segments.len(), 1);
    let segment = table
        .state()
        .segments
        .values()
        .next()
        .ok_or_else(|| io::Error::other("segment missing"))?;
    assert_ne!(segment.path, rel_path.to_string_lossy());
    assert!(table_root.join(&segment.path).exists());
    Ok(())
}

#[test]
fn cli_append_outside_root_streams_without_copying() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;

    let source_path = tmp.path().join("outside.parquet");
    write_parquet_rows(&source_path, &[(0, "A", 1.0)])?;
    let source_before = std::fs::read(&source_path)?;

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source_path.to_string_lossy().as_ref(),
    ])?;
    assert_cli_success(&output);
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "Appended table version: 2\n"
    );
    assert_eq!(std::fs::read(&source_path)?, source_before);

    let expected_rel = PathBuf::from("data/outside.parquet");
    let expected_dst = table_root.join(&expected_rel);
    assert!(!expected_dst.exists(), "source filename must not be copied");

    let table = open_table_blocking(&table_root)?;
    assert_eq!(table.state().segments.len(), 1);
    let segment = table
        .state()
        .segments
        .values()
        .next()
        .ok_or_else(|| io::Error::other("segment missing"))?;
    assert_ne!(segment.path, expected_rel.to_string_lossy());
    assert!(table_root.join(&segment.path).exists());
    Ok(())
}

#[test]
fn cli_append_timing_reports_version_and_elapsed() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;
    let source = tmp.path().join("timed.parquet");
    write_parquet_rows(&source, &[(0, "A", 1.0)])?;

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source.to_string_lossy().as_ref(),
        "--timing",
    ])?;
    assert_cli_success(&output);
    let stdout = String::from_utf8(output.stdout)?;
    let elapsed = stdout
        .strip_prefix("Appended table version: 2 (elapsed_ms: ")
        .and_then(|value| value.strip_suffix(")\n"))
        .ok_or_else(|| io::Error::other(format!("unexpected stdout: {stdout}")))?;
    elapsed.parse::<u128>()?;
    Ok(())
}

#[test]
fn cli_append_consumes_multiple_source_row_groups() -> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;
    let source = tmp.path().join("row-groups.parquet");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    write_parquet_rows_with_properties(
        &source,
        &[(0, "A", 1.0), (60_000, "B", 2.0), (120_000, "C", 3.0)],
        Some(properties),
    )?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&source)?)?;
    assert_eq!(builder.metadata().num_row_groups(), 3);

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source.to_string_lossy().as_ref(),
    ])?;
    assert_cli_success(&output);
    let table = open_table_blocking(&table_root)?;
    let segment = table
        .state()
        .segments
        .values()
        .next()
        .ok_or_else(|| io::Error::other("segment missing"))?;
    assert_eq!(segment.row_count, 3);
    Ok(())
}

#[test]
fn cli_append_missing_source_reports_path_without_mutation()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;
    let source = tmp.path().join("missing.parquet");
    let state_before = open_table_blocking(&table_root)?.state().clone();

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source.to_string_lossy().as_ref(),
    ])?;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Failed to read Parquet source"));
    assert!(stderr.contains(&source.display().to_string()));
    assert_eq!(open_table_blocking(&table_root)?.state(), &state_before);
    assert!(!table_root.join("data").exists());
    Ok(())
}

#[test]
fn cli_append_corrupt_source_fails_before_transaction() -> StdResult<(), Box<dyn std::error::Error>>
{
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;
    let source = tmp.path().join("corrupt.parquet");
    std::fs::write(&source, b"not parquet")?;
    let source_before = std::fs::read(&source)?;
    let state_before = open_table_blocking(&table_root)?.state().clone();

    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source.to_string_lossy().as_ref(),
    ])?;
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Failed to read Parquet source"));
    assert!(stderr.contains(&source.display().to_string()));
    assert_eq!(std::fs::read(&source)?, source_before);
    assert_eq!(open_table_blocking(&table_root)?.state(), &state_before);
    assert!(!table_root.join("data").exists());
    Ok(())
}

#[test]
fn cli_append_late_decode_failure_leaves_no_partial_append()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;
    let source = tmp.path().join("late-corruption.parquet");
    let properties = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(2_048))
        .build();
    let rows = (0..=2_048)
        .map(|value| (i64::from(value) * 60_000, "A", f64::from(value)))
        .collect::<Vec<_>>();
    write_parquet_rows_with_properties(&source, &rows, Some(properties))?;

    let reader = SerializedFileReader::new(std::fs::File::open(&source)?)?;
    let second_page = reader.metadata().row_group(1).column(0).data_page_offset() as u64;
    drop(reader);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&source)?;
    file.seek(SeekFrom::Start(second_page))?;
    file.write_all(&[0xFF; 16])?;
    file.flush()?;
    drop(file);

    let source_before = std::fs::read(&source)?;
    let state_before = open_table_blocking(&table_root)?.state().clone();
    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source.to_string_lossy().as_ref(),
    ])?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Arrow input error"));
    assert!(stderr.contains(&source.display().to_string()));
    assert!(stderr.contains(&table_root.display().to_string()));
    assert_eq!(std::fs::read(&source)?, source_before);
    assert_eq!(open_table_blocking(&table_root)?.state(), &state_before);
    let data_dir = table_root.join("data");
    assert!(!data_dir.exists() || std::fs::read_dir(data_dir)?.next().is_none());
    Ok(())
}

#[test]
fn cli_failed_external_append_preserves_its_source() -> StdResult<(), Box<dyn std::error::Error>> {
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
        "--index-granularity",
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
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(&source_path.display().to_string()));
    assert!(stderr.contains(&table_root.display().to_string()));
    assert_eq!(std::fs::read(&source_path)?, source_before);
    assert!(!table_root.join("data/invalid-external.parquet").exists());
    assert!(!table_root.join("_coverage").exists());
    assert_eq!(open_table_blocking(&table_root)?.state(), &state_before);
    Ok(())
}

#[test]
fn cli_append_overlap_reports_source_and_preserves_state()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;
    let first = tmp.path().join("first.parquet");
    write_parquet_rows(&first, &[(0, "A", 1.0)])?;
    assert_cli_success(&run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        first.to_string_lossy().as_ref(),
    ])?);

    let overlap = tmp.path().join("overlap.parquet");
    write_parquet_rows(&overlap, &[(30_000, "B", 2.0)])?;
    let source_before = std::fs::read(&overlap)?;
    let state_before = open_table_blocking(&table_root)?.state().clone();
    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        overlap.to_string_lossy().as_ref(),
    ])?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("overlap"));
    assert!(stderr.contains(&overlap.display().to_string()));
    assert!(stderr.contains(&table_root.display().to_string()));
    assert_eq!(std::fs::read(&overlap)?, source_before);
    assert_eq!(open_table_blocking(&table_root)?.state(), &state_before);
    Ok(())
}

#[test]
fn cli_append_generates_path_without_overwriting_existing_data_file()
-> StdResult<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_root = tmp.path().join("table");
    create_table_via_cli(&table_root, "1m", &[])?;

    let existing_rel = PathBuf::from("data/seg.parquet");
    let existing_path = table_root.join(&existing_rel);
    write_parquet_rows(&existing_path, &[(0, "A", 1.0)])?;

    let source_path = tmp.path().join("seg.parquet");
    write_parquet_rows(&source_path, &[(1, "B", 2.0)])?;

    let existing_before = std::fs::read(&existing_path)?;
    let output = run_cli(&[
        "append",
        "--table",
        table_root.to_string_lossy().as_ref(),
        "--parquet",
        source_path.to_string_lossy().as_ref(),
    ])?;

    assert_cli_success(&output);
    assert_eq!(std::fs::read(&existing_path)?, existing_before);
    let table = open_table_blocking(&table_root)?;
    assert_eq!(table.state().segments.len(), 1);
    let segment = table
        .state()
        .segments
        .values()
        .next()
        .ok_or_else(|| io::Error::other("segment missing"))?;
    assert_ne!(segment.path, existing_rel.to_string_lossy());
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
fn cli_invalid_timestamp_granularity_reports_user_friendly_error()
-> StdResult<(), Box<dyn std::error::Error>> {
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
        "--index-granularity",
        "1x",
    ])?;

    assert!(!output.status.success(), "create should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Invalid --index-granularity"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        stderr.contains("expected s|m|h|d"),
        "unexpected stderr: {stderr}"
    );
    assert!(!table_root.exists());
    Ok(())
}
