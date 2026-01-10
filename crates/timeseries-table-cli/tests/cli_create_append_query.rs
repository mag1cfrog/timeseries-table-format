//! Integration tests for the CLI binary.

use assert_cmd::Command;
use predicates::str::contains;
use tempfile::TempDir;

mod common;

use common::{table_root, write_parquet_rows};

fn cli() -> Command {
    Command::new(assert_cmd::cargo::cargo_bin!("tstable"))
}

#[test]
fn cli_append_with_explicit_time_column() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table = table_root(&tmp, "t");
    let parquet = tmp.path().join("data.parquet");
    write_parquet_rows(&parquet, 2)?;

    cli()
        .args([
            "create",
            "--table",
            table.to_string_lossy().as_ref(),
            "--time-column",
            "ts",
            "--bucket",
            "1m",
            "--entity",
            "symbol",
        ])
        .assert()
        .success();

    cli()
        .args([
            "append",
            "--table",
            table.to_string_lossy().as_ref(),
            "--parquet",
            parquet.to_string_lossy().as_ref(),
            "--time-column",
            "ts",
        ])
        .assert()
        .success()
        .stdout(contains("Appended segment"));

    Ok(())
}

#[test]
fn cli_create_append_query_with_max_rows_zero() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table = table_root(&tmp, "t");
    let parquet = tmp.path().join("data.parquet");
    write_parquet_rows(&parquet, 7)?;

    cli()
        .args([
            "create",
            "--table",
            table.to_string_lossy().as_ref(),
            "--time-column",
            "ts",
            "--bucket",
            "1m",
            "--entity",
            "symbol",
        ])
        .assert()
        .success()
        .stdout(contains("Created table at"));

    cli()
        .args([
            "append",
            "--table",
            table.to_string_lossy().as_ref(),
            "--parquet",
            parquet.to_string_lossy().as_ref(),
        ])
        .assert()
        .success()
        .stdout(contains("Appended segment"));

    cli()
        .args([
            "query",
            "--table",
            table.to_string_lossy().as_ref(),
            "--sql",
            "SELECT * FROM t ORDER BY ts",
            "--max-rows",
            "0",
        ])
        .assert()
        .success()
        .stdout(contains("preview suppressed"))
        .stdout(contains("total_rows: 7"));

    Ok(())
}

#[test]
fn cli_query_zero_rows_prints_no_rows() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table = table_root(&tmp, "t");
    let parquet = tmp.path().join("data.parquet");
    write_parquet_rows(&parquet, 5)?;

    cli()
        .args([
            "create",
            "--table",
            table.to_string_lossy().as_ref(),
            "--time-column",
            "ts",
            "--bucket",
            "1m",
            "--entity",
            "symbol",
        ])
        .assert()
        .success();

    cli()
        .args([
            "append",
            "--table",
            table.to_string_lossy().as_ref(),
            "--parquet",
            parquet.to_string_lossy().as_ref(),
        ])
        .assert()
        .success();

    cli()
        .args([
            "query",
            "--table",
            table.to_string_lossy().as_ref(),
            "--sql",
            "SELECT * FROM t WHERE 1 = 0",
        ])
        .assert()
        .success()
        .stdout(contains("(no rows)"))
        .stdout(contains("total_rows: 0"));

    Ok(())
}

#[test]
fn cli_query_rejects_empty_output_path() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table = table_root(&tmp, "t");
    let parquet = tmp.path().join("data.parquet");
    write_parquet_rows(&parquet, 3)?;

    cli()
        .args([
            "create",
            "--table",
            table.to_string_lossy().as_ref(),
            "--time-column",
            "ts",
            "--bucket",
            "1m",
            "--entity",
            "symbol",
        ])
        .assert()
        .success();

    cli()
        .args([
            "append",
            "--table",
            table.to_string_lossy().as_ref(),
            "--parquet",
            parquet.to_string_lossy().as_ref(),
        ])
        .assert()
        .success();

    cli()
        .args([
            "query",
            "--table",
            table.to_string_lossy().as_ref(),
            "--sql",
            "SELECT * FROM t",
            "--output",
            " ",
        ])
        .assert()
        .failure()
        .stderr(contains("output location is empty"));

    Ok(())
}

#[test]
fn cli_query_with_sanitized_table_name() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = TempDir::new()?;
    let table_name = "My Table-1";
    let table = table_root(&tmp, table_name);
    let parquet = tmp.path().join("data.parquet");
    write_parquet_rows(&parquet, 4)?;

    cli()
        .args([
            "create",
            "--table",
            table.to_string_lossy().as_ref(),
            "--time-column",
            "ts",
            "--bucket",
            "1m",
            "--entity",
            "symbol",
        ])
        .assert()
        .success();

    cli()
        .args([
            "append",
            "--table",
            table.to_string_lossy().as_ref(),
            "--parquet",
            parquet.to_string_lossy().as_ref(),
        ])
        .assert()
        .success();

    let expected = "my_table_1";
    let sql = format!("SELECT * FROM {} ORDER BY ts", expected);

    cli()
        .args([
            "query",
            "--table",
            table.to_string_lossy().as_ref(),
            "--sql",
            sql.as_str(),
        ])
        .assert()
        .success()
        .stderr(contains(format!("Registered table as '{}'", expected)))
        .stderr(contains(format!("quoted: \"{}\"", expected)))
        .stdout(contains("total_rows: 4"));

    Ok(())
}
