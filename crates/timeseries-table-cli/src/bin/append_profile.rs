//! Append profiling binary: create a fresh benchmark table and append a segment.

use std::path::{Path, PathBuf};

use clap::Parser;

use timeseries_table_core::{
    metadata::table_metadata::{TableMeta, TimeBucket, TimeIndexSpec},
    storage::TableLocation,
    table::{
        TimeSeriesTable,
        append_report::{AppendReport, AppendStep},
    },
};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    table: PathBuf,

    #[arg(long)]
    parquet: PathBuf,

    /// Timestamp column name for create + append
    #[arg(long = "time-column")]
    time_column: String,

    /// Time bucket (e.g. 1s, 1m, 1h, 1d)
    #[arg(long)]
    bucket: String,

    /// Optional IANA timezone string
    #[arg(long)]
    timezone: Option<String>,

    /// Repeatable entity column names
    #[arg(long = "entity")]
    entity: Vec<String>,

    /// Optional CSV output path
    #[arg(long = "csv-out")]
    csv_out: Option<PathBuf>,
}

fn format_fields(step: &AppendStep) -> String {
    if step.fields.is_empty() {
        return String::new();
    }
    step.fields
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(";")
}

fn get_context_value<'a>(report: &'a AppendReport, key: &str) -> &'a str {
    report
        .context
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .unwrap_or("")
}

fn csv_escape(raw: &str) -> String {
    if raw.contains(',') || raw.contains('"') || raw.contains('\n') {
        let escaped = raw.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        raw.to_string()
    }
}

fn write_csv(report: &AppendReport, out_path: &Path) -> std::io::Result<()> {
    let mut out = String::new();
    out.push_str(
        "relative_path,time_column,segment_id,bytes_len,step,elapsed_ms,percent,total_ms,fields\n",
    );

    let rel = get_context_value(report, "relative_path");
    let time_column = get_context_value(report, "time_column");
    let segment_id = get_context_value(report, "segment_id");
    let bytes_len = get_context_value(report, "bytes_len");

    for step in &report.steps {
        let percent = if report.total_ms == 0 {
            0.0
        } else {
            (step.elapsed_ms as f64) * 100.0 / (report.total_ms as f64)
        };

        let fields = format_fields(step);
        let line = format!(
            "{},{},{},{},{},{},{:.2},{},{}\n",
            csv_escape(rel),
            csv_escape(time_column),
            csv_escape(segment_id),
            csv_escape(bytes_len),
            csv_escape(&step.name),
            step.elapsed_ms,
            percent,
            report.total_ms,
            csv_escape(&fields),
        );
        out.push_str(&line);
    }

    std::fs::write(out_path, out)
}

fn print_report(report: &AppendReport) {
    let rel = get_context_value(report, "relative_path");
    let bytes_len = get_context_value(report, "bytes_len");
    println!(
        "Append profile: {rel} (bytes: {bytes_len}, total_ms: {})",
        report.total_ms
    );

    for step in &report.steps {
        let percent = if report.total_ms == 0 {
            0.0
        } else {
            (step.elapsed_ms as f64) * 100.0 / (report.total_ms as f64)
        };

        let fields = format_fields(step);
        if fields.is_empty() {
            println!(
                "{:<24} {:>8} ms  {:>6.2}%",
                step.name, step.elapsed_ms, percent
            );
        } else {
            println!(
                "{:<24} {:>8} ms  {:>6.2}%  {}",
                step.name, step.elapsed_ms, percent, fields
            );
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let benchmark_root = args.table.join("benchmark");
    if tokio::fs::metadata(&benchmark_root).await.is_ok() {
        tokio::fs::remove_dir_all(&benchmark_root).await?;
    }
    tokio::fs::create_dir_all(&benchmark_root).await?;

    let location = TableLocation::parse(benchmark_root.to_string_lossy().as_ref())?;

    let bucket = args.bucket.parse::<TimeBucket>()?;
    let index = TimeIndexSpec {
        timestamp_column: args.time_column.clone(),
        bucket,
        timezone: args.timezone.clone(),
        entity_columns: args.entity.clone(),
    };
    let meta = TableMeta::new_time_series(index);
    TimeSeriesTable::create(location.clone(), meta).await?;

    let mut table = TimeSeriesTable::open(location.clone()).await?;

    let rel = location.ensure_parquet_under_root(&args.parquet).await?;
    let rel_str = if cfg!(windows) {
        rel.to_string_lossy().replace('\\', "/")
    } else {
        rel.to_string_lossy().to_string()
    };

    let (_version, report) = table
        .append_parquet_segment_with_report(&rel_str, &args.time_column)
        .await?;

    print_report(&report);

    if let Some(csv_out) = args.csv_out.as_deref() {
        write_csv(&report, csv_out)?;
        println!("Wrote CSV report to {}", csv_out.display());
    }

    Ok(())
}
