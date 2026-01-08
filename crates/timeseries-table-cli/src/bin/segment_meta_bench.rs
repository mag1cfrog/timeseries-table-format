//! Segment meta benchmark: compare scan strategies for min/max timestamp extraction.

use std::cmp::{max, min};
use std::path::{Path, PathBuf};
use std::time::Instant;

use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use bytes::Bytes;
use clap::Parser;
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
};
use parquet::basic::{LogicalType, TimeUnit as ParquetTimeUnit, Type as PhysicalType};
use parquet::column::reader::ColumnReader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rayon::prelude::*;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    parquet: PathBuf,

    #[arg(long = "time-column")]
    time_column: String,

    /// Number of warmup runs per strategy
    #[arg(long, default_value_t = 1)]
    warmup: usize,

    /// Number of timed runs per strategy
    #[arg(long, default_value_t = 3)]
    repeat: usize,

    /// Optional CSV output path
    #[arg(long = "csv-out")]
    csv_out: Option<PathBuf>,

    /// Override threads for parallel strategies (0 = auto)
    #[arg(long, default_value_t = 0)]
    threads: usize,
}

#[derive(Debug, Clone)]
struct BenchResult {
    name: String,
    avg_ms: u128,
    rows_sec: f64,
    row_groups: usize,
    row_count: u64,
    used_stats: bool,
    scanned_rows: u64,
}

fn resolve_rg_settings(num_row_groups: usize, threads_override: usize) -> (usize, usize) {
    if threads_override > 0 {
        let threads_used = threads_override.max(1);
        let rg_chunk = if num_row_groups == 0 {
            1
        } else {
            num_row_groups.div_ceil(threads_used)
        };
        return (threads_used, rg_chunk);
    }
    let logical_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let max_threads = logical_threads.saturating_mul(2).max(1);
    let threads_used = if num_row_groups == 0 {
        logical_threads.max(1)
    } else if num_row_groups <= max_threads {
        num_row_groups
    } else {
        logical_threads.max(1)
    };
    let rg_chunk = if num_row_groups == 0 {
        1
    } else {
        num_row_groups.div_ceil(threads_used)
    };
    (threads_used, rg_chunk)
}

fn csv_escape(raw: &str) -> String {
    if raw.contains(',') || raw.contains('"') || raw.contains('\n') {
        let escaped = raw.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        raw.to_string()
    }
}

fn write_csv(results: &[BenchResult], out_path: &Path) -> std::io::Result<()> {
    let mut out = String::new();
    out.push_str("name,avg_ms,rows_sec,row_count,row_groups,used_stats,scanned_rows\n");
    for r in results {
        let line = format!(
            "{},{},{:.2},{},{},{},{}\n",
            csv_escape(&r.name),
            r.avg_ms,
            r.rows_sec,
            r.row_count,
            r.row_groups,
            r.used_stats,
            r.scanned_rows,
        );
        out.push_str(&line);
    }
    std::fs::write(out_path, out)
}

fn time_idx_and_unit(
    reader: &SerializedFileReader<Bytes>,
    time_column: &str,
) -> Result<(usize, TimeUnit), String> {
    let schema = reader.metadata().file_metadata().schema_descr();
    let time_idx = schema
        .columns()
        .iter()
        .position(|c| c.path().string() == time_column)
        .ok_or_else(|| format!("time column not found: {time_column}"))?;

    let col_descr = &schema.column(time_idx);
    let physical = col_descr.physical_type();
    let logical = col_descr.logical_type_ref();

    if physical != PhysicalType::INT64 {
        return Err(format!(
            "unsupported physical type for {time_column}: {physical:?}"
        ));
    }

    match logical {
        Some(LogicalType::Timestamp { unit, .. }) => {
            let unit = match unit {
                ParquetTimeUnit::MILLIS => TimeUnit::Millisecond,
                ParquetTimeUnit::MICROS => TimeUnit::Microsecond,
                ParquetTimeUnit::NANOS => TimeUnit::Nanosecond,
            };
            Ok((time_idx, unit))
        }
        other => Err(format!(
            "unsupported logical type for {time_column}: {other:?}"
        )),
    }
}

fn scan_row_iter(bytes: &Bytes, time_idx: usize) -> Result<(i64, i64, u64), String> {
    use parquet::record::Field;

    let reader =
        SerializedFileReader::new(bytes.clone()).map_err(|e| format!("parquet read: {e}"))?;
    let iter = reader
        .get_row_iter(None)
        .map_err(|e| format!("row iter: {e}"))?;

    let mut min_val: Option<i64> = None;
    let mut max_val: Option<i64> = None;
    let mut scanned_rows: u64 = 0;

    for row_res in iter {
        let row = row_res.map_err(|e| format!("row read: {e}"))?;
        let field = row.get_column_iter().nth(time_idx).map(|(_, f)| f);
        let v = match field {
            Some(Field::Long(val)) => *val,
            Some(Field::TimestampMillis(val)) => *val,
            Some(Field::TimestampMicros(val)) => *val,
            _ => return Err(format!("unexpected timestamp field at index {time_idx}")),
        };

        min_val = Some(match min_val {
            Some(prev) => prev.min(v),
            None => v,
        });
        max_val = Some(match max_val {
            Some(prev) => prev.max(v),
            None => v,
        });
        scanned_rows = scanned_rows.saturating_add(1);
    }

    match (min_val, max_val) {
        (Some(lo), Some(hi)) => Ok((lo, hi, scanned_rows)),
        _ => Err("no values found".to_string()),
    }
}

fn scan_direct_column(bytes: &Bytes, time_idx: usize) -> Result<(i64, i64, u64), String> {
    let reader =
        SerializedFileReader::new(bytes.clone()).map_err(|e| format!("parquet read: {e}"))?;
    let mut min_val: Option<i64> = None;
    let mut max_val: Option<i64> = None;
    let mut scanned_rows: u64 = 0;

    for rg_index in 0..reader.metadata().num_row_groups() {
        let rg = reader
            .get_row_group(rg_index)
            .map_err(|e| format!("row group read: {e}"))?;
        let col = rg
            .get_column_reader(time_idx)
            .map_err(|e| format!("column reader: {e}"))?;
        let mut values: Vec<i64> = Vec::with_capacity(64 * 1024);
        let mut def_levels: Vec<i16> = Vec::with_capacity(64 * 1024);

        match col {
            ColumnReader::Int64ColumnReader(mut typed) => loop {
                values.clear();
                def_levels.clear();
                let (records_read, values_read, levels_read) = typed
                    .read_records(64 * 1024, Some(&mut def_levels), None, &mut values)
                    .map_err(|e| format!("column read: {e}"))?;
                if records_read == 0 && levels_read == 0 {
                    break;
                }
                for v in values.iter().take(values_read) {
                    min_val = Some(match min_val {
                        Some(prev) => prev.min(*v),
                        None => *v,
                    });
                    max_val = Some(match max_val {
                        Some(prev) => prev.max(*v),
                        None => *v,
                    });
                    scanned_rows = scanned_rows.saturating_add(1);
                }
            },
            _ => return Err("unsupported column reader type (expected int64)".to_string()),
        }
    }

    match (min_val, max_val) {
        (Some(lo), Some(hi)) => Ok((lo, hi, scanned_rows)),
        _ => Err("no values found".to_string()),
    }
}

fn scan_direct_column_rg_parallel(
    bytes: &Bytes,
    time_idx: usize,
    threads_override: usize,
) -> Result<(i64, i64, u64), String> {
    let reader =
        SerializedFileReader::new(bytes.clone()).map_err(|e| format!("parquet read: {e}"))?;
    let row_groups = reader.metadata().num_row_groups();
    if row_groups == 0 {
        return Err("no row groups".to_string());
    }

    let (threads_used, rg_chunk) = resolve_rg_settings(row_groups, threads_override);
    let rg_indices: Vec<usize> = (0..row_groups).collect();
    let chunks: Vec<Vec<usize>> = rg_indices
        .chunks(rg_chunk)
        .map(|chunk| chunk.to_vec())
        .collect();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads_used)
        .build()
        .map_err(|e| format!("rayon pool: {e}"))?;

    let results: Result<Vec<(i64, i64, u64)>, String> = pool.install(|| {
        chunks
            .par_iter()
            .map(|chunk| {
                let reader = SerializedFileReader::new(bytes.clone())
                    .map_err(|e| format!("parquet read: {e}"))?;
                let mut min_val: Option<i64> = None;
                let mut max_val: Option<i64> = None;
                let mut scanned_rows: u64 = 0;

                for &rg_index in chunk {
                    let rg = reader
                        .get_row_group(rg_index)
                        .map_err(|e| format!("row group read: {e}"))?;
                    let col = rg
                        .get_column_reader(time_idx)
                        .map_err(|e| format!("column reader: {e}"))?;
                    let mut values: Vec<i64> = Vec::with_capacity(64 * 1024);
                    let mut def_levels: Vec<i16> = Vec::with_capacity(64 * 1024);

                    match col {
                        ColumnReader::Int64ColumnReader(mut typed) => loop {
                            values.clear();
                            def_levels.clear();
                            let (records_read, values_read, levels_read) = typed
                                .read_records(64 * 1024, Some(&mut def_levels), None, &mut values)
                                .map_err(|e| format!("column read: {e}"))?;
                            if records_read == 0 && levels_read == 0 {
                                break;
                            }
                            for v in values.iter().take(values_read) {
                                min_val = Some(match min_val {
                                    Some(prev) => prev.min(*v),
                                    None => *v,
                                });
                                max_val = Some(match max_val {
                                    Some(prev) => prev.max(*v),
                                    None => *v,
                                });
                                scanned_rows = scanned_rows.saturating_add(1);
                            }
                        },
                        _ => {
                            return Err(
                                "unsupported column reader type (expected int64)".to_string()
                            );
                        }
                    }
                }

                match (min_val, max_val) {
                    (Some(lo), Some(hi)) => Ok((lo, hi, scanned_rows)),
                    _ => Err("no values found".to_string()),
                }
            })
            .collect()
    });

    let results = results?;
    let mut min_val = i64::MAX;
    let mut max_val = i64::MIN;
    let mut scanned_rows: u64 = 0;
    for (lo, hi, rows) in results {
        min_val = min(min_val, lo);
        max_val = max(max_val, hi);
        scanned_rows = scanned_rows.saturating_add(rows);
    }
    Ok((min_val, max_val, scanned_rows))
}

fn scan_arrow_rg_parallel(
    bytes: &Bytes,
    time_column: &str,
    threads_override: usize,
) -> Result<(i64, i64, u64), String> {
    let metadata = ArrowReaderMetadata::load(bytes, ArrowReaderOptions::default())
        .map_err(|e| format!("arrow metadata: {e}"))?;
    let mask = ProjectionMask::columns(metadata.parquet_schema(), [time_column]);
    let row_groups = metadata.metadata().num_row_groups();

    let (threads_used, rg_chunk) = resolve_rg_settings(row_groups, threads_override);
    let rg_indices: Vec<usize> = (0..row_groups).collect();
    let chunks: Vec<Vec<usize>> = rg_indices
        .chunks(rg_chunk)
        .map(|chunk| chunk.to_vec())
        .collect();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads_used)
        .build()
        .map_err(|e| format!("rayon pool: {e}"))?;

    let results: Result<Vec<(i64, i64, u64)>, String> = pool.install(|| {
        chunks
            .par_iter()
            .map(|chunk| {
                let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                    bytes.clone(),
                    metadata.clone(),
                )
                .with_projection(mask.clone())
                .with_row_groups(chunk.clone());
                let reader = builder.build().map_err(|e| format!("arrow reader: {e}"))?;

                let mut min_val: Option<i64> = None;
                let mut max_val: Option<i64> = None;
                let mut scanned_rows: u64 = 0;

                for batch_res in reader {
                    let batch = batch_res.map_err(|e| format!("arrow batch: {e}"))?;
                    let col = batch.column(0);
                    match col.data_type() {
                        DataType::Timestamp(unit, _) => {
                            macro_rules! scan_arr {
                                ($arr:ty) => {{
                                    let arr = col
                                        .as_any()
                                        .downcast_ref::<$arr>()
                                        .ok_or_else(|| "downcast failed".to_string())?;
                                    if arr.null_count() == 0 {
                                        for &v in arr.values() {
                                            min_val = Some(match min_val {
                                                Some(prev) => prev.min(v),
                                                None => v,
                                            });
                                            max_val = Some(match max_val {
                                                Some(prev) => prev.max(v),
                                                None => v,
                                            });
                                            scanned_rows = scanned_rows.saturating_add(1);
                                        }
                                    } else {
                                        for v in arr.iter().flatten() {
                                            min_val = Some(match min_val {
                                                Some(prev) => prev.min(v),
                                                None => v,
                                            });
                                            max_val = Some(match max_val {
                                                Some(prev) => prev.max(v),
                                                None => v,
                                            });
                                            scanned_rows = scanned_rows.saturating_add(1);
                                        }
                                    }
                                }};
                            }

                            match unit {
                                TimeUnit::Second => scan_arr!(TimestampSecondArray),
                                TimeUnit::Millisecond => scan_arr!(TimestampMillisecondArray),
                                TimeUnit::Microsecond => scan_arr!(TimestampMicrosecondArray),
                                TimeUnit::Nanosecond => scan_arr!(TimestampNanosecondArray),
                            }
                        }
                        other => {
                            return Err(format!("unsupported arrow type: {other:?}"));
                        }
                    }
                }

                match (min_val, max_val) {
                    (Some(lo), Some(hi)) => Ok((lo, hi, scanned_rows)),
                    _ => Err("no values found".to_string()),
                }
            })
            .collect()
    });

    let results = results?;
    let mut min_val = i64::MAX;
    let mut max_val = i64::MIN;
    let mut scanned_rows: u64 = 0;
    for (lo, hi, rows) in results {
        min_val = min(min_val, lo);
        max_val = max(max_val, hi);
        scanned_rows = scanned_rows.saturating_add(rows);
    }
    Ok((min_val, max_val, scanned_rows))
}

fn run_bench<F>(
    name: &str,
    row_groups: usize,
    row_count: u64,
    used_stats: bool,
    repeat: usize,
    warmup: usize,
    mut f: F,
) -> Result<BenchResult, String>
where
    F: FnMut() -> Result<(i64, i64, u64), String>,
{
    for _ in 0..warmup {
        let _ = f()?;
    }

    let mut total_ms: u128 = 0;
    let mut last_scanned: u64 = 0;

    for _ in 0..repeat {
        let start = Instant::now();
        let (_min_v, _max_v, scanned_rows) = f()?;
        total_ms += start.elapsed().as_millis();
        last_scanned = scanned_rows;
    }

    let avg_ms = if repeat == 0 {
        0
    } else {
        total_ms / repeat as u128
    };
    let rows_sec = if avg_ms == 0 {
        0.0
    } else {
        (row_count as f64) / (avg_ms as f64 / 1000.0)
    };

    Ok(BenchResult {
        name: name.to_string(),
        avg_ms,
        rows_sec,
        row_groups,
        row_count,
        used_stats,
        scanned_rows: last_scanned,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let bytes = Bytes::from(std::fs::read(&args.parquet)?);

    let reader =
        SerializedFileReader::new(bytes.clone()).map_err(|e| format!("parquet read: {e}"))?;
    let meta = reader.metadata();
    let row_groups = meta.num_row_groups();
    let row_count = meta.file_metadata().num_rows() as u64;

    let (_time_idx, _unit) = time_idx_and_unit(&reader, &args.time_column)?;
    let time_idx = _time_idx;

    println!(
        "segment_meta_bench: {} (row_groups: {}, row_count: {})",
        args.parquet.display(),
        row_groups,
        row_count
    );

    let mut results = Vec::new();

    results.push(run_bench(
        "row_iter_baseline",
        row_groups,
        row_count,
        false,
        args.repeat,
        args.warmup,
        || scan_row_iter(&bytes, time_idx),
    )?);

    let (ref_min, ref_max, _) = scan_row_iter(&bytes, time_idx)?;

    results.push(run_bench(
        "direct_column_reader",
        row_groups,
        row_count,
        false,
        args.repeat,
        args.warmup,
        || scan_direct_column(&bytes, time_idx),
    )?);

    results.push(run_bench(
        "row_group_parallel_arrow",
        row_groups,
        row_count,
        false,
        args.repeat,
        args.warmup,
        || scan_arrow_rg_parallel(&bytes, &args.time_column, args.threads),
    )?);

    results.push(run_bench(
        "row_group_parallel_direct",
        row_groups,
        row_count,
        false,
        args.repeat,
        args.warmup,
        || scan_direct_column_rg_parallel(&bytes, time_idx, args.threads),
    )?);

    let strategies = [
        (
            "direct_column_reader",
            scan_direct_column(&bytes, time_idx)?,
        ),
        (
            "row_group_parallel_arrow",
            scan_arrow_rg_parallel(&bytes, &args.time_column, args.threads)?,
        ),
        (
            "row_group_parallel_direct",
            scan_direct_column_rg_parallel(&bytes, time_idx, args.threads)?,
        ),
    ];

    for (name, (min_v, max_v, _)) in strategies {
        if min_v != ref_min || max_v != ref_max {
            return Err(format!(
                "correctness check failed for {name}: expected min/max {ref_min}/{ref_max}, got {min_v}/{max_v}"
            )
            .into());
        }
    }

    for r in &results {
        println!(
            "{:<28} {:>8} ms  {:>10.2} rows/sec  scanned_rows={}",
            r.name, r.avg_ms, r.rows_sec, r.scanned_rows
        );
    }

    if let Some(csv_out) = args.csv_out.as_deref() {
        write_csv(&results, csv_out)?;
        println!("Wrote CSV report to {}", csv_out.display());
    }

    Ok(())
}
