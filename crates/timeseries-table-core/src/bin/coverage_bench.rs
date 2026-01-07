use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use rayon::prelude::*;

use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};

use roaring::RoaringBitmap;

use timeseries_table_core::helpers::time_bucket::bucket_id_from_epoch_secs;
use timeseries_table_core::helpers::segment_coverage::compute_segment_coverage_from_parquet_bytes;
use timeseries_table_core::transaction_log::TimeBucket;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Baseline,
    RgParallel,
    All,
}

impl Engine {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "baseline" => Ok(Self::Baseline),
            "rg-parallel" => Ok(Self::RgParallel),
            "all" => Ok(Self::All),
            other => Err(format!("unknown engine '{other}'")),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::RgParallel => "rg-parallel",
            Self::All => "all",
        }
    }
}

#[derive(Debug)]
struct Args {
    file: String,
    time_column: String,
    bucket: TimeBucket,
    engine: Engine,
    iters: usize,
    warmup: usize,
    csv: Option<String>,
}

fn usage() -> String {
    [
        "usage: coverage_bench --file <path> [options]",
        "",
        "options:",
        "  --time-column <name>    (default: ts)",
        "  --bucket <spec>         (must be 1s; default: 1s)",
        "  --engine <name>         (baseline | rg-parallel | all) (default: all)",
        "  --iters <n>             (default: 5)",
        "  --warmup <n>            (default: 1)",
        "  --csv <path>            (optional CSV output)",
    ]
    .join("\n")
}

fn parse_args() -> Result<Args, String> {
    let mut file = None;
    let mut time_column = "ts".to_string();
    let mut bucket = TimeBucket::parse("1s").map_err(|e| e.to_string())?;
    let mut engine = Engine::All;
    let mut iters = 5usize;
    let mut warmup = 1usize;
    let mut csv = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--file" => {
                file = args.next();
            }
            "--time-column" => {
                time_column = args.next().ok_or("missing value for --time-column")?;
            }
            "--bucket" => {
                let spec = args.next().ok_or("missing value for --bucket")?;
                bucket = TimeBucket::parse(&spec).map_err(|e| e.to_string())?;
            }
            "--engine" => {
                let name = args.next().ok_or("missing value for --engine")?;
                engine = Engine::parse(&name)?;
            }
            "--iters" => {
                let value = args.next().ok_or("missing value for --iters")?;
                iters = value
                    .parse::<usize>()
                    .map_err(|_| "invalid --iters value")?;
            }
            "--warmup" => {
                let value = args.next().ok_or("missing value for --warmup")?;
                warmup = value
                    .parse::<usize>()
                    .map_err(|_| "invalid --warmup value")?;
            }
            "--csv" => {
                csv = Some(args.next().ok_or("missing value for --csv")?);
            }
            "--help" | "-h" => {
                return Err(usage());
            }
            other => {
                return Err(format!("unknown argument '{other}'\n\n{}", usage()));
            }
        }
    }

    let file = file.ok_or_else(|| format!("--file is required\n\n{}", usage()))?;

    if bucket != TimeBucket::Seconds(1) {
        return Err("bucket must be 1s for this benchmark".to_string());
    }

    Ok(Args {
        file,
        time_column,
        bucket,
        engine,
        iters,
        warmup,
        csv,
    })
}

fn write_csv_row(path: &str, header: &str, row: &str) -> std::io::Result<()> {
    let exists = Path::new(path).exists();
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if !exists {
        writeln!(file, "{header}")?;
    }
    writeln!(file, "{row}")?;
    Ok(())
}

fn bucket_spec_string(bucket: &TimeBucket) -> String {
    match bucket {
        TimeBucket::Seconds(n) => format!("{n}s"),
        TimeBucket::Minutes(n) => format!("{n}m"),
        TimeBucket::Hours(n) => format!("{n}h"),
        TimeBucket::Days(n) => format!("{n}d"),
    }
}

fn secs_from_raw(unit: TimeUnit, raw: i64) -> i64 {
    match unit {
        TimeUnit::Second => raw,
        TimeUnit::Millisecond => raw.div_euclid(1_000),
        TimeUnit::Microsecond => raw.div_euclid(1_000_000),
        TimeUnit::Nanosecond => raw.div_euclid(1_000_000_000),
    }
}

fn insert_bucket(bitmap: &mut RoaringBitmap, bucket: u64) -> Result<(), String> {
    if bucket > u32::MAX as u64 {
        return Err(format!(
            "bucket id {bucket} does not fit into u32 bucket domain"
        ));
    }
    bitmap.insert(bucket as u32);
    Ok(())
}

fn add_buckets_from_iter(
    bitmap: &mut RoaringBitmap,
    spec: &TimeBucket,
    unit: TimeUnit,
    iter: impl Iterator<Item = Option<i64>>,
) -> Result<(), String> {
    for raw in iter.flatten() {
        let secs = secs_from_raw(unit, raw);
        let bucket = bucket_id_from_epoch_secs(spec, secs);
        insert_bucket(bitmap, bucket)?;
    }
    Ok(())
}

fn add_buckets_from_values(
    bitmap: &mut RoaringBitmap,
    spec: &TimeBucket,
    unit: TimeUnit,
    values: &[i64],
) -> Result<(), String> {
    for &raw in values {
        let secs = secs_from_raw(unit, raw);
        let bucket = bucket_id_from_epoch_secs(spec, secs);
        insert_bucket(bitmap, bucket)?;
    }
    Ok(())
}

fn compute_bitmap_from_reader(
    reader: impl Iterator<Item = Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>>,
    time_column: &str,
    bucket_spec: &TimeBucket,
) -> Result<RoaringBitmap, String> {
    let mut bitmap = RoaringBitmap::new();

    macro_rules! process_timestamp_array {
        ($array_type: ty, $col: expr, $unit: expr) => {{
            let arr = $col
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(|| {
                    format!(
                        "unsupported arrow type for column {time_column}: {}",
                        $col.data_type()
                    )
                })?;

            if arr.null_count() == 0 {
                add_buckets_from_values(&mut bitmap, bucket_spec, $unit, arr.values())
            } else {
                add_buckets_from_iter(&mut bitmap, bucket_spec, $unit, arr.iter())
            }
        }};
    }

    for batch_res in reader {
        let batch = batch_res.map_err(|e| format!("arrow read error: {e}"))?;
        let col = batch.column(0);

        match col.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => process_timestamp_array!(TimestampSecondArray, col, *unit)?,
                TimeUnit::Millisecond => {
                    process_timestamp_array!(TimestampMillisecondArray, col, *unit)?
                }
                TimeUnit::Microsecond => {
                    process_timestamp_array!(TimestampMicrosecondArray, col, *unit)?
                }
                TimeUnit::Nanosecond => {
                    process_timestamp_array!(TimestampNanosecondArray, col, *unit)?
                }
            },
            other => {
                return Err(format!(
                    "unsupported arrow type for column {time_column}: {other}"
                ));
            }
        }
    }

    Ok(bitmap)
}

fn median_ms(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn run_baseline(
    bytes: &Bytes,
    file: &str,
    time_column: &str,
    bucket: &TimeBucket,
    warmup: usize,
    iters: usize,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>> {
    let rel_path = Path::new(file);
    for _ in 0..warmup {
        let _cov = compute_segment_coverage_from_parquet_bytes(
            rel_path,
            time_column,
            bucket,
            bytes.clone(),
        )?;
    }

    let mut durations = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        let _cov = compute_segment_coverage_from_parquet_bytes(
            rel_path,
            time_column,
            bucket,
            bytes.clone(),
        )?;
        durations.push(start.elapsed());
    }
    Ok(durations)
}

fn compute_baseline_coverage(
    bytes: &Bytes,
    file: &str,
    time_column: &str,
    bucket: &TimeBucket,
) -> Result<timeseries_table_core::coverage::Coverage, Box<dyn std::error::Error>> {
    let rel_path = Path::new(file);
    Ok(compute_segment_coverage_from_parquet_bytes(
        rel_path,
        time_column,
        bucket,
        bytes.clone(),
    )?)
}

fn compute_rg_parallel_coverage(
    bytes: &Bytes,
    time_column: &str,
    bucket: &TimeBucket,
) -> Result<timeseries_table_core::coverage::Coverage, Box<dyn std::error::Error>> {
    let metadata = ArrowReaderMetadata::load(bytes, ArrowReaderOptions::default())?;
    let schema = metadata.schema();
    schema
        .index_of(time_column)
        .map_err(|_| format!("missing time column '{time_column}'"))?;
    let mask = ProjectionMask::columns(metadata.parquet_schema(), [time_column]);
    let row_groups = 0..metadata.metadata().num_row_groups();

    let bitmaps: Result<Vec<RoaringBitmap>, String> = row_groups
        .into_par_iter()
        .map(|rg| {
            let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                bytes.clone(),
                metadata.clone(),
            )
            .with_projection(mask.clone())
            .with_row_groups(vec![rg]);
            let reader = builder.build().map_err(|e| format!("parquet read error: {e}"))?;
            compute_bitmap_from_reader(reader, time_column, bucket)
        })
        .collect();

    let mut merged = RoaringBitmap::new();
    for bm in bitmaps? {
        merged |= bm;
    }

    Ok(timeseries_table_core::coverage::Coverage::from_bitmap(
        merged,
    ))
}

fn run_rg_parallel(
    bytes: &Bytes,
    _file: &str,
    time_column: &str,
    bucket: &TimeBucket,
    warmup: usize,
    iters: usize,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>> {
    let metadata = ArrowReaderMetadata::load(bytes, ArrowReaderOptions::default())?;
    let schema = metadata.schema();
    schema
        .index_of(time_column)
        .map_err(|_| format!("missing time column '{time_column}'"))?;
    let mask = ProjectionMask::columns(metadata.parquet_schema(), [time_column]);
    let row_groups = 0..metadata.metadata().num_row_groups();

    let run_once = || -> Result<(), Box<dyn std::error::Error>> {
        let bitmaps: Result<Vec<RoaringBitmap>, String> = row_groups
            .clone()
            .into_par_iter()
            .map(|rg| {
                let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                    bytes.clone(),
                    metadata.clone(),
                )
                .with_projection(mask.clone())
                .with_row_groups(vec![rg]);
                let reader = builder.build().map_err(|e| format!("parquet read error: {e}"))?;
                compute_bitmap_from_reader(reader, time_column, bucket)
            })
            .collect();

        let mut merged = RoaringBitmap::new();
        for bm in bitmaps? {
            merged |= bm;
        }

        // Use the result so the compiler doesn't optimize out the loop.
        let _cov = timeseries_table_core::coverage::Coverage::from_bitmap(merged);
        Ok(())
    };

    for _ in 0..warmup {
        let _ = run_once()?;
    }

    let mut durations = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        run_once()?;
        durations.push(start.elapsed());
    }
    Ok(durations)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = match parse_args() {
        Ok(args) => args,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(2);
        }
    };

    let bytes = Bytes::from(fs::read(&args.file)?);
    let metadata = ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::default())?;
    let rows = metadata.metadata().file_metadata().num_rows();

    let mut results = Vec::new();

    let engines = match args.engine {
        Engine::Baseline => vec![Engine::Baseline],
        Engine::RgParallel => vec![Engine::RgParallel],
        Engine::All => vec![Engine::Baseline, Engine::RgParallel],
    };

    if engines.contains(&Engine::Baseline) && engines.contains(&Engine::RgParallel) {
        let baseline_cov = compute_baseline_coverage(
            &bytes,
            &args.file,
            &args.time_column,
            &args.bucket,
        )?;
        let rg_cov = compute_rg_parallel_coverage(&bytes, &args.time_column, &args.bucket)?;
        if baseline_cov.present() != rg_cov.present() {
            return Err("rg-parallel coverage mismatch vs baseline".into());
        }
    }

    for engine in engines {
        let durations = match engine {
            Engine::Baseline => run_baseline(
                &bytes,
                &args.file,
                &args.time_column,
                &args.bucket,
                args.warmup,
                args.iters,
            )?,
            Engine::RgParallel => run_rg_parallel(
                &bytes,
                &args.file,
                &args.time_column,
                &args.bucket,
                args.warmup,
                args.iters,
            )?,
            Engine::All => unreachable!(),
        };

        let mut ms_values: Vec<f64> = durations
            .iter()
            .map(|d| d.as_secs_f64() * 1000.0)
            .collect();
        let avg_ms = ms_values.iter().sum::<f64>() / ms_values.len() as f64;
        let med_ms = median_ms(&mut ms_values);

        results.push((engine.label(), avg_ms, med_ms, durations));
    }

    println!(
        "{:<12} {:>10} {:>10} {:>14}",
        "engine", "avg_ms", "med_ms", "rows_per_sec"
    );
    for (label, avg_ms, med_ms, durations) in &results {
        let avg_secs = avg_ms / 1000.0;
        let rows_per_sec = if avg_secs > 0.0 {
            (rows as f64) / avg_secs
        } else {
            0.0
        };
        println!(
            "{:<12} {:>10.3} {:>10.3} {:>14.0}",
            label, avg_ms, med_ms, rows_per_sec
        );

        if let Some(csv_path) = &args.csv {
            let header = "engine,file,rows,bucket_spec,iter,elapsed_ms,throughput_rows_per_sec";
            for (idx, duration) in durations.iter().enumerate() {
                let elapsed_ms = duration.as_secs_f64() * 1000.0;
                let throughput = if duration.as_secs_f64() > 0.0 {
                    (rows as f64) / duration.as_secs_f64()
                } else {
                    0.0
                };
                let bucket = bucket_spec_string(&args.bucket);
                let row = format!(
                    "{label},{file},{rows},{bucket},{iter},{elapsed_ms:.3},{throughput:.0}",
                    label = label,
                    file = args.file,
                    rows = rows,
                    bucket = bucket,
                    iter = idx + 1,
                    elapsed_ms = elapsed_ms,
                    throughput = throughput
                );
                write_csv_row(csv_path, header, &row)?;
            }
        }
    }

    Ok(())
}
