//! Benchmark coverage bitmap computation across different parquet scan strategies.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::basic::{LogicalType, TimeUnit as ParquetTimeUnit, Type as PhysicalType};
use parquet::column::reader::ColumnReader;
use parquet::data_type::Int64Type;
use parquet::file::reader::{FileReader, SerializedFileReader};
use rayon::prelude::*;

use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};

use roaring::RoaringBitmap;

use timeseries_table_core::helpers::time_bucket::bucket_id_from_epoch_secs;
use timeseries_table_core::transaction_log::TimeBucket;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Baseline,
    RgParallel,
    ParquetDirect,
    All,
}

impl Engine {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "baseline" => Ok(Self::Baseline),
            "rg-parallel" => Ok(Self::RgParallel),
            "parquet-direct" => Ok(Self::ParquetDirect),
            "all" => Ok(Self::All),
            other => Err(format!("unknown engine '{other}'")),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::RgParallel => "rg-parallel",
            Self::ParquetDirect => "parquet-direct",
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
    batch_size: Option<usize>,
    rg_chunk: usize,
    threads: Option<usize>,
    csv: Option<String>,
}

fn usage() -> String {
    [
        "usage: coverage_bench --file <path> [options]",
        "",
        "options:",
        "  --time-column <name>    (default: ts)",
        "  --bucket <spec>         (must be 1s; default: 1s)",
        "  --engine <name>         (baseline | rg-parallel | parquet-direct | all) (default: all)",
        "  --iters <n>             (default: 5)",
        "  --warmup <n>            (default: 1)",
        "  --batch-size <n>        (optional; Arrow/parquet-direct batch size)",
        "  --rg-chunk <n>          (row groups per parallel task; default: 1)",
        "  --threads <n>           (rg-parallel only; overrides rayon thread count)",
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
    let mut batch_size = None;
    let mut rg_chunk = 1usize;
    let mut threads = None;
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
            "--batch-size" => {
                let value = args.next().ok_or("missing value for --batch-size")?;
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| "invalid --batch-size value")?;
                if parsed == 0 {
                    return Err("batch size must be > 0".to_string());
                }
                batch_size = Some(parsed);
            }
            "--rg-chunk" => {
                let value = args.next().ok_or("missing value for --rg-chunk")?;
                rg_chunk = value
                    .parse::<usize>()
                    .map_err(|_| "invalid --rg-chunk value")?;
                if rg_chunk == 0 {
                    return Err("rg-chunk must be > 0".to_string());
                }
            }
            "--threads" => {
                let value = args.next().ok_or("missing value for --threads")?;
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| "invalid --threads value")?;
                if parsed == 0 {
                    return Err("threads must be > 0".to_string());
                }
                threads = Some(parsed);
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
        batch_size,
        rg_chunk,
        threads,
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

fn secs_from_parquet_unit(unit: ParquetTimeUnit, raw: i64) -> i64 {
    match unit {
        ParquetTimeUnit::MILLIS => raw.div_euclid(1_000),
        ParquetTimeUnit::MICROS => raw.div_euclid(1_000_000),
        ParquetTimeUnit::NANOS => raw.div_euclid(1_000_000_000),
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
            let arr = $col.as_any().downcast_ref::<$array_type>().ok_or_else(|| {
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
    if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn compute_arrow_coverage(
    bytes: &Bytes,
    time_column: &str,
    bucket: &TimeBucket,
    batch_size: Option<usize>,
) -> Result<timeseries_table_core::coverage::Coverage, Box<dyn std::error::Error>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?;
    builder
        .schema()
        .index_of(time_column)
        .map_err(|_| format!("missing time column '{time_column}'"))?;
    let mask = ProjectionMask::columns(builder.parquet_schema(), [time_column]);
    let builder = builder.with_projection(mask);
    let builder = if let Some(size) = batch_size {
        builder.with_batch_size(size)
    } else {
        builder
    };
    let reader = builder.build()?;
    let bitmap = compute_bitmap_from_reader(reader, time_column, bucket)?;
    Ok(timeseries_table_core::coverage::Coverage::from_bitmap(
        bitmap,
    ))
}

fn run_baseline(
    bytes: &Bytes,
    time_column: &str,
    bucket: &TimeBucket,
    warmup: usize,
    iters: usize,
    batch_size: Option<usize>,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>> {
    for _ in 0..warmup {
        let _cov = compute_arrow_coverage(bytes, time_column, bucket, batch_size)?;
    }

    let mut durations = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        let _cov = compute_arrow_coverage(bytes, time_column, bucket, batch_size)?;
        durations.push(start.elapsed());
    }
    Ok(durations)
}

fn compute_rg_parallel_coverage(
    bytes: &Bytes,
    time_column: &str,
    bucket: &TimeBucket,
    batch_size: Option<usize>,
    rg_chunk: usize,
    threads: Option<usize>,
) -> Result<timeseries_table_core::coverage::Coverage, Box<dyn std::error::Error>> {
    let metadata = ArrowReaderMetadata::load(bytes, ArrowReaderOptions::default())?;
    let schema = metadata.schema();
    schema
        .index_of(time_column)
        .map_err(|_| format!("missing time column '{time_column}'"))?;
    let mask = ProjectionMask::columns(metadata.parquet_schema(), [time_column]);
    let row_groups: Vec<usize> = (0..metadata.metadata().num_row_groups()).collect();
    let chunks: Vec<Vec<usize>> = row_groups
        .chunks(rg_chunk)
        .map(|chunk| chunk.to_vec())
        .collect();

    let execute = || -> Result<Vec<RoaringBitmap>, String> {
        chunks
            .par_iter()
            .map(|chunk| {
                let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                    bytes.clone(),
                    metadata.clone(),
                )
                .with_projection(mask.clone())
                .with_row_groups(chunk.clone());
                let builder = if let Some(size) = batch_size {
                    builder.with_batch_size(size)
                } else {
                    builder
                };
                let reader = builder
                    .build()
                    .map_err(|e| format!("parquet read error: {e}"))?;
                compute_bitmap_from_reader(reader, time_column, bucket)
            })
            .collect()
    };

    let bitmaps = if let Some(count) = threads {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(count)
            .build()
            .map_err(|e| format!("failed to build rayon thread pool: {e}"))?;
        pool.install(execute)
    } else {
        execute()
    };

    let mut merged = RoaringBitmap::new();
    for bm in bitmaps? {
        merged |= bm;
    }

    Ok(timeseries_table_core::coverage::Coverage::from_bitmap(
        merged,
    ))
}

fn find_parquet_column_index(
    schema: &parquet::schema::types::SchemaDescriptor,
    time_column: &str,
) -> Result<usize, String> {
    schema
        .columns()
        .iter()
        .enumerate()
        .find_map(|(idx, col)| {
            col.path()
                .parts()
                .last()
                .map(|part| part == time_column)
                .unwrap_or(false)
                .then_some(idx)
        })
        .ok_or_else(|| format!("missing time column '{time_column}'"))
}

fn read_int64_column_into_bitmap(
    reader: &mut parquet::column::reader::ColumnReaderImpl<Int64Type>,
    max_def_level: i16,
    unit: ParquetTimeUnit,
    bucket: &TimeBucket,
    bitmap: &mut RoaringBitmap,
    batch_size: usize,
) -> Result<(), String> {
    let mut values: Vec<i64> = Vec::with_capacity(batch_size);
    let mut def_levels_storage: Option<Vec<i16>> = if max_def_level > 0 {
        Some(Vec::with_capacity(batch_size))
    } else {
        None
    };

    loop {
        values.clear();
        if let Some(def_levels) = def_levels_storage.as_mut() {
            def_levels.clear();
        }

        let (records_read, values_read, levels_read) = reader
            .read_records(batch_size, def_levels_storage.as_mut(), None, &mut values)
            .map_err(|e| format!("parquet read error: {e}"))?;

        if records_read == 0 {
            break;
        }

        if max_def_level == 0 {
            for &raw in &values[..values_read] {
                let secs = secs_from_parquet_unit(unit, raw);
                let bucket_id = bucket_id_from_epoch_secs(bucket, secs);
                insert_bucket(bitmap, bucket_id)?;
            }
        } else {
            let def_levels = def_levels_storage
                .as_ref()
                .ok_or_else(|| "missing definition levels buffer".to_string())?;
            let mut value_idx = 0usize;
            for &level in &def_levels[..levels_read] {
                if level == max_def_level {
                    let raw = values[value_idx];
                    value_idx += 1;
                    let secs = secs_from_parquet_unit(unit, raw);
                    let bucket_id = bucket_id_from_epoch_secs(bucket, secs);
                    insert_bucket(bitmap, bucket_id)?;
                }
            }
        }
    }

    Ok(())
}

fn compute_parquet_direct_coverage_with_batch(
    bytes: &Bytes,
    time_column: &str,
    bucket: &TimeBucket,
    batch_size: Option<usize>,
) -> Result<timeseries_table_core::coverage::Coverage, Box<dyn std::error::Error>> {
    let reader = SerializedFileReader::new(bytes.clone())?;
    let schema = reader.metadata().file_metadata().schema_descr();
    let time_idx = find_parquet_column_index(schema, time_column)?;
    let col_descr = schema.column(time_idx);

    if col_descr.max_rep_level() > 0 {
        return Err("nested time column is not supported".into());
    }

    let physical = col_descr.physical_type();
    if physical != PhysicalType::INT64 {
        return Err(
            format!("unsupported parquet physical type for {time_column}: {physical:?}").into(),
        );
    }

    let unit = match col_descr.logical_type_ref() {
        Some(LogicalType::Timestamp { unit, .. }) => *unit,
        other => {
            return Err(
                format!("unsupported parquet logical type for {time_column}: {other:?}").into(),
            );
        }
    };

    let mut bitmap = RoaringBitmap::new();
    let max_def_level = col_descr.max_def_level();
    let batch_size = batch_size.unwrap_or(8192);

    for rg_idx in 0..reader.num_row_groups() {
        let row_group = reader.get_row_group(rg_idx)?;
        let col_reader = row_group.get_column_reader(time_idx)?;
        match col_reader {
            ColumnReader::Int64ColumnReader(mut typed_reader) => {
                read_int64_column_into_bitmap(
                    &mut typed_reader,
                    max_def_level,
                    unit,
                    bucket,
                    &mut bitmap,
                    batch_size,
                )?;
            }
            _ => {
                return Err(format!("unsupported parquet column reader for {time_column}").into());
            }
        }
    }

    Ok(timeseries_table_core::coverage::Coverage::from_bitmap(
        bitmap,
    ))
}

fn run_rg_parallel(
    bytes: &Bytes,
    args: &Args,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>> {
    let run_once = || -> Result<(), Box<dyn std::error::Error>> {
        let cov = compute_rg_parallel_coverage(
            bytes,
            &args.time_column,
            &args.bucket,
            args.batch_size,
            args.rg_chunk,
            args.threads,
        )?;
        let _ = cov.present();
        Ok(())
    };

    for _ in 0..args.warmup {
        run_once()?;
    }

    let mut durations = Vec::with_capacity(args.iters);
    for _ in 0..args.iters {
        let start = Instant::now();
        run_once()?;
        durations.push(start.elapsed());
    }
    Ok(durations)
}

fn run_parquet_direct(
    bytes: &Bytes,
    time_column: &str,
    bucket: &TimeBucket,
    warmup: usize,
    iters: usize,
    batch_size: Option<usize>,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>> {
    for _ in 0..warmup {
        let _cov =
            compute_parquet_direct_coverage_with_batch(bytes, time_column, bucket, batch_size)?;
    }

    let mut durations = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        let _cov =
            compute_parquet_direct_coverage_with_batch(bytes, time_column, bucket, batch_size)?;
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
        Engine::ParquetDirect => vec![Engine::ParquetDirect],
        Engine::All => vec![Engine::Baseline, Engine::RgParallel, Engine::ParquetDirect],
    };

    if engines.contains(&Engine::Baseline) {
        let baseline_cov =
            compute_arrow_coverage(&bytes, &args.time_column, &args.bucket, args.batch_size)?;
        if engines.contains(&Engine::RgParallel) {
            let rg_cov = compute_rg_parallel_coverage(
                &bytes,
                &args.time_column,
                &args.bucket,
                args.batch_size,
                args.rg_chunk,
                args.threads,
            )?;
            if baseline_cov.present() != rg_cov.present() {
                return Err("rg-parallel coverage mismatch vs baseline".into());
            }
        }
        if engines.contains(&Engine::ParquetDirect) {
            let direct_cov = compute_parquet_direct_coverage_with_batch(
                &bytes,
                &args.time_column,
                &args.bucket,
                args.batch_size,
            )?;
            if baseline_cov.present() != direct_cov.present() {
                let diff = baseline_cov.present() ^ direct_cov.present();
                let example = diff.iter().next();
                return Err(format!(
                    "parquet-direct coverage mismatch vs baseline (baseline_len={}, direct_len={}, diff_len={}, example={example:?})",
                    baseline_cov.present().len(),
                    direct_cov.present().len(),
                    diff.len(),
                )
                .into());
            }
        }
    }

    for engine in engines {
        let durations = match engine {
            Engine::Baseline => run_baseline(
                &bytes,
                &args.time_column,
                &args.bucket,
                args.warmup,
                args.iters,
                args.batch_size,
            )?,
            Engine::RgParallel => run_rg_parallel(&bytes, &args)?,
            Engine::ParquetDirect => run_parquet_direct(
                &bytes,
                &args.time_column,
                &args.bucket,
                args.warmup,
                args.iters,
                args.batch_size,
            )?,
            Engine::All => unreachable!(),
        };

        let mut ms_values: Vec<f64> = durations.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
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
