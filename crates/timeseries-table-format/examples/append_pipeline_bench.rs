//! Compare path-first and streaming append pipelines with deterministic Arrow data.

use std::fs::File;
use std::io;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int64Array, RecordBatch, RecordBatchReader,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use clap::{Parser, ValueEnum};
use futures::StreamExt;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use serde_json::{Value, json};
use timeseries_table_format::{
    metadata::table_metadata::{IndexKind, IndexSpec, IndexValue, TableMeta},
    storage::TableLocation,
    table::TimeSeriesTable,
};

const INDEX_COLUMN: &str = "ts";
const SEQUENCE_COLUMN: &str = "sequence";
const PAYLOAD_COLUMN: &str = "payload";

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    PathFirst,
    Streaming,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::PathFirst => "path-first",
            Self::Streaming => "streaming",
        }
    }
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, value_enum)]
    mode: Mode,

    #[arg(long)]
    table: PathBuf,

    /// Required for path-first mode and rejected for streaming mode.
    #[arg(long)]
    external_parquet: Option<PathBuf>,

    #[arg(long)]
    row_count: NonZeroU64,

    #[arg(long)]
    batch_rows: NonZeroUsize,

    #[arg(long)]
    payload_bytes: NonZeroUsize,

    #[arg(long)]
    seed: u64,
}

#[derive(Clone, Copy, Debug)]
struct Workload {
    row_count: u64,
    batch_rows: usize,
    payload_bytes: usize,
    seed: u64,
}

struct DeterministicBatchReader {
    schema: SchemaRef,
    workload: Workload,
    next_row: u64,
}

impl DeterministicBatchReader {
    fn new(workload: Workload) -> Result<Self, ArrowError> {
        i64::try_from(workload.row_count)
            .map_err(|_| ArrowError::InvalidArgumentError("row count exceeds i64".to_string()))?;
        workload
            .batch_rows
            .checked_mul(workload.payload_bytes)
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError("batch payload size overflow".to_string())
            })?;

        Ok(Self {
            schema: benchmark_schema(),
            workload,
            next_row: 0,
        })
    }
}

impl Iterator for DeterministicBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_row == self.workload.row_count {
            return None;
        }

        let remaining = self.workload.row_count - self.next_row;
        let row_count = remaining.min(self.workload.batch_rows as u64) as usize;
        let result = make_batch(self.next_row, row_count, self.workload);
        if result.is_ok() {
            self.next_row += row_count as u64;
        }
        Some(result)
    }
}

impl RecordBatchReader for DeterministicBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn benchmark_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(INDEX_COLUMN, DataType::Int64, false),
        Field::new(SEQUENCE_COLUMN, DataType::UInt64, false),
        Field::new(PAYLOAD_COLUMN, DataType::Binary, false),
    ]))
}

fn payload_pattern(row: u64, seed: u64) -> [u8; 8] {
    let mut value = row ^ seed;
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    (value ^ (value >> 31)).to_le_bytes()
}

fn fill_payload(payload: &mut [u8], row: u64, seed: u64) {
    let pattern = payload_pattern(row, seed);
    for chunk in payload.chunks_mut(pattern.len()) {
        chunk.copy_from_slice(&pattern[..chunk.len()]);
    }
}

fn payload_matches(payload: &[u8], row: u64, seed: u64) -> bool {
    let pattern = payload_pattern(row, seed);
    payload
        .chunks(pattern.len())
        .all(|chunk| chunk == &pattern[..chunk.len()])
}

fn make_batch(
    first_row: u64,
    row_count: usize,
    workload: Workload,
) -> Result<RecordBatch, ArrowError> {
    let end_row = first_row
        .checked_add(row_count as u64)
        .ok_or_else(|| ArrowError::InvalidArgumentError("row range overflow".to_string()))?;
    let first_index = i64::try_from(first_row)
        .map_err(|_| ArrowError::InvalidArgumentError("index exceeds i64".to_string()))?;
    let end_index = i64::try_from(end_row)
        .map_err(|_| ArrowError::InvalidArgumentError("index exceeds i64".to_string()))?;
    let payload_capacity = row_count
        .checked_mul(workload.payload_bytes)
        .ok_or_else(|| ArrowError::InvalidArgumentError("batch payload overflow".to_string()))?;

    let timestamps = Int64Array::from_iter_values(first_index..end_index);
    let sequences = UInt64Array::from_iter_values(first_row..end_row);
    let mut payloads = BinaryBuilder::with_capacity(row_count, payload_capacity);
    let mut payload = vec![0; workload.payload_bytes];
    for row in first_row..end_row {
        fill_payload(&mut payload, row, workload.seed);
        payloads.append_value(&payload);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(timestamps),
        Arc::new(sequences),
        Arc::new(payloads.finish()),
    ];
    RecordBatch::try_new(benchmark_schema(), columns)
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn elapsed_ns(elapsed: Duration) -> Result<u64, io::Error> {
    u64::try_from(elapsed.as_nanos()).map_err(|_| invalid_data("duration exceeds u64 nanoseconds"))
}

fn write_external_parquet(
    path: &Path,
    workload: Workload,
) -> Result<(), Box<dyn std::error::Error>> {
    if path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("external Parquet path already exists: {}", path.display()),
        )
        .into());
    }
    std::fs::create_dir_all(
        path.parent()
            .ok_or_else(|| invalid_data("external Parquet path has no parent"))?,
    )?;

    let reader = DeterministicBatchReader::new(workload)?;
    let mut writer = ArrowWriter::try_new(
        File::create(path)?,
        reader.schema(),
        Some(WriterProperties::builder().build()),
    )?;
    for batch in reader {
        writer.write(&batch?)?;
    }
    writer.close()?;
    Ok(())
}

fn table_definition() -> TableMeta {
    TableMeta::new_time_series(IndexSpec {
        column: INDEX_COLUMN.to_string(),
        entity_columns: Vec::new(),
        kind: IndexKind::Int64 {
            bucket_width: NonZeroU64::MIN,
        },
    })
}

fn writer_properties_json() -> Value {
    let properties = WriterProperties::builder().build();
    let payload = ColumnPath::from(PAYLOAD_COLUMN);
    json!({
        "compression": format!("{:?}", properties.compression(&payload)),
        "data_page_size_bytes": properties.data_page_size_limit(),
        "dictionary_enabled": properties.dictionary_enabled(&payload),
        "max_row_group_rows": properties.max_row_group_row_count(),
        "statistics": format!("{:?}", properties.statistics_enabled(&payload)),
        "write_batch_rows": properties.write_batch_size(),
        "writer_version": format!("{:?}", properties.writer_version()),
    })
}

async fn validate_table(
    table: &TimeSeriesTable,
    table_root: &Path,
    workload: Workload,
) -> Result<Value, Box<dyn std::error::Error>> {
    if table.state().version != 2 {
        return Err(invalid_data(format!(
            "expected committed version 2, found {}",
            table.state().version
        ))
        .into());
    }
    let stored_schema = table.state().table_meta.arrow_schema_ref()?;
    let generated_schema = benchmark_schema();
    if stored_schema != generated_schema {
        return Err(invalid_data(format!(
            "stored schema {stored_schema:?} does not match generated schema {generated_schema:?}"
        ))
        .into());
    }
    if table.state().segments.len() != 1 {
        return Err(invalid_data(format!(
            "expected one committed segment, found {}",
            table.state().segments.len()
        ))
        .into());
    }

    let segment = table
        .state()
        .segments
        .values()
        .next()
        .ok_or_else(|| invalid_data("committed table has no segment"))?;
    if segment.row_count != workload.row_count {
        return Err(invalid_data(format!(
            "expected {} committed rows, found {}",
            workload.row_count, segment.row_count
        ))
        .into());
    }
    let expected_max = i64::try_from(workload.row_count - 1)?;
    if segment.index_min != IndexValue::Int64(0)
        || segment.index_max != IndexValue::Int64(expected_max)
    {
        return Err(invalid_data(format!(
            "unexpected index bounds: {}..{}",
            segment.index_min, segment.index_max
        ))
        .into());
    }
    let segment_path = segment.path.clone();
    let segment_file_bytes = std::fs::metadata(table_root.join(&segment_path))?.len();
    let row_group_count =
        ParquetRecordBatchReaderBuilder::try_new(File::open(table_root.join(&segment_path))?)?
            .metadata()
            .num_row_groups();

    let scan_end = i64::try_from(workload.row_count)?;
    let coverage_ratio = table.coverage_ratio_for_range(0_i64, scan_end).await?;
    if coverage_ratio != 1.0 {
        return Err(invalid_data(format!(
            "expected complete coverage, found ratio {coverage_ratio}"
        ))
        .into());
    }

    let mut stream = table.scan_range(0_i64, scan_end).await?;
    let mut expected_row = 0_u64;
    let mut index_checksum = blake3::Hasher::new();
    let mut sequence_checksum = blake3::Hasher::new();
    let mut payload_checksum = blake3::Hasher::new();
    while let Some(batch) = stream.next().await.transpose()? {
        if batch.schema() != generated_schema {
            return Err(invalid_data("scan returned an unexpected schema").into());
        }
        let indexes = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid_data("scan index column is not Int64"))?;
        let sequences = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid_data("scan sequence column is not UInt64"))?;
        let payloads = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid_data("scan payload column is not Binary"))?;
        if indexes.null_count() != 0 || sequences.null_count() != 0 || payloads.null_count() != 0 {
            return Err(invalid_data("scan returned null benchmark values").into());
        }

        for row in 0..batch.num_rows() {
            let expected_index = i64::try_from(expected_row)?;
            let payload = payloads.value(row);
            if indexes.value(row) != expected_index
                || sequences.value(row) != expected_row
                || payload.len() != workload.payload_bytes
                || !payload_matches(payload, expected_row, workload.seed)
            {
                return Err(invalid_data(format!(
                    "scan row {expected_row} does not match generated input"
                ))
                .into());
            }
            index_checksum.update(&expected_index.to_le_bytes());
            sequence_checksum.update(&expected_row.to_le_bytes());
            payload_checksum.update(payload);
            expected_row += 1;
        }
    }
    if expected_row != workload.row_count {
        return Err(invalid_data(format!(
            "expected {} scanned rows, found {expected_row}",
            workload.row_count
        ))
        .into());
    }

    Ok(json!({
        "column_checksums": {
            INDEX_COLUMN: index_checksum.finalize().to_hex().to_string(),
            SEQUENCE_COLUMN: sequence_checksum.finalize().to_hex().to_string(),
            PAYLOAD_COLUMN: payload_checksum.finalize().to_hex().to_string(),
        },
        "coverage_ratio": coverage_ratio,
        "index_max": expected_max,
        "index_min": 0,
        "ordered_full_scan_matches_generated": true,
        "row_count": expected_row,
        "schema_matches_generated": true,
        "segment_file_bytes": segment_file_bytes,
        "segment_path": segment_path,
        "segment_row_group_count": row_group_count,
    }))
}

async fn run_benchmark(args: &Args) -> Result<Value, Box<dyn std::error::Error>> {
    if args.table.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("table path already exists: {}", args.table.display()),
        )
        .into());
    }
    let external_parquet = match (args.mode, args.external_parquet.as_deref()) {
        (Mode::PathFirst, Some(path)) => Some(path),
        (Mode::PathFirst, None) => {
            return Err(invalid_data("path-first mode requires --external-parquet").into());
        }
        (Mode::Streaming, None) => None,
        (Mode::Streaming, Some(_)) => {
            return Err(invalid_data("streaming mode rejects --external-parquet").into());
        }
    };
    if external_parquet.is_some_and(|path| path.starts_with(&args.table)) {
        return Err(invalid_data("external Parquet path must be outside the table root").into());
    }

    let workload = Workload {
        row_count: args.row_count.get(),
        batch_rows: args.batch_rows.get(),
        payload_bytes: args.payload_bytes.get(),
        seed: args.seed,
    };
    let location = TableLocation::local(&args.table);
    let mut table = TimeSeriesTable::create(location, table_definition()).await?;

    eprintln!("Running {} append pipeline", args.mode.name());
    let pipeline_started = Instant::now();
    let (external_generation_ns, path_append_ns, streaming_append_ns, committed_version) =
        match args.mode {
            Mode::PathFirst => {
                let path = external_parquet
                    .ok_or_else(|| invalid_data("path-first mode requires --external-parquet"))?;
                let generation_started = Instant::now();
                write_external_parquet(path, workload)?;
                let generation_ns = elapsed_ns(generation_started.elapsed())?;
                let append_started = Instant::now();
                let (version, _) = table.append_parquet_from_path(path).await?;
                (
                    Some(generation_ns),
                    Some(elapsed_ns(append_started.elapsed())?),
                    None,
                    version,
                )
            }
            Mode::Streaming => {
                let append_started = Instant::now();
                let version = table
                    .append(DeterministicBatchReader::new(workload)?)
                    .await?;
                (
                    None,
                    None,
                    Some(elapsed_ns(append_started.elapsed())?),
                    version,
                )
            }
        };
    let pipeline_ns = elapsed_ns(pipeline_started.elapsed())?;
    if committed_version != table.state().version {
        return Err(invalid_data(format!(
            "append returned version {committed_version}, but table state is version {}",
            table.state().version
        ))
        .into());
    }

    eprintln!("Validating committed {} table", args.mode.name());
    let validation = validate_table(&table, &args.table, workload).await?;
    let segment_file_bytes = validation["segment_file_bytes"]
        .as_u64()
        .ok_or_else(|| invalid_data("validation omitted segment_file_bytes"))?;
    let external_parquet_bytes = external_parquet
        .map(std::fs::metadata)
        .transpose()?
        .map(|metadata| metadata.len());
    let retained_ingestion_bytes = segment_file_bytes
        .checked_add(external_parquet_bytes.unwrap_or(0))
        .ok_or_else(|| invalid_data("retained byte count overflow"))?;

    Ok(json!({
        "schema_version": 1,
        "mode": args.mode.name(),
        "process_id": std::process::id(),
        "table_path": args.table,
        "external_parquet_path": external_parquet,
        "workload": {
            "row_count": workload.row_count,
            "batch_rows": workload.batch_rows,
            "payload_bytes_per_row": workload.payload_bytes,
            "seed": workload.seed,
        },
        "table_definition": {
            "index_column": INDEX_COLUMN,
            "index_type": "int64",
            "bucket_width": 1,
            "entity_columns": [],
        },
        "writer_properties": writer_properties_json(),
        "timing": {
            "external_parquet_generation_ns": external_generation_ns,
            "path_append_copy_commit_ns": path_append_ns,
            "streaming_append_ns": streaming_append_ns,
            "end_to_end_pipeline_ns": pipeline_ns,
        },
        "artifacts": {
            "external_source_parquet_bytes": external_parquet_bytes,
            "table_owned_segment_bytes": segment_file_bytes,
            "total_retained_ingestion_bytes": retained_ingestion_bytes,
        },
        "committed_version": committed_version,
        "validation": validation,
    }))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = run_benchmark(&Args::parse()).await?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn path_first_and_streaming_commit_identical_logical_results() {
        let temp = tempfile::tempdir().expect("create temp directory");
        let path_first = Args {
            mode: Mode::PathFirst,
            table: temp.path().join("path-table"),
            external_parquet: Some(temp.path().join("source.parquet")),
            row_count: NonZeroU64::new(10).unwrap(),
            batch_rows: NonZeroUsize::new(3).unwrap(),
            payload_bytes: NonZeroUsize::new(7).unwrap(),
            seed: 42,
        };
        let streaming = Args {
            mode: Mode::Streaming,
            table: temp.path().join("stream-table"),
            external_parquet: None,
            row_count: path_first.row_count,
            batch_rows: path_first.batch_rows,
            payload_bytes: path_first.payload_bytes,
            seed: path_first.seed,
        };

        let path_report = run_benchmark(&path_first).await.expect("path-first report");
        let stream_report = run_benchmark(&streaming).await.expect("streaming report");

        assert_eq!(path_report["committed_version"], 2);
        assert_eq!(stream_report["committed_version"], 2);
        assert_eq!(path_report["workload"], stream_report["workload"]);
        assert_eq!(
            path_report["table_definition"],
            stream_report["table_definition"]
        );
        assert_eq!(
            path_report["writer_properties"],
            stream_report["writer_properties"]
        );
        assert_eq!(
            path_report["validation"]["column_checksums"],
            stream_report["validation"]["column_checksums"]
        );
        assert_eq!(
            path_report["validation"]["row_count"],
            stream_report["validation"]["row_count"]
        );
        assert_eq!(
            path_report["validation"]["segment_row_group_count"],
            stream_report["validation"]["segment_row_group_count"]
        );
        assert!(path_report["artifacts"]["external_source_parquet_bytes"].is_u64());
        assert!(stream_report["artifacts"]["external_source_parquet_bytes"].is_null());
        assert!(
            path_report["artifacts"]["total_retained_ingestion_bytes"]
                .as_u64()
                .unwrap()
                > path_report["artifacts"]["table_owned_segment_bytes"]
                    .as_u64()
                    .unwrap()
        );
    }
}
