//! Compare external normalization with direct append widening.

use std::fs::File;
use std::io;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, RecordBatch, RecordBatchReader, UInt32Builder,
    UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use serde::{Deserialize, Serialize};
use timeseries_table_format::{
    metadata::logical_schema::{
        LogicalDataType, LogicalField, LogicalSchema, LogicalSchemaValidationError,
    },
    metadata::table_metadata::{IndexKind, IndexSpec, IndexValue, TableMeta},
    storage::TableLocation,
    table::{TimeSeriesTable, append::AppendRequest},
};

const INDEX_COLUMN: &str = "ordered_index";
const SEQUENCE_COLUMN: &str = "sequence";
const PAYLOAD_COLUMN: &str = "payload";

#[path = "append_widening_bench/runner.rs"]
mod runner;

#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the isolated comparison and aggregate its measurements.
    Compare(runner::CompareArgs),
    /// Run exactly one append mode and emit its report.
    Run(RunArgs),
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum Mode {
    ExternalNormalization,
    DirectWidening,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::ExternalNormalization => "external-normalization",
            Self::DirectWidening => "direct-widening",
        }
    }
}

#[derive(Debug, ClapArgs)]
struct RunArgs {
    #[arg(long, value_enum)]
    mode: Mode,

    #[arg(long)]
    table: PathBuf,

    /// Required for external-normalization and rejected for direct-widening.
    #[arg(long)]
    external_normalized_parquet: Option<PathBuf>,

    #[arg(long)]
    row_count: NonZeroU64,

    #[arg(long)]
    batch_rows: NonZeroUsize,

    #[arg(long)]
    row_group_rows: NonZeroUsize,

    #[arg(long)]
    payload_bytes: NonZeroUsize,

    #[arg(long)]
    seed: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct Workload {
    row_count: u64,
    batch_rows: usize,
    row_group_rows: usize,
    #[serde(rename = "payload_bytes_per_row")]
    payload_bytes: usize,
    seed: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(untagged)]
enum RequiredNullable<T> {
    Value(T),
    Null,
}

impl<T> RequiredNullable<T> {
    fn from_option(value: Option<T>) -> Self {
        match value {
            Some(value) => Self::Value(value),
            None => Self::Null,
        }
    }

    fn as_ref(&self) -> Option<&T> {
        match self {
            Self::Value(value) => Some(value),
            Self::Null => None,
        }
    }
}

impl<T: Copy> RequiredNullable<T> {
    fn copied(self) -> Option<T> {
        match self {
            Self::Value(value) => Some(value),
            Self::Null => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct TableDefinitionReport {
    index_column: String,
    incoming_index_type: String,
    registered_index_type: String,
    index_granularity: u64,
    entity_columns: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct WriterPropertiesReport {
    compression: String,
    data_page_size_bytes: usize,
    dictionary_enabled: bool,
    max_row_group_rows: usize,
    statistics: String,
    write_batch_rows: usize,
    writer_version: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct TimingReport {
    external_normalization_wall_time_ns: RequiredNullable<u64>,
    append_and_commit_wall_time_ns: u64,
    end_to_end_pipeline_wall_time_ns: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct ArtifactReport {
    external_normalized_parquet_bytes: RequiredNullable<u64>,
    table_managed_committed_parquet_bytes: u64,
    total_retained_ingestion_parquet_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct ColumnChecksums {
    ordered_index: String,
    sequence: String,
    payload: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ValidationReport {
    column_checksums: ColumnChecksums,
    coverage_ratio: f64,
    index_max: u64,
    index_min: u64,
    boundary_index_values_round_trip_as_uint64: bool,
    committed_parquet_schema_matches_registered: bool,
    direct_pipeline_has_no_external_normalized_parquet: bool,
    external_normalized_parquet_schema_matches_registered: RequiredNullable<bool>,
    full_scan_matches_generated: bool,
    row_count: u64,
    schema_matches_registered: bool,
    segment_file_bytes: u64,
    segment_path: String,
    segment_row_group_count: usize,
    table_managed_parquet_file_count: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct BenchmarkReport {
    schema_version: u32,
    mode: Mode,
    process_id: u32,
    table_path: PathBuf,
    external_normalized_parquet_path: RequiredNullable<PathBuf>,
    workload: Workload,
    table_definition: TableDefinitionReport,
    writer_properties: WriterPropertiesReport,
    timing: TimingReport,
    artifacts: ArtifactReport,
    committed_version: u64,
    validation: ValidationReport,
}

struct DeterministicBatchReader {
    schema: SchemaRef,
    workload: Workload,
    next_row: u64,
}

impl DeterministicBatchReader {
    fn new(workload: Workload) -> Result<Self, ArrowError> {
        if !(2..=u64::from(u32::MAX) + 1).contains(&workload.row_count) {
            return Err(ArrowError::InvalidArgumentError(
                "row count must be between 2 and UInt32::MAX + 1".to_string(),
            ));
        }
        if !workload.row_group_rows.is_multiple_of(workload.batch_rows) {
            return Err(ArrowError::InvalidArgumentError(
                "row-group rows must be a multiple of batch rows".to_string(),
            ));
        }
        workload
            .batch_rows
            .checked_mul(workload.payload_bytes)
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError("batch payload size overflow".to_string())
            })?;

        Ok(Self {
            schema: incoming_schema(),
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

fn incoming_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(INDEX_COLUMN, DataType::UInt32, false),
        Field::new(SEQUENCE_COLUMN, DataType::UInt64, false),
        Field::new(PAYLOAD_COLUMN, DataType::Binary, false),
    ]))
}

fn registered_logical_schema() -> Result<LogicalSchema, LogicalSchemaValidationError> {
    LogicalSchema::new(vec![
        LogicalField {
            name: INDEX_COLUMN.to_string(),
            data_type: LogicalDataType::UInt64,
            nullable: false,
        },
        LogicalField {
            name: SEQUENCE_COLUMN.to_string(),
            data_type: LogicalDataType::UInt64,
            nullable: false,
        },
        LogicalField {
            name: PAYLOAD_COLUMN.to_string(),
            data_type: LogicalDataType::Binary,
            nullable: false,
        },
    ])
}

fn generated_index_value(row: u64, row_count: u64) -> Result<u32, ArrowError> {
    if row + 1 == row_count {
        Ok(u32::MAX)
    } else {
        u32::try_from(row).map_err(|_| {
            ArrowError::InvalidArgumentError("benchmark index exceeds UInt32".to_string())
        })
    }
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
    let payload_capacity = row_count
        .checked_mul(workload.payload_bytes)
        .ok_or_else(|| ArrowError::InvalidArgumentError("batch payload overflow".to_string()))?;

    let mut indexes = UInt32Builder::with_capacity(row_count);
    for row in first_row..end_row {
        indexes.append_value(generated_index_value(row, workload.row_count)?);
    }
    let sequences = UInt64Array::from_iter_values(first_row..end_row);
    let mut payloads = BinaryBuilder::with_capacity(row_count, payload_capacity);
    let mut payload = vec![0; workload.payload_bytes];
    for row in first_row..end_row {
        fill_payload(&mut payload, row, workload.seed);
        payloads.append_value(&payload);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(indexes.finish()),
        Arc::new(sequences),
        Arc::new(payloads.finish()),
    ];
    RecordBatch::try_new(incoming_schema(), columns)
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn elapsed_ns(elapsed: Duration) -> Result<u64, io::Error> {
    u64::try_from(elapsed.as_nanos()).map_err(|_| invalid_data("duration exceeds u64 nanoseconds"))
}

fn writer_properties(row_group_rows: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_rows))
        .build()
}

fn widen_index_for_external_baseline(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(
        Arc::clone(target_schema),
        vec![
            cast(batch.column(0), &DataType::UInt64)?,
            Arc::clone(batch.column(1)),
            Arc::clone(batch.column(2)),
        ],
    )
}

fn write_external_normalized_parquet(
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
    let target_schema = registered_logical_schema()?.to_arrow_schema_ref()?;
    let mut writer = ArrowWriter::try_new(
        File::create(path)?,
        Arc::clone(&target_schema),
        Some(writer_properties(workload.row_group_rows)),
    )?;
    for batch in reader {
        writer.write(&widen_index_for_external_baseline(&batch?, &target_schema)?)?;
    }
    writer.close()?;
    Ok(())
}

fn table_definition() -> Result<TableMeta, Box<dyn std::error::Error>> {
    let index_granularity = NonZeroU64::MIN;
    Ok(TableMeta::new_time_series_with_schema(
        IndexSpec {
            column: INDEX_COLUMN.to_string(),
            entity_columns: Vec::new(),
            kind: IndexKind::UInt64 { index_granularity },
        },
        registered_logical_schema()?,
    ))
}

fn benchmark_table_definition_report() -> TableDefinitionReport {
    TableDefinitionReport {
        index_column: INDEX_COLUMN.to_string(),
        incoming_index_type: "uint32".to_string(),
        registered_index_type: "uint64".to_string(),
        index_granularity: 1,
        entity_columns: Vec::new(),
    }
}

fn expected_coverage_ratio(workload: Workload) -> f64 {
    workload.row_count as f64 / (u64::from(u32::MAX) as f64 + 1.0)
}

fn writer_properties_report(row_group_rows: usize) -> Result<WriterPropertiesReport, io::Error> {
    let properties = writer_properties(row_group_rows);
    let payload = ColumnPath::from(PAYLOAD_COLUMN);
    Ok(WriterPropertiesReport {
        compression: format!("{:?}", properties.compression(&payload)),
        data_page_size_bytes: properties.data_page_size_limit(),
        dictionary_enabled: properties.dictionary_enabled(&payload),
        max_row_group_rows: properties
            .max_row_group_row_count()
            .ok_or_else(|| invalid_data("benchmark writer omitted its row-group limit"))?,
        statistics: format!("{:?}", properties.statistics_enabled(&payload)),
        write_batch_rows: properties.write_batch_size(),
        writer_version: format!("{:?}", properties.writer_version()),
    })
}

async fn validate_table(
    table: &TimeSeriesTable,
    table_root: &Path,
    workload: Workload,
) -> Result<ValidationReport, Box<dyn std::error::Error>> {
    if table.state().version != 2 {
        return Err(invalid_data(format!(
            "expected committed version 2, found {}",
            table.state().version
        ))
        .into());
    }
    let stored_schema = table.state().table_meta.arrow_schema_ref()?;
    let registered_schema = registered_logical_schema()?.to_arrow_schema_ref()?;
    if stored_schema != registered_schema {
        return Err(invalid_data(format!(
            "stored schema {stored_schema:?} does not match registered schema {registered_schema:?}"
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
    let expected_max = u64::from(u32::MAX);
    if segment.index_min != IndexValue::UInt64(0)
        || segment.index_max != IndexValue::UInt64(expected_max)
    {
        return Err(invalid_data(format!(
            "unexpected index bounds: {}..{}",
            segment.index_min, segment.index_max
        ))
        .into());
    }
    let segment_path = segment.path.clone();
    let segment_file_bytes = std::fs::metadata(table_root.join(&segment_path))?.len();
    let parquet_builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(table_root.join(&segment_path))?)?;
    if parquet_builder.schema() != &registered_schema {
        return Err(
            invalid_data("committed Parquet schema does not match registered schema").into(),
        );
    }
    let row_group_count = parquet_builder.metadata().num_row_groups();
    let expected_row_groups = workload.row_count.div_ceil(workload.row_group_rows as u64) as usize;
    if row_group_count != expected_row_groups {
        return Err(invalid_data(format!(
            "expected {expected_row_groups} row groups, found {row_group_count}"
        ))
        .into());
    }

    let scan_end = expected_max + 1;
    let coverage_ratio = table.coverage_ratio_for_range(0_u64, scan_end).await?;
    if coverage_ratio != expected_coverage_ratio(workload) {
        return Err(invalid_data(format!("unexpected coverage ratio {coverage_ratio}")).into());
    }

    let mut stream = table.scan_range(0_u64, scan_end).await?;
    let mut expected_row = 0_u64;
    let mut index_checksum = blake3::Hasher::new();
    let mut sequence_checksum = blake3::Hasher::new();
    let mut payload_checksum = blake3::Hasher::new();
    while let Some(batch) = stream.next().await.transpose()? {
        if batch.schema() != registered_schema {
            return Err(invalid_data("scan returned an unexpected schema").into());
        }
        let indexes = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid_data("scan index column is not UInt64"))?;
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
            let expected_index =
                u64::from(generated_index_value(expected_row, workload.row_count)?);
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

    Ok(ValidationReport {
        column_checksums: ColumnChecksums {
            ordered_index: index_checksum.finalize().to_hex().to_string(),
            sequence: sequence_checksum.finalize().to_hex().to_string(),
            payload: payload_checksum.finalize().to_hex().to_string(),
        },
        coverage_ratio,
        index_max: expected_max,
        index_min: 0,
        boundary_index_values_round_trip_as_uint64: true,
        committed_parquet_schema_matches_registered: true,
        direct_pipeline_has_no_external_normalized_parquet: false,
        external_normalized_parquet_schema_matches_registered: RequiredNullable::Null,
        full_scan_matches_generated: true,
        row_count: expected_row,
        schema_matches_registered: true,
        segment_file_bytes,
        segment_path,
        segment_row_group_count: row_group_count,
        table_managed_parquet_file_count: table.state().segments.len(),
    })
}

async fn run_append_mode(args: &RunArgs) -> Result<BenchmarkReport, Box<dyn std::error::Error>> {
    if args.table.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("table path already exists: {}", args.table.display()),
        )
        .into());
    }
    let external_normalized_parquet = match (args.mode, args.external_normalized_parquet.as_deref())
    {
        (Mode::ExternalNormalization, Some(path)) => Some(path),
        (Mode::ExternalNormalization, None) => {
            return Err(invalid_data(
                "external-normalization mode requires --external-normalized-parquet",
            )
            .into());
        }
        (Mode::DirectWidening, None) => None,
        (Mode::DirectWidening, Some(_)) => {
            return Err(
                invalid_data("direct-widening mode rejects --external-normalized-parquet").into(),
            );
        }
    };
    if external_normalized_parquet.is_some_and(|path| path.starts_with(&args.table)) {
        return Err(invalid_data("external Parquet path must be outside the table root").into());
    }

    let workload = Workload {
        row_count: args.row_count.get(),
        batch_rows: args.batch_rows.get(),
        row_group_rows: args.row_group_rows.get(),
        payload_bytes: args.payload_bytes.get(),
        seed: args.seed,
    };
    DeterministicBatchReader::new(workload)?;
    let location = TableLocation::local(&args.table);
    let mut table = TimeSeriesTable::create(location, table_definition()?).await?;

    eprintln!("Running {} append pipeline", args.mode.name());
    let pipeline_started = Instant::now();
    let (
        external_normalization_ns,
        append_and_commit_ns,
        external_normalized_schema_matches_registered,
        committed_version,
    ) = match args.mode {
        Mode::ExternalNormalization => {
            let path = external_normalized_parquet.ok_or_else(|| {
                invalid_data("external-normalization mode requires --external-normalized-parquet")
            })?;
            let normalization_started = Instant::now();
            write_external_normalized_parquet(path, workload)?;
            let normalization_ns = elapsed_ns(normalization_started.elapsed())?;
            let append_started = Instant::now();
            let external_builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
            if external_builder.schema() != &table.state().table_meta.arrow_schema_ref()? {
                return Err(invalid_data(
                    "external normalized Parquet schema does not match registered schema",
                )
                .into());
            }
            let external_reader = external_builder
                .with_batch_size(workload.batch_rows)
                .build()?;
            let version = table
                .append(
                    AppendRequest::new(external_reader)
                        .max_rows_per_row_group(workload.row_group_rows),
                )
                .await?;
            (
                Some(normalization_ns),
                elapsed_ns(append_started.elapsed())?,
                Some(true),
                version,
            )
        }
        Mode::DirectWidening => {
            let append_started = Instant::now();
            let version = table
                .append(
                    AppendRequest::new(DeterministicBatchReader::new(workload)?)
                        .max_rows_per_row_group(workload.row_group_rows),
                )
                .await?;
            (None, elapsed_ns(append_started.elapsed())?, None, version)
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
    let mut validation = validate_table(&table, &args.table, workload).await?;
    validation.direct_pipeline_has_no_external_normalized_parquet =
        external_normalized_parquet.is_none();
    validation.external_normalized_parquet_schema_matches_registered =
        RequiredNullable::from_option(external_normalized_schema_matches_registered);
    let segment_file_bytes = validation.segment_file_bytes;
    let external_normalized_parquet_bytes = external_normalized_parquet
        .map(std::fs::metadata)
        .transpose()?
        .map(|metadata| metadata.len());
    let retained_ingestion_bytes = segment_file_bytes
        .checked_add(external_normalized_parquet_bytes.unwrap_or(0))
        .ok_or_else(|| invalid_data("retained byte count overflow"))?;

    Ok(BenchmarkReport {
        schema_version: 1,
        mode: args.mode,
        process_id: std::process::id(),
        table_path: args.table.clone(),
        external_normalized_parquet_path: RequiredNullable::from_option(
            external_normalized_parquet.map(Path::to_path_buf),
        ),
        workload,
        table_definition: benchmark_table_definition_report(),
        writer_properties: writer_properties_report(workload.row_group_rows)?,
        timing: TimingReport {
            external_normalization_wall_time_ns: RequiredNullable::from_option(
                external_normalization_ns,
            ),
            append_and_commit_wall_time_ns: append_and_commit_ns,
            end_to_end_pipeline_wall_time_ns: pipeline_ns,
        },
        artifacts: ArtifactReport {
            external_normalized_parquet_bytes: RequiredNullable::from_option(
                external_normalized_parquet_bytes,
            ),
            table_managed_committed_parquet_bytes: segment_file_bytes,
            total_retained_ingestion_parquet_bytes: retained_ingestion_bytes,
        },
        committed_version,
        validation,
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = match Cli::parse().command {
        Command::Compare(args) => runner::run_comparison(&args)?,
        Command::Run(args) => serde_json::to_value(run_append_mode(&args).await?)?,
    };
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn external_normalization_and_direct_widening_commit_identical_results() {
        let temp = tempfile::tempdir().expect("create temp directory");
        let external = RunArgs {
            mode: Mode::ExternalNormalization,
            table: temp.path().join("external-table"),
            external_normalized_parquet: Some(temp.path().join("normalized.parquet")),
            row_count: NonZeroU64::new(10).unwrap(),
            batch_rows: NonZeroUsize::new(2).unwrap(),
            row_group_rows: NonZeroUsize::new(4).unwrap(),
            payload_bytes: NonZeroUsize::new(7).unwrap(),
            seed: 42,
        };
        let direct = RunArgs {
            mode: Mode::DirectWidening,
            table: temp.path().join("direct-table"),
            external_normalized_parquet: None,
            row_count: external.row_count,
            batch_rows: external.batch_rows,
            row_group_rows: external.row_group_rows,
            payload_bytes: external.payload_bytes,
            seed: external.seed,
        };

        let external_report = run_append_mode(&external)
            .await
            .expect("external-normalization report");
        let direct_report = run_append_mode(&direct)
            .await
            .expect("direct-widening report");

        runner::validate_benchmark_report(
            &external_report,
            external.mode,
            external_report.workload,
            &external.table,
            external.external_normalized_parquet.as_deref(),
        )
        .expect("validate external-normalization report contract");
        runner::validate_benchmark_report(
            &direct_report,
            direct.mode,
            direct_report.workload,
            &direct.table,
            None,
        )
        .expect("validate direct-widening report contract");
        runner::require_same_logical_result(&external_report, &direct_report)
            .expect("modes commit the same logical result");

        assert_eq!(external_report.committed_version, 2);
        assert_eq!(direct_report.committed_version, 2);
        assert_eq!(external_report.workload, direct_report.workload);
        assert_eq!(
            external_report.table_definition,
            direct_report.table_definition
        );
        assert_eq!(
            external_report.writer_properties,
            direct_report.writer_properties
        );
        assert_eq!(
            external_report.validation.column_checksums,
            direct_report.validation.column_checksums
        );
        assert_eq!(
            external_report.validation.row_count,
            direct_report.validation.row_count
        );
        assert_eq!(
            external_report.validation.segment_row_group_count,
            direct_report.validation.segment_row_group_count
        );
        assert_eq!(direct_report.validation.index_min, 0);
        assert_eq!(direct_report.validation.index_max, u64::from(u32::MAX));
        assert_eq!(direct_report.validation.segment_row_group_count, 3);
        assert_eq!(
            external_report
                .validation
                .external_normalized_parquet_schema_matches_registered,
            RequiredNullable::Value(true)
        );
        assert!(
            direct_report
                .validation
                .direct_pipeline_has_no_external_normalized_parquet
        );
        assert!(
            external_report
                .artifacts
                .external_normalized_parquet_bytes
                .as_ref()
                .is_some()
        );
        assert_eq!(
            direct_report.artifacts.external_normalized_parquet_bytes,
            RequiredNullable::Null
        );
        assert!(
            external_report
                .artifacts
                .total_retained_ingestion_parquet_bytes
                > external_report
                    .artifacts
                    .table_managed_committed_parquet_bytes
        );

        let mut mismatched_parameters = direct_report.clone();
        mismatched_parameters.workload.seed += 1;
        assert!(
            runner::require_same_logical_result(&external_report, &mismatched_parameters).is_err()
        );
        let mut mismatched_validation = direct_report;
        mismatched_validation.validation.column_checksums.payload = "0".repeat(64);
        assert!(
            runner::require_same_logical_result(&external_report, &mismatched_validation).is_err()
        );
    }

    #[test]
    fn rejects_workloads_that_cannot_preserve_batch_boundaries() {
        let workload = Workload {
            row_count: 10,
            batch_rows: 3,
            row_group_rows: 4,
            payload_bytes: 1,
            seed: 0,
        };

        assert!(DeterministicBatchReader::new(workload).is_err());
    }
}
