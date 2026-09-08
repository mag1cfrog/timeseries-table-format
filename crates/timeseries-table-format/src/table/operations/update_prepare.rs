//! Snapshot-bound preparation for keyed updates. No data or log publication.
//!
//! Source rows and projected target keys are externally sorted by an injective
//! typed encoding, joined, then sorted by segment/physical row. Encoding order
//! is private and has no relationship to index coverage or chronological order.
//! Two-way merges bound fan-in even for arbitrarily many runs or a hot key.
//!
//! Sort buffers are byte-budgeted. In addition, processing holds a caller batch,
//! one aligned/serialized row, at most three merge records, fixed IO buffers,
//! and a segment descriptor vector. Parquet's synchronous reader streams pages
//! rather than fetching entire projected row groups. Decoder pages/dictionaries
//! and footer metadata are library allocations, reported separately from the
//! sort budget; the budget is not a process RSS limit. Projected row groups have
//! a separate fixed decoded-size guard before any key pages are read, so huge
//! dictionaries cannot silently turn discovery into an unbounded fallback.
//! A largest source row may exceed
//! the budget and is spilled alone. No all-row map or run descriptor list exists.
//!
//! Scratch uses exclusive UUID directories under the reserved staging root.
//! Explicit completion/error cleanup reports failures; Drop retries best effort.
//! Vacuum recognizes leftover run files and applies its usual retention cutoff.
//! As with other staged artifacts, that cutoff must predate active operations.

mod spool;
#[cfg(test)]
mod tests;

use std::{
    collections::HashSet,
    fs::File,
    io::{self, BufReader, Cursor, Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow::{
    array::{Array, ArrayRef, AsArray, RecordBatch, RecordBatchReader},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use parquet::{
    arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder},
    errors::ParquetError,
    file::reader::{ChunkReader, Length},
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::{
    batch_schema::{BatchSchemaAlignment, MissingColumnPolicy},
    metadata::{
        index::IndexSpec,
        logical_schema::{LogicalSchema, LogicalTimestampUnit},
        schema_compat::SchemaCompatibilityError,
        segments::SegmentMeta,
    },
    storage::{StorageLocation, TableLocation, ensure_canonical_relative_storage_path},
    transaction_log::TableState,
};
pub(crate) use spool::PreparationMetrics;
use spool::{Record, RunReader, Scratch, Sorter, io_error};

const DEFAULT_SORT_BYTES: usize = 8 * 1024 * 1024;
// The synchronous decoder can retain dictionaries/pages. Bound their declared
// total uncompressed input independently of row count, with room above append's
// ordinary 128 MiB row-group target. Oversized physical layouts fail explicitly.
const MAX_PROJECTED_ROW_GROUP_BYTES: u64 = 256 * 1024 * 1024;
type Result<T> = std::result::Result<T, PrepareError>;

/// Complete typed components; nulls survive diagnostics, never successful keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum KeyValue {
    Utf8(String),
    Int32(i32),
    Int64(i64),
    UInt64(u64),
    Timestamp {
        ticks: i64,
        unit: LogicalTimestampUnit,
        timezone: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct UpdateKey(pub(crate) Vec<(String, Option<KeyValue>)>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyViolation {
    NullIdentity,
    DuplicateSource,
    UnmatchedSource,
    AmbiguousTarget,
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub(crate) enum PrepareError {
    #[snafu(display("Invalid update input: {reason}"))]
    InvalidInput { reason: String },
    #[snafu(display("Update schema: {source}"))]
    Schema {
        source: Box<SchemaCompatibilityError>,
    },
    #[snafu(display("Update batch: {source}"))]
    Arrow { source: ArrowError },
    #[snafu(display("Update reader: {source}"))]
    Reader { source: ArrowError },
    #[snafu(display("Update key violation {kind:?} after {input_rows_seen} source rows"))]
    Key {
        kind: KeyViolation,
        input_rows_seen: u64,
        observed_violations: u64,
        example_key: UpdateKey,
    },
    #[snafu(display("Update staging resource limit: {reason}"))]
    Resource { reason: &'static str },
    #[snafu(display(
        "Projected keys in {path}, row group {row_group}, require {uncompressed_bytes} uncompressed bytes; limit {limit}"
    ))]
    TargetResource {
        path: String,
        row_group: usize,
        uncompressed_bytes: u64,
        limit: u64,
    },
    #[snafu(display("Update IO at {}: {source}", path.display()))]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[snafu(display("Update target {path}: {source}"))]
    Parquet { path: String, source: ParquetError },
    #[snafu(display("Private update key encoding: {source}"))]
    Encoding { source: serde_json::Error },
    #[snafu(display("Update scratch cleanup at {}: {failures} failures; {source}", path.display()))]
    Cleanup {
        path: PathBuf,
        failures: u64,
        source: std::io::Error,
    },
    #[snafu(display("{source}; cleanup also failed: {cleanup}"))]
    CleanupAfterFailure {
        source: Box<PrepareError>,
        cleanup: Box<PrepareError>,
    },
}

fn invalid(reason: impl Into<String>) -> PrepareError {
    PrepareError::InvalidInput {
        reason: reason.into(),
    }
}
fn arrow_error(source: ArrowError) -> PrepareError {
    PrepareError::Arrow { source }
}
fn encode(key: &UpdateKey) -> Result<Vec<u8>> {
    serde_json::to_vec(key).map_err(|source| PrepareError::Encoding { source })
}
fn decode(key: &[u8]) -> Result<UpdateKey> {
    serde_json::from_slice(key).map_err(|source| PrepareError::Encoding { source })
}
fn violation(kind: KeyViolation, input_rows_seen: u64, example_key: UpdateKey) -> PrepareError {
    PrepareError::Key {
        kind,
        input_rows_seen,
        observed_violations: 1,
        example_key,
    }
}

/// Values contain keys in configured order (index last), then destinations in
/// caller-selected order. Each batch has one row, exact canonical types and
/// nullability. Addresses are ordered by segment path, then physical row number.
/// The exact key must be rechecked against that source row before replacement.
pub(crate) struct MatchedUpdate {
    pub(crate) segment_index: usize,
    pub(crate) row: u64,
    pub(crate) key: UpdateKey,
    pub(crate) values: RecordBatch,
}

pub(crate) struct PreparedUpdates {
    // Reader must close before Scratch drops (also on Windows).
    reader: Option<RunReader>,
    scratch: Scratch,
    pub(crate) version: u64,
    pub(crate) segments: Vec<SegmentMeta>,
    /// Canonical table positions corresponding to the destination suffix in values.
    pub(crate) destination_indices: Vec<usize>,
    pub(crate) schema: SchemaRef,
    pub(crate) matched_rows: u64,
}

impl PreparedUpdates {
    pub(crate) fn metrics(&self) -> &PreparationMetrics {
        &self.scratch.metrics
    }

    pub(crate) fn next(&mut self) -> Result<Option<MatchedUpdate>> {
        let result = self.read_next();
        if !matches!(result, Ok(Some(_))) {
            self.reader = None;
            return match (result, self.scratch.cleanup()) {
                (Err(source), Err(cleanup)) => Err(PrepareError::CleanupAfterFailure {
                    source: Box::new(source),
                    cleanup: Box::new(cleanup),
                }),
                (Ok(_), Err(cleanup)) => Err(cleanup),
                (result, Ok(())) => result,
            };
        }
        result
    }

    fn read_next(&mut self) -> Result<Option<MatchedUpdate>> {
        let Some(reader) = &mut self.reader else {
            return Ok(None);
        };
        let Some(record) = reader.next()? else {
            return Ok(None);
        };
        let segment_index =
            usize::try_from(record.segment).map_err(|_| invalid("invalid staged segment index"))?;
        let Some(segment) = self.segments.get(segment_index) else {
            return Err(invalid("invalid staged segment binding"));
        };
        if record.row >= segment.row_count {
            return Err(invalid("staged row outside segment"));
        }
        let mut ipc =
            StreamReader::try_new(Cursor::new(record.values), None).map_err(arrow_error)?;
        let values = ipc
            .next()
            .ok_or_else(|| invalid("missing staged row"))?
            .map_err(arrow_error)?;
        if values.num_rows() != 1 || values.schema() != self.schema || ipc.next().is_some() {
            return Err(invalid("invalid staged row schema or count"));
        }
        Ok(Some(MatchedUpdate {
            segment_index,
            row: record.row,
            key: decode(&record.key)?,
            values,
        }))
    }

    /// Explicit abandonment reports cleanup errors; Drop is a best-effort fallback.
    pub(crate) fn close(mut self) -> Result<()> {
        self.reader = None;
        self.scratch.cleanup()
    }
}

struct SourceSchema {
    alignment: BatchSchemaAlignment,
    key_count: usize,
    destination_indices: Vec<usize>,
}

impl SourceSchema {
    fn new(
        state: &TableState,
        index: &IndexSpec,
        incoming: SchemaRef,
        columns: &[String],
    ) -> Result<Self> {
        let logical = state
            .table_meta
            .logical_schema()
            .ok_or_else(|| invalid("table has no canonical schema"))?;
        if columns.is_empty() {
            return Err(invalid("destination columns must not be empty"));
        }
        let mut names = index.entity_columns.clone();
        names.push(index.column.clone());
        let key_count = names.len();
        let mut seen: HashSet<&str> = names.iter().map(String::as_str).collect();
        for name in columns {
            if !seen.insert(name) {
                return Err(invalid(format!(
                    "duplicate or identity destination column {name:?}"
                )));
            }
        }
        names.extend_from_slice(columns);
        let mut fields = Vec::with_capacity(names.len());
        let mut positions = Vec::with_capacity(names.len());
        for name in names {
            let position = logical
                .columns()
                .iter()
                .position(|field| field.name == name)
                .ok_or_else(|| invalid(format!("unknown column {name:?}")))?;
            positions.push(position);
            fields.push(logical.columns()[position].clone());
        }
        let projection =
            LogicalSchema::new(fields).map_err(|source| invalid(source.to_string()))?;
        let alignment = BatchSchemaAlignment::for_ingestion(
            incoming,
            &projection,
            index,
            MissingColumnPolicy::Reject,
        )
        .map_err(|source| PrepareError::Schema {
            source: Box::new(source),
        })?;
        Ok(Self {
            alignment,
            key_count,
            destination_indices: positions[key_count..].to_vec(),
        })
    }
}

/// Read the raw value, including timestamp ticks. Never round through coverage.
fn key_at(batch: &RecordBatch, key_count: usize, row: usize) -> Result<UpdateKey> {
    use arrow::datatypes::*;
    let mut components = Vec::with_capacity(key_count);
    for (field, array) in batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .take(key_count)
    {
        let value = if array.is_null(row) {
            None
        } else {
            Some(match field.data_type() {
                DataType::Utf8 => KeyValue::Utf8(array.as_string::<i32>().value(row).to_owned()),
                DataType::Int32 => KeyValue::Int32(array.as_primitive::<Int32Type>().value(row)),
                DataType::Int64 => KeyValue::Int64(array.as_primitive::<Int64Type>().value(row)),
                DataType::UInt64 => KeyValue::UInt64(array.as_primitive::<UInt64Type>().value(row)),
                DataType::Timestamp(unit, timezone) => {
                    let (ticks, unit) = match unit {
                        TimeUnit::Millisecond => (
                            array.as_primitive::<TimestampMillisecondType>().value(row),
                            LogicalTimestampUnit::Millis,
                        ),
                        TimeUnit::Microsecond => (
                            array.as_primitive::<TimestampMicrosecondType>().value(row),
                            LogicalTimestampUnit::Micros,
                        ),
                        TimeUnit::Nanosecond => (
                            array.as_primitive::<TimestampNanosecondType>().value(row),
                            LogicalTimestampUnit::Nanos,
                        ),
                        TimeUnit::Second => {
                            return Err(invalid(
                                "second timestamps are not a supported canonical logical type",
                            ));
                        }
                    };
                    KeyValue::Timestamp {
                        ticks,
                        unit,
                        timezone: timezone.as_ref().map(ToString::to_string),
                    }
                }
                other => {
                    return Err(invalid(format!(
                        "unsupported identity type {other:?} for {:?}",
                        field.name()
                    )));
                }
            })
        };
        components.push((field.name().clone(), value));
    }
    Ok(UpdateKey(components))
}

/// Check only visible nested values. Parent nulls hide all child constraints.
fn validate_value(field: &Field, array: &ArrayRef, row: usize, path: &str) -> Result<()> {
    if array.is_null(row) {
        return if field.is_nullable() {
            Ok(())
        } else {
            Err(invalid(format!(
                "null in non-nullable destination {path:?}"
            )))
        };
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            let value = array.as_struct();
            for (field, child) in fields.iter().zip(value.columns()) {
                validate_value(field, child, row, &format!("{path}.{}", field.name()))?;
            }
        }
        DataType::List(element) => {
            let values = array.as_list::<i32>().value(row);
            for i in 0..values.len() {
                validate_value(element, &values, i, path)?;
            }
        }
        DataType::Map(entries, _) => {
            let values: ArrayRef = Arc::new(array.as_map().value(row));
            for i in 0..values.len() {
                validate_value(entries, &values, i, path)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) async fn prepare_updates(
    location: &TableLocation,
    state: &TableState,
    index: &IndexSpec,
    source: impl RecordBatchReader,
    columns: &[String],
) -> Result<PreparedUpdates> {
    prepare_with_budget(location, state, index, source, columns, DEFAULT_SORT_BYTES).await
}

async fn prepare_with_budget(
    location: &TableLocation,
    state: &TableState,
    index: &IndexSpec,
    source: impl RecordBatchReader,
    columns: &[String],
    budget: usize,
) -> Result<PreparedUpdates> {
    if budget == 0 {
        return Err(invalid("sort budget must be positive"));
    }
    let schema = SourceSchema::new(state, index, source.schema(), columns)?;
    let StorageLocation::Local(root) = location.storage();
    let mut scratch = Scratch::create(root)?;
    let mut segments: Vec<_> = state.segments.values().cloned().collect();
    segments.sort_unstable_by(|a, b| a.path.cmp(&b.path));
    let staged = stage(&mut scratch, root, &segments, &schema, source, budget).await;
    match staged {
        Ok((id, matched_rows)) => {
            let reader = RunReader::open(&scratch, id);
            match reader {
                Ok(reader) => Ok(PreparedUpdates {
                    reader: Some(reader),
                    scratch,
                    version: state.version,
                    segments,
                    destination_indices: schema.destination_indices,
                    schema: schema.alignment.output_schema().clone(),
                    matched_rows,
                }),
                Err(source) => Err(cleanup_failure(&mut scratch, source)),
            }
        }
        Err(source) => Err(cleanup_failure(&mut scratch, source)),
    }
}

fn cleanup_failure(scratch: &mut Scratch, source: PrepareError) -> PrepareError {
    match scratch.cleanup() {
        Ok(()) => source,
        Err(cleanup) => PrepareError::CleanupAfterFailure {
            source: Box::new(source),
            cleanup: Box::new(cleanup),
        },
    }
}

/// Counts bytes requested from the filesystem, including metadata and read-ahead.
/// This uses the existing local backend; payload columns are never projected.
struct DiscoveryFile {
    file: File,
    bytes: Arc<AtomicU64>,
}
struct DiscoveryRead {
    file: File,
    bytes: Arc<AtomicU64>,
}

impl Read for DiscoveryRead {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let bytes = self.file.read(buffer)?;
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}
impl Length for DiscoveryFile {
    fn len(&self) -> u64 {
        self.file.len()
    }
}
impl ChunkReader for DiscoveryFile {
    type T = BufReader<DiscoveryRead>;
    fn get_read(&self, start: u64) -> std::result::Result<Self::T, ParquetError> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        Ok(BufReader::new(DiscoveryRead {
            file,
            bytes: self.bytes.clone(),
        }))
    }
    fn get_bytes(
        &self,
        start: u64,
        length: usize,
    ) -> std::result::Result<bytes::Bytes, ParquetError> {
        let bytes = self.file.get_bytes(start, length)?;
        self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}

async fn stage(
    scratch: &mut Scratch,
    root: &std::path::Path,
    segments: &[SegmentMeta],
    schema: &SourceSchema,
    source: impl RecordBatchReader,
    budget: usize,
) -> Result<(u64, u64)> {
    let mut sorter = Sorter::new(scratch, budget);
    let mut input_rows_seen = 0_u64;
    for incoming in source {
        let incoming = incoming.map_err(|source| PrepareError::Reader { source })?;
        // Validate drift even for empty batches, without casting an entire caller batch.
        schema
            .alignment
            .align_batch(&incoming.slice(0, 0))
            .map_err(arrow_error)?;
        for row in 0..incoming.num_rows() {
            input_rows_seen = input_rows_seen
                .checked_add(1)
                .ok_or(PrepareError::Resource {
                    reason: "source row counter overflow",
                })?;
            let batch = schema
                .alignment
                .align_batch(&incoming.slice(row, 1))
                .map_err(arrow_error)?;
            let key = key_at(&batch, schema.key_count, 0)?;
            if key.0.iter().any(|(_, value)| value.is_none()) {
                return Err(violation(KeyViolation::NullIdentity, input_rows_seen, key));
            }
            for (field, array) in batch
                .schema()
                .fields()
                .iter()
                .zip(batch.columns())
                .skip(schema.key_count)
            {
                validate_value(field, array, 0, field.name())?;
            }
            let key = encode(&key)?;
            let mut values = Vec::new();
            {
                // ponytail: per-row IPC repeats schema/framing; use batched
                // value blocks with offsets if measured scratch/CPU cost warrants it.
                let mut writer =
                    StreamWriter::try_new(&mut values, &batch.schema()).map_err(arrow_error)?;
                writer.write(&batch).map_err(arrow_error)?;
                writer.finish().map_err(arrow_error)?;
            }
            sorter.push(
                scratch,
                Record {
                    order: key.clone(),
                    key,
                    values,
                    segment: 0,
                    row: 0,
                },
            )?;
            if input_rows_seen.is_multiple_of(1024) {
                tokio::task::yield_now().await;
            }
        }
        tokio::task::yield_now().await;
    }
    let source_id = sorter.finish(scratch).await?;
    if input_rows_seen == 0 {
        return Ok((source_id, 0));
    }
    // Validate global source uniqueness before target discovery; only one prior key.
    {
        let mut reader = RunReader::open(scratch, source_id)?;
        let mut previous = None;
        let mut inspected = 0_u64;
        while let Some(record) = reader.next()? {
            if previous.as_ref() == Some(&record.key) {
                return Err(violation(
                    KeyViolation::DuplicateSource,
                    input_rows_seen,
                    decode(&record.key)?,
                ));
            }
            previous = Some(record.key);
            inspected += 1;
            if inspected.is_multiple_of(1024) {
                tokio::task::yield_now().await;
            }
        }
    }
    let mut targets = Sorter::new(scratch, budget);
    let key_fields = schema.alignment.output_schema().fields()[..schema.key_count].to_vec();
    let key_schema = Arc::new(Schema::new(key_fields));
    for (segment_index, segment) in segments.iter().enumerate() {
        ensure_canonical_relative_storage_path(&segment.path)
            .map_err(|source| invalid(format!("invalid source path: {source}")))?;
        let path = root.join(&segment.path);
        let bytes_read = Arc::new(AtomicU64::new(0));
        let file = DiscoveryFile {
            file: File::open(&path).map_err(|e| io_error(&path, e))?,
            bytes: bytes_read.clone(),
        };
        let parquet_error = |source| PrepareError::Parquet {
            path: segment.path.clone(),
            source,
        };
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(parquet_error)?;
        let mut projection = Vec::with_capacity(schema.key_count);
        for field in key_schema.fields() {
            let position = builder
                .schema()
                .index_of(field.name())
                .map_err(arrow_error)?;
            let physical = builder.schema().field(position);
            if physical.data_type() != field.data_type()
                || physical.is_nullable() != field.is_nullable()
            {
                return Err(invalid(format!(
                    "target key schema mismatch at {:?}: {:?}",
                    segment.path,
                    field.name()
                )));
            }
            projection.push(position);
        }
        let mask = ProjectionMask::roots(builder.parquet_schema(), projection.clone());
        for (row_group, group) in builder.metadata().row_groups().iter().enumerate() {
            let mut uncompressed_bytes = 0_u64;
            for (column, metadata) in group.columns().iter().enumerate() {
                if mask.leaf_included(column) {
                    scratch.metrics.projected_column_bytes += metadata.compressed_size() as u64;
                    uncompressed_bytes = uncompressed_bytes
                        .checked_add(
                            u64::try_from(metadata.uncompressed_size())
                                .map_err(|_| invalid("negative Parquet column size"))?,
                        )
                        .ok_or(PrepareError::Resource {
                            reason: "projected column size overflow",
                        })?;
                }
            }
            check_target_budget(
                &segment.path,
                row_group,
                uncompressed_bytes,
                MAX_PROJECTED_ROW_GROUP_BYTES,
            )?;
        }
        let reader = builder
            .with_projection(mask)
            .with_batch_size(1)
            .build()
            .map_err(parquet_error)?;
        let mut row = 0_u64;
        for batch in reader {
            let batch = batch.map_err(arrow_error)?;
            let arrays = key_schema
                .fields()
                .iter()
                .map(|field| {
                    batch
                        .column_by_name(field.name())
                        .cloned()
                        .ok_or_else(|| invalid("missing projected key"))
                })
                .collect::<Result<Vec<_>>>()?;
            let batch = RecordBatch::try_new(key_schema.clone(), arrays).map_err(arrow_error)?;
            for offset in 0..batch.num_rows() {
                let key = encode(&key_at(&batch, schema.key_count, offset)?)?;
                targets.push(
                    scratch,
                    Record {
                        order: key.clone(),
                        key,
                        values: Vec::new(),
                        segment: segment_index as u64,
                        row,
                    },
                )?;
                row = row.checked_add(1).ok_or(PrepareError::Resource {
                    reason: "target row counter overflow",
                })?;
                scratch.metrics.target_rows_read += 1;
                if row.is_multiple_of(1024) {
                    tokio::task::yield_now().await;
                }
            }
        }
        if row != segment.row_count {
            return Err(invalid(format!(
                "target row count mismatch at {:?}",
                segment.path
            )));
        }
        scratch.metrics.key_discovery_bytes_read += bytes_read.load(Ordering::Relaxed);
        tokio::task::yield_now().await;
    }
    let target_id = targets.finish(scratch).await?;
    let mut source = RunReader::open(scratch, source_id)?;
    let mut targets = RunReader::open(scratch, target_id)?;
    let mut target = targets.next()?;
    let mut matched = Sorter::new(scratch, budget);
    while let Some(mut update) = source.next()? {
        let mut skipped = 0_u64;
        while target
            .as_ref()
            .is_some_and(|target| target.key < update.key)
        {
            target = targets.next()?;
            skipped += 1;
            if skipped.is_multiple_of(1024) {
                tokio::task::yield_now().await;
            }
        }
        let Some(found) = target.take().filter(|target| target.key == update.key) else {
            return Err(violation(
                KeyViolation::UnmatchedSource,
                input_rows_seen,
                decode(&update.key)?,
            ));
        };
        update.segment = found.segment;
        update.row = found.row;
        target = targets.next()?;
        if target
            .as_ref()
            .is_some_and(|target| target.key == update.key)
        {
            return Err(violation(
                KeyViolation::AmbiguousTarget,
                input_rows_seen,
                decode(&update.key)?,
            ));
        }
        update.order = [update.segment.to_be_bytes(), update.row.to_be_bytes()].concat();
        matched.push(scratch, update)?;
        tokio::task::yield_now().await;
    }
    drop((source, targets));
    scratch.remove(source_id)?;
    scratch.remove(target_id)?;
    Ok((matched.finish(scratch).await?, input_rows_seen))
}

fn check_target_budget(
    path: &str,
    row_group: usize,
    uncompressed_bytes: u64,
    limit: u64,
) -> Result<()> {
    if uncompressed_bytes > limit {
        Err(PrepareError::TargetResource {
            path: path.into(),
            row_group,
            uncompressed_bytes,
            limit,
        })
    } else {
        Ok(())
    }
}
