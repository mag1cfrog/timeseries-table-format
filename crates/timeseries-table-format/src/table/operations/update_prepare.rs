//! Snapshot-bound preparation for keyed updates. No data or log publication.
//!
//! Source rows and projected target keys are externally sorted by an injective
//! typed encoding, joined, then sorted by segment/physical row. Encoding order
//! is private and has no relationship to index coverage or chronological order.
//! Two-way merges bound fan-in even for arbitrarily many runs or a hot key.
//! Values use Arrow's existing reversible row codec with one snapshot schema.
//! They are checksummed and written once; sort runs carry only keys and offsets.
//!
//! Sort buffers are byte-budgeted. In addition, processing holds a caller batch,
//! one aligned/serialized row, at most three merge records, fixed IO buffers,
//! and a segment descriptor vector. Parquet's synchronous reader streams pages
//! rather than fetching entire projected row groups. Footer input is checked
//! against a fixed 64 MiB limit before metadata parsing. Decoder pages/dictionaries
//! and footer metadata are library allocations, reported separately from the
//! sort budget; the budget is not a process RSS limit. Projected row groups have
//! a separate fixed decoded-size guard before any key pages are read, so huge
//! dictionaries cannot silently turn discovery into an unbounded fallback.
//! A largest encoded value is a separate allocation and is written immediately.
//! An oversized key record is spilled alone. No all-row
//! map or run descriptor list exists.
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
    row::{RowConverter, SortField},
};
use parquet::{
    arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder},
    errors::ParquetError,
    file::metadata::FooterTail,
    file::reader::{ChunkReader, Length},
};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::{
    batch_schema::{BatchSchemaAlignment, MissingColumnPolicy},
    formats::parquet::measured_file::MeasuredParquetFile,
    metadata::{
        index::IndexSpec,
        logical_schema::{LogicalSchema, LogicalTimestampUnit},
        schema_compat::SchemaCompatibilityError,
        segments::SegmentMeta,
        table::TableKind,
    },
    storage::{StorageLocation, TableLocation, ensure_canonical_relative_storage_path},
    transaction_log::TableState,
};
#[cfg(test)]
pub(crate) use spool::PreparationMetrics;
use spool::{
    Record, RunReader, Scratch, Sorter, ValueFile, ValueLocation, ValueReader, ValueWriter,
    io_error,
};

const DEFAULT_SORT_BYTES: usize = 8 * 1024 * 1024;
// The synchronous decoder can retain dictionaries/pages. Bound their declared
// total uncompressed input independently of row count, with room above append's
// ordinary 128 MiB row-group target. Oversized physical layouts fail explicitly.
const MAX_PROJECTED_ROW_GROUP_BYTES: u64 = 256 * 1024 * 1024;
const MAX_PARQUET_FOOTER_BYTES: usize = 64 * 1024 * 1024;
type Result<T> = std::result::Result<T, PrepareError>;

/// Complete typed components; nulls survive diagnostics, never successful keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KeyValue {
    /// UTF-8 identity component.
    Utf8(String),
    /// Signed 32-bit identity component.
    Int32(i32),
    /// Signed 64-bit identity or ordered-index component.
    Int64(i64),
    /// Unsigned 64-bit identity or ordered-index component.
    UInt64(u64),
    /// Exact timestamp, without coverage-bucket rounding.
    Timestamp {
        /// Raw ticks in the declared unit.
        ticks: i64,
        /// Timestamp precision.
        unit: LogicalTimestampUnit,
        /// Canonical time zone, if present.
        timezone: Option<String>,
    },
}

/// Named entity components in configured order, followed by the raw ordered index.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateKey(pub(crate) Vec<(String, Option<KeyValue>)>);

impl UpdateKey {
    /// Complete diagnostic key; `None` identifies a null component.
    pub fn components(&self) -> &[(String, Option<KeyValue>)] {
        &self.0
    }
}

/// Why an update key cannot be assigned to exactly one stored row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyViolation {
    /// An entity or ordered-index component is null.
    NullIdentity,
    /// Multiple source rows assign the same complete key.
    DuplicateSource,
    /// No stored row has the complete source key.
    UnmatchedSource,
    /// Multiple stored rows have the complete source key.
    AmbiguousTarget,
}

/// Typed input, exact-key matching, and scratch failures from update preparation.
#[derive(Debug, Snafu)]
#[snafu(module)]
#[non_exhaustive]
pub enum PrepareError {
    /// Invalid column selection, schema, or value.
    #[snafu(display("Invalid update input: {reason}"))]
    InvalidInput {
        /// Validation detail.
        reason: String,
    },
    /// Canonical schema conversion or alignment failed.
    #[snafu(display("Update schema: {source}"))]
    Schema {
        /// Original schema failure.
        #[snafu(backtrace)]
        source: Box<SchemaCompatibilityError>,
    },
    /// Arrow validation or conversion failed.
    #[snafu(display("Update batch: {source}"))]
    Arrow {
        /// Original Arrow failure.
        source: ArrowError,
    },
    /// The caller's reader failed, possibly after earlier valid batches.
    #[snafu(display("Update reader: {source}"))]
    Reader {
        /// Original reader failure.
        source: ArrowError,
    },
    /// A complete key violated the one-source-row to one-target-row contract.
    #[snafu(display("Update key violation {kind:?} after {input_rows_seen} source rows"))]
    Key {
        /// Specific key violation.
        kind: KeyViolation,
        /// Source rows observed before this diagnostic was produced.
        input_rows_seen: u64,
        /// Violations observed, not an exhaustive count of unread input.
        observed_violations: u64,
        /// Complete typed example, including any nulls.
        example_key: UpdateKey,
    },
    /// A staging counter or allocation limit was exceeded.
    #[snafu(display("Update staging resource limit: {reason}"))]
    Resource {
        /// Limit that was exceeded.
        reason: &'static str,
    },
    /// Projected target keys exceed the decoder's declared-size limit.
    #[snafu(display(
        "Projected keys in {path}, row group {row_group}, require {uncompressed_bytes} uncompressed bytes; limit {limit}"
    ))]
    TargetResource {
        /// Source segment path.
        path: String,
        /// Zero-based Parquet row group.
        row_group: usize,
        /// Declared uncompressed key-column bytes.
        uncompressed_bytes: u64,
        /// Maximum accepted bytes.
        limit: u64,
    },
    /// Target footer exceeds the limit checked before decoding metadata.
    #[snafu(display("Parquet footer at {path} declares {metadata_bytes} bytes; limit {limit}"))]
    TargetFooterResource {
        /// Source segment path.
        path: String,
        /// Declared footer bytes.
        metadata_bytes: usize,
        /// Maximum accepted bytes.
        limit: usize,
    },
    /// Local input or scratch IO failed.
    #[snafu(display("Update IO at {}: {source}", path.display()))]
    Io {
        /// Affected local path.
        path: PathBuf,
        /// Original IO failure.
        source: std::io::Error,
    },
    /// Target Parquet decoding failed.
    #[snafu(display("Update target {path}: {source}"))]
    Parquet {
        /// Source segment path.
        path: String,
        /// Original Parquet failure.
        source: ParquetError,
    },
    /// Private scratch key encoding failed.
    #[snafu(display("Private update key encoding: {source}"))]
    Encoding {
        /// Original encoding failure.
        source: serde_json::Error,
    },
    /// Explicit scratch cleanup failed; Drop also attempts best-effort cleanup.
    #[snafu(display("Update scratch cleanup at {}: {failures} failures; {source}", path.display()))]
    Cleanup {
        /// First path that failed cleanup.
        path: PathBuf,
        /// Number of cleanup failures observed.
        failures: u64,
        /// First cleanup IO failure.
        source: std::io::Error,
    },
    /// Preparation and its cleanup both failed.
    #[snafu(display("{source}; cleanup also failed: {cleanup}"))]
    CleanupAfterFailure {
        /// Original preparation failure.
        #[snafu(backtrace)]
        source: Box<PrepareError>,
        /// Cleanup failure.
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

/// Consumers must drain through `Ok(None)` before treating preparation as fully
/// consumed: this validates the completion footer and expected row count.
/// `close` abandons the cursor and cleans scratch without validating unread rows.
pub(crate) struct PreparedUpdates {
    // Reader must close before Scratch drops (also on Windows).
    reader: Option<RunReader>,
    values: Option<ValueReader>,
    value_codec: RowConverter,
    remaining_rows: u64,
    scratch: Scratch,
    pub(crate) version: u64,
    pub(crate) segments: Vec<SegmentMeta>,
    /// Canonical table positions corresponding to the destination suffix in values.
    pub(crate) destination_indices: Vec<usize>,
    pub(crate) schema: SchemaRef,
    pub(crate) matched_rows: u64,
}

impl PreparedUpdates {
    #[cfg(test)]
    pub(crate) fn metrics(&self) -> &PreparationMetrics {
        &self.scratch.metrics
    }

    pub(crate) fn next(&mut self) -> Result<Option<MatchedUpdate>> {
        let result = self.read_next();
        if !matches!(result, Ok(Some(_))) {
            self.reader = None;
            self.values = None;
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
            if self.remaining_rows != 0 {
                return Err(invalid(
                    "prepared cursor ended before its matched row count",
                ));
            }
            if let Some(values) = &mut self.values {
                values.validate_completion()?;
            }
            return Ok(None);
        };
        if self.remaining_rows == 0 {
            return Err(invalid("prepared cursor exceeds its matched row count"));
        }
        let segment_index =
            usize::try_from(record.segment).map_err(|_| invalid("invalid staged segment index"))?;
        let Some(segment) = self.segments.get(segment_index) else {
            return Err(invalid("invalid staged segment binding"));
        };
        if record.row >= segment.row_count {
            return Err(invalid("staged row outside segment"));
        }
        let bytes = self
            .values
            .as_mut()
            .ok_or_else(|| invalid("missing prepared value reader"))?
            .read(record.value)?;
        // Only checksum-verified bytes produced by this schema's codec reach the
        // Arrow row parser; it is not a decoder for arbitrary external input.
        let parser = self.value_codec.parser();
        let arrays = self
            .value_codec
            .convert_rows([parser.parse(&bytes)])
            .map_err(arrow_error)?;
        let values = RecordBatch::try_new(self.schema.clone(), arrays).map_err(arrow_error)?;
        let key = key_at(
            &values,
            self.schema.fields().len() - self.destination_indices.len(),
            0,
        )?;
        if encode(&key)? != record.key {
            return Err(invalid("prepared value key differs from its matched key"));
        }
        self.remaining_rows -= 1;
        Ok(Some(MatchedUpdate {
            segment_index,
            row: record.row,
            key,
            values,
        }))
    }

    /// Explicit abandonment reports cleanup errors; Drop is a best-effort fallback.
    pub(crate) fn close(mut self) -> Result<()> {
        self.reader = None;
        self.values = None;
        self.scratch.cleanup()
    }
}

struct SourceSchema {
    alignment: BatchSchemaAlignment,
    value_codec: RowConverter,
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
            value_codec: RowConverter::new(
                alignment
                    .output_schema()
                    .fields()
                    .iter()
                    .map(|field| SortField::new(field.data_type().clone()))
                    .collect(),
            )
            .map_err(arrow_error)?,
            alignment,
            key_count,
            destination_indices: positions[key_count..].to_vec(),
        })
    }
}

/// Read the raw value, including timestamp ticks. Never round through coverage.
pub(super) fn key_at(batch: &RecordBatch, key_count: usize, row: usize) -> Result<UpdateKey> {
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
    source: impl RecordBatchReader,
    columns: &[String],
) -> Result<PreparedUpdates> {
    prepare_with_budget(location, state, source, columns, DEFAULT_SORT_BYTES).await
}

async fn prepare_with_budget(
    location: &TableLocation,
    state: &TableState,
    source: impl RecordBatchReader,
    columns: &[String],
    budget: usize,
) -> Result<PreparedUpdates> {
    if budget == 0 {
        return Err(invalid("sort budget must be positive"));
    }
    let TableKind::TimeSeries(index) = &state.table_meta.kind else {
        return Err(invalid("keyed updates require a time-series snapshot"));
    };
    let schema = SourceSchema::new(state, index, source.schema(), columns)?;
    let StorageLocation::Local(root) = location.storage();
    let mut scratch = Scratch::create(root)?;
    let mut segments: Vec<_> = state.segments.values().cloned().collect();
    segments.sort_unstable_by(|a, b| a.path.cmp(&b.path));
    let staged = stage(&mut scratch, root, &segments, &schema, source, budget).await;
    match staged {
        Ok((id, matched_rows, value_file)) => {
            let reader = (|| -> Result<_> {
                Ok((
                    RunReader::open(&scratch, id)?,
                    ValueReader::open(&scratch, value_file)?,
                ))
            })();
            match reader {
                Ok((reader, values)) => Ok(PreparedUpdates {
                    reader: Some(reader),
                    values: Some(values),
                    value_codec: schema.value_codec,
                    remaining_rows: matched_rows,
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

async fn stage(
    scratch: &mut Scratch,
    root: &std::path::Path,
    segments: &[SegmentMeta],
    schema: &SourceSchema,
    source: impl RecordBatchReader,
    budget: usize,
) -> Result<(u64, u64, ValueFile)> {
    let mut values = ValueWriter::new(scratch)?;
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
            let encoded = schema
                .value_codec
                .convert_columns(batch.columns())
                .map_err(arrow_error)?;
            let value = values.push(scratch, encoded.row(0).as_ref())?;
            sorter.push(
                scratch,
                Record {
                    order: key.clone(),
                    key,
                    value,
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
    let value_file = values.finish(scratch)?;
    if input_rows_seen == 0 {
        return Ok((source_id, 0, value_file));
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
        if inspected != input_rows_seen {
            return Err(invalid(
                "staged source count differs from inspected source rows",
            ));
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
        let file = MeasuredParquetFile {
            file: File::open(&path).map_err(|e| io_error(&path, e))?,
            bytes: bytes_read.clone(),
        };
        let parquet_error = |source| PrepareError::Parquet {
            path: segment.path.clone(),
            source,
        };
        // Row-group limits run after footer parsing. Bound footer input first:
        // a file with many tiny row groups must not bypass all resource checks.
        let tail = file
            .get_bytes(file.len().saturating_sub(8), 8)
            .map_err(parquet_error)?;
        let metadata_bytes = FooterTail::try_from(tail.as_ref())
            .map_err(parquet_error)?
            .metadata_length();
        if metadata_bytes > MAX_PARQUET_FOOTER_BYTES {
            return Err(PrepareError::TargetFooterResource {
                path: segment.path.clone(),
                metadata_bytes,
                limit: MAX_PARQUET_FOOTER_BYTES,
            });
        }
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
                    // Parquet's byte_range() asserts these invariants. Validate
                    // persisted values before decoder construction can reach it.
                    let invalid_range = || {
                        invalid(format!(
                            "negative Parquet key-column byte range at {:?}, row group {row_group}, column {column}",
                            segment.path
                        ))
                    };
                    let compressed_bytes =
                        u64::try_from(metadata.compressed_size()).map_err(|_| invalid_range())?;
                    if metadata.data_page_offset() < 0
                        || metadata
                            .dictionary_page_offset()
                            .is_some_and(|offset| offset < 0)
                    {
                        return Err(invalid_range());
                    }
                    scratch.metrics.projected_column_bytes = scratch
                        .metrics
                        .projected_column_bytes
                        .checked_add(compressed_bytes)
                        .ok_or(PrepareError::Resource {
                            reason: "projected compressed byte counter overflow",
                        })?;
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
                        value: ValueLocation::default(),
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
    Ok((matched.finish(scratch).await?, input_rows_seen, value_file))
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
