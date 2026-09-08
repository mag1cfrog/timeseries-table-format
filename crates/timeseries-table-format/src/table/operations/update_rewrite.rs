//! Cleanup-owned Parquet replacements for a fully prepared immutable snapshot.
//! One source scan and one projected output verification scan per affected file.
//! No log writes, handle mutation, entity splitting, or public update API.

#[cfg(test)]
mod tests;

use arrow::{array::RecordBatch, compute::concat_batches, datatypes::SchemaRef};
use parquet::{
    arrow::{
        ArrowWriter, ProjectionMask,
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    },
    file::{
        metadata::FooterTail,
        properties::WriterProperties,
        reader::{ChunkReader, Length},
    },
};
use snafu::Snafu;
use std::{
    fs::File,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use uuid::Uuid;

use super::update_prepare::{MatchedUpdate, PrepareError, PreparedUpdates, key_at};
use crate::{
    batch_schema::{BatchSchemaAlignment, MissingColumnPolicy},
    coverage::{
        Coverage,
        io::{read_coverage_sidecar, read_entity_coverage_sidecar},
        layout::{
            coverage_file_id_for_attempt, segment_coverage_id_v2, segment_coverage_key,
            segment_entity_coverage_id_v1,
        },
        serde::{coverage_to_bytes, entity_coverage_to_bytes},
    },
    formats::parquet::{
        coverage::compute_coverage_bitmap_from_stream,
        entity_coverage::compute_entity_coverage_from_stream, measured_file::MeasuredParquetFile,
        segment_meta::segment_meta_from_parquet,
    },
    metadata::{
        index::IndexSpec,
        segments::{FileFormat, SegmentEntityLayout, SegmentMeta},
        table::TableKind,
    },
    storage::{
        self, FileCleanupGuard, StorageError, StorageLocation, TableLocation,
        layout::UPDATE_REWRITE_DATA_DIR,
    },
    transaction_log::TableState,
};

type Result<T> = std::result::Result<T, RewriteError>;

/// Typed replacement validation, storage, and cleanup failures during row updates.
#[derive(Debug, Snafu)]
#[snafu(module)]
#[non_exhaustive]
pub enum RewriteError {
    /// A checked rewrite counter overflowed.
    #[snafu(display("Update rewrite counter overflow: {counter}"))]
    CountOverflow {
        /// Counter that cannot be represented as u64.
        counter: &'static str,
    },
    /// Prepared input or replacement validation failed.
    #[snafu(display("Invalid keyed rewrite: {reason}"))]
    Invalid {
        /// Validation detail.
        reason: String,
    },
    /// A declared decoder allocation exceeds its limit.
    #[snafu(display("Rewrite {allocation} at {path} declares {bytes} bytes; limit {limit}"))]
    Resource {
        /// Affected path.
        path: String,
        /// Allocation being limited.
        allocation: &'static str,
        /// Declared bytes.
        bytes: u64,
        /// Maximum accepted bytes.
        limit: u64,
    },
    /// Reading the prepared cursor failed.
    #[snafu(context(false), display("Prepared updates: {source}"))]
    Preparation {
        /// Original typed failure.
        source: PrepareError,
    },
    /// Local source IO failed.
    #[snafu(display("Rewrite IO at {}: {source}", path.display()))]
    Io {
        /// Affected path.
        path: std::path::PathBuf,
        /// Original typed failure.
        source: std::io::Error,
    },
    /// Replacement storage failed.
    #[snafu(context(false), display("Rewrite storage: {source}"))]
    Storage {
        /// Original typed failure.
        source: StorageError,
    },
    /// Arrow assembly failed.
    #[snafu(context(false), display("Rewrite Arrow: {source}"))]
    Arrow {
        /// Original typed failure.
        source: arrow::error::ArrowError,
    },
    /// Parquet decoding or writing failed.
    #[snafu(context(false), display("Rewrite Parquet: {source}"))]
    Parquet {
        /// Original typed failure.
        source: parquet::errors::ParquetError,
    },
    /// Historical schema alignment failed.
    #[snafu(context(false), display("Rewrite schema: {source}"))]
    Schema {
        /// Original typed failure.
        source: Box<crate::metadata::schema_compat::SchemaCompatibilityError>,
    },
    /// Replacement metadata inspection failed.
    #[snafu(context(false), display("Rewrite inspection: {source}"))]
    Inspection {
        /// Original typed failure.
        source: crate::transaction_log::segments::SegmentError,
    },
    /// Replacement coverage verification failed.
    #[snafu(context(false), display("Rewrite coverage: {source}"))]
    Coverage {
        /// Original typed failure.
        source: crate::formats::parquet::SegmentCoverageError,
    },
    /// Source sidecar reading failed.
    #[snafu(context(false), display("Rewrite sidecar: {source}"))]
    Sidecar {
        /// Original typed failure.
        source: crate::coverage::io::CoverageSidecarError,
    },
    /// Replacement coverage encoding failed.
    #[snafu(context(false), display("Rewrite coverage encoding: {source}"))]
    Codec {
        /// Original typed failure.
        source: crate::coverage::serde::CoverageCodecError,
    },
    /// Sidecar path construction failed.
    #[snafu(context(false), display("Rewrite coverage path: {source}"))]
    Layout {
        /// Original typed failure.
        source: crate::coverage::layout::CoverageLayoutError,
    },
    /// Explicit replacement cleanup failed; Drop also attempts best effort.
    #[snafu(display("Replacement cleanup failures: {cleanup_errors:?}"))]
    Cleanup {
        /// All observed cleanup failures with their paths.
        cleanup_errors: Vec<StorageError>,
    },
    /// Rewriting and replacement cleanup both failed.
    #[snafu(display("{source}; cleanup also failed: {cleanup}"))]
    CleanupAfterFailure {
        /// Original typed failure.
        source: Box<RewriteError>,
        /// Cleanup failure.
        cleanup: Box<RewriteError>,
    },
    /// Rewriting and preparation cleanup both failed.
    #[snafu(display("{source}; preparation cleanup also failed: {cleanup}"))]
    PreparationCleanup {
        /// Original typed failure.
        source: Box<RewriteError>,
        /// Cleanup failure.
        cleanup: PrepareError,
    },
}
fn add_count(total: &mut u64, value: u64, counter: &'static str) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or(RewriteError::CountOverflow { counter })?;
    Ok(())
}
fn invalid(reason: impl Into<String>) -> RewriteError {
    RewriteError::Invalid {
        reason: reason.into(),
    }
}

/// File sizes are report metrics, not actual IO. Source reads include its footer
/// and decoder read-ahead. Output key reads exclude separate metadata inspection.
#[derive(Default, Debug, serde::Serialize)]
pub(crate) struct RewriteMetrics {
    pub(crate) rows_updated: u64,
    pub(crate) rows_rewritten: u64,
    pub(crate) source_file_bytes: u64,
    pub(crate) replacement_file_bytes: u64,
    pub(crate) source_bytes_read: u64,
    pub(crate) output_key_bytes_read: u64,
    pub(crate) peak_batch_bytes: usize,
    pub(crate) peak_writer_bytes: usize,
}

pub(crate) struct SegmentReplacement {
    pub(crate) source: SegmentMeta,
    pub(crate) replacement: SegmentMeta,
}

/// Armed until explicitly preserved by the publisher, including ambiguous commits.
/// Metadata is per affected segment; no row values survive in this result.
pub(crate) struct StagedUpdates {
    pub(crate) version: u64,
    pub(crate) replacements: Vec<SegmentReplacement>,
    pub(crate) metrics: RewriteMetrics,
    location: TableLocation,
    owned: Vec<(String, FileCleanupGuard)>,
}
impl StagedUpdates {
    pub(crate) fn owned_paths(&self) -> impl Iterator<Item = &str> {
        self.owned.iter().map(|(path, _)| path.as_str())
    }
    /// Call only after confirmed or ambiguous publication; never infer it here.
    pub(crate) fn preserve(&mut self) {
        for (_, guard) in &mut self.owned {
            guard.disarm();
        }
        self.owned.clear();
    }
    async fn cleanup(&mut self) -> Vec<StorageError> {
        let mut failures = Vec::new();
        for (path, guard) in self.owned.iter_mut().rev() {
            match storage::remove_file_if_exists(self.location.as_ref(), Path::new(path)).await {
                Ok(()) => guard.disarm(),
                Err(error) => failures.push(error),
            }
        }
        failures
    }
    pub(crate) async fn close(mut self) -> Result<()> {
        let cleanup_errors = self.cleanup().await;
        if cleanup_errors.is_empty() {
            Ok(())
        } else {
            Err(RewriteError::Cleanup { cleanup_errors })
        }
    }
    async fn create_owned_sink(&mut self, path: &str) -> Result<storage::OutputSink> {
        let mut guard = FileCleanupGuard::new_disarmed(self.location.as_ref(), Path::new(path))?;
        let sink = storage::open_new_output_sink(self.location.as_ref(), Path::new(path)).await?;
        guard.arm();
        self.owned.push((path.into(), guard));
        Ok(sink)
    }
}

/// Decoder batches and pages are separate from the writer byte target. A single
/// oversized value can exceed either target; row counts are not byte/RSS caps.
const READ_ROWS: usize = 256;
const WRITE_BYTES: usize = 8 * 1024 * 1024;
const MAX_FOOTER_BYTES: usize = 64 * 1024 * 1024;
const MAX_ROW_GROUP_BYTES: u64 = 1024 * 1024 * 1024;

fn reader_builder(
    location: &TableLocation,
    path: &str,
    bytes: Arc<AtomicU64>,
) -> Result<ParquetRecordBatchReaderBuilder<MeasuredParquetFile>> {
    storage::ensure_canonical_relative_storage_path(path)?;
    let StorageLocation::Local(root) = location.as_ref();
    let file = File::open(root.join(path)).map_err(|source| RewriteError::Io {
        path: root.join(path),
        source,
    })?;
    let input = MeasuredParquetFile { file, bytes };
    let tail = input.get_bytes(input.len().saturating_sub(8), 8)?;
    let footer_bytes = FooterTail::try_from(tail.as_ref())?.metadata_length();
    if footer_bytes > MAX_FOOTER_BYTES {
        return Err(RewriteError::Resource {
            path: path.into(),
            allocation: "footer",
            bytes: footer_bytes as u64,
            limit: MAX_FOOTER_BYTES as u64,
        });
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(input)?;
    for group in builder.metadata().row_groups() {
        let mut size = 0_u64;
        for column in group.columns() {
            if column.compressed_size() < 0
                || column.data_page_offset() < 0
                || column
                    .dictionary_page_offset()
                    .is_some_and(|offset| offset < 0)
            {
                return Err(invalid("invalid rewrite Parquet byte range"));
            }
            size = size
                .checked_add(
                    u64::try_from(column.uncompressed_size())
                        .map_err(|_| invalid("negative rewrite column size"))?,
                )
                .ok_or_else(|| invalid("rewrite row group size overflow"))?;
        }
        if size > MAX_ROW_GROUP_BYTES {
            return Err(RewriteError::Resource {
                path: path.into(),
                allocation: "uncompressed row group",
                bytes: size,
                limit: MAX_ROW_GROUP_BYTES,
            });
        }
    }
    Ok(builder)
}

fn project_and_hash_keys(
    batch: &RecordBatch,
    positions: &[usize],
    hash: &mut blake3::Hasher,
) -> Result<RecordBatch> {
    let batch = batch.project(positions)?;
    for row in 0..batch.num_rows() {
        let key = key_at(&batch, positions.len(), row)?;
        let bytes = serde_json::to_vec(&key).map_err(|source| PrepareError::Encoding { source })?;
        hash.update(&(bytes.len() as u64).to_le_bytes());
        hash.update(&bytes);
    }
    Ok(batch)
}

pub(crate) async fn stage_update_replacements(
    location: &TableLocation,
    state: &TableState,
    mut prepared: PreparedUpdates,
) -> Result<StagedUpdates> {
    let mut staged = StagedUpdates {
        version: state.version,
        replacements: Vec::new(),
        metrics: RewriteMetrics::default(),
        location: location.clone(),
        owned: Vec::new(),
    };
    let result = rewrite(&mut staged, state, &mut prepared).await;
    let result = match (result, prepared.close()) {
        (Ok(()), result) => result.map_err(RewriteError::from),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => Err(RewriteError::PreparationCleanup {
            source: Box::new(error),
            cleanup,
        }),
    };
    if let Err(source) = result {
        let cleanup_errors = staged.cleanup().await;
        return if cleanup_errors.is_empty() {
            Err(source)
        } else {
            Err(RewriteError::CleanupAfterFailure {
                source: Box::new(source),
                cleanup: Box::new(RewriteError::Cleanup { cleanup_errors }),
            })
        };
    }
    Ok(staged)
}

async fn rewrite(
    staged: &mut StagedUpdates,
    state: &TableState,
    prepared: &mut PreparedUpdates,
) -> Result<()> {
    let TableKind::TimeSeries(index) = &state.table_meta.kind else {
        return Err(invalid("expected time-series snapshot"));
    };
    if prepared.version != state.version
        || prepared.segments.len() != state.segments.len()
        || prepared
            .segments
            .iter()
            .any(|segment| state.segments.get(&segment.path) != Some(segment))
    {
        return Err(invalid("prepared updates belong to another snapshot"));
    }
    let schema = state
        .table_meta
        .arrow_schema_ref()
        .map_err(|error| invalid(error.to_string()))?;
    let key_positions = index
        .entity_columns
        .iter()
        .chain(std::iter::once(&index.column))
        .map(|name| schema.index_of(name))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut projection = key_positions.clone();
    projection.extend_from_slice(&prepared.destination_indices);
    if projection
        .iter()
        .any(|position| *position >= schema.fields().len())
        || prepared.destination_indices.is_empty()
        || projection
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != projection.len()
        || schema.project(&projection)? != *prepared.schema
    {
        return Err(invalid(
            "prepared destination mapping differs from snapshot schema",
        ));
    }
    let attempt = Uuid::new_v4();
    let mut next = prepared.next()?;
    let mut last_segment = None;
    while let Some(update) = &next {
        let segment_index = update.segment_index;
        if last_segment.is_some_and(|last| segment_index <= last) {
            return Err(invalid("prepared segment order regressed"));
        }
        let source = prepared
            .segments
            .get(segment_index)
            .ok_or_else(|| invalid("invalid prepared segment"))?
            .clone();
        if source.format != FileFormat::Parquet {
            return Err(invalid("rewrite source is not Parquet"));
        }
        let path = format!("{UPDATE_REWRITE_DATA_DIR}/{attempt}/{segment_index:010}.parquet");
        rewrite_segment(
            staged,
            state,
            prepared,
            &mut next,
            segment_index,
            &source,
            &path,
            &schema,
            &key_positions,
            index,
        )
        .await?;
        last_segment = Some(segment_index);
    }
    if staged.metrics.rows_updated != prepared.matched_rows {
        return Err(invalid("not all prepared updates were applied"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn rewrite_segment(
    staged: &mut StagedUpdates,
    state: &TableState,
    prepared: &mut PreparedUpdates,
    next: &mut Option<MatchedUpdate>,
    segment_index: usize,
    source: &SegmentMeta,
    path: &str,
    schema: &SchemaRef,
    key_positions: &[usize],
    index: &IndexSpec,
) -> Result<()> {
    let source_bytes = Arc::new(AtomicU64::new(0));
    let builder = reader_builder(&staged.location, &source.path, source_bytes.clone())?;
    let logical = state
        .table_meta
        .logical_schema()
        .ok_or_else(|| invalid("missing canonical schema"))?;
    let alignment =
        BatchSchemaAlignment::for_historical_segment(builder.schema().clone(), logical, index)
            .map_err(Box::new)?;
    if MissingColumnPolicy::from_table_requirements(&state.table_meta)
        == MissingColumnPolicy::Reject
        && builder.schema().fields().len() != schema.fields().len()
    {
        return Err(invalid("historical missing columns require table feature"));
    }
    let source_size = storage::file_size(staged.location.as_ref(), Path::new(&source.path)).await?;
    if source
        .file_size
        .is_some_and(|expected| expected != source_size)
    {
        return Err(invalid("source file size changed"));
    }
    let mut reader = builder.with_batch_size(READ_ROWS).build()?;
    let sink = staged.create_owned_sink(path).await?;
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(64 * 1024))
        .set_max_row_group_bytes(Some(WRITE_BYTES))
        .set_compression(super::append::ParquetCompression::default().into())
        .build();
    let mut writer = ArrowWriter::try_new(sink, schema.clone(), Some(properties))?;
    let mut hash = blake3::Hasher::new();
    let mut row = 0_u64;
    for batch in &mut reader {
        let batch = alignment.align_batch(&batch?)?;
        staged.metrics.peak_batch_bytes = staged
            .metrics
            .peak_batch_bytes
            .max(batch.get_array_memory_size());
        let key_batch = project_and_hash_keys(&batch, key_positions, &mut hash)?;
        // Only changed batches are assembled; unselected arrays are sliced without casts.
        let mut pieces = Vec::new();
        let mut start = 0;
        while next.as_ref().is_some_and(|update| {
            update.segment_index == segment_index && update.row < row + batch.num_rows() as u64
        }) {
            let update = next
                .take()
                .ok_or_else(|| invalid("missing prepared update"))?;
            if update.row < row + start as u64 {
                return Err(invalid("duplicate or regressing prepared row"));
            }
            let offset = (update.row - row) as usize;
            if key_at(&key_batch, key_positions.len(), offset)? != update.key {
                return Err(invalid("prepared key does not match source row"));
            }
            if offset > start {
                pieces.push(batch.slice(start, offset - start));
            }
            let mut columns = batch.slice(offset, 1).columns().to_vec();
            for (value_index, destination) in prepared.destination_indices.iter().enumerate() {
                columns[*destination] = update
                    .values
                    .column(key_positions.len() + value_index)
                    .clone();
            }
            pieces.push(RecordBatch::try_new(schema.clone(), columns)?);
            start = offset + 1;
            add_count(&mut staged.metrics.rows_updated, 1, "rows_updated")?;
            *next = prepared.next()?;
        }
        let output = if pieces.is_empty() {
            batch.clone()
        } else {
            if start < batch.num_rows() {
                pieces.push(batch.slice(start, batch.num_rows() - start));
            }
            concat_batches(schema, &pieces)?
        };
        staged.metrics.peak_batch_bytes = staged
            .metrics
            .peak_batch_bytes
            .max(output.get_array_memory_size());
        writer.write(&output)?;
        staged.metrics.peak_writer_bytes =
            staged.metrics.peak_writer_bytes.max(writer.memory_size());
        row += batch.num_rows() as u64;
        tokio::task::yield_now().await;
    }
    if row != source.row_count
        || next
            .as_ref()
            .is_some_and(|update| update.segment_index == segment_index)
    {
        return Err(invalid(
            "source row count or prepared address exceeds source",
        ));
    }
    drop(reader);
    writer.into_inner()?.finish().await?;
    let (mut meta, _) = segment_meta_from_parquet(&staged.location, Path::new(path), index).await?;
    if meta.row_count != row
        || meta.index_min != source.index_min
        || meta.index_max != source.index_max
    {
        return Err(invalid(
            "replacement row count or index bounds differ from source",
        ));
    }
    let verification_bytes = Arc::new(AtomicU64::new(0));
    let builder = reader_builder(&staged.location, path, verification_bytes.clone())?;
    if builder.schema() != schema {
        return Err(invalid("replacement canonical schema mismatch"));
    }
    let mask = ProjectionMask::roots(builder.parquet_schema(), key_positions.iter().copied());
    let reader = builder
        .with_projection(mask)
        .with_batch_size(READ_ROWS)
        .build()?;
    let (coverage_bytes, content_id) = verify_replacement_keys_and_coverage(
        reader,
        &staged.location,
        source,
        index,
        hash.finalize(),
    )
    .await?;
    let sidecar =
        segment_coverage_key(&coverage_file_id_for_attempt(&content_id, &Uuid::new_v4()))?;
    let mut sink = staged.create_owned_sink(&sidecar).await?;
    std::io::Write::write_all(&mut sink, &coverage_bytes).map_err(|source| RewriteError::Io {
        path: Path::new(&sidecar).to_owned(),
        source,
    })?;
    sink.finish().await?;
    if storage::read_all_bytes(staged.location.as_ref(), Path::new(&sidecar)).await?
        != coverage_bytes
    {
        return Err(invalid("replacement coverage write mismatch"));
    }
    meta.coverage_path = Some(sidecar);
    meta.entity_layout = source.entity_layout.clone();
    add_count(&mut staged.metrics.rows_rewritten, row, "rows_rewritten")?;
    add_count(
        &mut staged.metrics.source_file_bytes,
        source_size,
        "source_file_bytes",
    )?;
    add_count(
        &mut staged.metrics.replacement_file_bytes,
        meta.file_size
            .ok_or_else(|| invalid("missing replacement size"))?,
        "replacement_file_bytes",
    )?;
    add_count(
        &mut staged.metrics.source_bytes_read,
        source_bytes.load(Ordering::Relaxed),
        "source_bytes_read",
    )?;
    add_count(
        &mut staged.metrics.output_key_bytes_read,
        verification_bytes.load(Ordering::Relaxed),
        "output_key_bytes_read",
    )?;
    staged.replacements.push(SegmentReplacement {
        source: source.clone(),
        replacement: meta,
    });
    Ok(())
}

async fn verify_replacement_keys_and_coverage(
    reader: ParquetRecordBatchReader,
    location: &TableLocation,
    source: &SegmentMeta,
    index: &IndexSpec,
    expected: blake3::Hash,
) -> Result<(Vec<u8>, String)> {
    let mut hash = blake3::Hasher::new();
    let mut rows = 0_u64;
    let stream = futures::stream::iter(reader.map(|batch| {
        let batch = batch?;
        let positions = index
            .entity_columns
            .iter()
            .chain(std::iter::once(&index.column))
            .map(|name| batch.schema().index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let batch = project_and_hash_keys(&batch, &positions, &mut hash)
            .map_err(|error| parquet::errors::ParquetError::General(error.to_string()))?;
        rows += batch.num_rows() as u64;
        Ok(batch)
    }));
    let sidecar = source
        .coverage_path
        .as_deref()
        .ok_or_else(|| invalid("source coverage missing"))?;
    storage::ensure_canonical_relative_storage_path(sidecar)?;
    let result = if index.entity_columns.is_empty() {
        if source.entity_layout != SegmentEntityLayout::NotApplicable {
            return Err(invalid("unexpected entity layout"));
        }
        let coverage = Coverage::from_treemap(
            compute_coverage_bitmap_from_stream(stream, &source.path, index).await?,
        );
        if coverage != read_coverage_sidecar(location, Path::new(sidecar)).await? {
            return Err(invalid("replacement coverage differs from source"));
        }
        let bytes = coverage_to_bytes(&coverage)?;
        let id = segment_coverage_id_v2(index, &bytes);
        (bytes, id)
    } else {
        let coverage = compute_entity_coverage_from_stream(stream, &source.path, index).await?;
        let layout_matches = match &source.entity_layout {
            SegmentEntityLayout::Single(identity) => {
                coverage.identity_count() == 1
                    && coverage
                        .iter()
                        .next()
                        .is_some_and(|(found, _)| found == identity)
            }
            SegmentEntityLayout::Mixed => coverage.identity_count() > 1,
            _ => false,
        };
        if !layout_matches
            || coverage != read_entity_coverage_sidecar(location, Path::new(sidecar)).await?
        {
            return Err(invalid(
                "replacement entity layout or coverage differs from source",
            ));
        }
        let bytes = entity_coverage_to_bytes(&coverage)?;
        let id = segment_entity_coverage_id_v1(index, &bytes);
        (bytes, id)
    };
    if rows != source.row_count || hash.finalize() != expected {
        return Err(invalid("replacement exact keys or physical order changed"));
    }
    Ok(result)
}
