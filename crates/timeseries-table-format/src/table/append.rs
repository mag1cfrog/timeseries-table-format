//! Append pipeline for `TimeSeriesTable`.
//!
//! This module contains the core append implementation plus the public
//! wrappers. It is responsible for:
//! - loading/deriving segment metadata and logical schema,
//! - enforcing v0.1 schema rules (adopt on first append, otherwise exact match),
//! - computing segment coverage, detecting overlaps, and writing coverage sidecars,
//! - optimistic commit to the transaction log and in-memory state update.
//!   Keep new append-time invariants here so the flow remains centralized.

use std::path::Path;
use std::time::Instant;

use arrow::{
    array::{RecordBatch as ArrowRecordBatch, RecordBatchIterator, RecordBatchReader},
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
};
use parquet::arrow::ArrowWriter as ParquetArrowWriter;
use snafu::prelude::*;
use uuid::Uuid;

use crate::{
    coverage::serde::{coverage_to_bytes, entity_coverage_to_bytes},
    coverage::{
        EntityCoverage,
        bucket::logical_bucket_range,
        io::{CoverageError, write_coverage_sidecar_new_bytes},
        layout::{
            coverage_file_id_for_attempt, segment_coverage_id_v2, segment_coverage_key,
            segment_entity_coverage_id_v1, table_coverage_id_v2, table_entity_coverage_id_v1,
            table_snapshot_key,
        },
    },
    formats::parquet::{
        compute_segment_entity_coverage, coverage::compute_segment_coverage,
        logical_schema_from_parquet, segment_meta::segment_meta_from_parquet,
    },
    metadata::{
        logical_schema::LogicalSchema,
        schema_compat::{ensure_index_spec_matches_schema, ensure_schema_exact_match},
        segments::SegmentEntityLayout,
    },
    storage,
    transaction_log::{CommitError, LogAction, TableState, table_state::TableCoveragePointer},
};

use super::{
    TimeSeriesTable,
    append_report::{AppendReport, AppendReportBuilder},
    error::{
        AppendParquetSnafu, AppendSourceSnafu, CoverageBucketSnafu, CoverageOverlapSnafu,
        DuplicateSegmentPathSnafu, EmptySegmentEntityCoverageSnafu, EntityCoverageOverlapSnafu,
        EntityWithoutIndexCoverageSnafu, ExistingSegmentMissingCoverageSnafu,
        MissingCanonicalSchemaSnafu, SchemaCompatibilitySnafu, SegmentCoverageSnafu,
        SegmentMetaSnafu, SegmentSchemaCompatibilitySnafu, StorageSnafu, TableError,
    },
};

fn classify_entity_layout(
    segment_path: &str,
    coverage: &EntityCoverage,
) -> Result<SegmentEntityLayout, TableError> {
    let first_identity = coverage
        .iter()
        .next()
        .map(|(identity, _)| identity)
        .context(EmptySegmentEntityCoverageSnafu {
            segment_path: segment_path.to_string(),
        })?;

    if let Some((identity, _)) = coverage.iter().find(|(_, coverage)| coverage.is_empty()) {
        return EntityWithoutIndexCoverageSnafu {
            segment_path: segment_path.to_string(),
            identity: identity.clone(),
        }
        .fail();
    }

    Ok(if coverage.identity_count() == 1 {
        SegmentEntityLayout::Single(first_identity.clone())
    } else {
        SegmentEntityLayout::Mixed
    })
}

fn ensure_existing_segments_have_coverage(state: &TableState) -> Result<(), TableError> {
    for seg in state.segments.values() {
        if seg.coverage_path.is_none() {
            return ExistingSegmentMissingCoverageSnafu {
                path: seg.path.clone(),
            }
            .fail();
        }
    }

    Ok(())
}

fn record_append_failure<T>(result: &Result<T, TableError>) {
    if let Err(error) = result {
        let outcome = if matches!(
            error,
            TableError::AppendCommitAmbiguous { .. }
                | TableError::TransactionLog {
                    source: CommitError::AmbiguousOutcome { .. },
                }
        ) {
            "ambiguous"
        } else {
            "failed"
        };
        tracing::Span::current().record("outcome", outcome);
    }
}

/// Marker used to distinguish reader inputs from materialized batch inputs.
///
/// This type exists only to keep the blanket [`RecordBatchReader`]
/// implementation disjoint from the direct batch implementations.
#[doc(hidden)]
pub struct RecordBatchReaderSource;

/// Convert an Arrow batch source into a schema-bearing [`RecordBatchReader`].
///
/// Implementations preserve non-`Send` readers. The `Source` parameter is an
/// inference-only coherence marker and should not normally be specified by
/// callers.
pub trait IntoBatchStream<Source = RecordBatchReaderSource> {
    /// Reader produced from this source.
    type Reader: RecordBatchReader;

    /// Convert this source without collecting its batches.
    fn into_batch_stream(self) -> Result<Self::Reader, TableError>;
}

impl<R> IntoBatchStream<RecordBatchReaderSource> for R
where
    R: RecordBatchReader,
{
    type Reader = R;

    fn into_batch_stream(self) -> Result<Self::Reader, TableError> {
        Ok(self)
    }
}

impl IntoBatchStream<ArrowRecordBatch> for ArrowRecordBatch {
    type Reader = Box<dyn RecordBatchReader + Send>;

    fn into_batch_stream(self) -> Result<Self::Reader, TableError> {
        let schema = self.schema();
        Ok(Box::new(RecordBatchIterator::new(
            std::iter::once(Ok(self)),
            schema,
        )))
    }
}

impl IntoBatchStream<Vec<ArrowRecordBatch>> for Vec<ArrowRecordBatch> {
    type Reader = Box<dyn RecordBatchReader + Send>;

    fn into_batch_stream(self) -> Result<Self::Reader, TableError> {
        let schema = self
            .first()
            .map(ArrowRecordBatch::schema)
            .ok_or(TableError::EmptyAppendSource)?;
        Ok(Box::new(RecordBatchIterator::new(
            self.into_iter().map(Ok::<_, ArrowError>),
            schema,
        )))
    }
}

fn logical_schema_from_arrow(schema: &Schema) -> Result<LogicalSchema, TableError> {
    LogicalSchema::try_from_arrow_schema(schema).map_err(|source| TableError::AppendSource {
        source: ArrowError::SchemaError(source.to_string()),
    })
}

fn ensure_batch_schema(schema: &SchemaRef, batch: &ArrowRecordBatch) -> Result<(), TableError> {
    if batch.schema() == *schema {
        Ok(())
    } else {
        Err(TableError::AppendSource {
            source: ArrowError::SchemaError(
                "record batch schema does not match its reader schema".to_string(),
            ),
        })
    }
}

impl TimeSeriesTable {
    async fn rollback_created_artifacts(
        &self,
        created_sidecars: &[String],
        source: TableError,
    ) -> TableError {
        let mut cleanup_errors = Vec::new();
        for path in created_sidecars.iter().rev() {
            if let Err(error) =
                storage::remove_file_if_exists(self.location().as_ref(), Path::new(path)).await
            {
                cleanup_errors.push(format!("{path}: {error}"));
            }
        }

        if cleanup_errors.is_empty() {
            source
        } else {
            TableError::AppendRollback {
                source: Box::new(source),
                cleanup_errors,
            }
        }
    }

    async fn normalize_new_segment_path(&self, relative_path: &str) -> Result<String, TableError> {
        let supplied_path = Path::new(relative_path);
        let (normalized, native_path) =
            storage::normalize_relative_storage_path(supplied_path).context(StorageSnafu)?;

        if self.state.segments.contains_key(&normalized) {
            return DuplicateSegmentPathSnafu { path: normalized }.fail();
        }

        self.location()
            .validate_segment_file(supplied_path, &native_path)
            .await
            .context(StorageSnafu)?;

        Ok(normalized)
    }

    fn validate_stream_schema(&self, schema: &Schema) -> Result<(), TableError> {
        ensure_existing_segments_have_coverage(&self.state)?;
        let segment_schema = logical_schema_from_arrow(schema)?;
        ensure_index_spec_matches_schema(&segment_schema, &self.index)
            .context(SchemaCompatibilitySnafu)?;

        match self.state.table_meta.logical_schema.as_ref() {
            None if self.state.version == 1 => Ok(()),
            None => MissingCanonicalSchemaSnafu {
                version: self.state.version,
            }
            .fail(),
            Some(table_schema) => {
                ensure_schema_exact_match(table_schema, &segment_schema, &self.index)
                    .context(SchemaCompatibilitySnafu)
            }
        }
    }

    async fn append_parquet_path_file(
        &mut self,
        parquet_path: &Path,
        mut report: Option<&mut AppendReportBuilder>,
    ) -> Result<(u64, String), TableError> {
        let prepared = self
            .location()
            .prepare_parquet_under_root(parquet_path)
            .await
            .context(StorageSnafu)?;
        let prepared_path = prepared.relative_path.to_string_lossy().into_owned();
        let mut prepared_guard = if prepared.created {
            let mut guard = storage::prepare_file_cleanup_guard(
                self.location().as_ref(),
                &prepared.relative_path,
            )
            .context(StorageSnafu)?;
            guard.arm();
            Some(guard)
        } else {
            None
        };

        let append_result = async {
            let relative_path = self.normalize_new_segment_path(&prepared_path).await?;
            tracing::Span::current().record("segment_path", relative_path.as_str());
            if let Some(r) = report.as_mut() {
                r.set_context("relative_path", &relative_path);
            }
            let version = self
                .append_parquet_segment_file(&relative_path, report, "external_path")
                .await?;
            Ok((version, relative_path))
        }
        .await;

        match append_result {
            Ok(result) => {
                if let Some(guard) = prepared_guard.as_mut() {
                    guard.disarm();
                }
                Ok(result)
            }
            Err(
                error @ TableError::TransactionLog {
                    source: CommitError::AmbiguousOutcome { .. },
                },
            ) => {
                if let Some(guard) = prepared_guard.as_mut() {
                    guard.disarm();
                }
                Err(error)
            }
            Err(source) if prepared.created => {
                let cleanup =
                    storage::remove_file(self.location().as_ref(), &prepared.relative_path).await;
                if let Some(guard) = prepared_guard.as_mut() {
                    guard.disarm();
                }
                match cleanup {
                    Ok(()) => Err(source),
                    Err(cleanup_error) => Err(TableError::ExternalParquetRollback {
                        path: prepared.relative_path.display().to_string(),
                        source: Box::new(source),
                        cleanup_error,
                    }),
                }
            }
            Err(source) => Err(source),
        }
    }

    async fn append_parquet_segment_file(
        &mut self,
        relative_path: &str,
        mut report: Option<&mut AppendReportBuilder>,
        source_mode: &'static str,
    ) -> Result<u64, TableError> {
        let rel_path = Path::new(relative_path);
        let expected_version = self.state.version;
        if let Some(r) = report.as_mut() {
            r.set_context("index_column", self.index.column.as_str());
        }

        // 0) Coverage readiness checks.
        ensure_existing_segments_have_coverage(&self.state)?;

        // 1) Segment meta + schema.
        let step_start = Instant::now();
        let (mut segment_meta, meta_report) =
            segment_meta_from_parquet(self.location(), rel_path, &self.index)
                .await
                .context(SegmentMetaSnafu)?;
        let row_count = segment_meta.row_count;
        let span = tracing::Span::current();
        span.record("row_count", row_count);
        if let Some(file_size) = segment_meta.file_size {
            span.record("file_size_bytes", file_size);
        }
        if let Some(r) = report.as_mut() {
            if let Some(file_size) = segment_meta.file_size {
                r.set_context("file_size_bytes", file_size.to_string());
            }
            let fields = vec![
                ("row_groups".to_string(), meta_report.row_groups.to_string()),
                ("row_count".to_string(), meta_report.row_count.to_string()),
                ("used_stats".to_string(), meta_report.used_stats.to_string()),
                (
                    "scanned_rows".to_string(),
                    meta_report.scanned_rows.to_string(),
                ),
            ];
            r.push_step("segment_meta", step_start.elapsed(), fields);
        }

        let step_start = Instant::now();
        let segment_schema = logical_schema_from_parquet(self.location(), rel_path)
            .await
            .context(SegmentMetaSnafu)?;
        ensure_index_spec_matches_schema(&segment_schema, &self.index).context(
            SegmentSchemaCompatibilitySnafu {
                path: relative_path.to_string(),
            },
        )?;
        if let Some(r) = report.as_mut() {
            r.push_step("logical_schema", step_start.elapsed(), Vec::new());
        }

        // 2) Schema behavior (return maybe_updated_meta, but do NOT build actions yet).
        //
        // - logical_schema == None && version == 1:
        //     first append after create() — adopt this segment’s schema.
        // - logical_schema == None && version != 1:
        //     table is in a bad state for v0.1 → error.
        // - logical_schema == Some(..):
        //     enforce “no schema evolution” via ensure_schema_exact_match.
        let maybe_table_schema = self.state.table_meta.logical_schema.as_ref();

        let maybe_updated_meta = match maybe_table_schema {
            None if expected_version == 1 => {
                let mut updated_meta = self.state.table_meta.clone();
                updated_meta.logical_schema = Some(segment_schema.clone());
                Some(updated_meta)
            }
            None => {
                return MissingCanonicalSchemaSnafu {
                    version: expected_version,
                }
                .fail();
            }
            Some(table_schema) => {
                ensure_schema_exact_match(table_schema, &segment_schema, &self.index).context(
                    SegmentSchemaCompatibilitySnafu {
                        path: relative_path.to_string(),
                    },
                )?;
                None
            }
        };

        let has_entity_columns = !self.index.entity_columns.is_empty();

        // 3-5) Load, compute, and compare coverage using the entity-column mode.
        let (seg_cov_bytes, new_snap_cov_bytes, entity_layout) = if has_entity_columns {
            let step_start = Instant::now();
            let table_cov = self.load_table_entity_snapshot_coverage_readonly().await?;
            if let Some(r) = report.as_mut() {
                r.push_step("load_table_snapshot", step_start.elapsed(), Vec::new());
            }

            let step_start = Instant::now();
            let segment_cov =
                compute_segment_entity_coverage(self.location(), rel_path, &self.index)
                    .await
                    .context(SegmentCoverageSnafu)?;
            let entity_layout = classify_entity_layout(relative_path, &segment_cov)?;
            if let Some(r) = report.as_mut() {
                r.push_step("segment_coverage", step_start.elapsed(), Vec::new());
            }

            let step_start = Instant::now();
            if let Some((identity, bucket)) = segment_cov.overlap_example(&table_cov) {
                let example_bucket_range =
                    logical_bucket_range(&self.index.kind, bucket).context(CoverageBucketSnafu)?;
                return EntityCoverageOverlapSnafu {
                    segment_path: relative_path.to_string(),
                    overlap_count: segment_cov.intersection_cardinality(&table_cov),
                    example_identity: identity.clone(),
                    example_bucket: bucket,
                    example_bucket_range,
                }
                .fail();
            }
            if let Some(r) = report.as_mut() {
                r.push_step("overlap_check", step_start.elapsed(), Vec::new());
            }

            let seg_bytes = entity_coverage_to_bytes(&segment_cov).map_err(|source| {
                TableError::CoverageSidecar {
                    source: CoverageError::EntitySerde { source },
                }
            })?;
            let snapshot_bytes =
                entity_coverage_to_bytes(&table_cov.union(&segment_cov)).map_err(|source| {
                    TableError::CoverageSidecar {
                        source: CoverageError::EntitySerde { source },
                    }
                })?;
            (seg_bytes, snapshot_bytes, entity_layout)
        } else {
            let step_start = Instant::now();
            let table_cov = self.load_table_snapshot_coverage_readonly().await?;
            if let Some(r) = report.as_mut() {
                r.push_step("load_table_snapshot", step_start.elapsed(), Vec::new());
            }

            let step_start = Instant::now();
            let segment_cov = compute_segment_coverage(self.location(), rel_path, &self.index)
                .await
                .context(SegmentCoverageSnafu)?;
            if let Some(r) = report.as_mut() {
                r.push_step("segment_coverage", step_start.elapsed(), Vec::new());
            }

            let step_start = Instant::now();
            let overlap = segment_cov.intersect(&table_cov);
            let overlap_count = overlap.cardinality();
            if let Some(example_bucket) = overlap.present().iter().next() {
                let example_bucket_range = logical_bucket_range(&self.index.kind, example_bucket)
                    .context(CoverageBucketSnafu)?;
                return CoverageOverlapSnafu {
                    segment_path: relative_path.to_string(),
                    overlap_count,
                    example_bucket: Some(example_bucket),
                    example_bucket_range,
                }
                .fail();
            }
            if let Some(r) = report.as_mut() {
                r.push_step("overlap_check", step_start.elapsed(), Vec::new());
            }

            let seg_bytes =
                coverage_to_bytes(&segment_cov).map_err(|source| TableError::CoverageSidecar {
                    source: CoverageError::Serde { source },
                })?;
            let snapshot_bytes =
                coverage_to_bytes(&table_cov.union(&segment_cov)).map_err(|source| {
                    TableError::CoverageSidecar {
                        source: CoverageError::Serde { source },
                    }
                })?;
            (
                seg_bytes,
                snapshot_bytes,
                SegmentEntityLayout::NotApplicable,
            )
        };
        let entity_layout_name = match &entity_layout {
            SegmentEntityLayout::NotApplicable => "not_applicable",
            SegmentEntityLayout::Single(_) => "single",
            SegmentEntityLayout::Mixed => "mixed",
        };
        span.record("entity_layout", entity_layout_name);

        // 6) Give this append private sidecar paths, then write them before commit.
        let attempt_id = Uuid::new_v4();
        let segment_content_id = if has_entity_columns {
            segment_entity_coverage_id_v1(&self.index, &seg_cov_bytes)
        } else {
            segment_coverage_id_v2(&self.index, &seg_cov_bytes)
        };
        let segment_file_id = coverage_file_id_for_attempt(&segment_content_id, &attempt_id);
        let seg_cov_path = segment_coverage_key(&segment_file_id).map_err(|source| {
            TableError::CoverageSidecar {
                source: CoverageError::Layout { source },
            }
        })?;

        let new_version_guess = expected_version + 1;
        let snapshot_content_id = if has_entity_columns {
            table_entity_coverage_id_v1(&self.index, &new_snap_cov_bytes)
        } else {
            table_coverage_id_v2(&self.index, &new_snap_cov_bytes)
        };
        let snapshot_file_id = coverage_file_id_for_attempt(&snapshot_content_id, &attempt_id);
        let snapshot_path =
            table_snapshot_key(new_version_guess, &snapshot_file_id).map_err(|source| {
                TableError::CoverageSidecar {
                    source: CoverageError::Layout { source },
                }
            })?;

        let step_start = Instant::now();
        let mut created_sidecars = Vec::new();
        let mut segment_sidecar_guard =
            storage::prepare_file_cleanup_guard(self.location().as_ref(), Path::new(&seg_cov_path))
                .context(StorageSnafu)?;
        write_coverage_sidecar_new_bytes(self.location(), Path::new(&seg_cov_path), &seg_cov_bytes)
            .await
            .map_err(|source| TableError::CoverageSidecar { source })?;
        segment_sidecar_guard.arm();
        created_sidecars.push(seg_cov_path.clone());
        if let Some(r) = report.as_mut() {
            r.push_step("write_segment_sidecar", step_start.elapsed(), Vec::new());
        }

        let step_start = Instant::now();
        let mut snapshot_sidecar_guard = storage::prepare_file_cleanup_guard(
            self.location().as_ref(),
            Path::new(&snapshot_path),
        )
        .context(StorageSnafu)?;
        if let Err(source) = write_coverage_sidecar_new_bytes(
            self.location(),
            Path::new(&snapshot_path),
            &new_snap_cov_bytes,
        )
        .await
        {
            let error = TableError::CoverageSidecar { source };
            let error = self
                .rollback_created_artifacts(&created_sidecars, error)
                .await;
            segment_sidecar_guard.disarm();
            return Err(error);
        }
        snapshot_sidecar_guard.arm();
        created_sidecars.push(snapshot_path.clone());
        if let Some(r) = report.as_mut() {
            r.push_step("write_snapshot_sidecar", step_start.elapsed(), Vec::new());
        }

        // 7) Build actions and atomically publish the commit.
        segment_meta.coverage_path = Some(seg_cov_path);
        segment_meta.entity_layout = entity_layout;

        let mut actions = Vec::new();
        if let Some(updated_meta) = maybe_updated_meta.clone() {
            actions.push(LogAction::UpdateTableMeta(updated_meta));
        }

        actions.push(LogAction::AddSegment(segment_meta.clone()));
        actions.push(LogAction::UpdateTableCoverage {
            index_kind: self.index.kind.clone(),
            coverage_path: snapshot_path.clone(),
        });

        let step_start = Instant::now();
        let new_version = match self
            .log
            .commit_with_expected_version(expected_version, actions)
            .await
        {
            Ok(version) => {
                segment_sidecar_guard.disarm();
                snapshot_sidecar_guard.disarm();
                version
            }
            Err(source @ crate::transaction_log::CommitError::AmbiguousOutcome { .. }) => {
                segment_sidecar_guard.disarm();
                snapshot_sidecar_guard.disarm();
                return Err(TableError::TransactionLog { source });
            }
            Err(source) => {
                let error = TableError::TransactionLog { source };
                let error = self
                    .rollback_created_artifacts(&created_sidecars, error)
                    .await;
                segment_sidecar_guard.disarm();
                snapshot_sidecar_guard.disarm();
                return Err(error);
            }
        };
        if let Some(r) = report.as_mut() {
            r.push_step("commit_log", step_start.elapsed(), Vec::new());
        }

        // OCC invariant: a successful commit_with_expected_version must return
        // the same "next" version we predicted when constructing `snapshot_path`.
        // If this ever diverges, it indicates a severe bug between snapshot path
        // construction and the transaction log implementation, so we panic rather
        // than continuing with an inconsistent in-memory state.
        assert_eq!(
            new_version, new_version_guess,
            "transaction log returned unexpected version: expected {}, got {}",
            new_version_guess, new_version
        );

        // 8) Update in-memory state.
        let step_start = Instant::now();
        self.state.version = new_version;

        if let Some(updated_meta) = maybe_updated_meta {
            self.state.table_meta = updated_meta
        }

        self.state
            .segments
            .insert(segment_meta.path.clone(), segment_meta);

        // Also update the snapshot pointer in state.
        self.state.table_coverage = Some(TableCoveragePointer {
            index_kind: self.index.kind.clone(),
            coverage_path: snapshot_path,
            version: new_version,
        });
        if let Some(r) = report.as_mut() {
            r.push_step("state_update", step_start.elapsed(), Vec::new());
        }

        span.record("committed_version", new_version);
        span.record("outcome", "succeeded");
        tracing::info!(
            name: "table.append",
            source_mode,
            expected_version,
            committed_version = new_version,
            row_count,
            entity_layout = entity_layout_name,
            outcome = "succeeded",
            "Appended Parquet segment"
        );
        Ok(new_version)
    }

    /// Append Arrow record batches into one table-managed Parquet segment.
    ///
    /// Rows need not be ordered by the table's ordered index. The source is
    /// consumed incrementally and is never collected by this method.
    #[tracing::instrument(
        name = "table.append",
        level = "debug",
        skip_all,
        fields(
            source_mode = "arrow_stream",
            expected_version = self.state.version,
            segment_path = tracing::field::Empty,
            row_count = tracing::field::Empty,
            file_size_bytes = tracing::field::Empty,
            committed_version = tracing::field::Empty,
            entity_layout = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    pub async fn append<S, Source>(&mut self, source: S) -> Result<u64, TableError>
    where
        S: IntoBatchStream<Source>,
    {
        let result = async {
            let mut reader = source.into_batch_stream()?;
            let schema = reader.schema();
            self.validate_stream_schema(schema.as_ref())?;

            let first_batch = loop {
                let Some(batch) = reader.next().transpose().context(AppendSourceSnafu)? else {
                    return Err(TableError::EmptyAppendSource);
                };
                ensure_batch_schema(&schema, &batch)?;
                if batch.num_rows() != 0 {
                    break batch;
                }
            };

            let relative_path = format!("data/{}.parquet", Uuid::new_v4());
            tracing::Span::current().record("segment_path", relative_path.as_str());
            let mut data_guard = storage::prepare_file_cleanup_guard(
                self.location().as_ref(),
                Path::new(&relative_path),
            )
            .context(StorageSnafu)?;
            let sink =
                storage::open_new_output_sink(self.location().as_ref(), Path::new(&relative_path))
                    .await
                    .context(StorageSnafu)?;
            let write_result = async {
                let mut writer = ParquetArrowWriter::try_new(sink, schema.clone(), None)
                    .context(AppendParquetSnafu)?;
                writer.write(&first_batch).context(AppendParquetSnafu)?;
                drop(first_batch);

                for batch in reader {
                    let batch = batch.context(AppendSourceSnafu)?;
                    ensure_batch_schema(&schema, &batch)?;
                    if batch.num_rows() != 0 {
                        writer.write(&batch).context(AppendParquetSnafu)?;
                    }
                }

                let sink = writer.into_inner().context(AppendParquetSnafu)?;
                sink.finish().await.context(StorageSnafu)
            }
            .await;
            if let Err(source) = write_result {
                let error = self
                    .rollback_created_artifacts(std::slice::from_ref(&relative_path), source)
                    .await;
                data_guard.disarm();
                return Err(error);
            }
            data_guard.arm();

            match self
                .append_parquet_segment_file(&relative_path, None, "arrow_stream")
                .await
            {
                Ok(version) => {
                    data_guard.disarm();
                    Ok(version)
                }
                Err(TableError::TransactionLog {
                    source: source @ CommitError::AmbiguousOutcome { .. },
                }) => {
                    data_guard.disarm();
                    Err(TableError::AppendCommitAmbiguous {
                        segment_path: relative_path,
                        source,
                    })
                }
                Err(source) => {
                    let error = self
                        .rollback_created_artifacts(std::slice::from_ref(&relative_path), source)
                        .await;
                    data_guard.disarm();
                    Err(error)
                }
            }
        }
        .await;
        record_append_failure(&result);
        result
    }

    /// Append a Parquet segment using its canonical relative path as identity.
    ///
    /// Rows need not be ordered by the table's ordered index.
    #[tracing::instrument(
        name = "table.append",
        level = "debug",
        skip_all,
        fields(
            source_mode = "table_relative",
            expected_version = self.state.version,
            segment_path = tracing::field::Empty,
            row_count = tracing::field::Empty,
            file_size_bytes = tracing::field::Empty,
            committed_version = tracing::field::Empty,
            entity_layout = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    pub async fn append_parquet_segment(&mut self, relative_path: &str) -> Result<u64, TableError> {
        let result = async {
            let relative_path = self.normalize_new_segment_path(relative_path).await?;
            tracing::Span::current().record("segment_path", relative_path.as_str());
            self.append_parquet_segment_file(&relative_path, None, "table_relative")
                .await
        }
        .await;
        record_append_failure(&result);
        result
    }

    /// Copy an external Parquet file into the table when needed and append it.
    ///
    /// Rows need not be ordered by the table's ordered index.
    ///
    /// A copy created by this operation is removed when append fails before
    /// publication. Files already under the table root and copies involved in
    /// an ambiguous commit outcome are preserved.
    /// Returns the committed version and normalized table-relative segment path.
    #[tracing::instrument(
        name = "table.append",
        level = "debug",
        skip_all,
        fields(
            source_mode = "external_path",
            expected_version = self.state.version,
            segment_path = tracing::field::Empty,
            row_count = tracing::field::Empty,
            file_size_bytes = tracing::field::Empty,
            committed_version = tracing::field::Empty,
            entity_layout = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    pub async fn append_parquet_from_path(
        &mut self,
        parquet_path: &Path,
    ) -> Result<(u64, String), TableError> {
        let result = self.append_parquet_path_file(parquet_path, None).await;
        record_append_failure(&result);
        result
    }

    /// Copy and append a Parquet file while collecting a profiling report.
    /// Returns the committed version, normalized table-relative path, and report.
    #[tracing::instrument(
        name = "table.append",
        level = "debug",
        skip_all,
        fields(
            source_mode = "external_path",
            expected_version = self.state.version,
            segment_path = tracing::field::Empty,
            row_count = tracing::field::Empty,
            file_size_bytes = tracing::field::Empty,
            committed_version = tracing::field::Empty,
            entity_layout = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    pub async fn append_parquet_from_path_with_report(
        &mut self,
        parquet_path: &Path,
    ) -> Result<(u64, String, AppendReport), TableError> {
        let result = async {
            let mut report = AppendReportBuilder::new();
            let (version, relative_path) = self
                .append_parquet_path_file(parquet_path, Some(&mut report))
                .await?;
            Ok((version, relative_path, report.finish()))
        }
        .await;
        record_append_failure(&result);
        result
    }

    /// Append a Parquet segment and return a profiling report.
    #[tracing::instrument(
        name = "table.append",
        level = "debug",
        skip_all,
        fields(
            source_mode = "table_relative",
            expected_version = self.state.version,
            segment_path = tracing::field::Empty,
            row_count = tracing::field::Empty,
            file_size_bytes = tracing::field::Empty,
            committed_version = tracing::field::Empty,
            entity_layout = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    pub async fn append_parquet_segment_with_report(
        &mut self,
        relative_path: &str,
    ) -> Result<(u64, AppendReport), TableError> {
        let result = async {
            let relative_path = self.normalize_new_segment_path(relative_path).await?;
            tracing::Span::current().record("segment_path", relative_path.as_str());
            let mut report = AppendReportBuilder::new();
            report.set_context("relative_path", &relative_path);
            let version = self
                .append_parquet_segment_file(&relative_path, Some(&mut report), "table_relative")
                .await?;
            Ok((version, report.finish()))
        }
        .await;
        record_append_failure(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use super::*;
    use crate::coverage::io::{read_coverage_sidecar, read_entity_coverage_sidecar};
    use crate::coverage::serde::entity_coverage_from_bytes;
    use crate::coverage::{EntityCoverage, EntityIdentity, EntityValue};
    use crate::metadata::logical_schema::{
        LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit,
    };
    use crate::metadata::segments::{ParquetIndexColumnError, SegmentEntityLayout};
    use crate::metadata::table_metadata::{IndexValue, TABLE_FORMAT_VERSION};
    use crate::storage::layout;
    use crate::storage::{StorageError, StorageLocation, TableLocation};
    use crate::transaction_log::segments::{SegmentError, SegmentMetaError};
    use crate::transaction_log::{
        CommitError, IndexKind, IndexSpec, TableKind, TableMeta, TimeBucket,
    };
    use arrow::{
        array::{
            ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
            TimestampMillisecondArray, UInt64Array,
        },
        datatypes::{DataType, Field, Schema, TimeUnit as ArrowTimeUnit},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::fs::{File, OpenOptions};
    use std::io::{Seek, SeekFrom, Write};
    use std::num::NonZeroU64;
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::{Arc, Weak};
    use tempfile::TempDir;

    #[derive(Default)]
    struct ReaderObservations {
        schema_calls: Cell<usize>,
        next_calls: Cell<usize>,
        next_before_schema: Cell<bool>,
        previous_batch_alive: Cell<bool>,
    }

    struct InstrumentedReader {
        schema: SchemaRef,
        batches: std::vec::IntoIter<Result<RecordBatch, ArrowError>>,
        observations: Rc<ReaderObservations>,
        previous_array: Option<Weak<dyn arrow::array::Array>>,
    }

    impl InstrumentedReader {
        fn new(
            schema: SchemaRef,
            batches: Vec<Result<RecordBatch, ArrowError>>,
        ) -> (Self, Rc<ReaderObservations>) {
            let observations = Rc::new(ReaderObservations::default());
            (
                Self {
                    schema,
                    batches: batches.into_iter(),
                    observations: Rc::clone(&observations),
                    previous_array: None,
                },
                observations,
            )
        }

        fn one(batch: RecordBatch) -> Self {
            let (reader, _) = Self::new(batch.schema(), vec![Ok(batch)]);
            reader
        }
    }

    impl Iterator for InstrumentedReader {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.observations
                .next_calls
                .set(self.observations.next_calls.get() + 1);
            if self.observations.schema_calls.get() == 0 {
                self.observations.next_before_schema.set(true);
            }
            if self
                .previous_array
                .as_ref()
                .is_some_and(|array| array.strong_count() != 0)
            {
                self.observations.previous_batch_alive.set(true);
            }

            let next = self.batches.next();
            if let Some(Ok(batch)) = &next {
                self.previous_array = Some(Arc::downgrade(batch.column(0)));
            }
            next
        }
    }

    impl RecordBatchReader for InstrumentedReader {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.observations
                .schema_calls
                .set(self.observations.schema_calls.get() + 1);
            Arc::clone(&self.schema)
        }
    }

    fn input_batch(values: Vec<i64>) -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
    }

    fn time_series_batch(
        timestamps: Vec<i64>,
        symbols: Vec<&str>,
        prices: Vec<f64>,
    ) -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps)),
                Arc::new(StringArray::from(symbols)),
                Arc::new(Float64Array::from(prices)),
            ],
        )
    }

    fn timestamp_only_meta() -> TableMeta {
        TableMeta::new_time_series_with_schema(
            IndexSpec {
                column: "ts".to_string(),
                entity_columns: Vec::new(),
                kind: IndexKind::Timestamp {
                    bucket: TimeBucket::Minutes(1),
                    timezone: None,
                },
            },
            LogicalSchema::new(vec![LogicalField {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Millis,
                    timezone: None,
                },
                nullable: false,
            }])
            .expect("valid timestamp-only schema"),
        )
    }

    fn timestamp_only_batch(row_count: usize) -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMillisecondArray::from_iter_values(
                (0..row_count).map(|value| value as i64),
            ))],
        )
    }

    #[tokio::test]
    async fn lossy_schema_is_rejected_before_reading_or_creating_artifacts() -> TestResult {
        let temp = TempDir::new()?;
        let mut meta = timestamp_only_meta();
        meta.logical_schema = None;
        let mut table = TimeSeriesTable::create(TableLocation::local(temp.path()), meta).await?;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int8, true),
        ]));
        let (reader, observations) = InstrumentedReader::new(schema, Vec::new());

        assert!(matches!(
            table.append(reader).await,
            Err(TableError::AppendSource {
                source: ArrowError::SchemaError(_)
            })
        ));
        assert_eq!(observations.next_calls.get(), 0);
        assert_eq!(table.state().version, 1);
        assert!(!temp.path().join("data").exists());
        Ok(())
    }

    fn assert_batches<R>(reader: R, expected: &[RecordBatch]) -> TestResult
    where
        R: RecordBatchReader,
    {
        assert_eq!(reader.schema(), expected[0].schema());
        let actual = reader.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn into_batch_stream_accepts_all_required_input_forms() -> TestResult {
        let first = input_batch(vec![1, 2])?;
        let second = input_batch(vec![3])?;

        assert_batches(
            first.clone().into_batch_stream()?,
            std::slice::from_ref(&first),
        )?;
        assert_batches(
            vec![first.clone(), second.clone()].into_batch_stream()?,
            &[first.clone(), second.clone()],
        )?;

        let iterator = RecordBatchIterator::new(vec![Ok(first.clone())], first.schema());
        assert_batches(iterator.into_batch_stream()?, std::slice::from_ref(&first))?;
        assert_batches(
            InstrumentedReader::one(first.clone()).into_batch_stream()?,
            std::slice::from_ref(&first),
        )?;

        let boxed: Box<dyn RecordBatchReader> = Box::new(RecordBatchIterator::new(
            vec![Ok(first.clone())],
            first.schema(),
        ));
        assert_batches(boxed.into_batch_stream()?, std::slice::from_ref(&first))?;

        let sendable: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(second.clone())],
            second.schema(),
        ));
        assert_batches(sendable.into_batch_stream()?, std::slice::from_ref(&second))?;
        Ok(())
    }

    #[test]
    fn into_batch_stream_rejects_schema_less_vec() {
        let result = Vec::<RecordBatch>::new().into_batch_stream();
        assert!(matches!(result, Err(TableError::EmptyAppendSource)));
    }

    #[tokio::test]
    async fn append_stream_writes_one_segment_and_returns_versions() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        let first = time_series_batch(vec![0], vec!["A"], vec![1.0])?;
        let second = time_series_batch(vec![60_000], vec!["A"], vec![2.0])?;
        assert_eq!(table.append(vec![first, second]).await?, 2);
        assert_eq!(table.state().segments.len(), 1);
        let Some(segment) = table.state().segments.values().next() else {
            return Err("missing streamed segment".into());
        };
        assert_eq!(segment.row_count, 2);
        assert!(temp.path().join(&segment.path).is_file());

        let third = time_series_batch(vec![120_000], vec!["A"], vec![3.0])?;
        assert_eq!(table.append(third).await?, 3);
        assert_eq!(table.state().segments.len(), 2);

        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state().version, 3);
        assert_eq!(
            reopened
                .state()
                .segments
                .values()
                .map(|segment| segment.row_count)
                .sum::<u64>(),
            3
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_consumes_non_send_reader_one_batch_at_a_time() -> TestResult {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), make_basic_table_meta())
                .await?;
        let first = time_series_batch(vec![0], vec!["A"], vec![1.0])?;
        let second = time_series_batch(vec![60_000], vec!["A"], vec![2.0])?;
        let (reader, observations) =
            InstrumentedReader::new(first.schema(), vec![Ok(first), Ok(second)]);

        assert_eq!(table.append(reader).await?, 2);
        assert!(observations.schema_calls.get() > 0);
        assert!(!observations.next_before_schema.get());
        assert!(!observations.previous_batch_alive.get());
        assert_eq!(observations.next_calls.get(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_rejects_empty_sources_before_creating_data() -> TestResult {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), make_basic_table_meta())
                .await?;
        let zero = time_series_batch(Vec::new(), Vec::new(), Vec::new())?;
        let schema = zero.schema();

        assert!(matches!(
            table.append(vec![zero.clone(), zero]).await,
            Err(TableError::EmptyAppendSource)
        ));
        let empty = RecordBatchIterator::new(Vec::<Result<RecordBatch, ArrowError>>::new(), schema);
        assert!(matches!(
            table.append(empty).await,
            Err(TableError::EmptyAppendSource)
        ));
        assert_eq!(table.state().version, 1);
        assert!(table.state().segments.is_empty());
        assert!(!temp.path().join("data").exists());

        let data = time_series_batch(vec![0], vec!["A"], vec![1.0])?;
        let leading_zero = time_series_batch(Vec::new(), Vec::new(), Vec::new())?;
        assert_eq!(table.append(vec![leading_zero, data]).await?, 2);
        assert_eq!(table.state().version, 2);
        assert_eq!(table.state().segments.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_cleans_up_after_later_schema_or_source_error() -> TestResult {
        for source_error in [false, true] {
            let temp = TempDir::new()?;
            let mut table =
                TimeSeriesTable::create(TableLocation::local(temp.path()), make_basic_table_meta())
                    .await?;
            let first = time_series_batch(vec![0], vec!["A"], vec![1.0])?;
            let later = if source_error {
                Err(ArrowError::ComputeError(
                    "injected batch source failure".to_string(),
                ))
            } else {
                Ok(input_batch(vec![1])?)
            };
            let (reader, _) = InstrumentedReader::new(first.schema(), vec![Ok(first), later]);

            let error = table
                .append(reader)
                .await
                .expect_err("later batch must fail the append");
            assert!(matches!(error, TableError::AppendSource { .. }));
            assert_eq!(table.state().version, 1);
            assert!(table.state().segments.is_empty());
            assert!(
                std::fs::read_dir(temp.path().join("data"))?
                    .next()
                    .is_none()
            );
            assert!(coverage_files(temp.path())?.is_empty());
            assert!(!temp.path().join(layout::commit_rel_path(2)).exists());
        }
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_cleans_up_writer_lifecycle_failures() -> TestResult {
        #[derive(Clone, Copy, Debug)]
        enum Stage {
            Write,
            Close,
            Finish,
        }

        for stage in [Stage::Write, Stage::Close, Stage::Finish] {
            let temp = TempDir::new()?;
            let mut table =
                TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta())
                    .await?;
            let data_dir = temp.path().join("data");
            match stage {
                Stage::Write => crate::storage::inject_output_write_failure(data_dir.clone(), 2),
                Stage::Close => crate::storage::inject_output_write_failure(data_dir.clone(), 1),
                Stage::Finish => crate::storage::inject_output_finish_failure(data_dir.clone()),
            }
            let row_count = if matches!(stage, Stage::Write) {
                parquet::file::properties::DEFAULT_MAX_ROW_GROUP_ROW_COUNT
            } else {
                1
            };

            let result = table.append(timestamp_only_batch(row_count)?).await;
            let Err(error) = result else {
                panic!("{stage:?} writer failure was not injected");
            };
            if matches!(stage, Stage::Finish) {
                assert!(matches!(error, TableError::Storage { .. }), "{stage:?}");
            } else {
                assert!(
                    matches!(error, TableError::AppendParquet { .. }),
                    "{stage:?}: {error}"
                );
            }
            assert_eq!(table.state().version, 1, "{stage:?}");
            assert!(table.state().segments.is_empty(), "{stage:?}");
            assert!(std::fs::read_dir(&data_dir)?.next().is_none(), "{stage:?}");
            assert!(coverage_files(temp.path())?.is_empty(), "{stage:?}");
            assert!(
                !temp.path().join(layout::commit_rel_path(2)).exists(),
                "{stage:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_streaming_append_before_publication_removes_owned_artifacts() -> TestResult
    {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let commit_path = temp.path().join(layout::commit_rel_path(2));
        let current_path = temp.path().join(layout::current_rel_path());
        let current_temp_path = current_path.with_extension("tmp");
        let mut pause = crate::storage::pause_atomic_write_before_rename(current_path);
        let mut append = Box::pin(table.append(time_series_batch(vec![0], vec!["A"], vec![1.0])?));

        tokio::select! {
            () = pause.wait_until_paused() => {}
            result = &mut append => panic!("append completed before cancellation: {result:?}"),
        }

        assert!(commit_path.is_file());
        assert!(current_temp_path.is_file());
        assert_eq!(std::fs::read_dir(temp.path().join("data"))?.count(), 1);
        assert_eq!(coverage_files(temp.path())?.len(), 2);
        let before_publication = TimeSeriesTable::open(location.clone()).await?;
        assert_eq!(before_publication.state().version, 1);
        assert!(before_publication.state().segments.is_empty());

        drop(append);
        pause.release();

        assert!(!commit_path.exists());
        assert!(!current_temp_path.exists());
        assert_eq!(std::fs::read_dir(temp.path().join("data"))?.count(), 0);
        assert!(coverage_files(temp.path())?.is_empty());
        assert_eq!(table.state().version, 1);
        assert!(table.state().segments.is_empty());
        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state().version, 1);
        assert!(reopened.state().segments.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn simultaneous_streaming_appends_publish_one_and_clean_loser() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut winner = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let mut loser = TimeSeriesTable::open(location.clone()).await?;
        let loser_state_before = loser.state().clone();
        let commit_path = temp.path().join(layout::commit_rel_path(2));
        let mut pause = crate::storage::pause_atomic_write_before_rename(
            temp.path().join(layout::current_rel_path()),
        );
        let mut winner_append =
            Box::pin(winner.append(time_series_batch(vec![0], vec!["A"], vec![1.0])?));

        tokio::select! {
            () = pause.wait_until_paused() => {}
            result = &mut winner_append => panic!("winner completed before race: {result:?}"),
        }

        let mut data_before = std::fs::read_dir(temp.path().join("data"))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?;
        data_before.sort();
        let coverage_before = coverage_files(temp.path())?;
        let commit_before = std::fs::read(&commit_path)?;

        let loser_error = loser
            .append(time_series_batch(vec![120_000], vec!["A"], vec![2.0])?)
            .await
            .expect_err("second simultaneous writer must lose commit 2");
        assert!(matches!(
            loser_error,
            TableError::TransactionLog {
                source: CommitError::Storage {
                    source: StorageError::AlreadyExists { .. },
                },
            }
        ));
        assert_eq!(loser.state(), &loser_state_before);

        let mut data_after = std::fs::read_dir(temp.path().join("data"))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?;
        data_after.sort();
        assert_eq!(data_after, data_before);
        assert_eq!(coverage_files(temp.path())?, coverage_before);
        assert_eq!(std::fs::read(&commit_path)?, commit_before);

        pause.release();
        assert_eq!(winner_append.await?, 2);
        assert_eq!(winner.state().version, 2);
        assert_eq!(winner.state().segments.len(), 1);
        assert!(!temp.path().join(layout::commit_rel_path(3)).exists());

        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state().version, 2);
        assert_eq!(reopened.state().segments.len(), 1);
        assert_eq!(
            reopened
                .state()
                .segments
                .values()
                .map(|segment| segment.row_count)
                .sum::<u64>(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn guarded_output_cleans_up_parquet_writer_creation_failure() -> TestResult {
        let temp = TempDir::new()?;
        let location = StorageLocation::local(temp.path());
        let path = Path::new("data/create-failure.parquet");
        let sink = storage::open_new_output_sink(&location, path).await?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "unsupported",
            DataType::Struct(arrow::datatypes::Fields::empty()),
            true,
        )]));

        assert!(ParquetArrowWriter::try_new(sink, schema, None).is_err());
        assert!(!temp.path().join(path).exists());
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_preserves_writer_error_and_failed_cleanup_path() -> TestResult {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta())
                .await?;
        let data_dir = temp.path().join("data");
        crate::storage::inject_output_write_failure(data_dir.clone(), 1);
        crate::storage::inject_cleanup_failure(data_dir.clone());

        let error = table
            .append(timestamp_only_batch(1)?)
            .await
            .expect_err("writer and cleanup failures must fail append");
        let TableError::AppendRollback {
            source,
            cleanup_errors,
        } = error
        else {
            panic!("unexpected error: {error}");
        };
        assert!(matches!(*source, TableError::AppendParquet { .. }));
        assert_eq!(cleanup_errors.len(), 1);
        let remaining = std::fs::read_dir(&data_dir)?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(remaining.len(), 1);
        let remaining_path = remaining[0].path();
        assert!(
            cleanup_errors[0].contains(
                remaining_path
                    .file_name()
                    .expect("remaining data filename")
                    .to_str()
                    .expect("UTF-8 data filename")
            )
        );
        assert_eq!(table.state().version, 1);
        assert!(table.state().segments.is_empty());
        assert!(coverage_files(temp.path())?.is_empty());
        assert!(!temp.path().join(layout::commit_rel_path(2)).exists());
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_conflict_cleans_attempt_and_stays_invisible() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut winner = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let mut loser = TimeSeriesTable::open(location.clone()).await?;
        let loser_state_before = loser.state().clone();

        winner
            .append(time_series_batch(vec![0], vec!["A"], vec![1.0])?)
            .await?;
        let data_before = data_files(temp.path())?;
        let coverage_before = coverage_files(temp.path())?;

        let error = loser
            .append(time_series_batch(vec![120_000], vec!["A"], vec![2.0])?)
            .await
            .expect_err("stale streaming append must conflict");

        assert!(matches!(
            error,
            TableError::TransactionLog {
                source: CommitError::Conflict {
                    expected: 1,
                    found: 2,
                    ..
                }
            }
        ));
        assert_eq!(loser.state(), &loser_state_before);
        assert_eq!(data_files(temp.path())?, data_before);
        assert_eq!(coverage_files(temp.path())?, coverage_before);
        assert!(!temp.path().join(layout::commit_rel_path(3)).exists());

        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state().version, 2);
        assert_eq!(reopened.state().segments.len(), 1);
        assert_eq!(
            reopened
                .state()
                .segments
                .values()
                .next()
                .map(|segment| { temp.path().join(&segment.path) }),
            data_before.first().cloned()
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_ambiguous_commit_reports_and_preserves_generated_path() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let commit_path = temp.path().join(layout::commit_rel_path(2));
        crate::storage::inject_write_new_failure(commit_path.clone(), true);
        let capture = TraceCapture::default();

        let error = capture
            .run(table.append(time_series_batch(vec![0], vec!["A"], vec![1.0])?))
            .await
            .expect_err("failed commit cleanup must be ambiguous");
        let TableError::AppendCommitAmbiguous {
            segment_path,
            source,
        } = error
        else {
            panic!("unexpected error: {error}");
        };

        assert!(matches!(source, CommitError::AmbiguousOutcome { .. }));
        assert!(segment_path.starts_with("data/"));
        assert!(temp.path().join(&segment_path).is_file());
        assert_eq!(
            data_files(temp.path())?,
            vec![temp.path().join(&segment_path)]
        );
        assert_eq!(table.state().version, 1);
        assert!(table.state().segments.is_empty());
        assert_eq!(table.log.load_current_version().await?, 1);
        assert!(commit_path.exists());
        assert_eq!(coverage_files(temp.path())?.len(), 2);
        let append_span = capture
            .spans()
            .into_iter()
            .find(|span| span.name == "table.append")
            .expect("table.append span");
        assert_eq!(append_span.fields.get("segment_path"), Some(&segment_path));
        assert_eq!(
            append_span.fields.get("outcome").map(String::as_str),
            Some("ambiguous")
        );

        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state().version, 1);
        assert!(reopened.state().segments.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_conflict_reports_every_failed_cleanup_path() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut winner = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let mut loser = TimeSeriesTable::open(location.clone()).await?;
        winner
            .append(time_series_batch(vec![0], vec!["A"], vec![1.0])?)
            .await?;
        let data_before = data_files(temp.path())?;
        let coverage_before = coverage_files(temp.path())?;

        for dir in [
            Path::new("data"),
            Path::new(layout::SEGMENT_COVERAGE_DIR),
            Path::new(layout::TABLE_SNAPSHOT_DIR),
        ] {
            crate::storage::inject_cleanup_failure(temp.path().join(dir));
        }
        let error = loser
            .append(time_series_batch(vec![120_000], vec!["A"], vec![2.0])?)
            .await
            .expect_err("conflict cleanup failures must be reported");
        let message = error.to_string();

        assert!(message.contains("Commit conflict"));
        let data_after = data_files(temp.path())?;
        assert_eq!(data_after.len(), data_before.len() + 1);
        let coverage_after = coverage_files(temp.path())?;
        assert_eq!(coverage_after.len(), coverage_before.len() + 2);
        let mut failed_paths = data_after
            .iter()
            .filter(|path| !data_before.contains(path))
            .cloned()
            .collect::<Vec<_>>();
        failed_paths.extend(
            coverage_after
                .keys()
                .filter(|path| !coverage_before.contains_key(*path))
                .map(|path| temp.path().join(path)),
        );
        for path in failed_paths {
            assert!(
                message.contains(
                    path.file_name()
                        .expect("failed cleanup filename")
                        .to_str()
                        .expect("UTF-8 cleanup filename")
                ),
                "missing cleanup diagnostic for {}",
                path.display()
            );
        }

        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state().version, 2);
        assert_eq!(reopened.state().segments.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_validates_schema_before_creating_data() -> TestResult {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), make_basic_table_meta())
                .await?;

        assert!(matches!(
            table.append(input_batch(vec![1])?).await,
            Err(TableError::SchemaCompatibility { .. })
        ));
        assert_eq!(table.state().version, 1);
        assert!(!temp.path().join("data").exists());
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_removes_owned_data_after_overlap() -> TestResult {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), make_basic_table_meta())
                .await?;
        let batch = time_series_batch(vec![0], vec!["A"], vec![1.0])?;
        assert_eq!(table.append(batch.clone()).await?, 2);

        assert!(matches!(
            table.append(batch).await,
            Err(TableError::EntityCoverageOverlap { .. })
        ));
        assert_eq!(table.state().version, 2);
        assert_eq!(
            std::fs::read_dir(temp.path().join("data"))?
                .collect::<Result<Vec<_>, _>>()?
                .len(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_stream_adopts_schema_on_first_append() -> TestResult {
        let temp = TempDir::new()?;
        let mut meta = make_basic_table_meta();
        meta.logical_schema = None;
        let mut table = TimeSeriesTable::create(TableLocation::local(temp.path()), meta).await?;

        let batch = time_series_batch(vec![0], vec!["A"], vec![1.0])?;
        assert_eq!(table.append(batch).await?, 2);
        assert!(table.state().table_meta.logical_schema.is_some());
        Ok(())
    }

    fn registered_index(kind: IndexKind) -> IndexSpec {
        IndexSpec {
            column: "ts".to_string(),
            entity_columns: Vec::new(),
            kind,
        }
    }

    fn write_single_index_parquet(
        path: &Path,
        data_type: DataType,
        values: ArrayRef,
    ) -> TestResult {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![Field::new("ts", data_type, false)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values])?;
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn write_composite_entity_parquet(path: &Path, rows: &[(i64, &str, &str, f64)]) -> TestResult {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampMillisecondArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|row| row.3).collect::<Vec<_>>(),
                )),
            ],
        )?;
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn coverage_files(root: &Path) -> std::io::Result<BTreeMap<PathBuf, Vec<u8>>> {
        let mut files = BTreeMap::new();
        for rel_dir in [layout::SEGMENT_COVERAGE_DIR, layout::TABLE_SNAPSHOT_DIR] {
            let dir = root.join(rel_dir);
            if !dir.exists() {
                continue;
            }
            for entry in std::fs::read_dir(dir)? {
                let path = entry?.path();
                if path.is_file() {
                    files.insert(
                        path.strip_prefix(root)
                            .expect("coverage path under root")
                            .to_owned(),
                        std::fs::read(path)?,
                    );
                }
            }
        }
        Ok(files)
    }

    fn data_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
        let dir = root.join("data");
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = std::fs::read_dir(dir)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?;
        files.sort();
        Ok(files)
    }

    fn assert_append_diagnostics(
        capture: &TraceCapture,
        source_mode: &str,
        segment_path: &str,
        file_size: u64,
        absolute_root: &str,
    ) {
        let spans: Vec<_> = capture
            .spans()
            .into_iter()
            .filter(|span| span.name == "table.append")
            .collect();
        assert_eq!(spans.len(), 1, "expected one table.append span");
        assert_eq!(spans[0].level, tracing::Level::DEBUG);
        for (field, expected) in [
            ("source_mode", source_mode.to_string()),
            ("expected_version", "1".to_string()),
            ("segment_path", segment_path.to_string()),
            ("row_count", "1".to_string()),
            ("file_size_bytes", file_size.to_string()),
            ("committed_version", "2".to_string()),
            ("entity_layout", "single".to_string()),
            ("outcome", "succeeded".to_string()),
        ] {
            assert_eq!(
                spans[0].fields.get(field),
                Some(&expected),
                "unexpected table.append.{field}"
            );
        }

        let events: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| event.name == "table.append")
            .collect();
        assert_eq!(events.len(), 1, "expected one table.append event");
        assert_eq!(events[0].level, tracing::Level::INFO);
        for (field, expected) in [
            ("source_mode", source_mode),
            ("expected_version", "1"),
            ("committed_version", "2"),
            ("row_count", "1"),
            ("entity_layout", "single"),
            ("outcome", "succeeded"),
        ] {
            assert_eq!(
                events[0].fields.get(field).map(String::as_str),
                Some(expected),
                "unexpected table.append event field {field}"
            );
        }
        assert!(
            events[0]
                .fields
                .get("message")
                .is_some_and(|message| message.contains("Appended Parquet segment"))
        );

        for value in spans
            .into_iter()
            .flat_map(|span| span.fields.into_values())
            .chain(
                events
                    .into_iter()
                    .flat_map(|event| event.fields.into_values()),
            )
        {
            assert!(
                !value.contains(absolute_root),
                "diagnostic value contains absolute table path: {value}"
            );
            for forbidden in ["SECRET_ENTITY", "LogicalSchema", "RecordBatch"] {
                assert!(
                    !value.contains(forbidden),
                    "diagnostic value contains sensitive data '{forbidden}': {value}"
                );
            }
        }
    }

    #[test]
    fn entity_layout_classification_rejects_empty_coverage() {
        assert!(matches!(
            classify_entity_layout("data/empty.parquet", &EntityCoverage::empty()),
            Err(TableError::EmptySegmentEntityCoverage { segment_path })
                if segment_path == "data/empty.parquet"
        ));
    }

    #[tokio::test]
    async fn append_parquet_segment_missing_time_column_errors() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel = "data/seg-no-ts.parquet";
        let path = tmp.path().join(rel);
        write_parquet_without_time_column(&path, &["A"], &[1.0])?;

        let err = table
            .append_parquet_segment(rel)
            .await
            .expect_err("expected missing time column");

        match err {
            TableError::SegmentMeta { source } => {
                assert!(matches!(
                    source,
                    SegmentError::Meta {
                        source: SegmentMetaError::OrderedIndexColumn {
                            source: ParquetIndexColumnError {
                                expected_domain: "timestamp",
                                observed_type,
                                ..
                            }
                        }
                    } if observed_type == "missing",
                ));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn entity_aware_validation_failure_leaves_no_state_or_sidecars() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        let location = TableLocation::local(&table_root);
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let state_before = table.state.clone();
        let coverage_before = coverage_files(&table_root)?;
        let source = tmp.path().join("wrong-time-column.parquet");
        write_parquet_without_time_column(&source, &["A"], &[1.0])?;
        let source_bytes = std::fs::read(&source)?;

        let capture = TraceCapture::default();
        let err = capture
            .run(table.append_parquet_from_path(&source))
            .await
            .expect_err("missing time column should fail");

        assert!(matches!(err, TableError::SegmentMeta { .. }));
        assert!(!table_root.join("data/wrong-time-column.parquet").exists());
        assert_eq!(std::fs::read(source)?, source_bytes);
        assert_eq!(table.state, state_before);
        assert_eq!(table.log.load_current_version().await?, 1);
        assert_eq!(coverage_files(&table_root)?, coverage_before);
        assert!(!table_root.join(layout::commit_rel_path(2)).exists());
        let spans: Vec<_> = capture
            .spans()
            .into_iter()
            .filter(|span| span.name == "table.append")
            .collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].fields.get("source_mode").map(String::as_str),
            Some("external_path")
        );
        assert_eq!(
            spans[0].fields.get("outcome").map(String::as_str),
            Some("failed")
        );
        assert!(
            !capture
                .events()
                .iter()
                .any(|event| event.name == "table.append")
        );
        Ok(())
    }

    #[tokio::test]
    async fn every_public_append_variant_emits_one_logical_operation() -> TestResult {
        let tmp = TempDir::new()?;

        for (case, external, with_report) in [
            ("segment", false, false),
            ("external", true, false),
            ("segment-report", false, true),
            ("external-report", true, true),
        ] {
            let table_root = tmp.path().join(format!("table-{case}"));
            let location = TableLocation::local(&table_root);
            let mut table = TimeSeriesTable::create(location, make_basic_table_meta()).await?;
            let filename = format!("{case}.parquet");
            let segment_path = format!("data/{filename}");
            let source = if external {
                tmp.path().join(&filename)
            } else {
                table_root.join(&segment_path)
            };
            write_test_parquet(
                &source,
                true,
                false,
                &[TestRow {
                    ts_millis: 1_000,
                    symbol: "SECRET_ENTITY",
                    price: 10.0,
                }],
            )?;
            let file_size = source.metadata()?.len();
            let capture = TraceCapture::default();

            let report = match (external, with_report) {
                (false, false) => {
                    assert_eq!(
                        capture
                            .run(table.append_parquet_segment(&segment_path))
                            .await?,
                        2
                    );
                    None
                }
                (true, false) => {
                    let (version, actual_path) =
                        capture.run(table.append_parquet_from_path(&source)).await?;
                    assert_eq!(version, 2);
                    assert_eq!(actual_path, segment_path);
                    None
                }
                (false, true) => {
                    let (version, report) = capture
                        .run(table.append_parquet_segment_with_report(&segment_path))
                        .await?;
                    assert_eq!(version, 2);
                    Some(report)
                }
                (true, true) => {
                    let (version, actual_path, report) = capture
                        .run(table.append_parquet_from_path_with_report(&source))
                        .await?;
                    assert_eq!(version, 2);
                    assert_eq!(actual_path, segment_path);
                    Some(report)
                }
            };

            if let Some(report) = report {
                assert!(
                    report
                        .context
                        .iter()
                        .any(|(key, value)| { key == "relative_path" && value == &segment_path })
                );
                assert!(!report.steps.is_empty());
            }
            assert_eq!(table.state.version, 2);
            assert_append_diagnostics(
                &capture,
                if external {
                    "external_path"
                } else {
                    "table_relative"
                },
                &segment_path,
                file_size,
                &tmp.path().display().to_string(),
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_from_path_preserves_failed_in_root_file() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location, make_basic_table_meta()).await?;
        let source = tmp.path().join("data/in-root-invalid.parquet");
        write_parquet_without_time_column(&source, &["A"], &[1.0])?;
        let source_bytes = std::fs::read(&source)?;

        let err = table
            .append_parquet_from_path(&source)
            .await
            .expect_err("missing time column should fail");

        assert!(matches!(err, TableError::SegmentMeta { .. }));
        assert_eq!(std::fs::read(source)?, source_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_from_path_retains_successful_external_copy() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        let location = TableLocation::local(&table_root);
        let mut table = TimeSeriesTable::create(location, make_basic_table_meta()).await?;
        let source = tmp.path().join("external-success.parquet");
        write_test_parquet(
            &source,
            true,
            false,
            &[TestRow {
                ts_millis: 10_000,
                symbol: "X",
                price: 100.0,
            }],
        )?;
        let source_bytes = std::fs::read(&source)?;

        let (version, relative_path) = table.append_parquet_from_path(&source).await?;

        assert_eq!(version, 2);
        assert_eq!(relative_path, "data/external-success.parquet");
        assert_eq!(
            std::fs::read(table_root.join(&relative_path))?,
            source_bytes
        );
        assert_eq!(std::fs::read(source)?, source_bytes);
        assert!(table.state.segments.contains_key(&relative_path));
        Ok(())
    }

    #[tokio::test]
    async fn entity_aware_ambiguous_commit_retains_copy_and_sidecars() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        let location = TableLocation::local(&table_root);
        let mut table = TimeSeriesTable::create(location, make_basic_table_meta()).await?;
        let state_before = table.state.clone();
        let coverage_before = coverage_files(&table_root)?;
        let source = tmp.path().join("ambiguous-external.parquet");
        write_test_parquet(
            &source,
            true,
            false,
            &[TestRow {
                ts_millis: 10_000,
                symbol: "X",
                price: 100.0,
            }],
        )?;
        let source_bytes = std::fs::read(&source)?;
        let commit_path = table_root.join(layout::commit_rel_path(2));
        crate::storage::inject_write_new_failure(commit_path.clone(), true);

        let err = table
            .append_parquet_from_path(&source)
            .await
            .expect_err("commit outcome should be ambiguous");

        assert!(matches!(
            err,
            TableError::TransactionLog {
                source: CommitError::AmbiguousOutcome { .. }
            }
        ));
        assert_eq!(
            std::fs::read(table_root.join("data/ambiguous-external.parquet"))?,
            source_bytes
        );
        assert_eq!(std::fs::read(source)?, source_bytes);
        assert_eq!(table.state, state_before);
        assert_eq!(table.log.load_current_version().await?, 1);
        assert!(commit_path.exists());
        let coverage_after = coverage_files(&table_root)?;
        assert_eq!(coverage_after.len(), coverage_before.len() + 2);
        for bytes in coverage_after.values() {
            entity_coverage_from_bytes(bytes)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_from_path_reports_copy_rollback_failure() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        let location = TableLocation::local(&table_root);
        let mut table = TimeSeriesTable::create(location, make_basic_table_meta()).await?;
        let source = tmp.path().join("rollback-cleanup.parquet");
        write_parquet_without_time_column(&source, &["A"], &[1.0])?;
        let destination = table_root.join("data/rollback-cleanup.parquet");
        crate::storage::inject_cleanup_failure(destination.clone());

        let err = table
            .append_parquet_from_path(&source)
            .await
            .expect_err("copy rollback should fail");
        let message = err.to_string();

        assert!(matches!(
            err,
            TableError::ExternalParquetRollback {
                path,
                source,
                cleanup_error: StorageError::OtherIo { .. },
            } if path.contains("rollback-cleanup.parquet")
                && matches!(*source, TableError::SegmentMeta { .. })
        ));
        assert!(message.contains("rollback-cleanup.parquet"));
        assert!(message.contains("injected cleanup failure"));
        assert!(destination.exists());
        tokio::fs::remove_file(destination).await?;
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_updates_state_and_log() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();

        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel_path = "data/seg1.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_test_parquet(
            &abs_path,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;

        let new_version = table.append_parquet_segment(rel_path).await?;

        assert_eq!(new_version, 2);
        assert_eq!(table.state.version, 2);
        let seg = table.state.segments.get(rel_path).expect("segment present");
        assert_eq!(seg.path, rel_path);
        assert_eq!(seg.row_count, 1);
        assert_eq!(
            seg.entity_layout,
            SegmentEntityLayout::Single(EntityIdentity::try_new(vec!["A".into()])?)
        );
        assert!(matches!(
            &seg.index_min,
            IndexValue::Timestamp(value) if value.timestamp_millis() == 1_000
        ));
        assert!(matches!(
            &seg.index_max,
            IndexValue::Timestamp(value) if value.timestamp_millis() == 1_000
        ));

        let commit_path = tmp.path().join(layout::commit_rel_path(2));
        assert!(commit_path.is_file());
        let current =
            tokio::fs::read_to_string(tmp.path().join(layout::current_rel_path())).await?;
        assert_eq!(current.trim(), "2");

        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state.segments.get(rel_path), Some(seg));
        Ok(())
    }

    #[tokio::test]
    async fn version_six_no_entity_int64_append_uses_global_coverage() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let index = registered_index(IndexKind::Int64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        });
        let mut table =
            TimeSeriesTable::create(location.clone(), TableMeta::new_time_series(index.clone()))
                .await?;
        let rel_path = "data/int64.parquet";
        write_arrow_parquet_int_time(
            &tmp.path().join(rel_path),
            &[i64::MIN, -1, 0, i64::MAX],
            &["A", "A", "A", "A"],
            &[1.0, 2.0, 3.0, 4.0],
        )?;

        assert_eq!(table.append_parquet_segment(rel_path).await?, 2);

        let segment = table.state.segments.get(rel_path).expect("segment present");
        assert_eq!(segment.entity_layout, SegmentEntityLayout::NotApplicable);
        assert_eq!(segment.index_min, IndexValue::Int64(i64::MIN));
        assert_eq!(segment.index_max, IndexValue::Int64(i64::MAX));
        let pointer = table.state.table_coverage.as_ref().expect("table coverage");
        assert_eq!(pointer.index_kind, index.kind);
        let persisted = read_coverage_sidecar(&location, Path::new(&pointer.coverage_path)).await?;
        let expected =
            compute_segment_coverage(&location, Path::new(rel_path), table.index_spec()).await?;
        assert_eq!(persisted, expected);
        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state, table.state);
        Ok(())
    }

    #[tokio::test]
    async fn int64_appends_enforce_coverage_and_exact_later_schema() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let index = registered_index(IndexKind::Int64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        });
        let mut table =
            TimeSeriesTable::create(location, TableMeta::new_time_series(index)).await?;

        for (path, values) in [
            ("data/negative.parquet", &[-25, -15][..]),
            ("data/positive.parquet", &[5, 15][..]),
        ] {
            write_arrow_parquet_int_time(&tmp.path().join(path), values, &["A", "A"], &[1.0, 2.0])?;
            table.append_parquet_segment(path).await?;
        }
        assert_eq!(table.state.version, 3);

        let state_before = table.state.clone();
        let coverage_before = coverage_files(tmp.path())?;
        let overlap_path = "data/negative-overlap.parquet";
        write_arrow_parquet_int_time(&tmp.path().join(overlap_path), &[-19], &["A"], &[3.0])?;
        let overlap_error = table
            .append_parquet_segment(overlap_path)
            .await
            .expect_err("negative bucket overlap must fail");
        assert!(matches!(
            &overlap_error,
            TableError::CoverageOverlap {
                example_bucket_range,
                ..
            } if example_bucket_range.to_string() == "[-20, -10)"
        ));
        assert!(
            overlap_error
                .to_string()
                .contains("example_bucket_range=[-20, -10)")
        );

        let mismatch_path = "data/schema-mismatch.parquet";
        write_single_index_parquet(
            &tmp.path().join(mismatch_path),
            DataType::Int64,
            Arc::new(Int64Array::from(vec![100])),
        )?;
        assert!(matches!(
            table
                .append_parquet_segment(mismatch_path)
                .await
                .expect_err("later schema mismatch must fail"),
            TableError::SegmentSchemaCompatibility { .. }
        ));
        assert_eq!(table.state, state_before);
        assert_eq!(coverage_files(tmp.path())?, coverage_before);
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_supports_registered_uint64_index() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let index = registered_index(IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        });
        let mut table =
            TimeSeriesTable::create(location.clone(), TableMeta::new_time_series(index.clone()))
                .await?;
        let rel_path = "data/uint64.parquet";
        write_single_index_parquet(
            &tmp.path().join(rel_path),
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![0, i64::MAX as u64 + 1, u64::MAX])),
        )?;

        assert_eq!(table.append_parquet_segment(rel_path).await?, 2);

        let segment = table.state.segments.get(rel_path).expect("segment present");
        assert_eq!(segment.index_min, IndexValue::UInt64(0));
        assert_eq!(segment.index_max, IndexValue::UInt64(u64::MAX));
        assert_eq!(
            table
                .state
                .table_meta
                .logical_schema
                .as_ref()
                .expect("schema adopted")
                .columns()[0]
                .data_type,
            LogicalDataType::UInt64
        );
        assert_eq!(
            table
                .state
                .table_coverage
                .as_ref()
                .expect("table coverage")
                .index_kind,
            index.kind
        );

        let non_overlap_path = "data/uint64-non-overlap.parquet";
        write_single_index_parquet(
            &tmp.path().join(non_overlap_path),
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![u64::MAX - 20])),
        )?;
        assert_eq!(table.append_parquet_segment(non_overlap_path).await?, 3);

        let state_before = table.state.clone();
        let coverage_before = coverage_files(tmp.path())?;
        let overlap_path = "data/uint64-overlap.parquet";
        write_single_index_parquet(
            &tmp.path().join(overlap_path),
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![u64::MAX - 1])),
        )?;
        let overlap_error = table
            .append_parquet_segment(overlap_path)
            .await
            .expect_err("large uint64 bucket overlap must fail");
        assert!(matches!(
            overlap_error,
            TableError::CoverageOverlap {
                example_bucket_range,
                ..
            } if example_bucket_range.to_string()
                == "[18446744073709551610, 18446744073709551615]"
        ));
        assert_eq!(table.state, state_before);
        assert_eq!(coverage_files(tmp.path())?, coverage_before);
        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state, table.state);
        Ok(())
    }

    #[tokio::test]
    async fn append_rejects_signed_data_for_uint64_index_without_mutation() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let index = registered_index(IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        });
        let mut table =
            TimeSeriesTable::create(location.clone(), TableMeta::new_time_series(index)).await?;
        let state_before = table.state.clone();
        let coverage_before = coverage_files(tmp.path())?;
        let rel_path = "data/signed.parquet";
        write_arrow_parquet_int_time(&tmp.path().join(rel_path), &[1], &["A"], &[1.0])?;

        let error = table
            .append_parquet_segment(rel_path)
            .await
            .expect_err("signed data must not append to a uint64 index");

        assert!(matches!(
            error,
            TableError::SegmentMeta {
                source: SegmentError::Meta {
                    source: SegmentMetaError::OrderedIndexColumn {
                        source: ParquetIndexColumnError {
                            expected_domain: "uint64",
                            observed_type,
                            ..
                        }
                    }
                }
            } if observed_type.contains("logical=None")
        ));
        assert_eq!(table.state, state_before);
        assert_eq!(table.log.load_current_version().await?, 1);
        assert_eq!(coverage_files(tmp.path())?, coverage_before);
        Ok(())
    }

    #[tokio::test]
    async fn append_inspects_file_without_reading_unrelated_column_data() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let rel_path = "data/corrupt-price.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_test_parquet(
            &abs_path,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;

        let reader = SerializedFileReader::new(File::open(&abs_path)?)?;
        let price_page = reader.metadata().row_group(0).column(2).data_page_offset() as u64;
        drop(reader);
        let mut file = OpenOptions::new().read(true).write(true).open(&abs_path)?;
        file.seek(SeekFrom::Start(price_page))?;
        file.write_all(&[0xFF; 16])?;
        file.flush()?;
        drop(file);

        let file_size = std::fs::metadata(&abs_path)?.len().to_string();
        let (version, report) = table.append_parquet_segment_with_report(rel_path).await?;

        assert_eq!(version, 2);
        assert_eq!(
            report.context,
            vec![
                ("relative_path".to_string(), rel_path.to_string()),
                ("index_column".to_string(), "ts".to_string()),
                ("file_size_bytes".to_string(), file_size),
            ]
        );
        assert_eq!(
            report
                .steps
                .iter()
                .map(|step| step.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "segment_meta",
                "logical_schema",
                "load_table_snapshot",
                "segment_coverage",
                "overlap_check",
                "write_segment_sidecar",
                "write_snapshot_sidecar",
                "commit_log",
                "state_update",
            ]
        );
        assert_eq!(
            report.steps[0]
                .fields
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            vec!["row_groups", "row_count", "used_stats", "scanned_rows"]
        );
        assert!(report.steps[1..].iter().all(|step| step.fields.is_empty()));
        Ok(())
    }

    #[tokio::test]
    async fn version_six_records_single_layout_for_each_entity_segment() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        for (path, symbol) in [
            ("data/entity-a.parquet", "A"),
            ("data/entity-b.parquet", "B"),
        ] {
            write_test_parquet(
                &tmp.path().join(path),
                true,
                false,
                &[TestRow {
                    ts_millis: 1_000,
                    symbol,
                    price: 10.0,
                }],
            )?;
            table.append_parquet_segment(path).await?;
            assert_eq!(
                table
                    .state
                    .segments
                    .get(path)
                    .expect("segment present")
                    .entity_layout,
                SegmentEntityLayout::Single(EntityIdentity::try_new(vec![symbol.into()])?)
            );
        }

        assert_eq!(table.state.version, 3);
        let pointer = table
            .state
            .table_coverage
            .as_ref()
            .expect("table coverage pointer");
        let coverage =
            read_entity_coverage_sidecar(&location, Path::new(&pointer.coverage_path)).await?;
        assert_eq!(coverage.identity_count(), 2);
        assert_eq!(coverage.cardinality(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn version_six_records_mixed_layout_for_multiple_identities() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let path = "data/multiple-identities.parquet";
        write_test_parquet(
            &tmp.path().join(path),
            true,
            false,
            &[
                TestRow {
                    ts_millis: 1_000,
                    symbol: "A",
                    price: 10.0,
                },
                TestRow {
                    ts_millis: 1_000,
                    symbol: "B",
                    price: 20.0,
                },
            ],
        )?;

        table.append_parquet_segment(path).await?;

        let segment = table.state.segments.get(path).expect("segment present");
        assert_eq!(segment.entity_layout, SegmentEntityLayout::Mixed);
        let coverage = read_entity_coverage_sidecar(
            &location,
            Path::new(segment.coverage_path.as_ref().expect("coverage path")),
        )
        .await?;
        assert_eq!(coverage.identity_count(), 2);
        assert_eq!(coverage.cardinality(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn numeric_entities_append_overlap_and_recover_with_exact_types() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table =
            TimeSeriesTable::create(location.clone(), make_int32_entity_table_meta()).await?;

        let negative_path = "data/negative-device.parquet";
        write_int32_entity_parquet(
            &tmp.path().join(negative_path),
            &[1_000, 61_000],
            &[-1, -1],
            &[10.0, 11.0],
        )?;
        table.append_parquet_segment(negative_path).await?;
        let negative_identity = EntityIdentity::try_new(vec![EntityValue::Int32(-1)])?;
        assert_eq!(
            table.state.segments[negative_path].entity_layout,
            SegmentEntityLayout::Single(negative_identity.clone())
        );

        let maximum_path = "data/maximum-device.parquet";
        write_int32_entity_parquet(
            &tmp.path().join(maximum_path),
            &[1_000],
            &[i32::MAX],
            &[20.0],
        )?;
        table.append_parquet_segment(maximum_path).await?;
        assert_eq!(
            table.state.segments[maximum_path].entity_layout,
            SegmentEntityLayout::Single(EntityIdentity::try_new(vec![EntityValue::Int32(
                i32::MAX,
            )])?)
        );

        let overlap_path = "data/negative-overlap.parquet";
        write_int32_entity_parquet(&tmp.path().join(overlap_path), &[1_500], &[-1], &[12.0])?;
        let error = table
            .append_parquet_segment(overlap_path)
            .await
            .expect_err("same typed identity and bucket must overlap");
        assert!(matches!(
            error,
            TableError::EntityCoverageOverlap {
                overlap_count: 1,
                example_identity,
                ..
            } if example_identity == negative_identity
        ));

        let snapshot = table.load_table_entity_snapshot_coverage_readonly().await?;
        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state(), table.state());
        assert_eq!(
            reopened
                .recover_table_entity_coverage_from_segments()
                .await?,
            snapshot
        );
        Ok(())
    }

    #[tokio::test]
    async fn version_six_preserves_composite_identity_order_in_layout() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let index = IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string(), "venue".to_string()],
            kind: IndexKind::Timestamp {
                bucket: TimeBucket::Minutes(1),
                timezone: None,
            },
        };
        let schema = LogicalSchema::new(vec![
            LogicalField {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Millis,
                    timezone: None,
                },
                nullable: false,
            },
            LogicalField {
                name: "symbol".to_string(),
                data_type: LogicalDataType::Utf8,
                nullable: false,
            },
            LogicalField {
                name: "venue".to_string(),
                data_type: LogicalDataType::Utf8,
                nullable: false,
            },
            LogicalField {
                name: "price".to_string(),
                data_type: LogicalDataType::Float64,
                nullable: false,
            },
        ])?;
        let mut table = TimeSeriesTable::create(
            location,
            TableMeta::new_time_series_with_schema(index, schema),
        )
        .await?;

        for (path, venue) in [
            ("data/composite-x.parquet", "X"),
            ("data/composite-y.parquet", "Y"),
        ] {
            write_composite_entity_parquet(&tmp.path().join(path), &[(1_000, "A", venue, 10.0)])?;
            table.append_parquet_segment(path).await?;
            assert_eq!(
                table
                    .state
                    .segments
                    .get(path)
                    .expect("segment present")
                    .entity_layout,
                SegmentEntityLayout::Single(EntityIdentity::try_new(vec![
                    "A".into(),
                    venue.into(),
                ])?)
            );
        }

        let overlap_path = "data/composite-x-overlap.parquet";
        write_composite_entity_parquet(&tmp.path().join(overlap_path), &[(1_500, "A", "X", 20.0)])?;
        let error = table
            .append_parquet_segment(overlap_path)
            .await
            .expect_err("matching composite identity and bucket must overlap");
        assert!(matches!(
            error,
            TableError::EntityCoverageOverlap {
                overlap_count: 1,
                example_identity,
                ..
            } if example_identity.components()
                == [EntityValue::from("A"), EntityValue::from("X")]
        ));
        Ok(())
    }

    #[tokio::test]
    async fn entity_with_only_null_index_values_is_rejected() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(
            location,
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        let state_before = table.state.clone();
        let path = "data/entity-without-index-coverage.parquet";
        write_arrow_parquet_with_unit(
            &tmp.path().join(path),
            ArrowTimeUnit::Millisecond,
            &[Some(1_000), None],
            &["A", "B"],
            &[10.0, 20.0],
        )?;

        let error = table
            .append_parquet_segment(path)
            .await
            .expect_err("identity without index coverage must be rejected");

        match error {
            TableError::EntityWithoutIndexCoverage {
                segment_path,
                identity,
            } => {
                assert_eq!(segment_path, path);
                assert_eq!(identity.components(), [EntityValue::from("B")]);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(table.state, state_before);
        assert!(coverage_files(tmp.path())?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_adopts_schema_when_missing() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let index = IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec![],
            kind: IndexKind::Timestamp {
                bucket: TimeBucket::Minutes(1),
                timezone: None,
            },
        };
        let meta = TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: None,
            created_at: utc_datetime(2025, 1, 1, 0, 0, 0),
            format_version: TABLE_FORMAT_VERSION,
        };

        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel_path = "data/seg-adopt.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_test_parquet(
            &abs_path,
            true,
            false,
            &[TestRow {
                ts_millis: 5_000,
                symbol: "B",
                price: 20.0,
            }],
        )?;

        let new_version = table.append_parquet_segment(rel_path).await?;

        assert_eq!(new_version, 2);
        let schema = table
            .state
            .table_meta
            .logical_schema
            .as_ref()
            .expect("schema adopted");
        let names: Vec<_> = schema.columns().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["ts", "symbol", "price"]);
        let ts_col = &schema.columns()[0];
        assert_eq!(
            ts_col.data_type,
            LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Millis,
                timezone: None,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_rejects_schema_mismatch() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel_path = "data/seg-missing-symbol.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_test_parquet(
            &abs_path,
            false,
            false,
            &[TestRow {
                ts_millis: 10_000,
                symbol: "C",
                price: 30.0,
            }],
        )?;

        let err = table
            .append_parquet_segment(rel_path)
            .await
            .expect_err("expected schema mismatch");

        match err {
            TableError::SegmentSchemaCompatibility { path, source } => {
                assert_eq!(path, rel_path);
                assert!(matches!(
                    source,
                    crate::metadata::schema_compat::SchemaCompatibilityError::MissingEntityColumn { .. }
                ));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn first_append_rejects_unsupported_entity_type_without_publication() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let index = IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec!["device_id".to_string()],
            kind: IndexKind::Timestamp {
                bucket: TimeBucket::Minutes(1),
                timezone: None,
            },
        };
        let mut table =
            TimeSeriesTable::create(location, TableMeta::new_time_series(index)).await?;
        let rel_path = "data/unsupported-entity.parquet";
        let abs_path = tmp.path().join(rel_path);
        std::fs::create_dir_all(abs_path.parent().expect("data parent"))?;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false,
            ),
            Field::new("device_id", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1_000])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )?;
        let mut writer = ArrowWriter::try_new(File::create(abs_path)?, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        let state_before = table.state.clone();

        let error = table
            .append_parquet_segment(rel_path)
            .await
            .expect_err("Boolean entity columns must be rejected");

        assert!(matches!(
            error,
            TableError::SegmentSchemaCompatibility { path, source }
                if path == rel_path
                    && matches!(
                        &source,
                        crate::metadata::schema_compat::SchemaCompatibilityError::UnsupportedEntityColumnType {
                            column,
                            actual: LogicalDataType::Bool,
                        } if column == "device_id"
                    )
        ));
        assert_eq!(table.state, state_before);
        assert!(coverage_files(tmp.path())?.is_empty());
        assert_eq!(table.log.load_current_version().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn append_rejects_duplicate_path_before_parquet_read_without_mutation() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel_path = "data/dup.parquet";
        let abs_path = tmp.path().join(rel_path);

        write_test_parquet(
            &abs_path,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        table.append_parquet_segment(rel_path).await?;
        let state_before = table.state.clone();
        let sidecar_counts_before = [
            std::fs::read_dir(tmp.path().join(layout::SEGMENT_COVERAGE_DIR))?.count(),
            std::fs::read_dir(tmp.path().join(layout::TABLE_SNAPSHOT_DIR))?.count(),
        ];

        // Removing the file proves duplicate detection depends only on the
        // normalized live identity, not filesystem or Parquet inspection.
        tokio::fs::remove_file(&abs_path).await?;

        let err = table
            .append_parquet_segment(rel_path)
            .await
            .expect_err("live path must be rejected");
        assert!(matches!(
            err,
            TableError::DuplicateSegmentPath { ref path } if path == rel_path
        ));

        let err = table
            .append_parquet_segment_with_report(r"data\dup.parquet")
            .await
            .expect_err("normalized live path must be rejected");
        assert!(matches!(
            err,
            TableError::DuplicateSegmentPath { ref path } if path == rel_path
        ));

        assert_eq!(table.state, state_before);
        assert_eq!(table.log.load_current_version().await?, 2);
        assert!(!tmp.path().join(layout::commit_rel_path(3)).exists());
        assert_eq!(
            [
                std::fs::read_dir(tmp.path().join(layout::SEGMENT_COVERAGE_DIR))?.count(),
                std::fs::read_dir(tmp.path().join(layout::TABLE_SNAPSHOT_DIR))?.count(),
            ],
            sidecar_counts_before
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_keys_paths_and_updates_snapshot() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        let rel1 = "data/seg-auto-1.parquet";
        let rel2 = "data/seg-auto-2.parquet";
        let path1 = tmp.path().join(rel1);
        let path2 = tmp.path().join(rel2);

        write_test_parquet(
            &path1,
            true,
            false,
            &[
                TestRow {
                    ts_millis: 1_000,
                    symbol: "A",
                    price: 10.0,
                },
                TestRow {
                    ts_millis: 2_000,
                    symbol: "A",
                    price: 20.0,
                },
            ],
        )?;
        write_test_parquet(
            &path2,
            true,
            false,
            &[
                TestRow {
                    ts_millis: 120_000,
                    symbol: "A",
                    price: 30.0,
                },
                TestRow {
                    ts_millis: 121_000,
                    symbol: "A",
                    price: 40.0,
                },
            ],
        )?;

        let v2 = table.append_parquet_segment(rel1).await?;
        let v3 = table.append_parquet_segment(rel2).await?;
        assert_eq!(v2, 2);
        assert_eq!(v3, 3);

        let seg1 = table.state.segments.get(rel1).expect("segment 1 present");
        let seg2 = table.state.segments.get(rel2).expect("segment 2 present");
        assert_eq!(seg1.path, rel1);
        assert_eq!(seg2.path, rel2);
        assert!(seg1.coverage_path.is_some());
        assert!(seg2.coverage_path.is_some());

        let cov1 =
            compute_segment_entity_coverage(&location, Path::new(rel1), table.index_spec()).await?;
        let cov2 =
            compute_segment_entity_coverage(&location, Path::new(rel2), table.index_spec()).await?;
        let expected_snapshot = cov1.union(&cov2);

        let ptr = table
            .state
            .table_coverage
            .as_ref()
            .expect("table snapshot pointer present after append");
        assert_eq!(ptr.version, v3);
        assert_eq!(ptr.index_kind, table.index_spec().kind);

        let snapshot_cov =
            read_entity_coverage_sidecar(&location, Path::new(&ptr.coverage_path)).await?;

        assert_eq!(snapshot_cov, expected_snapshot);
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_rejects_overlap() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location, make_basic_table_meta()).await?;

        let rel1 = "data/seg-overlap-a.parquet";
        let rel2 = "data/seg-overlap-b.parquet";
        let path1 = tmp.path().join(rel1);
        let path2 = tmp.path().join(rel2);

        write_test_parquet(
            &path1,
            true,
            false,
            &[
                TestRow {
                    ts_millis: 1_000,
                    symbol: "B",
                    price: 10.0,
                },
                TestRow {
                    ts_millis: 1_000,
                    symbol: "A",
                    price: 20.0,
                },
                TestRow {
                    ts_millis: 61_000,
                    symbol: "A",
                    price: 30.0,
                },
            ],
        )?;
        write_test_parquet(
            &path2,
            true,
            false,
            &[
                TestRow {
                    ts_millis: 1_500,
                    symbol: "B",
                    price: 40.0,
                },
                TestRow {
                    ts_millis: 1_500,
                    symbol: "A",
                    price: 50.0,
                },
                TestRow {
                    ts_millis: 61_500,
                    symbol: "A",
                    price: 60.0,
                },
            ],
        )?;

        table.append_parquet_segment(rel1).await?;

        let err = table
            .append_parquet_segment(rel2)
            .await
            .expect_err("overlapping append should fail");

        assert!(matches!(
            err,
            TableError::EntityCoverageOverlap {
                segment_path,
                overlap_count: 3,
                example_identity,
                example_bucket: 0x8000_0000_0000_0000,
                example_bucket_range,
            } if segment_path == rel2
                && example_identity.components() == [EntityValue::from("A")]
                && example_bucket_range.to_string()
                    == "[1970-01-01T00:00:00Z, 1970-01-01T00:01:00Z)"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_snapshot_survives_reopen() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        let rel1 = "data/seg-reopen-a.parquet";
        let rel2 = "data/seg-reopen-b.parquet";
        let path1 = tmp.path().join(rel1);
        let path2 = tmp.path().join(rel2);

        write_test_parquet(
            &path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        write_test_parquet(
            &path2,
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "A",
                price: 20.0,
            }],
        )?;

        table.append_parquet_segment(rel1).await?;
        table.append_parquet_segment(rel2).await?;

        let reopened = TimeSeriesTable::open(location.clone()).await?;
        let ptr = reopened
            .state()
            .table_coverage
            .as_ref()
            .expect("table snapshot pointer present after reopen");

        assert_eq!(ptr.index_kind, reopened.index_spec().kind);

        let cov1 =
            compute_segment_entity_coverage(&location, Path::new(rel1), reopened.index_spec())
                .await?;
        let cov2 =
            compute_segment_entity_coverage(&location, Path::new(rel2), reopened.index_spec())
                .await?;
        let expected = cov1.union(&cov2);

        let snapshot_cov =
            read_entity_coverage_sidecar(&location, Path::new(&ptr.coverage_path)).await?;
        assert_eq!(snapshot_cov, expected);
        Ok(())
    }

    #[tokio::test]
    async fn load_snapshot_recovers_when_missing_file() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        // Append two segments so we have segment sidecars plus a table snapshot pointer.
        let rel1 = "data/seg-missing-snap-a.parquet";
        let rel2 = "data/seg-missing-snap-b.parquet";
        let path1 = tmp.path().join(rel1);
        let path2 = tmp.path().join(rel2);
        write_test_parquet(
            &path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        write_test_parquet(
            &path2,
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "A",
                price: 20.0,
            }],
        )?;

        table.append_parquet_segment(rel1).await?;
        table.append_parquet_segment(rel2).await?;

        let state = table.state.clone();
        let ptr = state
            .table_coverage
            .as_ref()
            .expect("snapshot pointer present");
        let snapshot_abs = match &location.as_ref() {
            StorageLocation::Local(root) => root.join(&ptr.coverage_path),
        };

        tokio::fs::remove_file(&snapshot_abs).await?;

        let recovered = table.load_table_entity_snapshot_coverage_readonly().await?;

        let mut expected = EntityCoverage::empty();
        for seg in state.segments.values() {
            let cov_path = seg.coverage_path.as_ref().expect("coverage path");
            let cov = read_entity_coverage_sidecar(&location, Path::new(cov_path)).await?;
            expected.union_inplace(&cov);
        }

        assert_eq!(recovered, expected);
        Ok(())
    }

    #[tokio::test]
    async fn load_snapshot_recovers_when_corrupt_file() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        let rel1 = "data/seg-corrupt-snap-a.parquet";
        let rel2 = "data/seg-corrupt-snap-b.parquet";
        let path1 = tmp.path().join(rel1);
        let path2 = tmp.path().join(rel2);
        write_test_parquet(
            &path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        write_test_parquet(
            &path2,
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "A",
                price: 20.0,
            }],
        )?;

        table.append_parquet_segment(rel1).await?;
        table.append_parquet_segment(rel2).await?;

        let state = table.state.clone();
        let ptr = state
            .table_coverage
            .as_ref()
            .expect("snapshot pointer present");
        let snapshot_abs = match &location.as_ref() {
            StorageLocation::Local(root) => root.join(&ptr.coverage_path),
        };

        tokio::fs::write(&snapshot_abs, b"garbage").await?;

        let recovered = table.load_table_entity_snapshot_coverage_readonly().await?;

        let mut expected = EntityCoverage::empty();
        for seg in state.segments.values() {
            let cov_path = seg.coverage_path.as_ref().expect("coverage path");
            let cov = read_entity_coverage_sidecar(&location, Path::new(cov_path)).await?;
            expected.union_inplace(&cov);
        }

        assert_eq!(recovered, expected);
        Ok(())
    }

    #[tokio::test]
    async fn rejected_append_does_not_heal_corrupt_snapshot() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        let existing = "data/existing.parquet";
        write_test_parquet(
            &tmp.path().join(existing),
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        table.append_parquet_segment(existing).await?;

        let snapshot_path = table
            .state
            .table_coverage
            .as_ref()
            .expect("snapshot pointer present")
            .coverage_path
            .clone();
        let snapshot_abs = tmp.path().join(snapshot_path);
        tokio::fs::write(&snapshot_abs, b"garbage").await?;

        let overlapping = "data/overlapping.parquet";
        write_test_parquet(
            &tmp.path().join(overlapping),
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 20.0,
            }],
        )?;

        let err = table
            .append_parquet_segment(overlapping)
            .await
            .expect_err("overlap must be rejected");
        assert!(matches!(err, TableError::EntityCoverageOverlap { .. }));
        assert_eq!(tokio::fs::read(snapshot_abs).await?, b"garbage");
        Ok(())
    }

    #[tokio::test]
    async fn load_snapshot_errors_when_segment_missing_coverage_path() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        let rel1 = "data/seg-missing-cov-path.parquet";
        let path1 = tmp.path().join(rel1);
        write_test_parquet(
            &path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;

        table.append_parquet_segment(rel1).await?;

        let mut state = table.state.clone();
        state.table_coverage = None;

        let segment_path = state
            .segments
            .keys()
            .next()
            .expect("segment present")
            .clone();
        state
            .segments
            .get_mut(&segment_path)
            .expect("segment present")
            .coverage_path = None;

        // Overwrite table state with the modified snapshot missing coverage_path.
        table.state = state;

        let err = table
            .load_table_entity_snapshot_coverage_readonly()
            .await
            .expect_err("missing coverage_path should error");

        assert!(matches!(
            err,
            TableError::ExistingSegmentMissingCoverage { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn load_snapshot_errors_when_segment_sidecar_corrupt() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        let rel1 = "data/seg-corrupt-sidecar.parquet";
        let rel2 = "data/seg-corrupt-sidecar-ok.parquet";
        let path1 = tmp.path().join(rel1);
        let path2 = tmp.path().join(rel2);
        write_test_parquet(
            &path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        write_test_parquet(
            &path2,
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "A",
                price: 20.0,
            }],
        )?;

        table.append_parquet_segment(rel1).await?;
        table.append_parquet_segment(rel2).await?;

        let mut state = table.state.clone();
        state.table_coverage = None;
        let (corrupt_segment_path, corrupt_cov_path) = state
            .segments
            .values()
            .next()
            .map(|meta| {
                (
                    meta.path.clone(),
                    meta.coverage_path.as_ref().expect("coverage path").clone(),
                )
            })
            .expect("at least one segment");
        table.state = state;

        let corrupt_abs = match &location.as_ref() {
            StorageLocation::Local(root) => root.join(&corrupt_cov_path),
        };
        tokio::fs::write(&corrupt_abs, b"not a coverage bitmap").await?;

        let err = table
            .load_table_entity_snapshot_coverage_readonly()
            .await
            .expect_err("corrupt sidecar should error");

        match err {
            TableError::SegmentCoverageSidecarRead {
                path,
                coverage_path,
                ..
            } => {
                assert_eq!(path, corrupt_segment_path);
                assert_eq!(coverage_path, corrupt_cov_path);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn entity_aware_stale_append_cleans_sidecars_without_state_mutation() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut winner = TimeSeriesTable::create(location.clone(), meta).await?;
        let mut loser = TimeSeriesTable::open(location.clone()).await?;
        let loser_state_before = loser.state.clone();

        let winner_path = "data/winner.parquet";
        let loser_path = "data/loser.parquet";
        write_test_parquet(
            &tmp.path().join(winner_path),
            true,
            false,
            &[TestRow {
                ts_millis: 10_000,
                symbol: "X",
                price: 100.0,
            }],
        )?;
        write_test_parquet(
            &tmp.path().join(loser_path),
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "X",
                price: 200.0,
            }],
        )?;

        assert_eq!(winner.append_parquet_segment(winner_path).await?, 2);
        let coverage_before = coverage_files(tmp.path())?;

        let err = loser
            .append_parquet_segment(loser_path)
            .await
            .expect_err("expected conflict due to stale version");

        match err {
            TableError::TransactionLog { source } => {
                assert!(matches!(
                    source,
                    CommitError::Conflict {
                        expected: 1,
                        found: 2,
                        ..
                    }
                ));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        assert_eq!(loser.state, loser_state_before);
        assert_eq!(loser.log.load_current_version().await?, 2);
        let committed = loser.load_latest_state().await?;
        assert!(committed.segments.contains_key(winner_path));
        assert!(!committed.segments.contains_key(loser_path));
        assert_eq!(coverage_files(tmp.path())?, coverage_before);
        for bytes in coverage_before.values() {
            entity_coverage_from_bytes(bytes)?;
        }
        assert!(!tmp.path().join(layout::commit_rel_path(3)).exists());
        Ok(())
    }

    #[tokio::test]
    async fn stale_int64_append_cleans_only_its_writer_owned_sidecars() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let index = registered_index(IndexKind::Int64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        });
        let mut winner =
            TimeSeriesTable::create(location.clone(), TableMeta::new_time_series(index)).await?;
        let mut loser = TimeSeriesTable::open(location).await?;
        let winner_path = "data/writer-owned-winner.parquet";
        let loser_path = "data/writer-owned-loser.parquet";

        write_arrow_parquet_int_time(&tmp.path().join(winner_path), &[0], &["X"], &[100.0])?;
        write_arrow_parquet_int_time(&tmp.path().join(loser_path), &[100], &["X"], &[200.0])?;

        winner.append_parquet_segment(winner_path).await?;
        let coverage_before = coverage_files(tmp.path())?;

        let err = loser
            .append_parquet_segment(loser_path)
            .await
            .expect_err("stale append should conflict");

        assert!(matches!(
            err,
            TableError::TransactionLog {
                source: CommitError::Conflict { .. }
            }
        ));
        assert_eq!(coverage_files(tmp.path())?, coverage_before);
        Ok(())
    }

    #[tokio::test]
    async fn ambiguous_int64_commit_retains_writer_owned_sidecars() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let index = registered_index(IndexKind::Int64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        });
        let mut table =
            TimeSeriesTable::create(location, TableMeta::new_time_series(index)).await?;
        let state_before = table.state.clone();
        let coverage_before = coverage_files(tmp.path())?;
        let segment_path = "data/ambiguous.parquet";

        write_arrow_parquet_int_time(&tmp.path().join(segment_path), &[10], &["X"], &[100.0])?;

        let commit_path = tmp.path().join(layout::commit_rel_path(2));
        crate::storage::inject_write_new_failure(commit_path.clone(), true);

        let err = table
            .append_parquet_segment(segment_path)
            .await
            .expect_err("failed commit cleanup should make the outcome ambiguous");

        assert!(matches!(
            err,
            TableError::TransactionLog {
                source: CommitError::AmbiguousOutcome { .. }
            }
        ));
        assert_eq!(table.state, state_before);
        assert_eq!(table.log.load_current_version().await?, 1);
        assert!(commit_path.exists());
        assert_eq!(coverage_files(tmp.path())?.len(), coverage_before.len() + 2);
        Ok(())
    }

    #[tokio::test]
    async fn entity_sidecar_cleanup_failures_preserve_error_and_reverse_order() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let table = TimeSeriesTable::create(location, make_basic_table_meta()).await?;
        let sidecars = [
            format!("{}/first-stuck.roar", layout::SEGMENT_COVERAGE_DIR),
            format!("{}/second-stuck.roar", layout::TABLE_SNAPSHOT_DIR),
        ];
        for sidecar in &sidecars {
            tokio::fs::create_dir_all(tmp.path().join(sidecar)).await?;
        }
        let example_bucket = crate::coverage::bucket::bucket_id(
            &table.index.kind,
            &IndexValue::Timestamp(utc_datetime(1970, 1, 1, 0, 0, 0)),
        )?;
        let source = TableError::EntityCoverageOverlap {
            segment_path: "data/failed.parquet".to_string(),
            overlap_count: 1,
            example_identity: EntityIdentity::try_new(vec!["A".into()])?,
            example_bucket,
            example_bucket_range: logical_bucket_range(&table.index.kind, example_bucket)?,
        };
        let err = table.rollback_created_artifacts(&sidecars, source).await;
        let message = err.to_string();

        assert!(matches!(
            err,
            TableError::AppendRollback {
                source,
                cleanup_errors,
            } if matches!(*source, TableError::EntityCoverageOverlap { .. })
                && cleanup_errors.len() == 2
                && cleanup_errors[0].contains("second-stuck.roar")
                && cleanup_errors[1].contains("first-stuck.roar")
        ));
        assert!(message.contains("data/failed.parquet"));
        assert!(message.contains("first-stuck.roar"));
        assert!(message.contains("second-stuck.roar"));
        Ok(())
    }

    #[tokio::test]
    async fn append_fails_when_existing_segment_missing_coverage_path() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        let rel1 = "data/seg-missing-cov.parquet";
        let rel2 = "data/seg-next.parquet";
        let path1 = tmp.path().join(rel1);
        let path2 = tmp.path().join(rel2);

        write_test_parquet(
            &path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        write_test_parquet(
            &path2,
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "A",
                price: 20.0,
            }],
        )?;

        table.append_parquet_segment(rel1).await?;

        // Simulate legacy/bad state: drop coverage_path on the existing segment.
        let seg = table.state.segments.get_mut(rel1).expect("segment present");
        seg.coverage_path = None;

        let err = table
            .append_parquet_segment(rel2)
            .await
            .expect_err("append should fail when existing segment lacks coverage");

        assert!(matches!(
            err,
            TableError::ExistingSegmentMissingCoverage { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    // Unlike load_snapshot_recovers_when_missing_file (which exercises recovery when
    // the pointer exists but the snapshot file is gone), this covers the case where
    // the in-memory pointer itself is missing while segments exist, and append
    // must rebuild + rewrite the pointer as part of the append flow.
    async fn append_recovers_when_table_snapshot_pointer_missing() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        let rel1 = "data/seg-no-pointer-a.parquet";
        let rel2 = "data/seg-no-pointer-b.parquet";
        let path1 = tmp.path().join(rel1);
        let path2 = tmp.path().join(rel2);

        write_test_parquet(
            &path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        write_test_parquet(
            &path2,
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "A",
                price: 20.0,
            }],
        )?;

        table.append_parquet_segment(rel1).await?;

        // Simulate missing snapshot pointer while segments exist.
        table.state.table_coverage = None;

        table.append_parquet_segment(rel2).await?;

        // Snapshot pointer should be restored after a successful append.
        let ptr = table
            .state
            .table_coverage
            .as_ref()
            .expect("snapshot pointer restored");

        let cov = read_entity_coverage_sidecar(&location, Path::new(&ptr.coverage_path)).await?;

        let mut expected = EntityCoverage::empty();
        for seg in table.state.segments.values() {
            let path = seg.coverage_path.as_ref().expect("coverage path");
            let seg_cov = read_entity_coverage_sidecar(&location, Path::new(path)).await?;
            expected.union_inplace(&seg_cov);
        }

        assert_eq!(cov, expected);
        Ok(())
    }

    #[tokio::test]
    async fn append_fails_when_table_snapshot_bucket_mismatches_index() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;

        let rel1 = "data/seg-bucket-a.parquet";
        let rel2 = "data/seg-bucket-b.parquet";
        let path1 = tmp.path().join(rel1);
        let path2 = tmp.path().join(rel2);

        write_test_parquet(
            &path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        write_test_parquet(
            &path2,
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "A",
                price: 20.0,
            }],
        )?;

        table.append_parquet_segment(rel1).await?;

        // Tamper snapshot pointer to a mismatching bucket spec.
        let bad_bucket = TimeBucket::Hours(1);
        let ptr = table
            .state
            .table_coverage
            .as_ref()
            .expect("pointer present")
            .clone();
        table.state.table_coverage = Some(TableCoveragePointer {
            index_kind: IndexKind::Timestamp {
                bucket: bad_bucket.clone(),
                timezone: None,
            },
            coverage_path: ptr.coverage_path.clone(),
            version: ptr.version,
        });

        let err = table
            .append_parquet_segment(rel2)
            .await
            .expect_err("append should fail when snapshot bucket mismatches index");

        assert!(matches!(
            err,
            TableError::TableCoverageIndexKindMismatch { .. }
        ));
        Ok(())
    }
}
