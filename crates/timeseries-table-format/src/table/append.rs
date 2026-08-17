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

use snafu::prelude::*;
use uuid::Uuid;

use crate::{
    coverage::serde::{coverage_to_bytes, entity_coverage_to_bytes},
    coverage::{
        EntityCoverage,
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
        schema_compat::{ensure_index_matches_schema, ensure_schema_exact_match},
        segments::SegmentEntityLayout,
    },
    storage,
    transaction_log::{CommitError, LogAction, TableState, table_state::TableCoveragePointer},
};

use super::{
    TimeSeriesTable,
    append_report::{AppendReport, AppendReportBuilder},
    error::{
        CoverageOverlapSnafu, DuplicateSegmentPathSnafu, EmptySegmentEntityCoverageSnafu,
        EntityCoverageOverlapSnafu, EntityWithoutIndexCoverageSnafu,
        ExistingSegmentMissingCoverageSnafu, MissingCanonicalSchemaSnafu, SchemaCompatibilitySnafu,
        SegmentCoverageSnafu, SegmentMetaSnafu, StorageSnafu, TableError,
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

impl TimeSeriesTable {
    async fn rollback_created_sidecars(
        &self,
        created_sidecars: &[String],
        source: TableError,
    ) -> TableError {
        let mut cleanup_errors = Vec::new();
        for path in created_sidecars.iter().rev() {
            if let Err(error) =
                storage::remove_file(self.location().as_ref(), Path::new(path)).await
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

        let append_result = async {
            let relative_path = self.normalize_new_segment_path(&prepared_path).await?;
            if let Some(r) = report.as_mut() {
                r.set_context("relative_path", &relative_path);
            }
            let version = self
                .append_parquet_segment_file(&relative_path, report)
                .await?;
            Ok((version, relative_path))
        }
        .await;

        match append_result {
            Ok(result) => Ok(result),
            Err(
                error @ TableError::TransactionLog {
                    source: CommitError::AmbiguousOutcome { .. },
                },
            ) => Err(error),
            Err(source) if prepared.created => {
                match storage::remove_file(self.location().as_ref(), &prepared.relative_path).await
                {
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
        ensure_index_matches_schema(&segment_schema, &self.index)
            .context(SchemaCompatibilitySnafu)?;
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
                ensure_schema_exact_match(table_schema, &segment_schema, &self.index)
                    .context(SchemaCompatibilitySnafu)?;
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
                return EntityCoverageOverlapSnafu {
                    segment_path: relative_path.to_string(),
                    overlap_count: segment_cov.intersection_cardinality(&table_cov),
                    example_identity: identity.clone(),
                    example_bucket: bucket,
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
            if overlap_count > 0 {
                let example_bucket = overlap.present().iter().next();
                return CoverageOverlapSnafu {
                    segment_path: relative_path.to_string(),
                    overlap_count,
                    example_bucket,
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
        write_coverage_sidecar_new_bytes(self.location(), Path::new(&seg_cov_path), &seg_cov_bytes)
            .await
            .map_err(|source| TableError::CoverageSidecar { source })?;
        created_sidecars.push(seg_cov_path.clone());
        if let Some(r) = report.as_mut() {
            r.push_step("write_segment_sidecar", step_start.elapsed(), Vec::new());
        }

        let step_start = Instant::now();
        if let Err(source) = write_coverage_sidecar_new_bytes(
            self.location(),
            Path::new(&snapshot_path),
            &new_snap_cov_bytes,
        )
        .await
        {
            let error = TableError::CoverageSidecar { source };
            return Err(self
                .rollback_created_sidecars(&created_sidecars, error)
                .await);
        }
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
            Ok(version) => version,
            Err(source @ crate::transaction_log::CommitError::AmbiguousOutcome { .. }) => {
                return Err(TableError::TransactionLog { source });
            }
            Err(source) => {
                let error = TableError::TransactionLog { source };
                return Err(self
                    .rollback_created_sidecars(&created_sidecars, error)
                    .await);
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

        Ok(new_version)
    }

    /// Append a Parquet segment using its canonical relative path as identity.
    pub async fn append_parquet_segment(&mut self, relative_path: &str) -> Result<u64, TableError> {
        let relative_path = self.normalize_new_segment_path(relative_path).await?;
        self.append_parquet_segment_file(&relative_path, None).await
    }

    /// Copy an external Parquet file into the table when needed and append it.
    ///
    /// A copy created by this operation is removed when append fails before
    /// publication. Files already under the table root and copies involved in
    /// an ambiguous commit outcome are preserved.
    /// Returns the committed version and normalized table-relative segment path.
    pub async fn append_parquet_from_path(
        &mut self,
        parquet_path: &Path,
    ) -> Result<(u64, String), TableError> {
        self.append_parquet_path_file(parquet_path, None).await
    }

    /// Copy and append a Parquet file while collecting a profiling report.
    /// Returns the committed version, normalized table-relative path, and report.
    pub async fn append_parquet_from_path_with_report(
        &mut self,
        parquet_path: &Path,
    ) -> Result<(u64, String, AppendReport), TableError> {
        let mut report = AppendReportBuilder::new();
        let (version, relative_path) = self
            .append_parquet_path_file(parquet_path, Some(&mut report))
            .await?;
        Ok((version, relative_path, report.finish()))
    }

    /// Append a Parquet segment and return a profiling report.
    pub async fn append_parquet_segment_with_report(
        &mut self,
        relative_path: &str,
    ) -> Result<(u64, AppendReport), TableError> {
        let relative_path = self.normalize_new_segment_path(relative_path).await?;
        let mut report = AppendReportBuilder::new();
        report.set_context("relative_path", &relative_path);

        let version = self
            .append_parquet_segment_file(&relative_path, Some(&mut report))
            .await?;

        Ok((version, report.finish()))
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
            ArrayRef, Float64Array, Int64Array, StringArray, TimestampMillisecondArray, UInt64Array,
        },
        datatypes::{DataType, Field, Schema, TimeUnit as ArrowTimeUnit},
        record_batch::RecordBatch,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::collections::BTreeMap;
    use std::fs::{File, OpenOptions};
    use std::io::{Seek, SeekFrom, Write};
    use std::num::NonZeroU64;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

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

        let err = table
            .append_parquet_from_path(&source)
            .await
            .expect_err("missing time column should fail");

        assert!(matches!(err, TableError::SegmentMeta { .. }));
        assert!(!table_root.join("data/wrong-time-column.parquet").exists());
        assert_eq!(std::fs::read(source)?, source_bytes);
        assert_eq!(table.state, state_before);
        assert_eq!(table.log.load_current_version().await?, 1);
        assert_eq!(coverage_files(&table_root)?, coverage_before);
        assert!(!table_root.join(layout::commit_rel_path(2)).exists());
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
        assert!(matches!(
            table
                .append_parquet_segment(overlap_path)
                .await
                .expect_err("negative bucket overlap must fail"),
            TableError::CoverageOverlap { .. }
        ));

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
            TableError::SchemaCompatibility { .. }
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
        assert!(matches!(
            table
                .append_parquet_segment(overlap_path)
                .await
                .expect_err("large uint64 bucket overlap must fail"),
            TableError::CoverageOverlap { .. }
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
            TableError::SchemaCompatibility { source } => {
                assert!(matches!(
                    source,
                    crate::metadata::schema_compat::SchemaCompatibilityError::MissingColumn { .. }
                ));
            }
            other => panic!("unexpected error: {other:?}"),
        }
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
            } if segment_path == rel2
                && example_identity.components() == [EntityValue::from("A")]
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
        let source = TableError::EntityCoverageOverlap {
            segment_path: "data/failed.parquet".to_string(),
            overlap_count: 1,
            example_identity: EntityIdentity::try_new(vec!["A".into()])?,
            example_bucket: 0,
        };
        let err = table.rollback_created_sidecars(&sidecars, source).await;
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
