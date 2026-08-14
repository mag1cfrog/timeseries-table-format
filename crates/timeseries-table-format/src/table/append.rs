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

use crate::{
    coverage::serde::coverage_to_bytes,
    coverage::{
        io::{CoverageError, write_coverage_sidecar_new_bytes},
        layout::{
            segment_coverage_id_v1, segment_coverage_path, table_coverage_id_v1,
            table_snapshot_path,
        },
    },
    formats::parquet::{
        coverage::compute_segment_coverage, logical_schema_from_parquet,
        segment_entity_identity_from_parquet, segment_meta::segment_meta_from_parquet,
    },
    metadata::schema_compat::ensure_schema_exact_match,
    storage::{self, StorageError},
    transaction_log::{LogAction, TableState, table_state::TableCoveragePointer},
};

use super::{
    TimeSeriesTable,
    append_report::{AppendReport, AppendReportBuilder},
    error::{
        CoverageOverlapSnafu, DuplicateSegmentPathSnafu, EntityMismatchSnafu,
        ExistingSegmentMissingCoverageSnafu, MissingCanonicalSchemaSnafu, SchemaCompatibilitySnafu,
        SegmentCoverageSnafu, SegmentEntityIdentitySnafu, SegmentMetaSnafu, StorageSnafu,
        TableError, TransactionLogSnafu,
    },
};

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
    async fn normalize_new_segment_path(&self, relative_path: &str) -> Result<String, TableError> {
        let supplied_path = Path::new(relative_path);
        let (normalized, native_path) =
            storage::normalize_relative_segment_path(supplied_path).context(StorageSnafu)?;

        if self.state.segments.contains_key(&normalized) {
            return DuplicateSegmentPathSnafu { path: normalized }.fail();
        }

        self.location()
            .validate_segment_file(supplied_path, &native_path)
            .await
            .context(StorageSnafu)?;

        Ok(normalized)
    }

    async fn append_parquet_segment_file(
        &mut self,
        relative_path: &str,
        time_column: &str,
        mut report: Option<&mut AppendReportBuilder>,
    ) -> Result<u64, TableError> {
        let rel_path = Path::new(relative_path);
        let expected_version = self.state.version;
        let bucket_spec = self.index.bucket.clone();

        // 0) Coverage readiness checks.
        ensure_existing_segments_have_coverage(&self.state)?;

        // 1) Segment meta + schema.
        let step_start = Instant::now();
        let (mut segment_meta, meta_report) =
            segment_meta_from_parquet(self.location(), rel_path, time_column)
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

        let mut maybe_updated_meta = match maybe_table_schema {
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

        // 2.5) Entity identity enforcement / pinning (v0.1 single-entity-per-table)
        if !self.index.entity_columns.is_empty() {
            let step_start = Instant::now();
            let seg_ident = segment_entity_identity_from_parquet(
                self.location(),
                rel_path,
                &self.index.entity_columns,
            )
            .await
            .context(SegmentEntityIdentitySnafu)?;
            if let Some(r) = report.as_mut() {
                r.push_step("entity_identity", step_start.elapsed(), Vec::new());
            }

            match &self.state.table_meta.entity_identity {
                Some(expected) => {
                    if expected != &seg_ident {
                        return EntityMismatchSnafu {
                            segment_path: relative_path.to_string(),
                            expected: expected.clone(),
                            found: seg_ident,
                        }
                        .fail();
                    }
                }
                None => {
                    // pin the first append that includes entity columns
                    let updated =
                        maybe_updated_meta.get_or_insert_with(|| self.state.table_meta.clone());
                    updated.entity_identity = Some(seg_ident);
                }
            }
        }

        // 3) Load current table snapshot coverage (or empty if first append).
        let step_start = Instant::now();
        let table_cov = self.load_table_snapshot_coverage_with_heal().await?;
        if let Some(r) = report.as_mut() {
            r.push_step("load_table_snapshot", step_start.elapsed(), Vec::new());
        }

        // 4) Compute segment coverage.
        let step_start = Instant::now();
        let segment_cov =
            compute_segment_coverage(self.location(), rel_path, time_column, &bucket_spec)
                .await
                .context(SegmentCoverageSnafu)?;
        if let Some(r) = report.as_mut() {
            r.push_step("segment_coverage", step_start.elapsed(), Vec::new());
        }

        // 5) Overlap detection.
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
        let seg_cov_bytes =
            coverage_to_bytes(&segment_cov).map_err(|source| TableError::CoverageSidecar {
                source: CoverageError::Serde { source },
            })?;

        // 6) Write sidecars BEFORE commit (orphan files OK on commit failure)
        let coverage_id = segment_coverage_id_v1(&bucket_spec, time_column, &seg_cov_bytes);
        let seg_cov_path =
            segment_coverage_path(&coverage_id).map_err(|source| TableError::CoverageSidecar {
                source: CoverageError::Layout { source },
            })?;
        let step_start = Instant::now();
        match write_coverage_sidecar_new_bytes(self.location(), &seg_cov_path, &seg_cov_bytes).await
        {
            Ok(()) => {}
            Err(CoverageError::Storage {
                source: StorageError::AlreadyExists { .. },
            }) => {
                // ok: same id implies same intended content
            }
            Err(e) => return Err(TableError::CoverageSidecar { source: e }),
        }
        if let Some(r) = report.as_mut() {
            r.push_step("write_segment_sidecar", step_start.elapsed(), Vec::new());
        }

        let new_version_guess = expected_version + 1;

        let new_table_cov = table_cov.union(&segment_cov);

        let new_snap_cov_bytes =
            coverage_to_bytes(&new_table_cov).map_err(|source| TableError::CoverageSidecar {
                source: CoverageError::Serde { source },
            })?;
        let snapshot_id = table_coverage_id_v1(&bucket_spec, time_column, &new_snap_cov_bytes);

        let snapshot_path = table_snapshot_path(new_version_guess, &snapshot_id).map_err(|e| {
            TableError::CoverageSidecar {
                source: CoverageError::Layout { source: e },
            }
        })?;

        let step_start = Instant::now();
        match write_coverage_sidecar_new_bytes(self.location(), &snapshot_path, &new_snap_cov_bytes)
            .await
        {
            Ok(()) => {}
            Err(CoverageError::Storage {
                source: StorageError::AlreadyExists { .. },
            }) => {
                // ok: same id implies same intended content
            }
            Err(e) => return Err(TableError::CoverageSidecar { source: e }),
        }
        if let Some(r) = report.as_mut() {
            r.push_step("write_snapshot_sidecar", step_start.elapsed(), Vec::new());
        }

        // 7) Build actions and commit.
        segment_meta.coverage_path = Some(seg_cov_path.to_string_lossy().to_string());

        let mut actions = Vec::new();
        if let Some(updated_meta) = maybe_updated_meta.clone() {
            actions.push(LogAction::UpdateTableMeta(updated_meta));
        }

        actions.push(LogAction::AddSegment(segment_meta.clone()));
        actions.push(LogAction::UpdateTableCoverage {
            bucket_spec: bucket_spec.clone(),
            coverage_path: snapshot_path.to_string_lossy().to_string(),
        });

        let step_start = Instant::now();
        let new_version = self
            .log
            .commit_with_expected_version(expected_version, actions)
            .await
            .context(TransactionLogSnafu)?;
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
            bucket_spec,
            coverage_path: snapshot_path.to_string_lossy().to_string(),
            version: new_version,
        });
        if let Some(r) = report.as_mut() {
            r.push_step("state_update", step_start.elapsed(), Vec::new());
        }

        Ok(new_version)
    }

    /// Append a Parquet segment using its canonical relative path as identity.
    pub async fn append_parquet_segment(
        &mut self,
        relative_path: &str,
        time_column: &str,
    ) -> Result<u64, TableError> {
        let relative_path = self.normalize_new_segment_path(relative_path).await?;
        self.append_parquet_segment_file(&relative_path, time_column, None)
            .await
    }

    /// Append a Parquet segment and return a profiling report.
    pub async fn append_parquet_segment_with_report(
        &mut self,
        relative_path: &str,
        time_column: &str,
    ) -> Result<(u64, AppendReport), TableError> {
        let relative_path = self.normalize_new_segment_path(relative_path).await?;
        let mut report = AppendReportBuilder::new();
        report.set_context("relative_path", &relative_path);
        report.set_context("time_column", time_column);

        let version = self
            .append_parquet_segment_file(&relative_path, time_column, Some(&mut report))
            .await?;

        Ok((version, report.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use super::*;
    use crate::coverage::Coverage;
    use crate::coverage::io::read_coverage_sidecar;
    use crate::metadata::logical_schema::{LogicalDataType, LogicalTimestampUnit};
    use crate::metadata::table_metadata::TABLE_FORMAT_VERSION;
    use crate::metadata::time_column::TimeColumnError;
    use crate::storage::layout;
    use crate::storage::{StorageLocation, TableLocation};
    use crate::transaction_log::segments::{SegmentError, SegmentMetaError};
    use crate::transaction_log::{
        Commit, CommitError, TableKind, TableMeta, TimeBucket, TimeIndexSpec,
    };
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::collections::BTreeMap;
    use std::fs::{File, OpenOptions};
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::TempDir;

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
            .append_parquet_segment(rel, "ts")
            .await
            .expect_err("expected missing time column");

        match err {
            TableError::SegmentMeta { source } => {
                assert!(matches!(
                    source,
                    SegmentError::Meta {
                        source: SegmentMetaError::TimeColumn {
                            source: TimeColumnError::Missing { .. },
                            ..
                        }
                    },
                ));
            }
            other => panic!("unexpected error: {other:?}"),
        }

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

        let new_version = table.append_parquet_segment(rel_path, "ts").await?;

        assert_eq!(new_version, 2);
        assert_eq!(table.state.version, 2);
        let seg = table.state.segments.get(rel_path).expect("segment present");
        assert_eq!(seg.path, rel_path);
        assert_eq!(seg.row_count, 1);
        assert_eq!(seg.ts_min.timestamp_millis(), 1_000);
        assert_eq!(seg.ts_max.timestamp_millis(), 1_000);

        let commit_path = tmp.path().join(layout::commit_rel_path(2));
        assert!(commit_path.is_file());
        let current =
            tokio::fs::read_to_string(tmp.path().join(layout::current_rel_path())).await?;
        assert_eq!(current.trim(), "2");

        let reopened = TimeSeriesTable::open(location).await?;
        assert!(reopened.state.segments.contains_key(rel_path));
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
        let (version, report) = table
            .append_parquet_segment_with_report(rel_path, "ts")
            .await?;

        assert_eq!(version, 2);
        assert_eq!(
            report.context,
            vec![
                ("relative_path".to_string(), rel_path.to_string()),
                ("time_column".to_string(), "ts".to_string()),
                ("file_size_bytes".to_string(), file_size),
            ]
        );
        assert!(
            report
                .steps
                .iter()
                .all(|step| step.name != "read_parquet_bytes")
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_pins_entity_identity_and_commits_actions() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel_path = "data/seg-entity-a.parquet";
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

        let version = table.append_parquet_segment(rel_path, "ts").await?;
        assert_eq!(version, 2);

        let expected_identity = BTreeMap::from([("symbol".to_string(), "A".to_string())]);
        assert_eq!(
            table.state.table_meta.entity_identity,
            Some(expected_identity.clone())
        );

        let commit_path = tmp.path().join(layout::commit_rel_path(2));
        let contents = tokio::fs::read_to_string(&commit_path).await?;
        let commit: Commit = serde_json::from_str(&contents)?;

        assert_eq!(commit.actions.len(), 3);
        match &commit.actions[0] {
            LogAction::UpdateTableMeta(meta) => {
                assert_eq!(meta.entity_identity.as_ref(), Some(&expected_identity));
            }
            other => panic!("expected UpdateTableMeta, got {other:?}"),
        }
        assert!(matches!(commit.actions[1], LogAction::AddSegment(_)));
        assert!(matches!(
            commit.actions[2],
            LogAction::UpdateTableCoverage { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn append_allows_same_entity_identity() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel_path1 = "data/seg-entity-a-1.parquet";
        let abs_path1 = tmp.path().join(rel_path1);
        write_test_parquet(
            &abs_path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;

        table.append_parquet_segment(rel_path1, "ts").await?;

        let rel_path2 = "data/seg-entity-a-2.parquet";
        let abs_path2 = tmp.path().join(rel_path2);
        write_test_parquet(
            &abs_path2,
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "A",
                price: 20.0,
            }],
        )?;

        let version = table.append_parquet_segment(rel_path2, "ts").await?;
        assert_eq!(version, 3);

        let expected_identity = BTreeMap::from([("symbol".to_string(), "A".to_string())]);
        assert_eq!(
            table.state.table_meta.entity_identity,
            Some(expected_identity.clone())
        );

        let commit_path = tmp.path().join(layout::commit_rel_path(3));
        let contents = tokio::fs::read_to_string(&commit_path).await?;
        let commit: Commit = serde_json::from_str(&contents)?;
        assert_eq!(commit.actions.len(), 2);
        assert!(matches!(commit.actions[0], LogAction::AddSegment(_)));
        assert!(matches!(
            commit.actions[1],
            LogAction::UpdateTableCoverage { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn append_rejects_mismatched_entity_identity() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel_path1 = "data/seg-entity-a.parquet";
        let abs_path1 = tmp.path().join(rel_path1);
        write_test_parquet(
            &abs_path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        table.append_parquet_segment(rel_path1, "ts").await?;

        let rel_path2 = "data/seg-entity-b.parquet";
        let abs_path2 = tmp.path().join(rel_path2);
        write_test_parquet(
            &abs_path2,
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "B",
                price: 20.0,
            }],
        )?;

        let err = table
            .append_parquet_segment(rel_path2, "ts")
            .await
            .expect_err("expected entity identity mismatch");

        let expected_identity = BTreeMap::from([("symbol".to_string(), "A".to_string())]);
        let found_identity = BTreeMap::from([("symbol".to_string(), "B".to_string())]);

        match err {
            TableError::EntityMismatch {
                expected, found, ..
            } => {
                assert_eq!(expected, expected_identity);
                assert_eq!(found, found_identity);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let commit_path = tmp.path().join(layout::commit_rel_path(3));
        assert!(!commit_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_adopts_schema_when_missing() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let index = TimeIndexSpec {
            timestamp_column: "ts".to_string(),
            entity_columns: vec![],
            bucket: TimeBucket::Minutes(1),
            timezone: None,
        };
        let meta = TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: None,
            created_at: utc_datetime(2025, 1, 1, 0, 0, 0),
            format_version: TABLE_FORMAT_VERSION,
            entity_identity: None,
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

        let new_version = table.append_parquet_segment(rel_path, "ts").await?;

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
            .append_parquet_segment(rel_path, "ts")
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
        table.append_parquet_segment(rel_path, "ts").await?;
        let state_before = table.state.clone();
        let sidecar_counts_before = [
            std::fs::read_dir(tmp.path().join(layout::SEGMENT_COVERAGE_DIR))?.count(),
            std::fs::read_dir(tmp.path().join(layout::TABLE_SNAPSHOT_DIR))?.count(),
        ];

        // Removing the file proves duplicate detection depends only on the
        // normalized live identity, not filesystem or Parquet inspection.
        tokio::fs::remove_file(&abs_path).await?;

        let err = table
            .append_parquet_segment(rel_path, "ts")
            .await
            .expect_err("live path must be rejected");
        assert!(matches!(
            err,
            TableError::DuplicateSegmentPath { ref path } if path == rel_path
        ));

        let err = table
            .append_parquet_segment_with_report(r"data\dup.parquet", "ts")
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

        let v2 = table.append_parquet_segment(rel1, "ts").await?;
        let v3 = table.append_parquet_segment(rel2, "ts").await?;
        assert_eq!(v2, 2);
        assert_eq!(v3, 3);

        let seg1 = table.state.segments.get(rel1).expect("segment 1 present");
        let seg2 = table.state.segments.get(rel2).expect("segment 2 present");
        assert_eq!(seg1.path, rel1);
        assert_eq!(seg2.path, rel2);
        assert!(seg1.coverage_path.is_some());
        assert!(seg2.coverage_path.is_some());

        let bucket_spec = table.index_spec().bucket.clone();

        let cov1 = compute_segment_coverage(&location, Path::new(rel1), "ts", &bucket_spec).await?;
        let cov2 = compute_segment_coverage(&location, Path::new(rel2), "ts", &bucket_spec).await?;
        let expected_snapshot = cov1.union(&cov2);

        let ptr = table
            .state
            .table_coverage
            .as_ref()
            .expect("table snapshot pointer present after append");
        assert_eq!(ptr.version, v3);
        assert_eq!(ptr.bucket_spec, bucket_spec);

        let snapshot_cov = read_coverage_sidecar(&location, Path::new(&ptr.coverage_path)).await?;

        assert_eq!(snapshot_cov.present(), expected_snapshot.present());
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
                ts_millis: 1_500,
                symbol: "A",
                price: 20.0,
            }],
        )?;

        table.append_parquet_segment(rel1, "ts").await?;

        let err = table
            .append_parquet_segment(rel2, "ts")
            .await
            .expect_err("overlapping append should fail");

        assert!(matches!(err, TableError::CoverageOverlap { .. }));
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

        table.append_parquet_segment(rel1, "ts").await?;
        table.append_parquet_segment(rel2, "ts").await?;

        let reopened = TimeSeriesTable::open(location.clone()).await?;
        let ptr = reopened
            .state()
            .table_coverage
            .as_ref()
            .expect("table snapshot pointer present after reopen");

        let bucket_spec = reopened.index_spec().bucket.clone();
        assert_eq!(ptr.bucket_spec, bucket_spec);

        let cov1 = compute_segment_coverage(&location, Path::new(rel1), "ts", &bucket_spec).await?;
        let cov2 = compute_segment_coverage(&location, Path::new(rel2), "ts", &bucket_spec).await?;
        let expected = cov1.union(&cov2);

        let snapshot_cov = read_coverage_sidecar(&location, Path::new(&ptr.coverage_path)).await?;
        assert_eq!(snapshot_cov.present(), expected.present());
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

        table.append_parquet_segment(rel1, "ts").await?;
        table.append_parquet_segment(rel2, "ts").await?;

        let state = table.state.clone();
        let ptr = state
            .table_coverage
            .as_ref()
            .expect("snapshot pointer present");
        let snapshot_abs = match &location.as_ref() {
            StorageLocation::Local(root) => root.join(&ptr.coverage_path),
        };

        tokio::fs::remove_file(&snapshot_abs).await?;

        let recovered = table.load_table_snapshot_coverage_with_heal().await?;

        let mut expected = Coverage::empty();
        for seg in state.segments.values() {
            let cov_path = seg.coverage_path.as_ref().expect("coverage path");
            let cov = read_coverage_sidecar(&location, Path::new(cov_path)).await?;
            expected.union_inplace(&cov);
        }

        assert_eq!(recovered.present(), expected.present());
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

        table.append_parquet_segment(rel1, "ts").await?;
        table.append_parquet_segment(rel2, "ts").await?;

        let state = table.state.clone();
        let ptr = state
            .table_coverage
            .as_ref()
            .expect("snapshot pointer present");
        let snapshot_abs = match &location.as_ref() {
            StorageLocation::Local(root) => root.join(&ptr.coverage_path),
        };

        tokio::fs::write(&snapshot_abs, b"garbage").await?;

        let recovered = table.load_table_snapshot_coverage_with_heal().await?;

        let mut expected = Coverage::empty();
        for seg in state.segments.values() {
            let cov_path = seg.coverage_path.as_ref().expect("coverage path");
            let cov = read_coverage_sidecar(&location, Path::new(cov_path)).await?;
            expected.union_inplace(&cov);
        }

        assert_eq!(recovered.present(), expected.present());
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

        table.append_parquet_segment(rel1, "ts").await?;

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
            .load_table_snapshot_coverage_with_heal()
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

        table.append_parquet_segment(rel1, "ts").await?;
        table.append_parquet_segment(rel2, "ts").await?;

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
            .load_table_snapshot_coverage_with_heal()
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
    async fn stale_append_of_different_path_fails_occ_without_state_mutation() -> TestResult {
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

        assert_eq!(winner.append_parquet_segment(winner_path, "ts").await?, 2);

        let err = loser
            .append_parquet_segment(loser_path, "ts")
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

        table.append_parquet_segment(rel1, "ts").await?;

        // Simulate legacy/bad state: drop coverage_path on the existing segment.
        let seg = table.state.segments.get_mut(rel1).expect("segment present");
        seg.coverage_path = None;

        let err = table
            .append_parquet_segment(rel2, "ts")
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

        table.append_parquet_segment(rel1, "ts").await?;

        // Simulate missing snapshot pointer while segments exist.
        table.state.table_coverage = None;

        table.append_parquet_segment(rel2, "ts").await?;

        // Snapshot pointer should be restored after a successful append.
        let ptr = table
            .state
            .table_coverage
            .as_ref()
            .expect("snapshot pointer restored");

        let cov = read_coverage_sidecar(&location, Path::new(&ptr.coverage_path)).await?;

        let mut expected = Coverage::empty();
        for seg in table.state.segments.values() {
            let path = seg.coverage_path.as_ref().expect("coverage path");
            let seg_cov = read_coverage_sidecar(&location, Path::new(path)).await?;
            expected.union_inplace(&seg_cov);
        }

        assert_eq!(cov.present(), expected.present());
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

        table.append_parquet_segment(rel1, "ts").await?;

        // Tamper snapshot pointer to a mismatching bucket spec.
        let bad_bucket = TimeBucket::Hours(1);
        let ptr = table
            .state
            .table_coverage
            .as_ref()
            .expect("pointer present")
            .clone();
        table.state.table_coverage = Some(TableCoveragePointer {
            bucket_spec: bad_bucket.clone(),
            coverage_path: ptr.coverage_path.clone(),
            version: ptr.version,
        });

        let err = table
            .append_parquet_segment(rel2, "ts")
            .await
            .expect_err("append should fail when snapshot bucket mismatches index");

        assert!(matches!(
            err,
            TableError::TableCoverageBucketMismatch { .. }
        ));
        Ok(())
    }
}
