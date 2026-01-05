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

use bytes::Bytes;

use snafu::prelude::*;

use crate::{
    coverage::serde::coverage_to_bytes,
    helpers::{
        coverage_sidecar::{CoverageError, write_coverage_sidecar_new_bytes},
        parquet::{logical_schema_from_parquet_bytes, segment_meta_from_parquet_bytes},
        schema::ensure_schema_exact_match,
        segment_coverage::compute_segment_coverage_from_parquet_bytes,
        segment_entity_identity::segment_entity_identity_from_parquet_bytes,
    },
    layout::coverage::{
        segment_coverage_id_v1, segment_coverage_path, table_coverage_id_v1, table_snapshot_path,
    },
    storage::{self, StorageError},
    time_series_table::{
        SegmentMetaSnafu, TimeSeriesTable,
        error::{
            CoverageOverlapSnafu, EntityMismatchSnafu, ExistingSegmentMissingCoverageSnafu,
            MissingCanonicalSchemaSnafu, SchemaCompatibilitySnafu, SegmentCoverageSnafu,
            SegmentEntityIdentitySnafu, StorageSnafu, TableError, TransactionLogSnafu,
        },
    },
    transaction_log::{
        LogAction, SegmentId, TableState, segments::segment_id_v1,
        table_state::TableCoveragePointer,
    },
};

fn ensure_existing_segments_have_coverage(state: &TableState) -> Result<(), TableError> {
    for seg in state.segments.values() {
        if seg.coverage_path.is_none() {
            return ExistingSegmentMissingCoverageSnafu {
                segment_id: seg.segment_id.clone(),
            }
            .fail();
        }
    }

    Ok(())
}

impl TimeSeriesTable {
    /// Core append implementation that operates on already-loaded Parquet bytes.
    ///
    /// This contains the full v0.1 append flow (schema adoption/enforcement,
    /// coverage computation + overlap detection, sidecar writes, OCC commit,
    /// and in-memory state update). The public
    /// `append_parquet_segment_with_id` wrapper is responsible only for
    /// fetching the bytes from storage before delegating here.
    ///
    /// Callers must ensure `data` corresponds to `relative_path`; the function
    /// does not re-read from storage.
    async fn append_parquet_segment_with_id_and_bytes(
        &mut self,
        segment_id: SegmentId,
        relative_path: &str,
        time_column: &str,
        data: Bytes,
    ) -> Result<u64, TableError> {
        let rel_path = Path::new(relative_path);
        let expected_version = self.state.version;
        let bucket_spec = self.index.bucket.clone();

        // 0) Coverage readiness checks.
        ensure_existing_segments_have_coverage(&self.state)?;

        // 1) Segment meta + schema.
        let mut segment_meta =
            segment_meta_from_parquet_bytes(rel_path, segment_id, time_column, data.clone())
                .context(SegmentMetaSnafu)?;

        let segment_schema =
            logical_schema_from_parquet_bytes(rel_path, data.clone()).context(SegmentMetaSnafu)?;

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
            let seg_ident = segment_entity_identity_from_parquet_bytes(
                data.clone(),
                rel_path,
                &self.index.entity_columns,
            )
            .context(SegmentEntityIdentitySnafu)?;

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
        let table_cov = self.load_table_snapshot_coverage_with_heal().await?;

        // 4) Compute segment coverage.
        let segment_cov = compute_segment_coverage_from_parquet_bytes(
            rel_path,
            time_column,
            &bucket_spec,
            data.clone(),
        )
        .context(SegmentCoverageSnafu)?;

        // 5) Overlap detection.
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

        let new_version = self
            .log
            .commit_with_expected_version(expected_version, actions)
            .await
            .context(TransactionLogSnafu)?;

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
        self.state.version = new_version;

        if let Some(updated_meta) = maybe_updated_meta {
            self.state.table_meta = updated_meta
        }

        self.state
            .segments
            .insert(segment_meta.segment_id.clone(), segment_meta);

        // Also update the snapshot pointer in state.
        self.state.table_coverage = Some(TableCoveragePointer {
            bucket_spec,
            coverage_path: snapshot_path.to_string_lossy().to_string(),
            version: new_version,
        });

        Ok(new_version)
    }

    /// Append a new Parquet segment with a caller-provided `segment_id`, registering it in the transaction log.
    ///
    /// v0.1 behavior:
    /// - Build SegmentMeta from the Parquet file (ts_min, ts_max, row_count).
    /// - Derive the segment logical schema from the Parquet file.
    /// - If the table has no logical_schema yet, adopt this segment schema
    ///   as canonical and write an UpdateTableMeta + AddSegment commit.
    /// - Otherwise, enforce "no schema evolution" via schema_helpers.
    /// - Compute coverage for the segment and table; reject if coverage overlaps.
    /// - Write the segment coverage sidecar before committing (safe to orphan on failure).
    /// - Commit with OCC on the current version.
    /// - Update in-memory TableState on success.
    ///
    /// v0.1: duplicates (same segment_id/path) are allowed if their coverage
    /// does not overlap existing data; otherwise overlap is rejected.
    ///
    /// This wrapper reads the Parquet bytes from storage, then delegates to
    /// `append_parquet_segment_with_id_and_bytes` for the core logic.
    pub async fn append_parquet_segment_with_id(
        &mut self,
        segment_id: SegmentId,
        relative_path: &str,
        time_column: &str,
    ) -> Result<u64, TableError> {
        let rel_path = Path::new(relative_path);

        let bytes = storage::read_all_bytes(self.location().as_ref(), rel_path)
            .await
            .context(StorageSnafu)?;

        self.append_parquet_segment_with_id_and_bytes(
            segment_id,
            relative_path,
            time_column,
            Bytes::from(bytes),
        )
        .await
    }

    /// Append a Parquet segment using a deterministic, content-derived `segment_id`.
    ///
    /// This wrapper reads the Parquet bytes from storage, derives `segment_id`
    /// via `segment_id_v1(relative_path, bytes)`, then delegates to
    /// `append_parquet_segment_with_id_and_bytes` for the core logic.
    /// Behavior (schema adoption/enforcement, coverage, OCC, state updates)
    /// matches `append_parquet_segment_with_id`.
    pub async fn append_parquet_segment(
        &mut self,
        relative_path: &str,
        time_column: &str,
    ) -> Result<u64, TableError> {
        let rel_path = Path::new(relative_path);
        let bytes = storage::read_all_bytes(self.location().as_ref(), rel_path)
            .await
            .context(StorageSnafu)?;
        let data = Bytes::from(bytes);

        let segment_id = segment_id_v1(relative_path, &data);
        self.append_parquet_segment_with_id_and_bytes(segment_id, relative_path, time_column, data)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use super::*;
    use crate::common::time_column::TimeColumnError;
    use crate::coverage::Coverage;
    use crate::helpers::coverage_sidecar::read_coverage_sidecar;
    use crate::storage::{StorageLocation, TableLocation};
    use crate::transaction_log::logical_schema::{LogicalDataType, LogicalTimestampUnit};
    use crate::transaction_log::segments::SegmentMetaError;
    use crate::transaction_log::{
        Commit, CommitError, TableKind, TableMeta, TimeBucket, TimeIndexSpec, TransactionLogStore,
    };
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    #[tokio::test]
    async fn append_parquet_segment_with_id_missing_time_column_errors() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel = "data/seg-no-ts.parquet";
        let path = tmp.path().join(rel);
        write_parquet_without_time_column(&path, &["A"], &[1.0])?;

        let err = table
            .append_parquet_segment_with_id(SegmentId("seg-no-ts".to_string()), rel, "ts")
            .await
            .expect_err("expected missing time column");

        match err {
            TableError::SegmentMeta { source } => {
                assert!(matches!(
                    source,
                    SegmentMetaError::TimeColumn {
                        source: TimeColumnError::Missing { .. },
                        ..
                    }
                ));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_with_id_updates_state_and_log() -> TestResult {
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

        let new_version = table
            .append_parquet_segment_with_id(SegmentId("seg-1".to_string()), rel_path, "ts")
            .await?;

        assert_eq!(new_version, 2);
        assert_eq!(table.state.version, 2);
        let seg = table
            .state
            .segments
            .get(&SegmentId("seg-1".to_string()))
            .expect("segment present");
        assert_eq!(seg.path, rel_path);
        assert_eq!(seg.row_count, 1);
        assert_eq!(seg.ts_min.timestamp_millis(), 1_000);
        assert_eq!(seg.ts_max.timestamp_millis(), 1_000);

        let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
        let commit_path = log_dir.join("0000000002.json");
        assert!(commit_path.is_file());
        let current =
            tokio::fs::read_to_string(log_dir.join(TransactionLogStore::CURRENT_FILE_NAME)).await?;
        assert_eq!(current.trim(), "2");

        let reopened = TimeSeriesTable::open(location).await?;
        assert!(
            reopened
                .state
                .segments
                .contains_key(&SegmentId("seg-1".to_string()))
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

        let version = table
            .append_parquet_segment_with_id(SegmentId("seg-entity-a".to_string()), rel_path, "ts")
            .await?;
        assert_eq!(version, 2);

        let expected_identity = BTreeMap::from([("symbol".to_string(), "A".to_string())]);
        assert_eq!(
            table.state.table_meta.entity_identity,
            Some(expected_identity.clone())
        );

        let commit_path = tmp
            .path()
            .join(TransactionLogStore::LOG_DIR_NAME)
            .join("0000000002.json");
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

        table
            .append_parquet_segment_with_id(
                SegmentId("seg-entity-a-1".to_string()),
                rel_path1,
                "ts",
            )
            .await?;

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

        let version = table
            .append_parquet_segment_with_id(
                SegmentId("seg-entity-a-2".to_string()),
                rel_path2,
                "ts",
            )
            .await?;
        assert_eq!(version, 3);

        let expected_identity = BTreeMap::from([("symbol".to_string(), "A".to_string())]);
        assert_eq!(
            table.state.table_meta.entity_identity,
            Some(expected_identity.clone())
        );

        let commit_path = tmp
            .path()
            .join(TransactionLogStore::LOG_DIR_NAME)
            .join("0000000003.json");
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
        table
            .append_parquet_segment_with_id(SegmentId("seg-entity-a".to_string()), rel_path1, "ts")
            .await?;

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
            .append_parquet_segment_with_id(SegmentId("seg-entity-b".to_string()), rel_path2, "ts")
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

        let commit_path = tmp
            .path()
            .join(TransactionLogStore::LOG_DIR_NAME)
            .join("0000000003.json");
        assert!(!commit_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_with_id_adopts_schema_when_missing() -> TestResult {
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
            format_version: 1,
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

        let new_version = table
            .append_parquet_segment_with_id(SegmentId("seg-adopt".to_string()), rel_path, "ts")
            .await?;

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
    async fn append_parquet_segment_with_id_rejects_schema_mismatch() -> TestResult {
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
            .append_parquet_segment_with_id(SegmentId("seg-bad".to_string()), rel_path, "ts")
            .await
            .expect_err("expected schema mismatch");

        match err {
            TableError::SchemaCompatibility { source } => {
                assert!(matches!(
                    source,
                    crate::helpers::schema::SchemaCompatibilityError::MissingColumn { .. }
                ));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_with_id_allows_same_id_with_nonoverlapping_coverage()
    -> TestResult {
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
        let v2 = table
            .append_parquet_segment_with_id(SegmentId("seg-dup".to_string()), rel_path, "ts")
            .await?;
        assert_eq!(v2, 2);
        assert_eq!(table.state.version, 2);
        assert_eq!(
            table
                .state
                .segments
                .get(&SegmentId("seg-dup".to_string()))
                .unwrap()
                .row_count,
            1
        );

        // Overwrite the same path with different content and append again with the same segment ID.
        write_test_parquet(
            &abs_path,
            true,
            false,
            &[
                TestRow {
                    ts_millis: 120_000,
                    symbol: "A",
                    price: 20.0,
                },
                TestRow {
                    ts_millis: 121_000,
                    symbol: "A",
                    price: 30.0,
                },
            ],
        )?;

        let v3 = table
            .append_parquet_segment_with_id(SegmentId("seg-dup".to_string()), rel_path, "ts")
            .await?;
        assert_eq!(v3, 3);
        assert_eq!(table.state.version, 3);

        let seg = table
            .state
            .segments
            .get(&SegmentId("seg-dup".to_string()))
            .expect("segment retained after duplicate append");
        assert_eq!(seg.row_count, 2);
        assert_eq!(seg.ts_min.timestamp_millis(), 120_000);
        assert_eq!(seg.ts_max.timestamp_millis(), 121_000);

        let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
        assert!(log_dir.join("0000000003.json").is_file());
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_generates_id_and_updates_snapshot() -> TestResult {
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

        let data1 = Bytes::from(tokio::fs::read(&path1).await?);
        let data2 = Bytes::from(tokio::fs::read(&path2).await?);

        let expected_id1 = segment_id_v1(rel1, &data1);
        let expected_id2 = segment_id_v1(rel2, &data2);

        let seg1 = table
            .state
            .segments
            .get(&expected_id1)
            .expect("segment 1 present");
        let seg2 = table
            .state
            .segments
            .get(&expected_id2)
            .expect("segment 2 present");
        assert!(seg1.coverage_path.is_some());
        assert!(seg2.coverage_path.is_some());

        let bucket_spec = table.index_spec().bucket.clone();

        let cov1 = compute_segment_coverage_from_parquet_bytes(
            Path::new(rel1),
            "ts",
            &bucket_spec,
            data1.clone(),
        )?;
        let cov2 = compute_segment_coverage_from_parquet_bytes(
            Path::new(rel2),
            "ts",
            &bucket_spec,
            data2.clone(),
        )?;
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

        let data1 = Bytes::from(tokio::fs::read(&path1).await?);
        let data2 = Bytes::from(tokio::fs::read(&path2).await?);

        let cov1 = compute_segment_coverage_from_parquet_bytes(
            Path::new(rel1),
            "ts",
            &bucket_spec,
            data1,
        )?;
        let cov2 = compute_segment_coverage_from_parquet_bytes(
            Path::new(rel2),
            "ts",
            &bucket_spec,
            data2,
        )?;
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

        let seg_id = state
            .segments
            .keys()
            .next()
            .expect("segment present")
            .clone();
        state
            .segments
            .get_mut(&seg_id)
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
        let (corrupt_seg_id, corrupt_cov_path) = state
            .segments
            .iter()
            .next()
            .map(|(id, meta)| {
                (
                    id.clone(),
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
                segment_id,
                coverage_path,
                ..
            } => {
                assert_eq!(segment_id, corrupt_seg_id);
                assert_eq!(coverage_path, corrupt_cov_path);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_with_id_conflict_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel_path = "data/conflict.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_test_parquet(
            &abs_path,
            true,
            false,
            &[TestRow {
                ts_millis: 10_000,
                symbol: "X",
                price: 100.0,
            }],
        )?;

        // Simulate an external writer advancing CURRENT to version 2 while this handle is at version 1.
        table
            .log
            .commit_with_expected_version(1, vec![])
            .await
            .expect("external commit succeeds");

        let err = table
            .append_parquet_segment_with_id(SegmentId("seg-conflict".to_string()), rel_path, "ts")
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

        table
            .append_parquet_segment_with_id(SegmentId("seg-a".to_string()), rel1, "ts")
            .await?;

        // Simulate legacy/bad state: drop coverage_path on the existing segment.
        let seg = table
            .state
            .segments
            .get_mut(&SegmentId("seg-a".to_string()))
            .expect("segment present");
        seg.coverage_path = None;

        let err = table
            .append_parquet_segment_with_id(SegmentId("seg-b".to_string()), rel2, "ts")
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

        table
            .append_parquet_segment_with_id(SegmentId("seg-a".to_string()), rel1, "ts")
            .await?;

        // Simulate missing snapshot pointer while segments exist.
        table.state.table_coverage = None;

        table
            .append_parquet_segment_with_id(SegmentId("seg-b".to_string()), rel2, "ts")
            .await?;

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

        table
            .append_parquet_segment_with_id(SegmentId("seg-a".to_string()), rel1, "ts")
            .await?;

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
            .append_parquet_segment_with_id(SegmentId("seg-b".to_string()), rel2, "ts")
            .await
            .expect_err("append should fail when snapshot bucket mismatches index");

        assert!(matches!(
            err,
            TableError::TableCoverageBucketMismatch { .. }
        ));
        Ok(())
    }
}
