//! Reconstructing the current table state by replaying log commits.
//!
//! `TableState` materializes the metadata stored in `_timeseries_log/` and the
//! [`LogStore::rebuild_table_state`] helper walks all commits from version 1 up
//! to the `CURRENT` pointer, applying their actions in order. This keeps read
//! logic isolated from the append-only write path and documents the invariant
//! that table readers must see a state consistent with the latest committed
//! version.
use std::collections::HashMap;

use crate::transaction_log::*;

/// Pointer to table coverage metadata including bucket specification, path, and version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCoveragePointer {
    /// Time bucket specification for the coverage metadata.
    pub bucket_spec: TimeBucket,
    /// Path to the coverage metadata file.
    pub coverage_path: String,
    /// Version number associated with this coverage pointer.
    pub version: u64,
}

/// In-memory view of table metadata and live segments, reconstructed from the log.
///
/// Invariant:
/// - `version` matches the CURRENT pointer.
/// - `table_meta` and `segments` are the result of applying all commits from
///   version 1 through `version` in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableState {
    /// Latest committed version recorded in CURRENT.
    pub version: u64,
    /// Table-level metadata reconstructed from the log.
    pub table_meta: TableMeta,
    /// Current live segments keyed by SegmentId.
    pub segments: HashMap<SegmentId, SegmentMeta>,

    /// Optional pointer to the latest table coverage metadata.
    pub table_coverage: Option<TableCoveragePointer>,
}

impl TransactionLogStore {
    /// Rebuild the current TableState by replaying all commits up to CURRENT.
    ///
    /// v0.1 behavior:
    /// - If CURRENT == 0 (no commits), this returns CommitError::CorruptState.
    /// - The first commit must include at least one UpdateTableMeta action
    ///   to bootstrap TableMeta; the last UpdateTableMeta wins.
    pub async fn rebuild_table_state(&self) -> Result<TableState, CommitError> {
        let current_version = self.load_current_version().await?;

        if current_version == 0 {
            // v0.1: treat "no commits" as an uninitialized / corrupt table.
            return CorruptStateSnafu {
                msg: "Cannot rebuild TableState: CURRENT is 0 (no commits)".to_string(),
            }
            .fail();
        }

        let mut table_meta: Option<TableMeta> = None;
        let mut segments: HashMap<SegmentId, SegmentMeta> = HashMap::new();

        let mut table_coverage: Option<TableCoveragePointer> = None;

        // Replay all commits from 1..=current_version in order
        for v in 1..=current_version {
            let commit = self.load_commit(v).await?;

            // Defensive: file name version should match payload
            if commit.version != v {
                return CorruptStateSnafu {
                    msg: format!(
                        "Commit version mismatch: expected {v}, found {} in payload",
                        commit.version
                    ),
                }
                .fail();
            }

            for action in commit.actions {
                match action {
                    LogAction::AddSegment(meta) => {
                        // Insert or replace the segment; latest info wins.
                        segments.insert(meta.segment_id.clone(), meta);
                    }
                    LogAction::RemoveSegment { segment_id } => {
                        segments.remove(&segment_id);
                    }
                    LogAction::UpdateTableMeta(delta) => {
                        // v0.1: full replacement of TableMeta
                        table_meta = Some(delta);
                    }
                    LogAction::UpdateTableCoverage {
                        bucket_spec,
                        coverage_path,
                    } => {
                        table_coverage = Some(TableCoveragePointer {
                            bucket_spec,
                            coverage_path,
                            version: v,
                        })
                    }
                }
            }
        }

        let table_meta = table_meta.context(CorruptStateSnafu {
            msg: format!("No TableMeta found in commits up to version {current_version}",),
        })?;

        Ok(TableState {
            version: current_version,
            table_meta,
            segments,
            table_coverage,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{StorageError, TableLocation};
    use crate::transaction_log::{
        FileFormat, LogAction, SegmentId, SegmentMeta, TableKind, TableMeta, TimeBucket,
        TimeIndexSpec, TransactionLogStore,
    };
    use chrono::TimeZone;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn create_test_log_store() -> (TempDir, TransactionLogStore) {
        let tmp = TempDir::new().expect("create temp dir");
        let location = TableLocation::local(tmp.path());
        let store = TransactionLogStore::new(location);
        (tmp, store)
    }

    fn sample_table_meta() -> TableMeta {
        TableMeta {
            kind: TableKind::TimeSeries(TimeIndexSpec {
                timestamp_column: "ts".to_string(),
                entity_columns: vec!["symbol".to_string()],
                bucket: TimeBucket::Minutes(1),
                timezone: None,
            }),
            logical_schema: None,
            created_at: chrono::Utc
                .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
                .single()
                .expect("valid sample table metadata timestamp"),
            format_version: 1,
            entity_identity: None,
        }
    }

    fn sample_segment(id: &str) -> SegmentMeta {
        SegmentMeta {
            segment_id: SegmentId(id.to_string()),
            path: format!("data/{id}.parquet"),
            format: FileFormat::Parquet,
            ts_min: chrono::Utc
                .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
                .single()
                .expect("valid sample segment ts_min"),
            ts_max: chrono::Utc
                .with_ymd_and_hms(2025, 1, 1, 1, 0, 0)
                .single()
                .expect("valid sample segment ts_max"),
            row_count: 42,
            coverage_path: None,
        }
    }

    #[tokio::test]
    async fn rebuild_table_state_happy_path() -> TestResult {
        let (_tmp, store) = create_test_log_store();
        let meta = sample_table_meta();
        let seg1 = sample_segment("seg1");
        let seg2 = sample_segment("seg2");

        let v1 = store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta.clone())])
            .await?;
        let v2 = store
            .commit_with_expected_version(
                v1,
                vec![
                    LogAction::AddSegment(seg1.clone()),
                    LogAction::AddSegment(seg2.clone()),
                ],
            )
            .await?;
        let v3 = store
            .commit_with_expected_version(
                v2,
                vec![LogAction::RemoveSegment {
                    segment_id: seg1.segment_id.clone(),
                }],
            )
            .await?;

        let state = store.rebuild_table_state().await?;
        assert_eq!(state.version, v3);
        assert_eq!(state.table_meta, meta);
        assert!(state.segments.contains_key(&seg2.segment_id));
        assert!(!state.segments.contains_key(&seg1.segment_id));
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_errors_when_current_zero() {
        let (_tmp, store) = create_test_log_store();

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("expected error");
        assert!(matches!(err, CommitError::CorruptState { .. }));
    }

    #[tokio::test]
    async fn rebuild_table_state_errors_when_no_table_meta() -> TestResult {
        let (_tmp, store) = create_test_log_store();
        let seg = sample_segment("seg");

        store
            .commit_with_expected_version(0, vec![LogAction::AddSegment(seg.clone())])
            .await?;

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("expected error");
        assert!(matches!(err, CommitError::CorruptState { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_fails_on_corrupt_commit_payload() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let meta = sample_table_meta();

        store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
            .await?;

        let commit_path = tmp
            .path()
            .join(TransactionLogStore::LOG_DIR_NAME)
            .join("0000000001.json");
        tokio::fs::write(&commit_path, b"not-json").await?;

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("expected error");
        assert!(matches!(err, CommitError::CorruptState { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_fails_when_commit_missing() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let meta = sample_table_meta();

        store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
            .await?;

        let commit_path = tmp
            .path()
            .join(TransactionLogStore::LOG_DIR_NAME)
            .join("0000000001.json");
        tokio::fs::remove_file(&commit_path).await?;

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("expected error");
        match err {
            CommitError::Storage { source } => match source {
                StorageError::NotFound { .. } => {}
                other => panic!("unexpected storage error: {other:?}"),
            },
            other => panic!("expected storage error, got {other:?}"),
        }
        Ok(())
    }
}
