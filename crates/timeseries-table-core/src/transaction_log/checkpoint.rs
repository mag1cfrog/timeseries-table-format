use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    storage,
    transaction_log::{table_state::TableCoveragePointer, *},
};

/// Version number for the `_checkpoints/table_state/*.json` wrie format.
const TABLE_STATE_CHECKPOINT_FORMAT_VERSION: u32 = 1;

/// Versioned checkpoint encoding for table coverage pointers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TableCoveragePointerCheckpointV1 {
    bucket_spec: TimeBucket,
    coverage_path: String,
    version: u64,
}

/// Versioned checkpoint encoding for materialized table state.
///
/// This intentionally uses a sorted `Vec<SegmentMeta>` rather than a `HashMap`
/// so checkpoint bytes are deterministic and easier to inspect.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TableStateCheckpointV1 {
    checkpoint_format_version: u32,
    table_version: u64,
    table_meta: TableMeta,
    live_segments: Vec<SegmentMeta>,
    table_coverage: Option<TableCoveragePointerCheckpointV1>,
}

impl TransactionLogStore {
    async fn load_latest_table_state_checkpoint_version(&self) -> Result<Option<u64>, CommitError> {
        let rel = storage::layout::table_state_checkpoint_latest_rel_path();

        let contents = match storage::read_to_string(self.location().as_ref(), &rel).await {
            Ok(s) => s,
            Err(storage::StorageError::NotFound { .. }) => return Ok(None),
            Err(source) => return Err(CommitError::Storage { source }),
        };

        let trimmed = contents.trim();
        if trimmed.is_empty() {
            return CorruptStateSnafu {
                msg: format!("checkpoint LATEST has empty content at {rel:?}"),
            }
            .fail();
        }

        let version = trimmed
            .parse::<u64>()
            .map_err(|e| CommitError::CorruptState {
                msg: format!("checkpoint LATEST has invalid content {trimmed:?}: {e}"),
                backtrace: Backtrace::capture(),
            })?;

        if version == 0 {
            return CorruptStateSnafu {
                msg: format!("checkpoint LATEST points to invalid version 0 at {rel:?}"),
            }
            .fail();
        }

        Ok(Some(version))
    }

    async fn load_table_state_checkpoint(&self, version: u64) -> Result<TableState, CommitError> {
        let rel = storage::layout::table_state_checkpoint_rel_path(version);

        let json = match storage::read_to_string(self.location().as_ref(), &rel).await {
            Ok(s) => s,
            Err(storage::StorageError::NotFound { .. }) => {
                return CorruptStateSnafu {
                    msg: format!("checkpoint LATEST points to version {version}, but checkpoint file is missing at {rel:?}")
                }.fail();
            }
            Err(source) => return Err(CommitError::Storage { source }),
        };

        let checkpoint: TableStateCheckpointV1 =
            serde_json::from_str(&json).map_err(|e| CommitError::CorruptState {
                msg: format!("failed to parse table-state checkpoint {version}: {e}"),
                backtrace: Backtrace::capture(),
            })?;

        if checkpoint.checkpoint_format_version != TABLE_STATE_CHECKPOINT_FORMAT_VERSION {
            return CorruptStateSnafu {
                msg: format!(
                    "unsupported table-state checkpoint format version {} in checkpoint {}",
                    checkpoint.checkpoint_format_version, version
                ),
            }
            .fail();
        }

        if checkpoint.table_version != version {
            return CorruptStateSnafu {
                msg: format!(
                    "checkpoint file version mismatch: file points to {version}, payload contains {}",
                    checkpoint.table_version
                ),
            }.fail();
        }

        let mut segments = HashMap::with_capacity(checkpoint.live_segments.len());
        for seg in checkpoint.live_segments {
            let segment_id = seg.segment_id.clone();
            if segments.insert(segment_id.clone(), seg).is_some() {
                return CorruptStateSnafu {
                    msg: format!(
                        "checkpoint {version} contains duplicate segement_id {}",
                        segment_id.0
                    ),
                }
                .fail();
            }
        }

        let table_coverage = checkpoint.table_coverage.map(|ptr| TableCoveragePointer {
            bucket_spec: ptr.bucket_spec,
            coverage_path: ptr.coverage_path,
            version: ptr.version,
        });

        if let Some(ptr) = &table_coverage
            && ptr.version > version
        {
            return CorruptStateSnafu {
                msg: format!("checkpoint {version} contains table coverage version {} newer than the checkpoint itself", ptr.version)
            }.fail();
        }

        Ok(TableState {
            version,
            table_meta: checkpoint.table_meta,
            segments,
            table_coverage,
        })
    }

    pub(super) async fn load_latest_table_state_checkpoint(
        &self,
        current_version: u64,
    ) -> Result<Option<TableState>, CommitError> {
        let Some(version) = self.load_latest_table_state_checkpoint_version().await? else {
            return Ok(None);
        };

        if version > current_version {
            return Ok(None);
        }

        self.load_table_state_checkpoint(version).await.map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tempfile::TempDir;

    use crate::{
        storage::{StorageError, TableLocation, layout},
        transaction_log::{
            FileFormat, LogAction, SegmentId, SegmentMeta, TableKind, TableMeta, TimeBucket,
            TimeIndexSpec,
        },
    };

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

    fn sample_segment(id: &str, hour: u32) -> SegmentMeta {
        SegmentMeta {
            segment_id: SegmentId(id.to_string()),
            path: format!("data/{id}.parquet"),
            format: FileFormat::Parquet,
            ts_min: chrono::Utc
                .with_ymd_and_hms(2025, 1, 1, hour, 0, 0)
                .single()
                .expect("valid sample segment ts_min"),
            ts_max: chrono::Utc
                .with_ymd_and_hms(2025, 1, 1, hour + 1, 0, 0)
                .single()
                .expect("valid sample segment ts_max"),
            row_count: 42,
            file_size: None,
            coverage_path: None,
        }
    }

    async fn write_checkpoint_files(
        tmp: &TempDir,
        checkpoint: &TableStateCheckpointV1,
    ) -> TestResult {
        let checkpoint_abs = tmp.path().join(layout::table_state_checkpoint_rel_path(
            checkpoint.table_version,
        ));
        if let Some(parent) = checkpoint_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&checkpoint_abs, serde_json::to_vec(checkpoint)?).await?;

        let latest_abs = tmp
            .path()
            .join(layout::table_state_checkpoint_latest_rel_path());
        if let Some(parent) = latest_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&latest_abs, format!("{}\n", checkpoint.table_version)).await?;

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_ignores_newer_checkpoint_than_anchor() -> TestResult
    {
        let (tmp, store) = create_test_log_store();
        let meta = sample_table_meta();
        let seg1 = sample_segment("seg1", 0);

        store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta.clone())])
            .await?;

        let checkpoint = TableStateCheckpointV1 {
            checkpoint_format_version: TABLE_STATE_CHECKPOINT_FORMAT_VERSION,
            table_version: 2,
            table_meta: meta,
            live_segments: vec![seg1],
            table_coverage: None,
        };
        write_checkpoint_files(&tmp, &checkpoint).await?;

        let loaded = store.load_latest_table_state_checkpoint(1).await?;
        assert!(loaded.is_none(), "newer checkpoint should be ignored");

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_returns_none_when_latest_missing() -> TestResult {
        let (_tmp, store) = create_test_log_store();

        let loaded = store.load_latest_table_state_checkpoint(5).await?;
        assert!(loaded.is_none(), "missing LATEST should mean no checkpoint");

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_fails_when_latest_is_empty() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let latest_abs = tmp
            .path()
            .join(layout::table_state_checkpoint_latest_rel_path());
        if let Some(parent) = latest_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&latest_abs, b" \n\t").await?;

        let err = store
            .load_latest_table_state_checkpoint(1)
            .await
            .expect_err("expected empty latest pointer to fail");
        assert!(matches!(err, CommitError::CorruptState { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_fails_when_latest_points_to_zero() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let latest_abs = tmp
            .path()
            .join(layout::table_state_checkpoint_latest_rel_path());
        if let Some(parent) = latest_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&latest_abs, b"0\n").await?;

        let err = store
            .load_latest_table_state_checkpoint(1)
            .await
            .expect_err("expected invalid version 0 to fail");
        assert!(matches!(err, CommitError::CorruptState { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn load_table_state_checkpoint_reads_valid_checkpoint() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let meta = sample_table_meta();
        let seg1 = sample_segment("seg1", 0);
        let seg2 = sample_segment("seg2", 1);

        let checkpoint = TableStateCheckpointV1 {
            checkpoint_format_version: TABLE_STATE_CHECKPOINT_FORMAT_VERSION,
            table_version: 7,
            table_meta: meta.clone(),
            live_segments: vec![seg1.clone(), seg2.clone()],
            table_coverage: Some(TableCoveragePointerCheckpointV1 {
                bucket_spec: TimeBucket::Minutes(1),
                coverage_path: "_coverage/table/7-tblcov.roar".to_string(),
                version: 7,
            }),
        };
        write_checkpoint_files(&tmp, &checkpoint).await?;

        let state = store
            .load_latest_table_state_checkpoint(7)
            .await?
            .expect("checkpoint should load");
        assert_eq!(state.version, 7);
        assert_eq!(state.table_meta, meta);
        assert_eq!(state.segments.len(), 2);
        assert!(state.segments.contains_key(&seg1.segment_id));
        assert!(state.segments.contains_key(&seg2.segment_id));
        let ptr = state.table_coverage.expect("coverage pointer");
        assert_eq!(ptr.version, 7);
        assert_eq!(ptr.coverage_path, "_coverage/table/7-tblcov.roar");

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_fails_on_unsupported_format_version() -> TestResult
    {
        let (tmp, store) = create_test_log_store();
        let checkpoint = TableStateCheckpointV1 {
            checkpoint_format_version: TABLE_STATE_CHECKPOINT_FORMAT_VERSION + 1,
            table_version: 4,
            table_meta: sample_table_meta(),
            live_segments: vec![],
            table_coverage: None,
        };
        write_checkpoint_files(&tmp, &checkpoint).await?;

        let err = store
            .load_latest_table_state_checkpoint(4)
            .await
            .expect_err("expected unsupported checkpoint format to fail");
        assert!(matches!(err, CommitError::CorruptState { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_fails_on_payload_version_mismatch() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let checkpoint_abs = tmp.path().join(layout::table_state_checkpoint_rel_path(4));
        if let Some(parent) = checkpoint_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let checkpoint = TableStateCheckpointV1 {
            checkpoint_format_version: TABLE_STATE_CHECKPOINT_FORMAT_VERSION,
            table_version: 5,
            table_meta: sample_table_meta(),
            live_segments: vec![],
            table_coverage: None,
        };
        tokio::fs::write(&checkpoint_abs, serde_json::to_vec(&checkpoint)?).await?;

        let latest_abs = tmp
            .path()
            .join(layout::table_state_checkpoint_latest_rel_path());
        if let Some(parent) = latest_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&latest_abs, b"4\n").await?;

        let err = store
            .load_latest_table_state_checkpoint(4)
            .await
            .expect_err("expected file/payload version mismatch to fail");
        assert!(matches!(err, CommitError::CorruptState { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_fails_on_duplicate_segment_ids() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let seg = sample_segment("dup", 0);

        let checkpoint = TableStateCheckpointV1 {
            checkpoint_format_version: TABLE_STATE_CHECKPOINT_FORMAT_VERSION,
            table_version: 6,
            table_meta: sample_table_meta(),
            live_segments: vec![seg.clone(), seg],
            table_coverage: None,
        };
        write_checkpoint_files(&tmp, &checkpoint).await?;

        let err = store
            .load_latest_table_state_checkpoint(6)
            .await
            .expect_err("expected duplicate segment ids to fail");
        assert!(matches!(err, CommitError::CorruptState { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_fails_on_coverage_version_newer_than_checkpoint()
    -> TestResult {
        let (tmp, store) = create_test_log_store();
        let checkpoint = TableStateCheckpointV1 {
            checkpoint_format_version: TABLE_STATE_CHECKPOINT_FORMAT_VERSION,
            table_version: 8,
            table_meta: sample_table_meta(),
            live_segments: vec![],
            table_coverage: Some(TableCoveragePointerCheckpointV1 {
                bucket_spec: TimeBucket::Minutes(1),
                coverage_path: "_coverage/table/9-tblcov.roar".to_string(),
                version: 9,
            }),
        };
        write_checkpoint_files(&tmp, &checkpoint).await?;

        let err = store
            .load_latest_table_state_checkpoint(8)
            .await
            .expect_err("expected impossible coverage pointer version to fail");
        assert!(matches!(err, CommitError::CorruptState { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_fails_when_latest_is_invalid() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let latest_abs = tmp
            .path()
            .join(layout::table_state_checkpoint_latest_rel_path());
        if let Some(parent) = latest_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&latest_abs, b"not-a-version\n").await?;

        let err = store
            .load_latest_table_state_checkpoint(1)
            .await
            .expect_err("expected corrupt latest pointer");
        assert!(matches!(err, CommitError::CorruptState { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_fails_when_checkpoint_file_missing() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let latest_abs = tmp
            .path()
            .join(layout::table_state_checkpoint_latest_rel_path());
        if let Some(parent) = latest_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&latest_abs, b"3\n").await?;

        let err = store
            .load_latest_table_state_checkpoint(3)
            .await
            .expect_err("expected missing checkpoint file");
        assert!(matches!(err, CommitError::CorruptState { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn load_latest_table_state_checkpoint_fails_on_corrupt_payload() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let checkpoint_abs = tmp.path().join(layout::table_state_checkpoint_rel_path(4));
        if let Some(parent) = checkpoint_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&checkpoint_abs, b"not-json").await?;

        let latest_abs = tmp
            .path()
            .join(layout::table_state_checkpoint_latest_rel_path());
        if let Some(parent) = latest_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&latest_abs, b"4\n").await?;

        let err = store
            .load_latest_table_state_checkpoint(4)
            .await
            .expect_err("expected corrupt checkpoint payload");
        assert!(matches!(err, CommitError::CorruptState { .. }));

        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_uses_exact_checkpoint_without_replaying_any_commits() -> TestResult
    {
        let (tmp, store) = create_test_log_store();
        let meta = sample_table_meta();
        let seg1 = sample_segment("seg1", 0);

        let v1 = store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta.clone())])
            .await?;
        let v2 = store
            .commit_with_expected_version(v1, vec![LogAction::AddSegment(seg1.clone())])
            .await?;

        let checkpoint = TableStateCheckpointV1 {
            checkpoint_format_version: TABLE_STATE_CHECKPOINT_FORMAT_VERSION,
            table_version: v2,
            table_meta: meta.clone(),
            live_segments: vec![seg1.clone()],
            table_coverage: None,
        };
        write_checkpoint_files(&tmp, &checkpoint).await?;

        // If rebuild tries to replay even the anchored version, this corruption will break it.
        let current_commit_abs = tmp.path().join(layout::commit_rel_path(v2));
        tokio::fs::write(&current_commit_abs, b"not-json").await?;

        let state = store.rebuild_table_state().await?;
        assert_eq!(state.version, v2);
        assert_eq!(state.table_meta, meta);
        assert_eq!(state.segments.len(), 1);
        assert!(state.segments.contains_key(&seg1.segment_id));

        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_uses_checkpoint_and_replays_only_newer_commits() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let meta = sample_table_meta();
        let seg1 = sample_segment("seg1", 0);
        let seg2 = sample_segment("seg2", 1);

        let v1 = store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta.clone())])
            .await?;
        let v2 = store
            .commit_with_expected_version(v1, vec![LogAction::AddSegment(seg1.clone())])
            .await?;
        let v3 = store
            .commit_with_expected_version(v2, vec![LogAction::AddSegment(seg2.clone())])
            .await?;

        let checkpoint = TableStateCheckpointV1 {
            checkpoint_format_version: TABLE_STATE_CHECKPOINT_FORMAT_VERSION,
            table_version: v2,
            table_meta: meta.clone(),
            live_segments: vec![seg1.clone()],
            table_coverage: None,
        };
        write_checkpoint_files(&tmp, &checkpoint).await?;

        let old_commit_abs = tmp.path().join(layout::commit_rel_path(v1));
        tokio::fs::write(&old_commit_abs, b"not-json").await?;

        let state = store.rebuild_table_state().await?;
        assert_eq!(state.version, v3);
        assert_eq!(state.table_meta, meta);
        assert!(state.segments.contains_key(&seg1.segment_id));
        assert!(state.segments.contains_key(&seg2.segment_id));

        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_without_checkpoint_still_errors_on_missing_commit() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let meta = sample_table_meta();

        store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
            .await?;

        let commit_path = tmp.path().join(layout::commit_rel_path(1));
        tokio::fs::remove_file(&commit_path).await?;

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("expected missing commit error");
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
