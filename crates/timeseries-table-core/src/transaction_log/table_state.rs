//! Reconstructing the current table state by replaying log commits.
//!
//! `TableState` materializes the metadata stored in `_timeseries_log/` and the
//! [`LogStore::rebuild_table_state`] helper walks all commits from version 1 up
//! to the `CURRENT` pointer, applying their actions in order. This keeps read
//! logic isolated from the append-only write path and documents the invariant
//! that table readers must see a state consistent with the latest committed
//! version.
use std::collections::HashMap;

#[cfg(feature = "test-counters")]
use std::cell::Cell;

#[cfg(feature = "test-counters")]
thread_local! {
    static REBUILD_TABLE_STATE_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[cfg(feature = "test-counters")]
/// Return the number of rebuilds invoked on the current thread (test-only).
pub fn rebuild_table_state_count() -> usize {
    REBUILD_TABLE_STATE_COUNT.with(|c| c.get())
}

#[cfg(feature = "test-counters")]
/// Reset the rebuild counter to zero (test-only).
pub fn reset_rebuild_table_state_count() {
    REBUILD_TABLE_STATE_COUNT.with(|c| c.set(0));
}

use crate::{metadata::segments::cmp_segment_meta_by_time, transaction_log::*};

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

impl TableState {
    /// Return live segments sorted deterministically by time.
    ///
    /// Ordering is by `ts_min`, then `ts_max`, and finally `segment_id` as a
    /// stable tie-breaker.
    pub fn segments_sorted_by_time(&self) -> Vec<&SegmentMeta> {
        let mut v: Vec<&SegmentMeta> = self.segments.values().collect();
        v.sort_unstable_by(|a, b| cmp_segment_meta_by_time(a, b));
        v
    }
}

fn apply_action_to_materialized_state(
    table_meta: &mut Option<TableMeta>,
    segments: &mut HashMap<SegmentId, SegmentMeta>,
    table_coverage: &mut Option<TableCoveragePointer>,
    version: u64,
    action: LogAction,
) {
    match action {
        LogAction::AddSegment(meta) => {
            segments.insert(meta.segment_id.clone(), meta);
        }
        LogAction::RemoveSegment { segment_id } => {
            segments.remove(&segment_id);
        }
        LogAction::UpdateTableMeta(delta) => *table_meta = Some(delta),
        LogAction::UpdateTableCoverage {
            bucket_spec,
            coverage_path,
        } => {
            *table_coverage = Some(TableCoveragePointer {
                bucket_spec,
                coverage_path,
                version,
            });
        }
    }
}

impl TransactionLogStore {
    async fn replay_commits_into_state(
        &self,
        start_version: u64,
        end_version: u64,
        table_meta: &mut Option<TableMeta>,
        segments: &mut HashMap<SegmentId, SegmentMeta>,
        table_coverage: &mut Option<TableCoveragePointer>,
    ) -> Result<(), CommitError> {
        if start_version > end_version {
            return Ok(());
        }

        for v in start_version..=end_version {
            let commit = self.load_commit(v).await?;

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
                apply_action_to_materialized_state(table_meta, segments, table_coverage, v, action)
            }
        }
        Ok(())
    }

    /// Rebuild the current TableState by loading the latest checkpoint, if any,
    /// and replaying only after commits up to CURRENT.
    ///
    /// v0.1 behavior:
    /// - If CURRENT == 0 (no commits), this returns CommitError::CorruptState.
    /// - The first commit must include at least one UpdateTableMeta action
    ///   to bootstrap TableMeta unless a checkpoint already carries it.
    pub async fn rebuild_table_state(&self) -> Result<TableState, CommitError> {
        #[cfg(feature = "test-counters")]
        REBUILD_TABLE_STATE_COUNT.with(|c| c.set(c.get() + 1));

        let current_version = self.load_current_version().await?;

        if current_version == 0 {
            // v0.1: treat "no commits" as an uninitialized / corrupt table.
            return CorruptStateSnafu {
                msg: "Cannot rebuild TableState: CURRENT is 0 (no commits)".to_string(),
            }
            .fail();
        }

        // let mut table_meta: Option<TableMeta> = None;
        // let mut segments: HashMap<SegmentId, SegmentMeta> = HashMap::new();

        // let mut table_coverage: Option<TableCoveragePointer> = None;

        // // Replay all commits from 1..=current_version in order
        // for v in 1..=current_version {
        //     let commit = self.load_commit(v).await?;

        //     // Defensive: file name version should match payload
        //     if commit.version != v {
        //         return CorruptStateSnafu {
        //             msg: format!(
        //                 "Commit version mismatch: expected {v}, found {} in payload",
        //                 commit.version
        //             ),
        //         }
        //         .fail();
        //     }

        //     for action in commit.actions {
        //         match action {
        //             LogAction::AddSegment(meta) => {
        //                 // Insert or replace the segment; latest info wins.
        //                 segments.insert(meta.segment_id.clone(), meta);
        //             }
        //             LogAction::RemoveSegment { segment_id } => {
        //                 segments.remove(&segment_id);
        //             }
        //             LogAction::UpdateTableMeta(delta) => {
        //                 // v0.1: full replacement of TableMeta
        //                 table_meta = Some(delta);
        //             }
        //             LogAction::UpdateTableCoverage {
        //                 bucket_spec,
        //                 coverage_path,
        //             } => {
        //                 table_coverage = Some(TableCoveragePointer {
        //                     bucket_spec,
        //                     coverage_path,
        //                     version: v,
        //                 })
        //             }
        //         }
        //     }
        // }

        let seed = self
            .load_latest_table_state_checkpoint(current_version)
            .await?;

        let (start_version, mut table_meta, mut segments, mut table_coverage) = match seed {
            Some(state) if state.version == current_version => return Ok(state),
            Some(state) => (
                state.version.checked_add(1).context(CorruptStateSnafu {
                    msg: "checkpoint version overflow".to_string(),
                })?,
                Some(state.table_meta),
                state.segments,
                state.table_coverage,
            ),
            None => (1, None, HashMap::new(), None),
        };

        self.replay_commits_into_state(
            start_version,
            current_version,
            &mut table_meta,
            &mut segments,
            &mut table_coverage,
        )
        .await?;

        let table_meta = table_meta.context(CorruptStateSnafu {
            msg: format!(
                "No TableMeta found in checkpoint/commits up to version {current_version}",
            ),
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
    use crate::metadata::logical_schema::{LogicalDataType, LogicalField, LogicalSchema};
    use crate::storage::layout;
    use crate::storage::{StorageError, TableLocation};
    use crate::transaction_log::{
        FileFormat, LogAction, SegmentId, SegmentMeta, TableKind, TableMeta, TimeBucket,
        TimeIndexSpec, TransactionLogStore,
    };
    use chrono::TimeZone;
    use serde_json::json;
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

    fn sample_table_meta_with_schema(extra_col: &str) -> TableMeta {
        let schema = LogicalSchema::new(vec![
            LogicalField {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: crate::metadata::logical_schema::LogicalTimestampUnit::Micros,
                    timezone: None,
                },
                nullable: false,
            },
            LogicalField {
                name: extra_col.to_string(),
                data_type: LogicalDataType::Float64,
                nullable: true,
            },
        ])
        .expect("valid logical schema");

        TableMeta::new_time_series_with_schema(
            TimeIndexSpec {
                timestamp_column: "ts".to_string(),
                entity_columns: vec!["symbol".to_string()],
                bucket: TimeBucket::Minutes(1),
                timezone: None,
            },
            schema,
        )
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
            file_size: None,
            coverage_path: None,
        }
    }

    fn segment_with_ts(id: &str, ts_min: i64, ts_max: i64) -> SegmentMeta {
        SegmentMeta {
            segment_id: SegmentId(id.to_string()),
            path: format!("data/{id}.parquet"),
            format: FileFormat::Parquet,
            ts_min: chrono::Utc.timestamp_opt(ts_min, 0).single().unwrap(),
            ts_max: chrono::Utc.timestamp_opt(ts_max, 0).single().unwrap(),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        }
    }

    async fn write_checkpoint_json(
        tmp: &TempDir,
        version: u64,
        table_meta: &TableMeta,
        live_segments: &[SegmentMeta],
        table_coverage: Option<&TableCoveragePointer>,
    ) -> TestResult {
        let checkpoint_abs = tmp
            .path()
            .join(layout::table_state_checkpoint_rel_path(version));
        if let Some(parent) = checkpoint_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let payload = json!({
            "checkpoint_format_version": 1,
            "table_version": version,
            "table_meta": table_meta,
            "live_segments": live_segments,
            "table_coverage": table_coverage.map(|ptr| json!({
                "bucket_spec": ptr.bucket_spec,
                "coverage_path": ptr.coverage_path,
                "version": ptr.version,
            })),
        });
        tokio::fs::write(&checkpoint_abs, serde_json::to_vec(&payload)?).await?;

        let latest_abs = tmp
            .path()
            .join(layout::table_state_checkpoint_latest_rel_path());
        if let Some(parent) = latest_abs.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&latest_abs, format!("{version}\n")).await?;

        Ok(())
    }

    #[test]
    fn segments_sorted_by_time_orders_hashmap_deterministically() {
        let mut segments = HashMap::new();
        let seg_c = segment_with_ts("c", 10, 30);
        let seg_a = segment_with_ts("a", 10, 20);
        let seg_d = segment_with_ts("d", 5, 7);
        let seg_b = segment_with_ts("b", 10, 20);

        segments.insert(seg_c.segment_id.clone(), seg_c);
        segments.insert(seg_a.segment_id.clone(), seg_a);
        segments.insert(seg_d.segment_id.clone(), seg_d);
        segments.insert(seg_b.segment_id.clone(), seg_b);

        let state = TableState {
            version: 3,
            table_meta: sample_table_meta(),
            segments,
            table_coverage: None,
        };

        let ordered: Vec<(i64, i64, String)> = state
            .segments_sorted_by_time()
            .iter()
            .map(|seg| {
                (
                    seg.ts_min.timestamp(),
                    seg.ts_max.timestamp(),
                    seg.segment_id.0.clone(),
                )
            })
            .collect();

        let mut expected = ordered.clone();
        expected.sort();
        assert_eq!(ordered, expected);
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

        let commit_path = tmp.path().join(layout::commit_rel_path(1));
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

        let commit_path = tmp.path().join(layout::commit_rel_path(1));
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

    #[tokio::test]
    async fn rebuild_table_state_checkpoint_tail_remove_segment() -> TestResult {
        let (tmp, store) = create_test_log_store();
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

        write_checkpoint_json(&tmp, v2, &meta, &[seg1.clone(), seg2.clone()], None).await?;

        let state = store.rebuild_table_state().await?;
        assert_eq!(state.version, v3);
        assert!(state.segments.contains_key(&seg2.segment_id));
        assert!(!state.segments.contains_key(&seg1.segment_id));

        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_checkpoint_tail_update_table_meta() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let meta_v1 = sample_table_meta_with_schema("price");
        let meta_v2 = sample_table_meta_with_schema("close");

        let v1 = store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta_v1.clone())])
            .await?;
        let v2 = store
            .commit_with_expected_version(v1, vec![LogAction::UpdateTableMeta(meta_v2.clone())])
            .await?;

        write_checkpoint_json(&tmp, v1, &meta_v1, &[], None).await?;

        let state = store.rebuild_table_state().await?;
        assert_eq!(state.version, v2);
        assert_eq!(state.table_meta, meta_v2);

        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_checkpoint_tail_corrupt_commit_fails() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let meta = sample_table_meta();
        let seg1 = sample_segment("seg1");
        let seg2 = sample_segment("seg2");

        let v1 = store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta.clone())])
            .await?;
        let v2 = store
            .commit_with_expected_version(v1, vec![LogAction::AddSegment(seg1.clone())])
            .await?;
        let v3 = store
            .commit_with_expected_version(v2, vec![LogAction::AddSegment(seg2)])
            .await?;

        write_checkpoint_json(&tmp, v2, &meta, &[seg1], None).await?;

        let tail_commit_path = tmp.path().join(layout::commit_rel_path(v3));
        tokio::fs::write(&tail_commit_path, b"not-json").await?;

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("corrupt tail commit should still fail");
        assert!(matches!(err, CommitError::CorruptState { .. }));

        Ok(())
    }
}
