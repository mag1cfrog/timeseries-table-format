//! Integration tests for log-based metadata core.
//!
//! These tests validate end-to-end behavior of the async log writer and reader:
//! - Happy path commit sequences with TableState reconstruction,
//! - Conflict handling via version guards,
//! - Robust handling of missing/malformed metadata.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use chrono::{DateTime, TimeZone, Utc};
use tempfile::TempDir;
use timeseries_table_core::storage::{StorageError, TableLocation};
use timeseries_table_core::transaction_log::table_metadata::{
    LogicalDataType, LogicalTimestampUnit, TABLE_FORMAT_VERSION,
};
use timeseries_table_core::transaction_log::{
    CommitError, FileFormat, LogAction, LogicalColumn, LogicalSchema, SegmentId, SegmentMeta,
    TableKind, TableMeta, TimeBucket, TimeIndexSpec, TransactionLogStore,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

// =============================================================================
// Test Helpers
// =============================================================================

fn create_test_log_store() -> (TempDir, TransactionLogStore) {
    let tmp = TempDir::new().expect("create temp dir");
    let location = TableLocation::local(tmp.path());
    let store = TransactionLogStore::new(location);
    (tmp, store)
}

fn sample_time_index_spec() -> TimeIndexSpec {
    TimeIndexSpec {
        timestamp_column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        bucket: TimeBucket::Minutes(1),
        timezone: None,
    }
}

fn sample_table_meta() -> TableMeta {
    let schema = LogicalSchema::new(vec![
        LogicalColumn {
            name: "ts".to_string(),
            data_type: LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Micros,
                timezone: None,
            },
            nullable: false,
        },
        LogicalColumn {
            name: "symbol".to_string(),
            data_type: LogicalDataType::Utf8,
            nullable: false,
        },
        LogicalColumn {
            name: "price".to_string(),
            data_type: LogicalDataType::Float64,
            nullable: true,
        },
    ])
    .expect("valid logical schema");

    TableMeta::new_time_series_with_schema(sample_time_index_spec(), schema)
}

fn sample_segment(id: &str, ts_hour: u32) -> SegmentMeta {
    SegmentMeta {
        segment_id: SegmentId(id.to_string()),
        path: format!("data/{id}.parquet"),
        format: FileFormat::Parquet,
        ts_min: utc_datetime(2025, 1, 1, ts_hour, 0, 0),
        ts_max: utc_datetime(2025, 1, 1, ts_hour + 1, 0, 0),
        row_count: 1000,
        coverage_path: None,
    }
}

fn utc_datetime(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .expect("valid UTC timestamp")
}

// =============================================================================
// Happy Path Tests
// =============================================================================

/// Test: Fresh directory with no CURRENT or _timeseries_log/ should return version 0.
#[tokio::test]
async fn fresh_directory_returns_version_zero() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let version = store.load_current_version().await?;
    assert_eq!(version, 0);

    Ok(())
}

/// Test: Full happy path - bootstrap table, add segments, verify state reconstruction.
#[tokio::test]
async fn happy_path_commit_and_rebuild_table_state() -> TestResult {
    let (tmp, store) = create_test_log_store();

    let meta = sample_table_meta();
    let seg1 = sample_segment("seg-001", 0);
    let seg2 = sample_segment("seg-002", 1);

    // Commit 1: Bootstrap with UpdateTableMeta + first segment
    let v1 = store
        .commit_with_expected_version(
            0,
            vec![
                LogAction::UpdateTableMeta(meta.clone()),
                LogAction::AddSegment(seg1.clone()),
            ],
        )
        .await?;
    assert_eq!(v1, 1);

    // Verify commit file exists
    let commit_1_path = tmp
        .path()
        .join(TransactionLogStore::LOG_DIR_NAME)
        .join("0000000001.json");
    assert!(
        commit_1_path.exists(),
        "commit file for version 1 should exist"
    );

    // Commit 2: Add second segment
    let v2 = store
        .commit_with_expected_version(v1, vec![LogAction::AddSegment(seg2.clone())])
        .await?;
    assert_eq!(v2, 2);

    // Verify commit file exists
    let commit_2_path = tmp
        .path()
        .join(TransactionLogStore::LOG_DIR_NAME)
        .join("0000000002.json");
    assert!(
        commit_2_path.exists(),
        "commit file for version 2 should exist"
    );

    // Rebuild and verify TableState
    let state = store.rebuild_table_state().await?;

    assert_eq!(state.version, 2);
    assert_eq!(state.segments.len(), 2);
    assert!(state.segments.contains_key(&seg1.segment_id));
    assert!(state.segments.contains_key(&seg2.segment_id));

    // Verify table_meta.kind is TableKind::TimeSeries
    match state.table_meta.kind() {
        TableKind::TimeSeries(spec) => {
            assert_eq!(spec.timestamp_column, "ts");
            assert_eq!(spec.entity_columns, vec!["symbol".to_string()]);
        }
        TableKind::Generic => panic!("expected TimeSeries, got Generic"),
    }

    // Verify logical schema was preserved
    assert!(state.table_meta.logical_schema().is_some());
    let schema = state
        .table_meta
        .logical_schema()
        .expect("logical schema must be present");
    assert_eq!(schema.columns().len(), 3);

    Ok(())
}

/// Test: Multiple sequential commits building up state.
#[tokio::test]
async fn sequential_commits_accumulate_segments() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta = sample_table_meta();

    // Commit 1: Bootstrap
    let v1 = store
        .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta.clone())])
        .await?;
    assert_eq!(v1, 1);

    // Commits 2-5: Add segments one by one
    let mut expected_version = v1;
    for i in 1..=4 {
        let seg = sample_segment(&format!("seg-{i:03}"), i as u32);
        let v = store
            .commit_with_expected_version(expected_version, vec![LogAction::AddSegment(seg)])
            .await?;
        assert_eq!(v, expected_version + 1);
        expected_version = v;
    }

    // Final state should have 4 segments
    let state = store.rebuild_table_state().await?;
    assert_eq!(state.version, 5);
    assert_eq!(state.segments.len(), 4);

    Ok(())
}

/// Test: AddSegment followed by RemoveSegment removes the segment from final state.
#[tokio::test]
async fn remove_segment_removes_from_state() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta = sample_table_meta();
    let seg1 = sample_segment("seg-to-keep", 0);
    let seg2 = sample_segment("seg-to-remove", 1);

    // Commit 1: Bootstrap + add both segments
    let v1 = store
        .commit_with_expected_version(
            0,
            vec![
                LogAction::UpdateTableMeta(meta),
                LogAction::AddSegment(seg1.clone()),
                LogAction::AddSegment(seg2.clone()),
            ],
        )
        .await?;

    // Commit 2: Remove seg2
    let v2 = store
        .commit_with_expected_version(
            v1,
            vec![LogAction::RemoveSegment {
                segment_id: seg2.segment_id.clone(),
            }],
        )
        .await?;

    let state = store.rebuild_table_state().await?;
    assert_eq!(state.version, v2);
    assert_eq!(state.segments.len(), 1);
    assert!(state.segments.contains_key(&seg1.segment_id));
    assert!(!state.segments.contains_key(&seg2.segment_id));

    Ok(())
}

// =============================================================================
// Conflict Tests
// =============================================================================

/// Test: Two "clients" trying to commit with the same expected version.
/// The second commit should fail with CommitError::Conflict.
#[tokio::test]
async fn conflict_when_expected_version_is_stale() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta = sample_table_meta();

    // Both "clients" read expected_version = 0
    let expected_version = store.load_current_version().await?;
    assert_eq!(expected_version, 0);

    // Client 1 commits successfully
    let v1 = store
        .commit_with_expected_version(
            expected_version,
            vec![LogAction::UpdateTableMeta(meta.clone())],
        )
        .await?;
    assert_eq!(v1, 1);

    // Client 2 tries to commit with stale expected_version = 0
    let result = store
        .commit_with_expected_version(
            expected_version,
            vec![LogAction::AddSegment(sample_segment("seg-conflict", 0))],
        )
        .await;

    match result {
        Err(CommitError::Conflict {
            expected, found, ..
        }) => {
            assert_eq!(expected, 0);
            assert_eq!(found, 1);
        }
        other => panic!("expected Conflict error, got: {other:?}"),
    }

    // Verify CURRENT still reflects version 1 (not corrupted by failed commit)
    let current = store.load_current_version().await?;
    assert_eq!(current, 1);

    Ok(())
}

/// Test: Simulate conflict at version 2+ (not just initial commit).
#[tokio::test]
async fn conflict_on_subsequent_version() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta = sample_table_meta();

    // Commit version 1
    store
        .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
        .await?;

    // Commit version 2
    store
        .commit_with_expected_version(1, vec![LogAction::AddSegment(sample_segment("seg-1", 0))])
        .await?;

    // Try to commit with expected=1 (stale, should be 2)
    let result = store
        .commit_with_expected_version(1, vec![LogAction::AddSegment(sample_segment("seg-2", 1))])
        .await;

    match result {
        Err(CommitError::Conflict {
            expected, found, ..
        }) => {
            assert_eq!(expected, 1);
            assert_eq!(found, 2);
        }
        other => panic!("expected Conflict error, got: {other:?}"),
    }

    Ok(())
}

// =============================================================================
// Robustness Tests - Corrupt/Missing State
// =============================================================================

/// Test: Corrupt CURRENT file (non-numeric content) returns CorruptState.
#[tokio::test]
async fn corrupt_current_file_returns_corrupt_state() -> TestResult {
    let (tmp, store) = create_test_log_store();

    // Create corrupt CURRENT file
    let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
    tokio::fs::create_dir_all(&log_dir).await?;
    let current_path = log_dir.join(TransactionLogStore::CURRENT_FILE_NAME);
    tokio::fs::write(&current_path, "not-a-number").await?;

    let result = store.load_current_version().await;
    assert!(
        matches!(result, Err(CommitError::CorruptState { .. })),
        "expected CorruptState, got: {result:?}"
    );

    Ok(())
}

/// Test: Empty CURRENT file returns CorruptState.
#[tokio::test]
async fn empty_current_file_returns_corrupt_state() -> TestResult {
    let (tmp, store) = create_test_log_store();

    let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
    tokio::fs::create_dir_all(&log_dir).await?;
    let current_path = log_dir.join(TransactionLogStore::CURRENT_FILE_NAME);
    tokio::fs::write(&current_path, "").await?;

    let result = store.load_current_version().await;
    assert!(
        matches!(result, Err(CommitError::CorruptState { .. })),
        "expected CorruptState, got: {result:?}"
    );

    Ok(())
}

/// Test: Corrupt commit file (invalid JSON) returns CorruptState on load_commit.
#[tokio::test]
async fn corrupt_commit_file_returns_corrupt_state() -> TestResult {
    let (tmp, store) = create_test_log_store();

    let meta = sample_table_meta();

    // Create a valid commit first
    store
        .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
        .await?;

    // Corrupt the commit file
    let commit_path = tmp
        .path()
        .join(TransactionLogStore::LOG_DIR_NAME)
        .join("0000000001.json");
    tokio::fs::write(&commit_path, "{ invalid json }}}").await?;

    // load_commit should fail with CorruptState
    let result = store.load_commit(1).await;
    assert!(
        matches!(result, Err(CommitError::CorruptState { .. })),
        "expected CorruptState, got: {result:?}"
    );

    // rebuild_table_state should also fail
    let result = store.rebuild_table_state().await;
    assert!(
        matches!(result, Err(CommitError::CorruptState { .. })),
        "expected CorruptState, got: {result:?}"
    );

    Ok(())
}

/// Test: Missing commit file returns Storage(NotFound).
#[tokio::test]
async fn missing_commit_file_returns_storage_not_found() -> TestResult {
    let (tmp, store) = create_test_log_store();

    let meta = sample_table_meta();

    // Create commit 1
    store
        .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
        .await?;

    // Delete the commit file
    let commit_path = tmp
        .path()
        .join(TransactionLogStore::LOG_DIR_NAME)
        .join("0000000001.json");
    tokio::fs::remove_file(&commit_path).await?;

    // load_commit should fail with Storage(NotFound)
    let result = store.load_commit(1).await;
    match result {
        Err(CommitError::Storage {
            source: StorageError::NotFound { .. },
        }) => {}
        other => panic!("expected Storage(NotFound), got: {other:?}"),
    }

    Ok(())
}

/// Test: Leftover .tmp files in _timeseries_log/ are ignored by the reader.
#[tokio::test]
async fn leftover_tmp_files_are_ignored() -> TestResult {
    let (tmp, store) = create_test_log_store();

    let meta = sample_table_meta();
    let seg = sample_segment("seg-1", 0);

    // Commit version 1
    store
        .commit_with_expected_version(
            0,
            vec![
                LogAction::UpdateTableMeta(meta.clone()),
                LogAction::AddSegment(seg.clone()),
            ],
        )
        .await?;

    // Create leftover .tmp files that might be from crashed writes
    let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
    tokio::fs::write(log_dir.join("0000000002.json.tmp"), b"garbage").await?;
    tokio::fs::write(log_dir.join(".tmp_random_file"), b"more garbage").await?;
    tokio::fs::write(log_dir.join("temp_commit.tmp"), b"even more garbage").await?;

    // rebuild_table_state should succeed and ignore .tmp files
    let state = store.rebuild_table_state().await?;
    assert_eq!(state.version, 1);
    assert_eq!(state.segments.len(), 1);

    // Verify .tmp files still exist (weren't cleaned up, just ignored)
    assert!(log_dir.join("0000000002.json.tmp").exists());

    Ok(())
}

/// Test: CURRENT points to version N, but commit file for version < N is missing.
/// This should fail during rebuild_table_state.
#[tokio::test]
async fn missing_intermediate_commit_fails_rebuild() -> TestResult {
    let (tmp, store) = create_test_log_store();

    let meta = sample_table_meta();

    // Create commits 1 and 2
    store
        .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta.clone())])
        .await?;
    store
        .commit_with_expected_version(1, vec![LogAction::AddSegment(sample_segment("seg-1", 0))])
        .await?;

    // Delete commit 1 (intermediate)
    let commit_1_path = tmp
        .path()
        .join(TransactionLogStore::LOG_DIR_NAME)
        .join("0000000001.json");
    tokio::fs::remove_file(&commit_1_path).await?;

    // rebuild_table_state should fail when trying to load commit 1
    let result = store.rebuild_table_state().await;
    match result {
        Err(CommitError::Storage {
            source: StorageError::NotFound { .. },
        }) => {}
        other => panic!("expected Storage(NotFound), got: {other:?}"),
    }

    Ok(())
}

/// Test: rebuild_table_state on empty table (CURRENT == 0) returns CorruptState.
#[tokio::test]
async fn rebuild_on_empty_table_returns_corrupt_state() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let result = store.rebuild_table_state().await;
    assert!(
        matches!(result, Err(CommitError::CorruptState { .. })),
        "expected CorruptState for empty table, got: {result:?}"
    );

    Ok(())
}

/// Test: Table with commits but no UpdateTableMeta action returns CorruptState.
#[tokio::test]
async fn rebuild_without_table_meta_returns_corrupt_state() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    // Commit only AddSegment, no UpdateTableMeta
    store
        .commit_with_expected_version(0, vec![LogAction::AddSegment(sample_segment("seg-1", 0))])
        .await?;

    let result = store.rebuild_table_state().await;
    assert!(
        matches!(result, Err(CommitError::CorruptState { .. })),
        "expected CorruptState for missing TableMeta, got: {result:?}"
    );

    Ok(())
}

// =============================================================================
// Edge Cases
// =============================================================================

/// Test: UpdateTableMeta can be applied multiple times; last one wins.
#[tokio::test]
async fn update_table_meta_last_one_wins() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta1 = TableMeta::new_time_series(TimeIndexSpec {
        timestamp_column: "ts".to_string(),
        entity_columns: vec![],
        bucket: TimeBucket::Minutes(1),
        timezone: None,
    });

    let meta2 = TableMeta::new_time_series(TimeIndexSpec {
        timestamp_column: "event_time".to_string(), // Changed!
        entity_columns: vec!["user_id".to_string()],
        bucket: TimeBucket::Hours(1),
        timezone: Some("UTC".to_string()),
    });

    // Commit 1: First TableMeta
    store
        .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta1)])
        .await?;

    // Commit 2: Updated TableMeta
    store
        .commit_with_expected_version(1, vec![LogAction::UpdateTableMeta(meta2.clone())])
        .await?;

    let state = store.rebuild_table_state().await?;

    // meta2 should win
    match state.table_meta.kind() {
        TableKind::TimeSeries(spec) => {
            assert_eq!(spec.timestamp_column, "event_time");
            assert_eq!(spec.entity_columns, vec!["user_id".to_string()]);
            assert_eq!(spec.bucket, TimeBucket::Hours(1));
        }
        _ => panic!("expected TimeSeries"),
    }
    // Format version is constant in v0.1; this test focuses on "last one wins" for
    // the index spec and related fields. We still sanity-check the version value.
    assert_eq!(state.table_meta.format_version(), TABLE_FORMAT_VERSION);

    Ok(())
}

/// Test: AddSegment with same ID replaces previous segment metadata.
#[tokio::test]
async fn add_segment_with_same_id_replaces() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta = sample_table_meta();

    let seg_v1 = SegmentMeta {
        segment_id: SegmentId("seg-001".to_string()),
        path: "data/seg-001-v1.parquet".to_string(),
        format: FileFormat::Parquet,
        ts_min: utc_datetime(2025, 1, 1, 0, 0, 0),
        ts_max: utc_datetime(2025, 1, 1, 1, 0, 0),
        row_count: 100,
        coverage_path: None,
    };

    let seg_v2 = SegmentMeta {
        segment_id: SegmentId("seg-001".to_string()), // Same ID
        path: "data/seg-001-v2.parquet".to_string(),  // Different path
        format: FileFormat::Parquet,
        ts_min: utc_datetime(2025, 1, 1, 0, 0, 0),
        ts_max: utc_datetime(2025, 1, 1, 2, 0, 0), // Different ts_max
        row_count: 200,                            // Different row_count
        coverage_path: None,
    };

    // Commit 1: Add seg_v1
    store
        .commit_with_expected_version(
            0,
            vec![
                LogAction::UpdateTableMeta(meta),
                LogAction::AddSegment(seg_v1),
            ],
        )
        .await?;

    // Commit 2: Add seg_v2 (same ID, should replace)
    store
        .commit_with_expected_version(1, vec![LogAction::AddSegment(seg_v2.clone())])
        .await?;

    let state = store.rebuild_table_state().await?;

    assert_eq!(state.segments.len(), 1);
    let seg = state
        .segments
        .get(&SegmentId("seg-001".to_string()))
        .expect("segment should be present after replacement");
    assert_eq!(seg.path, "data/seg-001-v2.parquet");
    assert_eq!(seg.row_count, 200);

    Ok(())
}

/// Test: RemoveSegment on non-existent segment is a no-op (doesn't error).
#[tokio::test]
async fn remove_nonexistent_segment_is_noop() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta = sample_table_meta();
    let seg = sample_segment("seg-exists", 0);

    // Commit with one segment
    store
        .commit_with_expected_version(
            0,
            vec![
                LogAction::UpdateTableMeta(meta),
                LogAction::AddSegment(seg.clone()),
            ],
        )
        .await?;

    // Remove a segment that doesn't exist
    store
        .commit_with_expected_version(
            1,
            vec![LogAction::RemoveSegment {
                segment_id: SegmentId("seg-does-not-exist".to_string()),
            }],
        )
        .await?;

    // Should succeed, original segment still there
    let state = store.rebuild_table_state().await?;
    assert_eq!(state.segments.len(), 1);
    assert!(state.segments.contains_key(&seg.segment_id));

    Ok(())
}

/// Test: UpdateTableCoverage actions are replayed into TableState.
#[tokio::test]
async fn table_coverage_pointer_is_replayed() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta = sample_table_meta();
    let coverage_bucket = TimeBucket::Minutes(5);
    let coverage_path = "coverage/0000000002.bitmap".to_string();

    let v1 = store
        .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
        .await?;

    let v2 = store
        .commit_with_expected_version(
            v1,
            vec![LogAction::UpdateTableCoverage {
                bucket_spec: coverage_bucket.clone(),
                coverage_path: coverage_path.clone(),
            }],
        )
        .await?;

    let state = store.rebuild_table_state().await?;
    assert_eq!(state.version, v2);

    let pointer = state
        .table_coverage
        .as_ref()
        .expect("table coverage pointer should be present");
    assert_eq!(pointer.bucket_spec, coverage_bucket);
    assert_eq!(pointer.coverage_path, coverage_path);
    assert_eq!(pointer.version, v2);

    Ok(())
}

/// Test: Multiple UpdateTableCoverage commits – last one wins.
#[tokio::test]
async fn table_coverage_last_one_wins() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta = sample_table_meta();
    let coverage_bucket_v1 = TimeBucket::Minutes(5);
    let coverage_bucket_v2 = TimeBucket::Hours(1);
    let coverage_path_v1 = "coverage/0000000002.bitmap".to_string();
    let coverage_path_v2 = "coverage/0000000003.bitmap".to_string();

    let v1 = store
        .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
        .await?;

    let v2 = store
        .commit_with_expected_version(
            v1,
            vec![LogAction::UpdateTableCoverage {
                bucket_spec: coverage_bucket_v1.clone(),
                coverage_path: coverage_path_v1.clone(),
            }],
        )
        .await?;

    let v3 = store
        .commit_with_expected_version(
            v2,
            vec![LogAction::UpdateTableCoverage {
                bucket_spec: coverage_bucket_v2.clone(),
                coverage_path: coverage_path_v2.clone(),
            }],
        )
        .await?;

    let state = store.rebuild_table_state().await?;
    assert_eq!(state.version, v3);

    let pointer = state
        .table_coverage
        .as_ref()
        .expect("table coverage pointer should be present");
    assert_eq!(pointer.bucket_spec, coverage_bucket_v2);
    assert_eq!(pointer.coverage_path, coverage_path_v2);
    assert_eq!(pointer.version, v3);

    Ok(())
}

/// Test: Absence of UpdateTableCoverage leaves TableState.table_coverage as None.
#[tokio::test]
async fn table_coverage_is_none_when_not_committed() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta = sample_table_meta();
    let seg = sample_segment("seg-without-coverage", 0);

    let v1 = store
        .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
        .await?;
    store
        .commit_with_expected_version(v1, vec![LogAction::AddSegment(seg)])
        .await?;

    let state = store.rebuild_table_state().await?;

    assert_eq!(state.version, 2);
    assert!(state.table_coverage.is_none());

    Ok(())
}

/// Test: Segments with coverage_path and a snapshot pointer are replayed.
#[tokio::test]
async fn table_coverage_rebuilds_with_segment_coverage_paths() -> TestResult {
    let (_tmp, store) = create_test_log_store();

    let meta = sample_table_meta();
    let coverage_bucket = TimeBucket::Minutes(5);
    let segment_cov_path = "coverage/seg-001.roar".to_string();
    let snapshot_cov_path = "coverage/table/0000000002.roar".to_string();

    let mut seg = sample_segment("seg-001", 0);
    seg.coverage_path = Some(segment_cov_path.clone());

    let v1 = store
        .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
        .await?;
    let v2 = store
        .commit_with_expected_version(v1, vec![LogAction::AddSegment(seg.clone())])
        .await?;
    let v3 = store
        .commit_with_expected_version(
            v2,
            vec![LogAction::UpdateTableCoverage {
                bucket_spec: coverage_bucket.clone(),
                coverage_path: snapshot_cov_path.clone(),
            }],
        )
        .await?;

    let state = store.rebuild_table_state().await?;
    assert_eq!(state.version, v3);

    let rebuilt_seg = state
        .segments
        .get(&seg.segment_id)
        .expect("segment present after rebuild");
    assert_eq!(
        rebuilt_seg.coverage_path.as_deref(),
        Some(segment_cov_path.as_str())
    );

    let pointer = state
        .table_coverage
        .as_ref()
        .expect("table coverage pointer should be present");
    assert_eq!(pointer.bucket_spec, coverage_bucket);
    assert_eq!(pointer.coverage_path, snapshot_cov_path);
    assert_eq!(pointer.version, v3);

    Ok(())
}
