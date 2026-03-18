//! Integration test for latest snapshot helpers on TimeSeriesTable.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use chrono::{TimeZone, Utc};
use serde_json::json;
use tempfile::TempDir;
use timeseries_table_core::metadata::segments::{FileFormat, SegmentId, SegmentMeta};
use timeseries_table_core::metadata::table_metadata::{TableMeta, TimeBucket, TimeIndexSpec};
use timeseries_table_core::storage::{TableLocation, layout};
use timeseries_table_core::table::TimeSeriesTable;
use timeseries_table_core::transaction_log::{LogAction, TransactionLogStore};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn make_basic_table_meta() -> TableMeta {
    let index = TimeIndexSpec {
        timestamp_column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        bucket: TimeBucket::Minutes(1),
        timezone: None,
    };

    TableMeta::new_time_series(index)
}

async fn write_checkpoint_json(
    tmp: &TempDir,
    version: u64,
    table_meta: &TableMeta,
    live_segments: &[SegmentMeta],
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
        "table_coverage": null,
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

#[tokio::test]
async fn load_latest_state_sees_new_commits() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());

    let meta = make_basic_table_meta();
    let _writer = TimeSeriesTable::create(location.clone(), meta).await?;

    // Open a "stale" handle (version == 1).
    let stale = TimeSeriesTable::open(location.clone()).await?;

    // Commit a new segment directly through the log (version 2).
    let log = TransactionLogStore::new(location.clone());
    let seg = SegmentMeta {
        segment_id: SegmentId("seg_0001".to_string()),
        path: "data/seg_0001.parquet".to_string(),
        format: FileFormat::Parquet,
        ts_min: Utc.timestamp_opt(10, 0).single().unwrap(),
        ts_max: Utc.timestamp_opt(20, 0).single().unwrap(),
        row_count: 1,
        file_size: None,
        coverage_path: None,
    };

    log.commit_with_expected_version(1, vec![LogAction::AddSegment(seg.clone())])
        .await?;

    // Stale in-memory snapshot is still old.
    assert_eq!(stale.state().version, 1);

    // But latest helpers see new version and segment.
    let v = stale.current_version().await?;
    assert_eq!(v, 2);

    let latest = stale.load_latest_state().await?;
    assert_eq!(latest.version, 2);
    assert!(latest.segments.contains_key(&seg.segment_id));
    let latest_seg = latest
        .segments
        .get(&seg.segment_id)
        .expect("segment present");
    assert_eq!(latest_seg.ts_min, seg.ts_min);
    assert_eq!(latest_seg.ts_max, seg.ts_max);
    assert!(latest.table_coverage.is_none());

    Ok(())
}

#[tokio::test]
async fn load_latest_state_no_change_returns_current_snapshot() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());

    let meta = make_basic_table_meta();
    let table = TimeSeriesTable::create(location.clone(), meta).await?;

    let v = table.current_version().await?;
    assert_eq!(v, table.state().version);

    let latest = table.load_latest_state().await?;
    assert_eq!(latest.version, table.state().version);
    assert!(latest.segments.is_empty());
    assert!(latest.table_coverage.is_none());
    match latest.table_meta.kind() {
        timeseries_table_core::metadata::table_metadata::TableKind::TimeSeries(_) => {}
        other => panic!("expected time series table kind, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn open_uses_checkpoint_and_replays_only_newer_commits() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());

    let meta = make_basic_table_meta();
    let _writer = TimeSeriesTable::create(location.clone(), meta.clone()).await?;

    let log = TransactionLogStore::new(location.clone());
    let seg1 = SegmentMeta {
        segment_id: SegmentId("seg_0001".to_string()),
        path: "data/seg_0001.parquet".to_string(),
        format: FileFormat::Parquet,
        ts_min: Utc.timestamp_opt(10, 0).single().unwrap(),
        ts_max: Utc.timestamp_opt(20, 0).single().unwrap(),
        row_count: 1,
        file_size: None,
        coverage_path: None,
    };
    let seg2 = SegmentMeta {
        segment_id: SegmentId("seg_0002".to_string()),
        path: "data/seg_0002.parquet".to_string(),
        format: FileFormat::Parquet,
        ts_min: Utc.timestamp_opt(21, 0).single().unwrap(),
        ts_max: Utc.timestamp_opt(30, 0).single().unwrap(),
        row_count: 1,
        file_size: None,
        coverage_path: None,
    };

    let v2 = log
        .commit_with_expected_version(1, vec![LogAction::AddSegment(seg1.clone())])
        .await?;
    let v3 = log
        .commit_with_expected_version(v2, vec![LogAction::AddSegment(seg2.clone())])
        .await?;

    write_checkpoint_json(&tmp, v2, &meta, &[seg1.clone()]).await?;

    let old_commit_abs = tmp.path().join(layout::commit_rel_path(1));
    tokio::fs::write(&old_commit_abs, b"not-json").await?;

    let opened = TimeSeriesTable::open(location.clone()).await?;
    assert_eq!(opened.state().version, v3);
    assert!(opened.state().segments.contains_key(&seg1.segment_id));
    assert!(opened.state().segments.contains_key(&seg2.segment_id));

    Ok(())
}
