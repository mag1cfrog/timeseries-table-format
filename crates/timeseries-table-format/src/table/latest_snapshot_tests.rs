//! Integration test for latest snapshot helpers on TimeSeriesTable.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use crate::coverage::EntityIdentity;
use crate::metadata::segments::{FileFormat, SegmentEntityLayout, SegmentMeta};
use crate::metadata::table_metadata::{IndexKind, IndexSpec, TableMeta, TimeBucket};
use crate::storage::TableLocation;
use crate::table::TimeSeriesTable;
use crate::transaction_log::{LogAction, TransactionLogStore};
use chrono::{TimeZone, Utc};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn make_basic_table_meta() -> TableMeta {
    let index = IndexSpec {
        column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        kind: IndexKind::Timestamp {
            bucket: TimeBucket::Minutes(1),
            timezone: None,
        },
    };

    TableMeta::new_time_series(index)
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
        path: "data/seg_0001.parquet".to_string(),
        format: FileFormat::Parquet,
        entity_layout: SegmentEntityLayout::Single(EntityIdentity::try_new(vec!["A".to_string()])?),
        index_min: (Utc.timestamp_opt(10, 0).single().unwrap()).into(),
        index_max: (Utc.timestamp_opt(20, 0).single().unwrap()).into(),
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
    assert!(latest.segments.contains_key(&seg.path));
    let latest_seg = latest.segments.get(&seg.path).expect("segment present");
    assert_eq!(latest_seg.index_min, seg.index_min);
    assert_eq!(latest_seg.index_max, seg.index_max);
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
        crate::metadata::table_metadata::TableKind::TimeSeries(_) => {}
        other => panic!("expected time series table kind, got {other:?}"),
    }

    Ok(())
}
