//! Integration test for latest snapshot helpers on TimeSeriesTable.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use chrono::{TimeZone, Utc};
use tempfile::TempDir;
use timeseries_table_core::storage::TableLocation;
use timeseries_table_core::time_series_table::TimeSeriesTable;
use timeseries_table_core::transaction_log::{
    FileFormat, LogAction, SegmentId, SegmentMeta, TableMeta, TimeBucket, TimeIndexSpec,
    TransactionLogStore,
};

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

    Ok(())
}
