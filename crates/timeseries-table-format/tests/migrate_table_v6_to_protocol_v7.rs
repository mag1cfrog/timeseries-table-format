#![allow(missing_docs)]

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use arrow::{
    array::{Int64Array, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use chrono::{DateTime, TimeZone, Utc};
use futures::TryStreamExt;
use tempfile::TempDir;
use timeseries_table_format::{
    coverage::EntityValue,
    metadata::index::{IndexKind, TimeIndexGranularity},
    storage::TableLocation,
    table::TimeSeriesTable,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn batch(timestamps: &[i64], values: &[i64]) -> TestResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("value", DataType::Int64, true),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )?)
}

fn tree_bytes(root: &Path) -> TestResult<BTreeMap<PathBuf, Vec<u8>>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.insert(path.strip_prefix(root)?.to_path_buf(), fs::read(path)?);
            }
        }
    }
    Ok(files)
}

fn copy_tree(source: &Path, destination: &Path) -> TestResult {
    fs::create_dir(destination)?;
    for (relative, contents) in tree_bytes(source)? {
        let path = destination.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, contents)?;
    }
    Ok(())
}

async fn scan_rows(
    table: &TimeSeriesTable,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> TestResult<Vec<(i64, i64)>> {
    let mut scan = table.scan_range(start, end).await?;
    let mut rows = Vec::new();
    while let Some(batch) = scan.try_next().await? {
        let timestamps = batch
            .column_by_name("ts")
            .and_then(|column| column.as_any().downcast_ref::<TimestampMicrosecondArray>())
            .ok_or("scan returned no timestamp-microsecond ts column")?;
        let values = batch
            .column_by_name("value")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or("scan returned no Int64 value column")?;
        rows.extend((0..batch.num_rows()).map(|row| (timestamps.value(row), values.value(row))));
    }
    rows.sort();
    Ok(rows)
}

fn hour(value: i64) -> TestResult<DateTime<Utc>> {
    Ok(Utc
        .timestamp_opt(value * 3_600, 0)
        .single()
        .ok_or("invalid test timestamp")?)
}

#[tokio::test]
async fn published_v6_fixture_migrates_and_operates_through_protocol_v7_core() -> TestResult {
    let tmp = TempDir::new()?;
    let source = tmp.path().join("source-v6");
    let destination = tmp.path().join("destination-v7");
    let disposable = tmp.path().join("disposable-v7");
    // Generated once with the published PyPI timeseries-table-format==0.4.0 wheel.
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format_v6_v0_4_0");
    copy_tree(&fixture, &source)?;
    let first_commit: serde_json::Value =
        serde_json::from_slice(&fs::read(source.join("_timeseries_log/0000000001.json"))?)?;
    assert_eq!(
        first_commit["actions"][0]["UpdateTableMeta"]["format_version"],
        6
    );
    assert!(
        first_commit["actions"][0]["UpdateTableMeta"]
            .get("protocol_version")
            .is_none()
    );
    let source_before = tree_bytes(&source)?;
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/migrate_table_v6_to_protocol_v7.py");
    let output = Command::new(env::var_os("PYTHON").unwrap_or_else(|| "python3".into()))
        .arg(script)
        .arg(&source)
        .arg(&destination)
        .output()?;
    assert!(
        output.status.success(),
        "migration failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = String::from_utf8(output.stdout)?;
    assert!(report.contains("validate_migrated_table_from_environment"));
    assert!(report.contains("timeseries_table_format as ttf"));
    assert_eq!(tree_bytes(&source)?, source_before);

    let migrated = TimeSeriesTable::open(TableLocation::local(&destination)).await?;
    assert_eq!(migrated.state().version, 2);
    assert_eq!(migrated.state().table_meta.protocol_version(), 7);
    assert!(
        migrated
            .state()
            .table_meta
            .required_reader_features()
            .is_empty()
    );
    assert!(
        migrated
            .state()
            .table_meta
            .required_writer_features()
            .is_empty()
    );
    assert_eq!(migrated.index_spec().column, "ts");
    assert!(migrated.index_spec().entity_columns.is_empty());
    assert_eq!(
        migrated.index_spec().kind,
        IndexKind::Timestamp {
            index_granularity: TimeIndexGranularity::Hours(1),
            timezone: None,
        }
    );
    assert!(migrated.state().table_meta.logical_schema().is_some());
    assert_eq!(migrated.state().segments.len(), 1);
    let segment = migrated.state().segments.values().next().unwrap();
    assert_eq!(segment.row_count, 3);
    assert!(segment.coverage_path.is_some());
    let coverage = migrated.state().table_coverage.as_ref().unwrap();
    assert_eq!(coverage.version, 2);
    assert_eq!(coverage.index_kind, migrated.index_spec().kind);
    assert_eq!(
        scan_rows(&migrated, hour(0)?, hour(3)?).await?,
        [(0, 10), (3_600_000_000, 20), (7_200_000_000, 30)]
    );
    assert_eq!(
        migrated
            .coverage_ratio_for_range(hour(0)?, hour(3)?)
            .await?,
        1.0
    );
    assert_eq!(migrated.max_gap_len_for_range(hour(0)?, hour(3)?).await?, 0);

    copy_tree(&destination, &disposable)?;
    let mut writable = TimeSeriesTable::open(TableLocation::local(&disposable)).await?;
    assert_eq!(
        writable
            .append(batch(&[10_800_000_000], &[40])?)
            .await?
            .committed_version,
        3
    );
    let reopened = TimeSeriesTable::open(TableLocation::local(&disposable)).await?;
    assert_eq!(
        scan_rows(&reopened, hour(0)?, hour(4)?).await?,
        [
            (0, 10),
            (3_600_000_000, 20),
            (7_200_000_000, 30),
            (10_800_000_000, 40),
        ]
    );
    assert_eq!(
        reopened
            .coverage_ratio_for_range(hour(0)?, hour(4)?)
            .await?,
        1.0
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a private migrated table path"]
async fn validate_migrated_table_from_environment() -> TestResult {
    let path = env::var("TTF_MIGRATED_TABLE")?;
    let expected_current: u64 = env::var("TTF_EXPECTED_CURRENT")?.parse()?;
    let table = TimeSeriesTable::open(TableLocation::local(&path)).await?;
    assert_eq!(table.state().version, expected_current);
    assert_eq!(table.state().table_meta.protocol_version(), 7);
    assert!(
        table
            .state()
            .table_meta
            .required_reader_features()
            .is_empty()
    );
    assert!(
        table
            .state()
            .table_meta
            .required_writer_features()
            .is_empty()
    );
    println!("index: {:?}", table.index_spec());
    println!(
        "logical schema: {:?}",
        table.state().table_meta.logical_schema()
    );
    println!("segments: {:?}", table.state().segments_sorted_by_index()?);
    println!("coverage pointer: {:?}", table.state().table_coverage);

    let Ok(start) = env::var("TTF_COVERAGE_START") else {
        println!("coverage query: skipped; set TTF_COVERAGE_START and TTF_COVERAGE_END");
        return Ok(());
    };
    let end = env::var("TTF_COVERAGE_END")?;
    let entity = env::var("TTF_COVERAGE_ENTITY_JSON")
        .ok()
        .map(|json| serde_json::from_str::<BTreeMap<String, EntityValue>>(&json))
        .transpose()?
        .unwrap_or_default();
    let entity: Vec<(&str, EntityValue)> = entity
        .iter()
        .map(|(column, value)| (column.as_str(), value.clone()))
        .collect();

    macro_rules! query {
        ($start:expr, $end:expr) => {
            if entity.is_empty() {
                (
                    table.coverage_ratio_for_range($start, $end).await?,
                    table.max_gap_len_for_range($start, $end).await?,
                )
            } else {
                (
                    table
                        .coverage_ratio_for_entity_range(&entity, $start, $end)
                        .await?,
                    table
                        .max_gap_len_for_entity_range(&entity, $start, $end)
                        .await?,
                )
            }
        };
    }
    let (ratio, max_gap) = match &table.index_spec().kind {
        IndexKind::Timestamp { .. } => query!(
            DateTime::parse_from_rfc3339(&start)?.with_timezone(&Utc),
            DateTime::parse_from_rfc3339(&end)?.with_timezone(&Utc)
        ),
        IndexKind::Int64 { .. } => query!(start.parse::<i64>()?, end.parse::<i64>()?),
        IndexKind::UInt64 { .. } => query!(start.parse::<u64>()?, end.parse::<u64>()?),
    };
    println!("coverage ratio: {ratio}");
    println!("maximum gap intervals: {max_gap}");
    Ok(())
}
