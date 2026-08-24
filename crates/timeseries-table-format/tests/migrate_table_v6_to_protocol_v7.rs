#![allow(missing_docs)]

use std::{
    collections::BTreeMap,
    env, fs,
    num::NonZeroU64,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use serde_json::Value;
use tempfile::TempDir;
use timeseries_table_format::{
    coverage::EntityValue,
    metadata::{
        index::{IndexKind, IndexSpec},
        table::TableMeta,
    },
    storage::TableLocation,
    table::TimeSeriesTable,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn batch(values: &[i64], tags: &[&str]) -> TestResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("idx", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(StringArray::from(tags.to_vec())),
        ],
    )?)
}

fn downgrade_int64_kind(kind: &mut Value) -> TestResult {
    let kind = kind.as_object_mut().ok_or("index kind is not an object")?;
    let granularity = kind
        .remove("index_granularity")
        .ok_or("index kind has no index_granularity")?;
    kind.insert("bucket_width".to_string(), granularity);
    Ok(())
}

fn downgrade_fixture_to_v6(root: &Path) -> TestResult {
    let log = root.join("_timeseries_log");
    let current: u64 = fs::read_to_string(log.join("CURRENT"))?.trim().parse()?;
    for version in 1..=current {
        let path = log.join(format!("{version:010}.json"));
        let mut commit: Value = serde_json::from_slice(&fs::read(&path)?)?;
        for action in commit["actions"]
            .as_array_mut()
            .ok_or("actions is not an array")?
        {
            if let Some(metadata) = action.get_mut("UpdateTableMeta") {
                let metadata = metadata
                    .as_object_mut()
                    .ok_or("metadata is not an object")?;
                if metadata.remove("protocol_version") != Some(Value::from(7)) {
                    return Err("fixture metadata is not protocol 7".into());
                }
                metadata.remove("required_reader_features");
                metadata.remove("required_writer_features");
                metadata.insert("format_version".to_string(), Value::from(6));
                downgrade_int64_kind(&mut metadata["kind"]["TimeSeries"]["kind"])?;
            } else if let Some(coverage) = action.get_mut("UpdateTableCoverage") {
                downgrade_int64_kind(&mut coverage["index_kind"])?;
            }
        }
        fs::write(path, serde_json::to_vec_pretty(&commit)?)?;
    }
    Ok(())
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
    start: i64,
    end: i64,
) -> TestResult<Vec<(i64, String)>> {
    let mut scan = table.scan_range(start, end).await?;
    let mut rows = Vec::new();
    while let Some(batch) = scan.try_next().await? {
        let indices = batch
            .column_by_name("idx")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or("scan returned no Int64 idx column")?;
        let tags = batch
            .column_by_name("tag")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or("scan returned no Utf8 tag column")?;
        rows.extend(
            (0..batch.num_rows()).map(|row| (indices.value(row), tags.value(row).to_string())),
        );
    }
    rows.sort();
    Ok(rows)
}

#[tokio::test]
async fn migration_fixture_opens_and_operates_through_protocol_v7_core() -> TestResult {
    let tmp = TempDir::new()?;
    let source = tmp.path().join("source-v6");
    let destination = tmp.path().join("destination-v7");
    let disposable = tmp.path().join("disposable-v7");
    let index = IndexSpec {
        column: "idx".to_string(),
        entity_columns: vec![],
        kind: IndexKind::Int64 {
            index_granularity: NonZeroU64::new(10).unwrap(),
        },
    };
    let mut created = TimeSeriesTable::create(
        TableLocation::local(&source),
        TableMeta::new_time_series(index),
    )
    .await?;
    created
        .append(batch(&[0, 10, 20], &["a", "b", "c"])?)
        .await?;
    let expected_state = created.state().clone();

    downgrade_fixture_to_v6(&source)?;
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
    assert_eq!(migrated.state(), &expected_state);
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
    assert_eq!(
        scan_rows(&migrated, 0, 30).await?,
        [(0, "a".into()), (10, "b".into()), (20, "c".into())]
    );
    assert_eq!(migrated.coverage_ratio_for_range(0i64, 30i64).await?, 1.0);
    assert_eq!(migrated.max_gap_len_for_range(0i64, 30i64).await?, 0);

    copy_tree(&destination, &disposable)?;
    let mut writable = TimeSeriesTable::open(TableLocation::local(&disposable)).await?;
    assert_eq!(writable.append(batch(&[30, 40], &["d", "e"])?).await?, 3);
    let reopened = TimeSeriesTable::open(TableLocation::local(&disposable)).await?;
    assert_eq!(
        scan_rows(&reopened, 0, 50).await?,
        [
            (0, "a".into()),
            (10, "b".into()),
            (20, "c".into()),
            (30, "d".into()),
            (40, "e".into()),
        ]
    );
    assert_eq!(reopened.coverage_ratio_for_range(0i64, 50i64).await?, 1.0);
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
