#![allow(missing_docs)]

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Builder, StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};
use parquet::arrow::{
    ArrowWriter,
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
};
use tempfile::TempDir;
use timeseries_table_format::{
    coverage::EntityValue,
    metadata::logical_schema::{
        LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit,
    },
    metadata::{
        index::{IndexKind, IndexSpec, TimeIndexGranularity},
        table::TableMeta,
    },
    storage::TableLocation,
    table::{AppendError, TableError, TimeSeriesTable},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn ts_from_secs(secs: i64) -> Result<chrono::DateTime<Utc>, &'static str> {
    Utc.timestamp_opt(secs, 0)
        .single()
        .ok_or("invalid timestamp")
}

fn open_parquet_batches(
    path: impl AsRef<Path>,
) -> Result<ParquetRecordBatchReader, Box<dyn std::error::Error>> {
    Ok(ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?)
}

#[tokio::test]
async fn coverage_pipeline_survives_create_open_and_append() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());
    let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta(true)?).await?;

    let rel1 = "data/cov-pipeline-a.parquet";
    let rel2 = "data/cov-pipeline-b.parquet";
    let rel3 = "data/cov-pipeline-c.parquet";
    let rel_overlap = "data/cov-pipeline-overlap.parquet";

    write_parquet_rows(
        &tmp.path().join(rel1),
        &[(1_000, "A", 10.0), (2_000, "B", 20.0)],
    )?;
    write_parquet_rows(
        &tmp.path().join(rel2),
        &[(120_000, "A", 30.0), (121_000, "B", 40.0)],
    )?;
    write_parquet_rows(
        &tmp.path().join(rel3),
        &[(240_000, "A", 50.0), (241_000, "B", 60.0)],
    )?;

    let v2 = table
        .append(open_parquet_batches(tmp.path().join(rel1))?)
        .await?;
    let v3 = table
        .append(open_parquet_batches(tmp.path().join(rel2))?)
        .await?;
    let v4 = table
        .append(open_parquet_batches(tmp.path().join(rel3))?)
        .await?;
    assert_eq!(
        (
            v2.committed_version,
            v3.committed_version,
            v4.committed_version
        ),
        (2, 3, 4)
    );
    assert_eq!(table.state().version, 4);

    for seg in table.state().segments.values() {
        assert!(
            seg.coverage_path.is_some(),
            "segment {} missing coverage_path",
            seg.path
        );
    }

    let ptr = table
        .state()
        .table_coverage
        .as_ref()
        .ok_or_else(|| "table snapshot pointer missing after appends".to_string())?;
    assert_eq!(ptr.index_kind, table.index_spec().kind);
    assert_eq!(ptr.version, table.state().version);

    assert_pipeline_coverage(&table).await?;

    let mut reopened = TimeSeriesTable::open(location.clone()).await?;
    assert_eq!(reopened.state(), table.state());
    let reopened_ptr = reopened
        .state()
        .table_coverage
        .as_ref()
        .ok_or_else(|| "snapshot pointer missing after reopen".to_string())?;
    assert_eq!(reopened_ptr.index_kind, table.index_spec().kind);
    assert_pipeline_coverage(&reopened).await?;
    for (id, seg) in reopened.state().segments.iter() {
        if seg.coverage_path.is_none() {
            return Err(format!("reopened segment {id:?} missing coverage_path").into());
        }
    }

    write_parquet_rows(&tmp.path().join(rel_overlap), &[(121_500, "A", 70.0)])?;
    let err = reopened
        .append(open_parquet_batches(tmp.path().join(rel_overlap))?)
        .await
        .expect_err("overlapping append should fail");
    assert!(matches!(
        err,
        TableError::Append {
            source: AppendError::PersistedIndexIntervalOverlap { .. }
        }
    ));

    assert_pipeline_coverage(&reopened).await?;
    Ok(())
}

#[tokio::test]
async fn coverage_queries_work_end_to_end() -> TestResult {
    // Build coverage over interval IDs 0, 1, 3, 4, and 5 (gap at 2).
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());
    let mut table =
        TimeSeriesTable::create(location.clone(), make_basic_table_meta(false)?).await?;

    write_parquet_rows(
        &tmp.path().join("data/cov-query-a.parquet"),
        &[(1_000, "A", 1.0), (61_000, "A", 2.0)],
    )?;
    write_parquet_rows(
        &tmp.path().join("data/cov-query-b.parquet"),
        &[(180_000, "A", 3.0)],
    )?;
    write_parquet_rows(
        &tmp.path().join("data/cov-query-c.parquet"),
        &[(240_000, "A", 4.0), (300_000, "A", 5.0)],
    )?;
    write_parquet_rows(
        &tmp.path().join("data/cov-query-d.parquet"),
        &[(480_000, "A", 6.0)], // isolated interval ID 8
    )?;

    table
        .append(open_parquet_batches(
            tmp.path().join("data/cov-query-a.parquet"),
        )?)
        .await?;
    table
        .append(open_parquet_batches(
            tmp.path().join("data/cov-query-b.parquet"),
        )?)
        .await?;
    table
        .append(open_parquet_batches(
            tmp.path().join("data/cov-query-c.parquet"),
        )?)
        .await?;
    table
        .append(open_parquet_batches(
            tmp.path().join("data/cov-query-d.parquet"),
        )?)
        .await?;

    // Re-open to exercise snapshot loading path.
    let table = TimeSeriesTable::open(location.clone()).await?;

    let start = ts_from_secs(0)?;
    let end = ts_from_secs(360)?; // [0, 360) spans interval IDs 0..=5

    let ratio = table.coverage_ratio_for_range(start, end).await?;
    assert!((ratio - (5.0 / 6.0)).abs() < 1e-12);

    let gap_len = table.max_gap_len_for_range(start, end).await?;
    assert_eq!(gap_len, 1);

    let last_window = table
        .last_fully_covered_window(end, 2)
        .await?
        .expect("should find contiguous window");
    assert_eq!(
        last_window,
        0x8000_0000_0000_0004u64..=0x8000_0000_0000_0005u64
    );

    // End at an interval boundary to exercise half-open logic.
    let short_end = ts_from_secs(180)?; // interval IDs 0, 1, and 2; IDs 0 and 1 covered
    let short_ratio = table.coverage_ratio_for_range(start, short_end).await?;
    assert!((short_ratio - (2.0 / 3.0)).abs() < 1e-12);

    let short_gap = table.max_gap_len_for_range(start, short_end).await?;
    assert_eq!(short_gap, 1);

    let short_window = table.last_fully_covered_window(short_end, 2).await?;
    assert_eq!(
        short_window,
        Some(0x8000_0000_0000_0000u64..=0x8000_0000_0000_0001u64)
    );

    // A trailing single-interval run at ID 8 is too short.
    // and return the last contiguous run of sufficient length.
    let later_end = ts_from_secs(600)?; // start of interval 10; includes through ID 9
    let window_len = 2;
    let window = table
        .last_fully_covered_window(later_end, window_len)
        .await?
        .expect("window of len >=2 should be found");
    assert_eq!(window, 0x8000_0000_0000_0004u64..=0x8000_0000_0000_0005u64);

    let window_len_three = 3;
    let window_three = table
        .last_fully_covered_window(later_end, window_len_three)
        .await?
        .expect("window of len >=3 should be found");
    assert_eq!(
        window_three,
        0x8000_0000_0000_0003u64..=0x8000_0000_0000_0005u64
    );

    Ok(())
}

async fn assert_pipeline_coverage(table: &TimeSeriesTable) -> TestResult {
    for symbol in ["A", "B"] {
        for interval in 0..5 {
            let ratio = table
                .coverage_ratio_for_entity_range(
                    &[("symbol", EntityValue::from(symbol))],
                    ts_from_secs(interval * 60)?,
                    ts_from_secs((interval + 1) * 60)?,
                )
                .await?;
            assert_eq!(ratio, if interval % 2 == 0 { 1.0 } else { 0.0 });
        }
    }
    Ok(())
}

fn make_basic_table_meta(
    with_entity_column: bool,
) -> Result<TableMeta, Box<dyn std::error::Error>> {
    let index = IndexSpec {
        column: "ts".to_string(),
        entity_columns: if with_entity_column {
            vec!["symbol".to_string()]
        } else {
            Vec::new()
        },
        kind: IndexKind::Timestamp {
            index_granularity: TimeIndexGranularity::Minutes(1),
            timezone: None,
        },
    };

    let logical_schema = LogicalSchema::new(vec![
        LogicalField {
            name: "ts".to_string(),
            data_type: LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Millis,
                timezone: None,
            },
            nullable: false,
        },
        LogicalField {
            name: "symbol".to_string(),
            data_type: LogicalDataType::Utf8,
            nullable: false,
        },
        LogicalField {
            name: "price".to_string(),
            data_type: LogicalDataType::Float64,
            nullable: false,
        },
    ])?;

    Ok(TableMeta::new_time_series_with_schema(
        index,
        logical_schema,
    ))
}

fn write_parquet_rows(
    path: &Path,
    rows: &[(i64, &str, f64)],
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut ts_builder = TimestampMillisecondBuilder::with_capacity(rows.len());
    let mut sym_builder =
        StringBuilder::with_capacity(rows.len(), rows.iter().map(|(_, s, _)| s.len()).sum());
    let mut price_builder = Float64Builder::with_capacity(rows.len());

    for (ts, sym, price) in rows {
        ts_builder.append_value(*ts);
        sym_builder.append_value(sym);
        price_builder.append_value(*price);
    }

    let schema = Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(ts_builder.finish()) as _,
            Arc::new(sym_builder.finish()),
            Arc::new(price_builder.finish()),
        ],
    )?;

    let file = std::fs::File::create(path)?;
    let props = parquet::file::properties::WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
