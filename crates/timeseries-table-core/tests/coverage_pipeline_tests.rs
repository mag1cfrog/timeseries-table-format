#![allow(missing_docs)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Builder, StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;
use timeseries_table_core::{
    coverage::Coverage,
    helpers::coverage_sidecar::read_coverage_sidecar,
    storage::TableLocation,
    time_series_table::{TimeSeriesTable, error::TableError},
    transaction_log::{
        LogicalColumn, LogicalSchema, TableMeta, TimeBucket, TimeIndexSpec,
        table_metadata::{LogicalDataType, LogicalTimestampUnit},
    },
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn coverage_pipeline_survives_create_open_and_append() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());
    let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()?).await?;

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
        &[(120_000, "C", 30.0), (121_000, "D", 40.0)],
    )?;
    write_parquet_rows(
        &tmp.path().join(rel3),
        &[(240_000, "E", 50.0), (241_000, "F", 60.0)],
    )?;

    let v2 = table.append_parquet_segment(rel1, "ts").await?;
    let v3 = table.append_parquet_segment(rel2, "ts").await?;
    let v4 = table.append_parquet_segment(rel3, "ts").await?;
    assert_eq!((v2, v3, v4), (2, 3, 4));
    assert_eq!(table.state().version, 4);

    for seg in table.state().segments.values() {
        assert!(
            seg.coverage_path.is_some(),
            "segment {} missing coverage_path",
            seg.segment_id.0
        );
    }

    let ptr = table
        .state()
        .table_coverage
        .as_ref()
        .ok_or_else(|| "table snapshot pointer missing after appends".to_string())?;
    assert_eq!(ptr.bucket_spec, table.index_spec().bucket);
    assert_eq!(ptr.version, table.state().version);

    let expected = union_segment_coverages(&location, table.state().segments.values()).await?;

    let snapshot_cov = read_coverage_sidecar(&location, Path::new(&ptr.coverage_path)).await?;
    assert_eq!(snapshot_cov.present(), expected.present());

    let mut reopened = TimeSeriesTable::open(location.clone()).await?;
    let reopened_ptr = reopened
        .state()
        .table_coverage
        .as_ref()
        .ok_or_else(|| "snapshot pointer missing after reopen".to_string())?
        .clone();
    assert_eq!(reopened_ptr.bucket_spec, table.index_spec().bucket);
    let reopened_cov =
        read_coverage_sidecar(&location, Path::new(&reopened_ptr.coverage_path)).await?;
    assert_eq!(reopened_cov.present(), expected.present());
    for (id, seg) in reopened.state().segments.iter() {
        if seg.coverage_path.is_none() {
            return Err(format!("reopened segment {id:?} missing coverage_path").into());
        }
    }

    write_parquet_rows(&tmp.path().join(rel_overlap), &[(121_500, "Z", 70.0)])?;
    let err = reopened
        .append_parquet_segment(rel_overlap, "ts")
        .await
        .expect_err("overlapping append should fail");
    assert!(matches!(err, TableError::CoverageOverlap { .. }));

    let snapshot_after =
        read_coverage_sidecar(&location, Path::new(&reopened_ptr.coverage_path)).await?;
    assert_eq!(snapshot_after.present(), expected.present());
    Ok(())
}

async fn union_segment_coverages<'a, I>(
    location: &TableLocation,
    segments: I,
) -> Result<Coverage, Box<dyn std::error::Error>>
where
    I: IntoIterator<Item = &'a timeseries_table_core::transaction_log::SegmentMeta>,
{
    let mut acc = Coverage::empty();
    for seg in segments {
        let cov_path = seg
            .coverage_path
            .as_ref()
            .ok_or_else(|| format!("missing coverage_path for segment {}", seg.segment_id.0))?;
        let cov = read_coverage_sidecar(location, Path::new(cov_path)).await?;
        acc.union_inplace(&cov);
    }
    Ok(acc)
}

fn make_basic_table_meta() -> Result<TableMeta, Box<dyn std::error::Error>> {
    let index = TimeIndexSpec {
        timestamp_column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        bucket: TimeBucket::Minutes(1),
        timezone: None,
    };

    let logical_schema = LogicalSchema::new(vec![
        LogicalColumn {
            name: "ts".to_string(),
            data_type:
                timeseries_table_core::transaction_log::table_metadata::LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Millis,
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
