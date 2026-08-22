//! Integration tests for the DataFusion table provider.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::fs::File;
use std::num::NonZeroU64;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, Int32Array, Int64Array, StringArray,
    StringBuilder, TimestampMillisecondArray, TimestampMillisecondBuilder, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit, UnionFields, UnionMode};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use chrono::Utc;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::expr_fn::create_udf;
use datafusion::logical_expr::{Expr, Volatility};
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tempfile::TempDir;
use timeseries_table_format::coverage::EntityValue;
use timeseries_table_format::datafusion::TsTableProvider;
use timeseries_table_format::metadata::logical_schema::{
    LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit,
};
use timeseries_table_format::metadata::segments::SegmentEntityLayout;
use timeseries_table_format::storage::{TableLocation, layout};
use timeseries_table_format::table::{TableError, TimeSeriesTable};
use timeseries_table_format::transaction_log::{IndexKind, IndexSpec, TableMeta, TimeBucket};
use timeseries_table_format::{AppendRequest, IntoBatchStream};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn append_with_single_row_groups<S, Kind>(
    table: &mut TimeSeriesTable,
    source: S,
) -> Result<u64, TableError>
where
    S: IntoBatchStream<Kind>,
{
    table
        .append(AppendRequest::new(source).max_rows_per_row_group(1))
        .await
}

#[derive(Clone)]
struct TestRow {
    ts_millis: i64,
    symbol: &'static str,
    price: Option<f64>,
}

const UTC_PRUNING_FILES: &[&str] = &[
    "time-before.parquet",
    "time-target.parquet",
    "time-after.parquet",
];

fn make_index_spec() -> IndexSpec {
    make_index_spec_with_timezone(None)
}

fn make_index_spec_with_timezone(timezone: Option<&str>) -> IndexSpec {
    IndexSpec {
        column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        kind: IndexKind::Timestamp {
            bucket: TimeBucket::Minutes(1),
            timezone: timezone.map(str::to_string),
        },
    }
}

fn make_table_meta(price_nullable: bool) -> Result<TableMeta, Box<dyn std::error::Error>> {
    make_table_meta_with_timezone(price_nullable, None)
}

fn make_table_meta_with_timezone(
    price_nullable: bool,
    timezone: Option<&str>,
) -> Result<TableMeta, Box<dyn std::error::Error>> {
    let logical_schema = LogicalSchema::new(vec![
        LogicalField {
            name: "ts".to_string(),
            data_type: LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Millis,
                timezone: timezone.map(str::to_string),
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
            nullable: price_nullable,
        },
    ])?;

    Ok(TableMeta::new_time_series_with_schema(
        make_index_spec_with_timezone(timezone),
        logical_schema,
    ))
}

fn make_nested_table_meta() -> Result<TableMeta, Box<dyn std::error::Error>> {
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
            name: "attrs".to_string(),
            data_type: LogicalDataType::Struct {
                fields: vec![
                    LogicalField {
                        name: "a".to_string(),
                        data_type: LogicalDataType::Int64,
                        nullable: false,
                    },
                    LogicalField {
                        name: "b".to_string(),
                        data_type: LogicalDataType::Utf8,
                        nullable: true,
                    },
                ],
            },
            nullable: true,
        },
        LogicalField {
            name: "tags".to_string(),
            data_type: LogicalDataType::List {
                elements: Box::new(LogicalField {
                    name: "item".to_string(),
                    data_type: LogicalDataType::Utf8,
                    nullable: true,
                }),
            },
            nullable: true,
        },
        LogicalField {
            name: "metrics".to_string(),
            data_type: LogicalDataType::Map {
                key: Box::new(LogicalField {
                    name: "key".to_string(),
                    data_type: LogicalDataType::Utf8,
                    nullable: false,
                }),
                value: Some(Box::new(LogicalField {
                    name: "value".to_string(),
                    data_type: LogicalDataType::Float64,
                    nullable: true,
                })),
                keys_sorted: false,
            },
            nullable: true,
        },
    ])?;

    Ok(TableMeta::new_time_series_with_schema(
        make_index_spec(),
        logical_schema,
    ))
}

fn make_composite_table_meta() -> TableMeta {
    TableMeta::new_time_series(IndexSpec {
        column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string(), "venue".to_string()],
        kind: IndexKind::Timestamp {
            bucket: TimeBucket::Minutes(1),
            timezone: None,
        },
    })
}

fn make_rows(start: i64, count: usize, symbol: &'static str, price_base: f64) -> Vec<TestRow> {
    (0..count)
        .map(|idx| TestRow {
            ts_millis: start + idx as i64,
            symbol,
            price: Some(price_base + idx as f64),
        })
        .collect()
}

fn make_append_batch(rows: &[(i64, &str, f64)]) -> TestResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

fn make_composite_batch(rows: &[(i64, &str, &str, f64)]) -> TestResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("venue", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

fn minutes_to_millis(minutes: i64) -> i64 {
    minutes * 60_000
}

fn ts_millis(s: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(s)
        .expect("valid rfc3339")
        .with_timezone(&Utc)
        .timestamp_millis()
}

fn write_parquet(path: &Path, rows: &[TestRow], price_nullable: bool) -> TestResult {
    write_parquet_with_props(path, rows, price_nullable, None)
}

fn write_parquet_with_props(
    path: &Path,
    rows: &[TestRow],
    price_nullable: bool,
    props: Option<WriterProperties>,
) -> TestResult {
    write_parquet_with_props_and_tz(path, rows, price_nullable, props, None)
}

fn write_parquet_with_props_and_tz(
    path: &Path,
    rows: &[TestRow],
    price_nullable: bool,
    props: Option<WriterProperties>,
    tz: Option<&str>,
) -> TestResult {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let schema = Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, tz.map(|tz| tz.into())),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, price_nullable),
    ]);

    let mut ts_builder = TimestampMillisecondBuilder::with_capacity(rows.len());
    let mut sym_builder =
        StringBuilder::with_capacity(rows.len(), rows.iter().map(|r| r.symbol.len()).sum());
    let mut price_builder = Float64Builder::with_capacity(rows.len());

    for row in rows {
        ts_builder.append_value(row.ts_millis);
        sym_builder.append_value(row.symbol);
        match (price_nullable, row.price) {
            (true, Some(v)) => price_builder.append_value(v),
            (true, None) => price_builder.append_null(),
            (false, Some(v)) => price_builder.append_value(v),
            (false, None) => return Err("price is None but schema marks it non-nullable".into()),
        }
    }

    let mut ts_array = ts_builder.finish();
    if let Some(tz) = tz {
        ts_array = ts_array.with_timezone_opt(Some(Arc::from(tz)));
    }

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(ts_array),
            Arc::new(sym_builder.finish()),
            Arc::new(price_builder.finish()),
        ],
    )?;

    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), props)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

async fn create_table(tmp: &TempDir, price_nullable: bool) -> TestResult<TimeSeriesTable> {
    let location = TableLocation::local(tmp.path());
    let meta = make_table_meta(price_nullable)?;
    let table = TimeSeriesTable::create(location, meta).await?;
    Ok(table)
}

fn write_segment(
    root: &Path,
    rel_path: &str,
    rows: &[TestRow],
    price_nullable: bool,
) -> TestResult {
    let abs = root.join(rel_path);
    write_parquet(&abs, rows, price_nullable)
}

async fn remove_committed_file_size(root: &Path, version: u64) -> TestResult {
    let commit_path = root.join(layout::commit_rel_path(version));
    let mut commit: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&commit_path).await?)?;
    let segment = commit["actions"]
        .as_array_mut()
        .and_then(|actions| {
            actions
                .iter_mut()
                .find_map(|action| action.get_mut("AddSegment"))
        })
        .and_then(serde_json::Value::as_object_mut)
        .ok_or("append commit contains no AddSegment action")?;
    segment.remove("file_size");
    tokio::fs::write(commit_path, serde_json::to_vec(&commit)?).await?;
    Ok(())
}

fn write_numeric_segment(
    root: &Path,
    rel_path: &str,
    index_type: DataType,
    index_values: ArrayRef,
    tags: &[&str],
) -> TestResult {
    let path = root.join(rel_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let batch = make_numeric_batch(index_type, index_values, tags)?;
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn make_numeric_batch(
    index_type: DataType,
    index_values: ArrayRef,
    tags: &[&str],
) -> TestResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("idx", index_type, false),
        Field::new("tag", DataType::Utf8, false),
    ]));
    Ok(RecordBatch::try_new(
        Arc::clone(&schema),
        vec![index_values, Arc::new(StringArray::from(tags.to_vec()))],
    )?)
}

fn make_numeric_table_meta(kind: IndexKind, data_type: LogicalDataType) -> TestResult<TableMeta> {
    let schema = LogicalSchema::new(vec![
        LogicalField {
            name: "idx".to_string(),
            data_type,
            nullable: false,
        },
        LogicalField {
            name: "tag".to_string(),
            data_type: LogicalDataType::Utf8,
            nullable: false,
        },
    ])?;
    Ok(TableMeta::new_time_series_with_schema(
        IndexSpec {
            column: "idx".to_string(),
            entity_columns: vec![],
            kind,
        },
        schema,
    ))
}

async fn append_int64_segment(
    table: &mut TimeSeriesTable,
    root: &Path,
    rel_path: &str,
    values: &[i64],
    tags: &[&str],
) -> TestResult {
    write_numeric_segment(
        root,
        rel_path,
        DataType::Int64,
        Arc::new(Int64Array::from(values.to_vec())),
        tags,
    )?;
    table.append_parquet_segment(rel_path).await?;
    Ok(())
}

async fn append_uint64_segment(
    table: &mut TimeSeriesTable,
    root: &Path,
    rel_path: &str,
    values: &[u64],
    tags: &[&str],
) -> TestResult {
    write_numeric_segment(
        root,
        rel_path,
        DataType::UInt64,
        Arc::new(UInt64Array::from(values.to_vec())),
        tags,
    )?;
    table.append_parquet_segment(rel_path).await?;
    Ok(())
}

async fn create_int64_table(tmp: &TempDir) -> TestResult<TimeSeriesTable> {
    let meta = make_numeric_table_meta(
        IndexKind::Int64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        },
        LogicalDataType::Int64,
    )?;
    let mut table = TimeSeriesTable::create(TableLocation::local(tmp.path()), meta).await?;
    append_int64_segment(
        &mut table,
        tmp.path(),
        "data/int-a.parquet",
        &[i64::MIN, -10],
        &["other", "other"],
    )
    .await?;
    append_int64_segment(
        &mut table,
        tmp.path(),
        "data/int-b.parquet",
        &[-1],
        &["other"],
    )
    .await?;
    append_int64_segment(
        &mut table,
        tmp.path(),
        "data/int-c.parquet",
        &[0],
        &["zero"],
    )
    .await?;
    append_int64_segment(
        &mut table,
        tmp.path(),
        "data/int-d.parquet",
        &[1, 10],
        &["other", "other"],
    )
    .await?;
    append_int64_segment(
        &mut table,
        tmp.path(),
        "data/int-e.parquet",
        &[20, i64::MAX],
        &["other", "other"],
    )
    .await?;
    Ok(table)
}

async fn create_uint64_table(tmp: &TempDir) -> TestResult<TimeSeriesTable> {
    let meta = make_numeric_table_meta(
        IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        },
        LogicalDataType::UInt64,
    )?;
    let mut table = TimeSeriesTable::create(TableLocation::local(tmp.path()), meta).await?;
    let signed_max = i64::MAX as u64;
    append_uint64_segment(
        &mut table,
        tmp.path(),
        "data/uint-a.parquet",
        &[0],
        &["zero"],
    )
    .await?;
    append_uint64_segment(
        &mut table,
        tmp.path(),
        "data/uint-b.parquet",
        &[signed_max],
        &["other"],
    )
    .await?;
    append_uint64_segment(
        &mut table,
        tmp.path(),
        "data/uint-c.parquet",
        &[signed_max + 1, signed_max + 2],
        &["other", "other"],
    )
    .await?;
    append_uint64_segment(
        &mut table,
        tmp.path(),
        "data/uint-d.parquet",
        &[u64::MAX],
        &["max"],
    )
    .await?;
    Ok(table)
}

async fn create_two_segment_table(tmp: &TempDir) -> TestResult<TimeSeriesTable> {
    let mut table = create_table(tmp, false).await?;

    let rows_a = make_rows(minutes_to_millis(1), 5, "A", 10.0);
    let rows_b = make_rows(minutes_to_millis(3), 5, "A", 20.0);

    write_segment(tmp.path(), "data/seg-a.parquet", &rows_a, false)?;
    table.append_parquet_segment("data/seg-a.parquet").await?;

    write_segment(tmp.path(), "data/seg-b.parquet", &rows_b, false)?;
    table.append_parquet_segment("data/seg-b.parquet").await?;

    Ok(table)
}

async fn create_entity_pruning_table(tmp: &TempDir) -> TestResult<TimeSeriesTable> {
    let mut table = create_table(tmp, false).await?;
    let segments = [
        (
            "data/entity-a.parquet",
            vec![TestRow {
                ts_millis: 0,
                symbol: "A",
                price: Some(10.0),
            }],
        ),
        (
            "data/entity-b.parquet",
            vec![TestRow {
                ts_millis: 60_000,
                symbol: "B",
                price: Some(20.0),
            }],
        ),
        (
            "data/entity-mixed.parquet",
            vec![
                TestRow {
                    ts_millis: 120_000,
                    symbol: "A",
                    price: Some(30.0),
                },
                TestRow {
                    ts_millis: 180_000,
                    symbol: "B",
                    price: Some(40.0),
                },
            ],
        ),
    ];

    for (path, rows) in segments {
        write_segment(tmp.path(), path, &rows, false)?;
        table.append_parquet_segment(path).await?;
    }

    Ok(table)
}

fn int32_entity_table_meta() -> TestResult<TableMeta> {
    Ok(TableMeta::new_time_series_with_schema(
        IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec!["device_id".to_string()],
            kind: IndexKind::Timestamp {
                bucket: TimeBucket::Minutes(1),
                timezone: None,
            },
        },
        LogicalSchema::new(vec![
            LogicalField {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Millis,
                    timezone: None,
                },
                nullable: false,
            },
            LogicalField {
                name: "device_id".to_string(),
                data_type: LogicalDataType::Int32,
                nullable: false,
            },
            LogicalField {
                name: "price".to_string(),
                data_type: LogicalDataType::Float64,
                nullable: false,
            },
        ])?,
    ))
}

fn write_int32_entity_segment(
    root: &Path,
    path: &str,
    timestamps: &[i64],
    device_ids: &[i32],
    prices: &[f64],
) -> TestResult {
    let absolute = root.join(path);
    std::fs::create_dir_all(absolute.parent().ok_or("segment parent")?)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("device_id", DataType::Int32, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
            Arc::new(Int32Array::from(device_ids.to_vec())),
            Arc::new(Float64Array::from(prices.to_vec())),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(std::fs::File::create(absolute)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

async fn create_int32_entity_pruning_table(tmp: &TempDir) -> TestResult<TimeSeriesTable> {
    let mut table =
        TimeSeriesTable::create(TableLocation::local(tmp.path()), int32_entity_table_meta()?)
            .await?;
    let segments = [
        (
            "data/device-negative.parquet",
            vec![0],
            vec![-1],
            vec![10.0],
        ),
        (
            "data/device-maximum.parquet",
            vec![60_000],
            vec![i32::MAX],
            vec![20.0],
        ),
        (
            "data/device-mixed.parquet",
            vec![120_000, 180_000],
            vec![-1, i32::MAX],
            vec![30.0, 40.0],
        ),
    ];
    for (path, timestamps, device_ids, prices) in segments {
        write_int32_entity_segment(tmp.path(), path, &timestamps, &device_ids, &prices)?;
        table.append_parquet_segment(path).await?;
    }
    Ok(table)
}

async fn create_utc_pruning_table(tmp: &TempDir) -> TestResult<TimeSeriesTable> {
    let mut table = create_table(tmp, false).await?;
    let segments = [
        (UTC_PRUNING_FILES[0], 0, 59_999, 10.0),
        (UTC_PRUNING_FILES[1], 60_000, 119_999, 20.0),
        (UTC_PRUNING_FILES[2], 120_000, 179_999, 30.0),
    ];

    for (file, min, max, price) in segments {
        let path = format!("data/{file}");
        let rows = [
            TestRow {
                ts_millis: min,
                symbol: "A",
                price: Some(price),
            },
            TestRow {
                ts_millis: max,
                symbol: "A",
                price: Some(price + 1.0),
            },
        ];
        write_segment(tmp.path(), &path, &rows, false)?;
        table.append_parquet_segment(&path).await?;
    }

    Ok(table)
}

async fn create_zoned_pruning_table(
    tmp: &TempDir,
    timezone: &str,
    segments: &[(&str, &str, &str, f64)],
) -> TestResult<TimeSeriesTable> {
    let meta = make_table_meta_with_timezone(false, Some(timezone))?;
    let mut table = TimeSeriesTable::create(TableLocation::local(tmp.path()), meta).await?;

    for &(file, min, max, price) in segments {
        let path = format!("data/{file}");
        let rows = [
            TestRow {
                ts_millis: ts_millis(min),
                symbol: "A",
                price: Some(price),
            },
            TestRow {
                ts_millis: ts_millis(max),
                symbol: "A",
                price: Some(price + 1.0),
            },
        ];
        write_parquet_with_props_and_tz(
            &tmp.path().join(&path),
            &rows,
            false,
            None,
            Some(timezone),
        )?;
        table.append_parquet_segment(&path).await?;
    }
    Ok(table)
}

async fn create_single_segment_table_with_props(
    tmp: &TempDir,
    rel_path: &str,
    rows: &[TestRow],
    props: WriterProperties,
) -> TestResult<TimeSeriesTable> {
    let mut table = create_table(tmp, false).await?;

    let abs = tmp.path().join(rel_path);
    write_parquet_with_props(&abs, rows, false, Some(props))?;
    table.append_parquet_segment(rel_path).await?;

    Ok(table)
}

fn register_provider(
    ctx: &SessionContext,
    table: Arc<TimeSeriesTable>,
) -> Result<Arc<TsTableProvider>, Box<dyn std::error::Error>> {
    let provider = Arc::new(TsTableProvider::try_new(table)?);
    let provider_dyn: Arc<dyn TableProvider> = provider.clone();

    ctx.register_table("t", provider_dyn)?;
    Ok(provider)
}

async fn collect_batches(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let df = ctx.sql(sql).await?;
    Ok(df.collect().await?)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn explain_plan_text(batches: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    for batch in batches {
        let idx = batch.schema().index_of("plan").unwrap_or_else(|_| {
            // Fallback: use last column if schema is unexpected.
            batch.num_columns().saturating_sub(1)
        });
        let array = batch.column(idx);
        let arr = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected StringArray for explain plan")?;
        for row in 0..arr.len() {
            if !arr.is_null(row) {
                out.push(arr.value(row).to_string());
            }
        }
    }
    Ok(out.join("\n"))
}

fn numeric_select(predicate: Option<&str>) -> String {
    match predicate {
        Some(predicate) => format!("SELECT idx FROM t WHERE {predicate} ORDER BY idx"),
        None => "SELECT idx FROM t ORDER BY idx".to_string(),
    }
}

async fn run_numeric_query(
    ctx: &SessionContext,
    predicate: Option<&str>,
) -> TestResult<(String, Vec<RecordBatch>)> {
    let query = numeric_select(predicate);
    let explain = collect_batches(ctx, &format!("EXPLAIN VERBOSE {query}")).await?;
    let plan = explain_plan_text(&explain)?;
    let results = collect_batches(ctx, &query).await?;
    Ok((plan, results))
}

fn assert_planned_files(plan: &str, all_files: &[&str], expected_files: &[&str]) {
    for file in all_files {
        assert_eq!(
            plan.contains(file),
            expected_files.contains(file),
            "unexpected selection for {file}; plan:\n{plan}"
        );
    }

    let positions = expected_files
        .iter()
        .map(|file| {
            plan.find(file)
                .unwrap_or_else(|| panic!("expected plan to contain {file}; plan:\n{plan}"))
        })
        .collect::<Vec<_>>();
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "selected files are not in deterministic index order; plan:\n{plan}"
    );
}

fn planned_file_names(plan: &dyn ExecutionPlan) -> TestResult<Vec<String>> {
    let exec = find_data_source_exec(plan).ok_or("expected DataSourceExec in physical plan")?;
    let scan = exec
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .ok_or("expected FileScanConfig in DataSourceExec")?;

    scan.file_groups
        .iter()
        .flat_map(|group| group.files())
        .map(|file| {
            file.object_meta
                .location
                .filename()
                .map(str::to_string)
                .ok_or_else(|| "planned file path has no filename".into())
        })
        .collect()
}

async fn run_timestamp_query(
    ctx: &SessionContext,
    predicate: &str,
) -> TestResult<(Vec<String>, Vec<RecordBatch>)> {
    let query = format!("SELECT ts FROM t WHERE {predicate} ORDER BY ts");
    let dataframe = ctx.sql(&query).await?;
    let plan = dataframe.create_physical_plan().await?;
    let files = planned_file_names(plan.as_ref())?;
    let batches = collect(plan, ctx.task_ctx()).await?;
    Ok((files, batches))
}

fn find_data_source_exec(plan: &dyn ExecutionPlan) -> Option<&DataSourceExec> {
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>() {
        return Some(exec);
    }
    for child in plan.children() {
        if let Some(exec) = find_data_source_exec(child.as_ref()) {
            return Some(exec);
        }
    }
    None
}

fn get_pruning_metric(
    metrics: &MetricsSet,
    metric_name: &str,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    match metrics.sum_by_name(metric_name) {
        Some(MetricValue::PruningMetrics {
            pruning_metrics, ..
        }) => Ok((pruning_metrics.pruned(), pruning_metrics.matched())),
        Some(_) => Err(format!("metric '{metric_name}' is not a pruning metric").into()),
        None => Err(format!("metric '{metric_name}' not found").into()),
    }
}

fn first_batch(batches: &[RecordBatch]) -> Result<&RecordBatch, Box<dyn std::error::Error>> {
    batches.first().ok_or_else(|| "no batches returned".into())
}

fn field_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect()
}

fn collect_i64_values(batches: &[RecordBatch]) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    for batch in batches {
        let array = batch.column(0);
        match array.data_type() {
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or("expected TimestampMillisecondArray")?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        return Err("unexpected null value".into());
                    }
                    out.push(arr.value(idx));
                }
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("expected Int64Array")?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        return Err("unexpected null value".into());
                    }
                    out.push(arr.value(idx));
                }
            }
            other => return Err(format!("unexpected value type {other:?}").into()),
        }
    }
    Ok(out)
}

fn collect_u64_values(batches: &[RecordBatch]) -> TestResult<Vec<u64>> {
    let mut out = Vec::new();
    for batch in batches {
        let array = batch.column(0);
        let values = array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or("expected UInt64Array")?;
        for idx in 0..values.len() {
            if values.is_null(idx) {
                return Err("unexpected null value".into());
            }
            out.push(values.value(idx));
        }
    }
    Ok(out)
}

fn scalar_u64(batches: &[RecordBatch]) -> Result<u64, Box<dyn std::error::Error>> {
    let batch = first_batch(batches)?;
    if batch.num_rows() != 1 {
        return Err(format!("expected 1 row, got {}", batch.num_rows()).into());
    }

    let array = batch.column(0);
    match array.data_type() {
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("expected UInt64Array")?;
            Ok(arr.value(0))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("expected Int64Array")?;
            Ok(arr.value(0) as u64)
        }
        other => Err(format!("unexpected count type {other:?}").into()),
    }
}

fn scalar_i64_from_array(array: &dyn Array) -> Result<i64, Box<dyn std::error::Error>> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or("expected TimestampMillisecondArray")?;
            if arr.is_null(0) {
                return Err("unexpected null timestamp".into());
            }
            Ok(arr.value(0))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("expected Int64Array")?;
            Ok(arr.value(0))
        }
        other => Err(format!("unexpected scalar type {other:?}").into()),
    }
}

#[tokio::test]
async fn streaming_append_public_sources_round_trip_exact_rows() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());
    let mut table = TimeSeriesTable::create(location.clone(), make_table_meta(false)?).await?;

    assert!(matches!(
        table.append(Vec::<RecordBatch>::new()).await,
        Err(TableError::EmptyAppendSource)
    ));
    assert_eq!(table.state().version, 1);
    assert!(table.state().segments.is_empty());

    assert_eq!(
        append_with_single_row_groups(
            &mut table,
            make_append_batch(&[(60_000, "A", 11.0), (0, "B", 10.0),])?,
        )
        .await?,
        2
    );
    let first_segment_path = table
        .state()
        .segments
        .values()
        .next()
        .map(|segment| segment.path.clone())
        .ok_or("missing configured append segment")?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(tmp.path().join(first_segment_path))?)?;
    let row_group_rows = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|row_group| row_group.num_rows())
        .collect::<Vec<_>>();
    assert_eq!(row_group_rows, vec![1, 1]);

    assert_eq!(
        append_with_single_row_groups(
            &mut table,
            vec![
                make_append_batch(&[(180_000, "B", 13.0)])?,
                make_append_batch(&[(120_000, "A", 12.0)])?,
            ],
        )
        .await?,
        3
    );

    let iterator_batch = make_append_batch(&[(240_000, "A", 14.0)])?;
    let iterator =
        RecordBatchIterator::new(vec![Ok(iterator_batch.clone())], iterator_batch.schema());
    assert_eq!(
        append_with_single_row_groups(&mut table, iterator).await?,
        4
    );

    let non_send_batch = make_append_batch(&[(300_000, "B", 15.0)])?;
    let non_send_schema = non_send_batch.schema();
    let not_send = Rc::new(());
    let mut non_send_batch = Some(non_send_batch);
    let non_send = RecordBatchIterator::new(
        std::iter::from_fn(move || {
            let _keep_reader_non_send = &not_send;
            non_send_batch.take().map(Ok::<_, ArrowError>)
        }),
        non_send_schema,
    );
    assert_eq!(
        append_with_single_row_groups(&mut table, non_send).await?,
        5
    );

    let boxed_batch = make_append_batch(&[(360_000, "A", 16.0)])?;
    let boxed: Box<dyn RecordBatchReader> = Box::new(RecordBatchIterator::new(
        vec![Ok(boxed_batch.clone())],
        boxed_batch.schema(),
    ));
    assert_eq!(append_with_single_row_groups(&mut table, boxed).await?, 6);

    let sendable_batch = make_append_batch(&[(420_000, "B", 17.0)])?;
    let sendable: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        vec![Ok(sendable_batch.clone())],
        sendable_batch.schema(),
    ));
    assert_eq!(
        append_with_single_row_groups(&mut table, sendable).await?,
        7
    );

    drop(table);
    let reopened = Arc::new(TimeSeriesTable::open(location).await?);
    assert_eq!(reopened.state().version, 7);
    assert_eq!(reopened.state().segments.len(), 6);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, reopened)?;
    let batches =
        collect_batches(&ctx, "SELECT ts, symbol, price FROM t ORDER BY ts, symbol").await?;
    let actual = arrow_select::concat::concat_batches(&first_batch(&batches)?.schema(), &batches)?;
    assert_eq!(
        actual,
        make_append_batch(&[
            (0, "B", 10.0),
            (60_000, "A", 11.0),
            (120_000, "A", 12.0),
            (180_000, "B", 13.0),
            (240_000, "A", 14.0),
            (300_000, "B", 15.0),
            (360_000, "A", 16.0),
            (420_000, "B", 17.0),
        ])?
    );
    Ok(())
}

#[tokio::test]
async fn reordered_parquet_columns_survive_append_query_and_optimize() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());
    let mut table = TimeSeriesTable::create(location, make_table_meta(false)?).await?;
    let source_path = "data/reordered.parquet";
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Float64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let source_batch = RecordBatch::try_new(
        Arc::clone(&source_schema),
        vec![
            Arc::new(Float64Array::from(vec![10.0, 20.0, 11.0, 21.0])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "B"])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_000, 2_000, 3_000, 4_000,
            ])),
        ],
    )?;
    std::fs::create_dir_all(tmp.path().join("data"))?;
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(tmp.path().join(source_path))?,
        source_schema,
        None,
    )?;
    writer.write(&source_batch)?;
    writer.close()?;

    table.append_parquet_segment(source_path).await?;
    assert!(matches!(
        table.state().segments[source_path].entity_layout,
        SegmentEntityLayout::Mixed
    ));
    let canonical_schema = table.state().table_meta.logical_schema().cloned();
    let expected = make_append_batch(&[
        (1_000, "A", 10.0),
        (2_000, "B", 20.0),
        (3_000, "A", 11.0),
        (4_000, "B", 21.0),
    ])?;

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::new(table.clone()))?;
    let batches = collect_batches(&ctx, "SELECT ts, symbol, price FROM t ORDER BY ts").await?;
    let actual = arrow_select::concat::concat_batches(&first_batch(&batches)?.schema(), &batches)?;
    assert_eq!(actual, expected);

    let report = table.optimize().await?;
    assert_eq!(report.source_segments_replaced, 1);
    assert_eq!(report.replacement_segments_written, 2);
    assert_eq!(
        table.state().table_meta.logical_schema(),
        canonical_schema.as_ref()
    );

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::new(table.clone()))?;
    let batches = collect_batches(&ctx, "SELECT ts, symbol, price FROM t ORDER BY ts").await?;
    let actual = arrow_select::concat::concat_batches(&first_batch(&batches)?.schema(), &batches)?;
    assert_eq!(actual, expected);

    assert!(
        table
            .state()
            .segments
            .values()
            .all(|segment| matches!(segment.entity_layout, SegmentEntityLayout::Single(_)))
    );
    Ok(())
}

#[tokio::test]
async fn streaming_append_rejects_panic_inducing_schemas_without_artifacts() -> TestResult {
    let union = DataType::Union(
        UnionFields::try_new([0], [Field::new("member", DataType::Int64, true)])?,
        UnionMode::Sparse,
    );
    let cases = [
        ("union", union.clone(), "cannot be represented exactly"),
        (
            "nested union",
            DataType::List(Arc::new(Field::new("item", union, true))),
            "cannot be represented exactly",
        ),
        (
            "invalid Time32",
            DataType::Time32(TimeUnit::Microsecond),
            "Time32",
        ),
        (
            "invalid Time64",
            DataType::Time64(TimeUnit::Millisecond),
            "Time64",
        ),
        (
            "malformed map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::empty()),
                    false,
                )),
                false,
            ),
            "cannot be represented exactly",
        ),
    ];

    for (case, hostile_type, expected_message) in cases {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_table_meta(false)?).await?;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("hostile", hostile_type, true),
        ]));
        let reader =
            RecordBatchIterator::new(Vec::<Result<RecordBatch, ArrowError>>::new(), schema);

        let error = table
            .append(reader)
            .await
            .expect_err("hostile schema must be rejected");
        let TableError::AppendSource {
            source: ArrowError::SchemaError(message),
        } = error
        else {
            panic!("unexpected {case} error: {error}");
        };
        assert!(message.contains("hostile"), "{case}: {message}");
        assert!(message.contains(expected_message), "{case}: {message}");
        assert_eq!(table.state().version, 1, "{case}");
        assert!(table.state().segments.is_empty(), "{case}");
        assert!(!tmp.path().join("data").exists(), "{case}");
        assert!(
            !tmp.path().join(layout::commit_rel_path(2)).exists(),
            "{case}"
        );

        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state().version, 1, "{case}");
        assert!(reopened.state().segments.is_empty(), "{case}");
    }
    Ok(())
}

#[tokio::test]
async fn streaming_append_rejects_adversarial_source_boundaries_without_artifacts() -> TestResult {
    let schema = make_append_batch(&[])?.schema();
    let wrong_zero = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
        "wrong",
        DataType::Int64,
        false,
    )])));
    let cases = vec![
        (
            "error before first batch",
            vec![Err(ArrowError::ComputeError("first failure".to_string()))],
        ),
        (
            "error after zero-row batch",
            vec![
                Ok(RecordBatch::new_empty(Arc::clone(&schema))),
                Err(ArrowError::ComputeError("failure after zero".to_string())),
            ],
        ),
        ("mismatched zero-row batch", vec![Ok(wrong_zero)]),
    ];

    for (case, batches) in cases {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_table_meta(false)?).await?;
        let reader = RecordBatchIterator::new(batches, Arc::clone(&schema));

        assert!(
            matches!(
                table.append(reader).await,
                Err(TableError::AppendSource { .. })
            ),
            "{case}"
        );
        assert_eq!(table.state().version, 1, "{case}");
        assert!(table.state().segments.is_empty(), "{case}");
        assert!(!tmp.path().join("data").exists(), "{case}");
        assert!(
            !tmp.path().join(layout::commit_rel_path(2)).exists(),
            "{case}"
        );
        for directory in [layout::SEGMENT_COVERAGE_DIR, layout::TABLE_SNAPSHOT_DIR] {
            assert!(!tmp.path().join(directory).exists(), "{case}: {directory}");
        }

        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state().version, 1, "{case}");
        assert!(reopened.state().segments.is_empty(), "{case}");
    }
    Ok(())
}

#[tokio::test]
async fn streaming_append_round_trips_signed_and_unsigned_indexes() -> TestResult {
    let signed_tmp = TempDir::new()?;
    let signed_location = TableLocation::local(signed_tmp.path());
    let mut signed = TimeSeriesTable::create(
        signed_location.clone(),
        make_numeric_table_meta(
            IndexKind::Int64 {
                bucket_width: NonZeroU64::new(1).expect("nonzero bucket"),
            },
            LogicalDataType::Int64,
        )?,
    )
    .await?;
    signed
        .append(make_numeric_batch(
            DataType::Int64,
            Arc::new(Int64Array::from(vec![4, -3, 0])),
            &["positive", "negative", "zero"],
        )?)
        .await?;
    let signed = Arc::new(TimeSeriesTable::open(signed_location).await?);
    assert!(matches!(
        signed
            .state()
            .segments
            .values()
            .next()
            .map(|segment| &segment.entity_layout),
        Some(SegmentEntityLayout::NotApplicable)
    ));
    let signed_ctx = SessionContext::new();
    let _provider = register_provider(&signed_ctx, signed)?;
    let (_, signed_batches) = run_numeric_query(&signed_ctx, None).await?;
    assert_eq!(collect_i64_values(&signed_batches)?, vec![-3, 0, 4]);

    let unsigned_tmp = TempDir::new()?;
    let unsigned_location = TableLocation::local(unsigned_tmp.path());
    let mut unsigned = TimeSeriesTable::create(
        unsigned_location.clone(),
        make_numeric_table_meta(
            IndexKind::UInt64 {
                bucket_width: NonZeroU64::new(1).expect("nonzero bucket"),
            },
            LogicalDataType::UInt64,
        )?,
    )
    .await?;
    let above_signed_max = i64::MAX as u64 + 1;
    unsigned
        .append(make_numeric_batch(
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![
                above_signed_max + 1,
                above_signed_max,
            ])),
            &["higher", "high"],
        )?)
        .await?;
    let unsigned = Arc::new(TimeSeriesTable::open(unsigned_location).await?);
    let unsigned_ctx = SessionContext::new();
    let _provider = register_provider(&unsigned_ctx, unsigned)?;
    let (_, unsigned_batches) = run_numeric_query(&unsigned_ctx, None).await?;
    assert_eq!(
        collect_u64_values(&unsigned_batches)?,
        vec![above_signed_max, above_signed_max + 1]
    );
    Ok(())
}

#[tokio::test]
async fn streaming_append_round_trips_composite_entity_identity_order() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());
    let mut table = TimeSeriesTable::create(location.clone(), make_composite_table_meta()).await?;
    table
        .append(make_composite_batch(&[
            (60_000, "A", "X", 11.0),
            (0, "A", "X", 10.0),
        ])?)
        .await?;

    let reopened = Arc::new(TimeSeriesTable::open(location).await?);
    let segment = reopened
        .state()
        .segments
        .values()
        .next()
        .ok_or("missing streamed segment")?;
    let SegmentEntityLayout::Single(identity) = &segment.entity_layout else {
        return Err("expected single composite entity layout".into());
    };
    assert_eq!(
        identity.components(),
        [EntityValue::from("A"), EntityValue::from("X")]
    );

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, reopened)?;
    let batches =
        collect_batches(&ctx, "SELECT ts, symbol, venue, price FROM t ORDER BY ts").await?;
    let actual = arrow_select::concat::concat_batches(&first_batch(&batches)?.schema(), &batches)?;
    assert_eq!(
        actual,
        make_composite_batch(&[(0, "A", "X", 10.0), (60_000, "A", "X", 11.0),])?
    );
    Ok(())
}

#[tokio::test]
async fn streaming_append_randomized_partitions_preserve_exact_rows() -> TestResult {
    for case_seed in [1_u64, 0x1234_5678, 0xdead_beef, u64::MAX] {
        let mut random = case_seed;
        let mut rows = (0..128)
            .map(|row| {
                (
                    i64::from(row) * 60_000,
                    if row % 2 == 0 { "A" } else { "B" },
                    f64::from(row) + 0.25,
                )
            })
            .collect::<Vec<_>>();
        for end in (1..rows.len()).rev() {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            rows.swap(end, random as usize % (end + 1));
        }

        let empty = make_append_batch(&[])?;
        let mut batches = vec![empty.clone(), empty.clone()];
        let mut offset = 0;
        while offset < rows.len() {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            if random & 3 == 0 {
                batches.push(empty.clone());
            }
            let end = (offset + 1 + random as usize % 19).min(rows.len());
            batches.push(make_append_batch(&rows[offset..end])?);
            offset = end;
        }
        batches.extend([empty.clone(), empty]);

        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_table_meta(false)?).await?;
        assert_eq!(table.append(batches).await?, 2, "seed {case_seed:#x}");

        drop(table);
        let reopened = Arc::new(TimeSeriesTable::open(location).await?);
        assert_eq!(reopened.state().version, 2, "seed {case_seed:#x}");
        assert_eq!(reopened.state().segments.len(), 1, "seed {case_seed:#x}");
        assert_eq!(
            reopened
                .state()
                .segments
                .values()
                .map(|segment| segment.row_count)
                .sum::<u64>(),
            128,
            "seed {case_seed:#x}"
        );

        let ctx = SessionContext::new();
        let _provider = register_provider(&ctx, reopened)?;
        let result =
            collect_batches(&ctx, "SELECT ts, symbol, price FROM t ORDER BY ts, symbol").await?;
        let actual =
            arrow_select::concat::concat_batches(&first_batch(&result)?.schema(), &result)?;
        rows.sort_by_key(|row| row.0);
        assert_eq!(actual, make_append_batch(&rows)?, "seed {case_seed:#x}");
    }
    Ok(())
}

#[tokio::test]
async fn count_star_returns_all_rows() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(&ctx, "SELECT COUNT(*) FROM t").await?;
    let count = scalar_u64(&batches)?;
    assert_eq!(count, 10);
    Ok(())
}

#[tokio::test]
async fn select_ts_limit_returns_five_rows() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(&ctx, "SELECT ts FROM t LIMIT 5").await?;
    assert_eq!(total_rows(&batches), 5);
    let batch = first_batch(&batches)?;
    assert_eq!(field_names(batch), vec!["ts".to_string()]);
    Ok(())
}

#[tokio::test]
async fn projection_sanity_ts_price() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(&ctx, "SELECT ts, price FROM t").await?;
    let batch = first_batch(&batches)?;
    assert_eq!(
        field_names(batch),
        vec!["ts".to_string(), "price".to_string()]
    );
    Ok(())
}

#[tokio::test]
async fn projection_with_limit_respects_row_count() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(&ctx, "SELECT ts, price FROM t LIMIT 3").await?;
    assert_eq!(total_rows(&batches), 3);
    let batch = first_batch(&batches)?;
    assert_eq!(
        field_names(batch),
        vec!["ts".to_string(), "price".to_string()]
    );
    Ok(())
}

#[tokio::test]
async fn projection_order_is_preserved() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(&ctx, "SELECT price, ts FROM t").await?;
    let batch = first_batch(&batches)?;
    assert_eq!(
        field_names(batch),
        vec!["price".to_string(), "ts".to_string()]
    );
    Ok(())
}

#[tokio::test]
async fn order_by_limit_returns_descending_rows() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(&ctx, "SELECT ts FROM t ORDER BY ts DESC LIMIT 3").await?;
    let values = collect_i64_values(&batches)?;
    assert_eq!(values.len(), 3);
    assert_eq!(
        values,
        vec![
            minutes_to_millis(3) + 4,
            minutes_to_millis(3) + 3,
            minutes_to_millis(3) + 2,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn empty_table_returns_zero_rows() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_table(&tmp, false).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let count_batches = collect_batches(&ctx, "SELECT COUNT(*) FROM t").await?;
    let count = scalar_u64(&count_batches)?;
    assert_eq!(count, 0);

    let limit_batches = collect_batches(&ctx, "SELECT ts FROM t LIMIT 1").await?;
    assert_eq!(total_rows(&limit_batches), 0);
    Ok(())
}

#[tokio::test]
async fn missing_file_size_falls_back_to_stat() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());

    let meta = make_table_meta(false)?;
    let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

    let rows = make_rows(minutes_to_millis(1), 5, "A", 10.0);
    let rel_path = "data/seg-missing-size.parquet";
    write_segment(tmp.path(), rel_path, &rows, false)?;

    table.append_parquet_segment(rel_path).await?;
    drop(table);

    remove_committed_file_size(tmp.path(), 2).await?;

    let table = Arc::new(TimeSeriesTable::open(location).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(&ctx, "SELECT COUNT(*) FROM t").await?;
    let count = scalar_u64(&batches)?;
    assert_eq!(count, 5);
    Ok(())
}

#[tokio::test]
async fn cache_refreshes_after_new_segments() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());
    let meta = make_table_meta(false)?;

    let mut writer = TimeSeriesTable::create(location.clone(), meta).await?;
    let rows_a = make_rows(minutes_to_millis(1), 5, "A", 10.0);
    write_segment(tmp.path(), "data/seg-a.parquet", &rows_a, false)?;
    writer.append_parquet_segment("data/seg-a.parquet").await?;

    let provider_table = Arc::new(TimeSeriesTable::open(location.clone()).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&provider_table))?;

    let initial_batches = collect_batches(&ctx, "SELECT COUNT(*) FROM t").await?;
    let initial_count = scalar_u64(&initial_batches)?;
    assert_eq!(initial_count, 5);

    let rows_b = make_rows(minutes_to_millis(3), 5, "A", 20.0);
    write_segment(tmp.path(), "data/seg-b.parquet", &rows_b, false)?;
    writer.append_parquet_segment("data/seg-b.parquet").await?;

    let refreshed_batches = collect_batches(&ctx, "SELECT COUNT(*) FROM t").await?;
    let refreshed_count = scalar_u64(&refreshed_batches)?;
    assert_eq!(refreshed_count, 10);
    Ok(())
}

#[tokio::test]
async fn provider_schema_matches_table_meta() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_table(&tmp, false).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let provider = register_provider(&ctx, Arc::clone(&table))?;

    let expected = table.state().table_meta.arrow_schema_ref()?;
    assert_eq!(provider.schema().as_ref(), expected.as_ref());
    Ok(())
}

#[tokio::test]
async fn provider_schema_supports_nested_types() -> TestResult {
    let tmp = TempDir::new()?;
    let location = TableLocation::local(tmp.path());
    let meta = make_nested_table_meta()?;
    let table = TimeSeriesTable::create(location, meta).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let provider = register_provider(&ctx, Arc::clone(&table))?;

    let schema = provider.schema();

    let attrs = schema.field_with_name("attrs")?.data_type();
    match attrs {
        DataType::Struct(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "a");
            assert_eq!(fields[1].name(), "b");
        }
        other => return Err(format!("attrs type mismatch: {other:?}").into()),
    }

    let tags = schema.field_with_name("tags")?.data_type();
    match tags {
        DataType::List(field) => {
            assert_eq!(field.name(), "item");
            assert!(matches!(field.data_type(), DataType::Utf8));
        }
        other => return Err(format!("tags type mismatch: {other:?}").into()),
    }

    let metrics = schema.field_with_name("metrics")?.data_type();
    match metrics {
        DataType::Map(entries, keys_sorted) => {
            assert!(!keys_sorted);
            assert_eq!(entries.name(), "entries");
            match entries.data_type() {
                DataType::Struct(fields) => {
                    assert_eq!(fields.len(), 2);
                    assert_eq!(fields[0].name(), "key");
                    assert_eq!(fields[1].name(), "value");
                }
                other => return Err(format!("metrics entries type mismatch: {other:?}").into()),
            }
        }
        other => return Err(format!("metrics type mismatch: {other:?}").into()),
    }

    Ok(())
}

#[tokio::test]
async fn pushdown_marks_all_filters_inexact() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_table(&tmp, false).await?;
    let provider = TsTableProvider::try_new(Arc::new(table))?;

    let filters = vec![
        col("ts").gt_eq(lit("1970-01-01T00:00:00Z")),
        col("symbol").eq(lit("A")),
    ];
    let refs: Vec<&Expr> = filters.iter().collect();
    let r = provider.supports_filters_pushdown(&refs)?;

    assert_eq!(r.len(), 2);
    assert!(
        r.iter()
            .all(|x| matches!(x, TableProviderFilterPushDown::Inexact))
    );
    Ok(())
}

#[tokio::test]
async fn scan_attaches_parquet_predicate_for_non_time_filters() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let df = ctx.sql("SELECT count(*) FROM t WHERE symbol = 'A'").await?;
    let plan = df.create_physical_plan().await?;
    let display = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();

    println!("{display}");

    assert!(display.contains("DataSourceExec"));
    assert!(display.contains("predicate="));
    assert!(display.contains("symbol") || display.contains("Symbol"));
    Ok(())
}

#[tokio::test]
async fn parquet_prunes_row_groups_for_non_time_predicate() -> TestResult {
    let tmp = TempDir::new()?;
    let mut rows = Vec::new();
    for i in 0..5 {
        rows.push(TestRow {
            ts_millis: minutes_to_millis(i as i64),
            symbol: "A",
            price: Some(1.0),
        });
    }
    for i in 5..10 {
        rows.push(TestRow {
            ts_millis: minutes_to_millis(i as i64),
            symbol: "A",
            price: Some(1000.0),
        });
    }

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(5))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();
    let table =
        create_single_segment_table_with_props(&tmp, "data/seg-rg.parquet", &rows, props).await?;
    let table = Arc::new(table);

    let config = SessionConfig::new().with_parquet_pruning(true);
    let ctx = SessionContext::new_with_config(config);
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let df = ctx.sql("SELECT count(*) FROM t WHERE price < 10.0").await?;
    let plan = df.create_physical_plan().await?;
    let _ = collect(plan.clone(), ctx.task_ctx()).await?;

    let exec = find_data_source_exec(plan.as_ref())
        .ok_or_else(|| "expected DataSourceExec in physical plan".to_string())?;
    let metrics = exec
        .metrics()
        .ok_or_else(|| "expected metrics for DataSourceExec".to_string())?;
    let (pruned, matched) = get_pruning_metric(&metrics, "row_groups_pruned_statistics")?;

    assert!(
        pruned >= 1,
        "expected at least 1 row group pruned (matched={matched}, pruned={pruned})"
    );
    Ok(())
}

#[tokio::test]
async fn explain_prunes_segments_on_time_filter() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(
        &ctx,
        "EXPLAIN VERBOSE SELECT * FROM t \
         WHERE ts >= '1970-01-01T00:03:00Z' AND ts < '1970-01-01T00:03:01Z'",
    )
    .await?;
    let plan = explain_plan_text(&batches)?;

    let seg_a = "seg-a.parquet";
    let seg_b = "seg-b.parquet";

    assert!(
        plan.contains(seg_b),
        "expected plan to include {seg_b}; plan:\n{plan}"
    );
    assert!(
        !plan.contains(seg_a),
        "expected plan to exclude {seg_a}; plan:\n{plan}"
    );
    Ok(())
}

#[tokio::test]
async fn explain_retains_matching_single_entity_segments() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches =
        collect_batches(&ctx, "EXPLAIN VERBOSE SELECT * FROM t WHERE symbol = 'A'").await?;
    let plan = explain_plan_text(&batches)?;

    let seg_a = "seg-a.parquet";
    let seg_b = "seg-b.parquet";

    assert!(
        plan.contains(seg_a),
        "expected plan to include {seg_a}; plan:\n{plan}"
    );
    assert!(
        plan.contains(seg_b),
        "expected plan to include {seg_b}; plan:\n{plan}"
    );
    Ok(())
}

#[tokio::test]
async fn entity_equality_prunes_conflicting_single_entity_segments() -> TestResult {
    let tmp = TempDir::new()?;
    let table = Arc::new(create_entity_pruning_table(&tmp).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;

    let (files, batches) = run_timestamp_query(&ctx, "symbol = 'A'").await?;
    assert_eq!(files, ["entity-a.parquet", "entity-mixed.parquet"]);
    assert_eq!(collect_i64_values(&batches)?, [0, 120_000]);

    let (files, batches) = run_timestamp_query(&ctx, "symbol = 'missing'").await?;
    assert_eq!(files, ["entity-mixed.parquet"]);
    assert!(batches.iter().all(|batch| batch.num_rows() == 0));

    let (files, batches) = run_timestamp_query(&ctx, "'B' = t.symbol").await?;
    assert_eq!(files, ["entity-b.parquet", "entity-mixed.parquet"]);
    assert_eq!(collect_i64_values(&batches)?, [60_000, 180_000]);

    let (files, batches) = run_timestamp_query(&ctx, "symbol > 'A'").await?;
    assert_eq!(
        files,
        [
            "entity-a.parquet",
            "entity-b.parquet",
            "entity-mixed.parquet"
        ]
    );
    assert_eq!(collect_i64_values(&batches)?, [60_000, 180_000]);
    Ok(())
}

#[tokio::test]
async fn numeric_entity_equality_prunes_conflicting_single_entity_segments() -> TestResult {
    let tmp = TempDir::new()?;
    let table = Arc::new(create_int32_entity_pruning_table(&tmp).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;

    let (files, batches) = run_timestamp_query(&ctx, "device_id = -1").await?;
    assert_eq!(files, ["device-negative.parquet", "device-mixed.parquet"]);
    assert_eq!(collect_i64_values(&batches)?, [0, 120_000]);

    let (files, batches) = run_timestamp_query(&ctx, "2147483647 = t.device_id").await?;
    assert_eq!(files, ["device-maximum.parquet", "device-mixed.parquet"]);
    assert_eq!(collect_i64_values(&batches)?, [60_000, 180_000]);

    let (files, batches) = run_timestamp_query(&ctx, "device_id = 42").await?;
    assert_eq!(files, ["device-mixed.parquet"]);
    assert!(batches.iter().all(|batch| batch.num_rows() == 0));
    Ok(())
}

#[tokio::test]
async fn entity_pruning_composes_safely_with_other_predicates() -> TestResult {
    let tmp = TempDir::new()?;
    let table = Arc::new(create_entity_pruning_table(&tmp).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;

    let (files, batches) = run_timestamp_query(&ctx, "symbol = 'A' AND price < 35.0").await?;
    assert_eq!(files, ["entity-a.parquet", "entity-mixed.parquet"]);
    assert_eq!(collect_i64_values(&batches)?, [0, 120_000]);

    let (files, batches) = run_timestamp_query(&ctx, "symbol = 'A' OR price >= 20.0").await?;
    assert_eq!(
        files,
        [
            "entity-a.parquet",
            "entity-b.parquet",
            "entity-mixed.parquet"
        ]
    );
    assert_eq!(collect_i64_values(&batches)?, [0, 60_000, 120_000, 180_000]);

    let (files, batches) =
        run_timestamp_query(&ctx, "symbol = 'A' AND ts >= '1970-01-01T00:02:00Z'").await?;
    assert_eq!(files, ["entity-mixed.parquet"]);
    assert_eq!(collect_i64_values(&batches)?, [120_000]);
    Ok(())
}

#[tokio::test]
async fn entity_pruning_precedes_missing_file_access() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_entity_pruning_table(&tmp).await?;
    drop(table);

    remove_committed_file_size(tmp.path(), 3).await?;
    tokio::fs::remove_file(tmp.path().join("data/entity-b.parquet")).await?;

    let table = Arc::new(TimeSeriesTable::open(TableLocation::local(tmp.path())).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;
    let (files, batches) = run_timestamp_query(&ctx, "symbol = 'A'").await?;

    assert_eq!(files, ["entity-a.parquet", "entity-mixed.parquet"]);
    assert_eq!(collect_i64_values(&batches)?, [0, 120_000]);
    Ok(())
}

#[tokio::test]
async fn entity_metadata_preserves_unfiltered_and_grouped_results() -> TestResult {
    let tmp = TempDir::new()?;
    let table = Arc::new(create_entity_pruning_table(&tmp).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;

    let batches = collect_batches(&ctx, "SELECT ts FROM t ORDER BY ts").await?;
    assert_eq!(collect_i64_values(&batches)?, [0, 60_000, 120_000, 180_000]);

    let batches = collect_batches(
        &ctx,
        "SELECT symbol, COUNT(*) FROM t GROUP BY symbol ORDER BY symbol",
    )
    .await?;
    let batch = first_batch(&batches)?;
    let symbols = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("expected StringArray")?;
    let counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("expected Int64Array")?;
    assert_eq!(symbols.iter().collect::<Vec<_>>(), [Some("A"), Some("B")]);
    assert_eq!(counts.values(), &[2, 2]);
    Ok(())
}

#[tokio::test]
async fn time_filter_returns_correct_rows() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(
        &ctx,
        "SELECT COUNT(*) FROM t \
         WHERE ts >= '1970-01-01T00:03:00Z' AND ts < '1970-01-01T00:03:01Z'",
    )
    .await?;
    let count = scalar_u64(&batches)?;
    assert_eq!(count, 5);
    Ok(())
}

#[tokio::test]
async fn date_trunc_filter_returns_correct_rows() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(
        &ctx,
        "SELECT COUNT(*) FROM t \
         WHERE date_trunc('minute', ts) = '1970-01-01T00:03:00Z'",
    )
    .await?;
    let count = scalar_u64(&batches)?;
    assert_eq!(count, 5);
    Ok(())
}

#[tokio::test]
async fn date_bin_filter_returns_correct_rows() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(
        &ctx,
        "SELECT COUNT(*) FROM t \
         WHERE date_bin(interval '1 minute', ts) = '1970-01-01T00:03:00Z'",
    )
    .await?;
    let count = scalar_u64(&batches)?;
    assert_eq!(count, 5);
    Ok(())
}

#[tokio::test]
async fn date_trunc_filter_returns_correct_rows_olson_tz() -> TestResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("America/New_York".into())),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));

    let rows = make_rows(ts_millis("2024-03-10T05:00:00Z"), 5, "A", 10.0);
    let mut ts_builder = TimestampMillisecondBuilder::with_capacity(rows.len());
    let mut sym_builder =
        StringBuilder::with_capacity(rows.len(), rows.iter().map(|r| r.symbol.len()).sum());
    let mut price_builder = Float64Builder::with_capacity(rows.len());

    for row in &rows {
        ts_builder.append_value(row.ts_millis);
        sym_builder.append_value(row.symbol);
        price_builder.append_value(row.price.expect("price"));
    }

    let mut ts_array = ts_builder.finish();
    ts_array = ts_array.with_timezone_opt(Some(Arc::from("America/New_York")));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(ts_array),
            Arc::new(sym_builder.finish()),
            Arc::new(price_builder.finish()),
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(table))?;

    let batches = collect_batches(
        &ctx,
        "SELECT COUNT(*) FROM t \
         WHERE date_trunc('day', ts) = '2024-03-10T00:00:00-05:00'",
    )
    .await?;
    let count = scalar_u64(&batches)?;
    assert_eq!(count, 5);
    Ok(())
}

#[tokio::test]
async fn multi_segment_min_max_reflects_all_data() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(&ctx, "SELECT MIN(ts), MAX(ts) FROM t").await?;
    let batch = first_batch(&batches)?;
    let min_ts = scalar_i64_from_array(batch.column(0).as_ref())?;
    let max_ts = scalar_i64_from_array(batch.column(1).as_ref())?;
    assert_eq!(min_ts, minutes_to_millis(1));
    assert_eq!(max_ts, minutes_to_millis(3) + 4);
    Ok(())
}

#[tokio::test]
async fn timestamp_direct_predicates_select_expected_files_and_rows() -> TestResult {
    let tmp = TempDir::new()?;
    let table = Arc::new(create_utc_pruning_table(&tmp).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;
    let cases = [
        (
            "ts = '1970-01-01T00:01:00Z'",
            vec!["time-target.parquet"],
            vec![60_000],
        ),
        (
            "ts < '1970-01-01T00:01:00Z'",
            vec!["time-before.parquet"],
            vec![0, 59_999],
        ),
        (
            "ts <= '1970-01-01T00:01:00Z'",
            vec!["time-before.parquet", "time-target.parquet"],
            vec![0, 59_999, 60_000],
        ),
        (
            "ts > '1970-01-01T00:01:59.999Z'",
            vec!["time-after.parquet"],
            vec![120_000, 179_999],
        ),
        (
            "ts >= '1970-01-01T00:01:59.999Z'",
            vec!["time-target.parquet", "time-after.parquet"],
            vec![119_999, 120_000, 179_999],
        ),
        (
            "to_timestamp('1970-01-01T00:01:00Z') > ts",
            vec!["time-before.parquet"],
            vec![0, 59_999],
        ),
        (
            "ts BETWEEN '1970-01-01T00:01:00Z' AND '1970-01-01T00:01:59.999Z'",
            vec!["time-target.parquet"],
            vec![60_000, 119_999],
        ),
        (
            "ts NOT BETWEEN '1970-01-01T00:01:00Z' AND '1970-01-01T00:01:59.999Z'",
            vec!["time-before.parquet", "time-after.parquet"],
            vec![0, 59_999, 120_000, 179_999],
        ),
        (
            "ts IN ('1970-01-01T00:01:00Z', '1970-01-01T00:01:59.999Z')",
            vec!["time-target.parquet"],
            vec![60_000, 119_999],
        ),
        (
            "ts NOT IN ('1970-01-01T00:01:00Z')",
            UTC_PRUNING_FILES.to_vec(),
            vec![0, 59_999, 119_999, 120_000, 179_999],
        ),
        (
            "ts >= '1970-01-01T00:01:00Z' AND ts <= '1970-01-01T00:01:59.999Z'",
            vec!["time-target.parquet"],
            vec![60_000, 119_999],
        ),
        (
            "ts <= '1970-01-01T00:00:59.999Z' OR ts >= '1970-01-01T00:02:00Z'",
            vec!["time-before.parquet", "time-after.parquet"],
            vec![0, 59_999, 120_000, 179_999],
        ),
        (
            "NOT (ts < '1970-01-01T00:01:00Z')",
            vec!["time-target.parquet", "time-after.parquet"],
            vec![60_000, 119_999, 120_000, 179_999],
        ),
        (
            "ts != '1970-01-01T00:01:00Z'",
            UTC_PRUNING_FILES.to_vec(),
            vec![0, 59_999, 119_999, 120_000, 179_999],
        ),
        (
            "price BETWEEN 20.0 AND 21.0",
            UTC_PRUNING_FILES.to_vec(),
            vec![60_000, 119_999],
        ),
        (
            "date_part('minute', ts) = 1",
            UTC_PRUNING_FILES.to_vec(),
            vec![60_000, 119_999],
        ),
        ("ts < '1969-12-31T23:59:59Z'", vec![], vec![]),
    ];

    for (predicate, expected_files, expected_values) in cases {
        let (files, batches) = run_timestamp_query(&ctx, predicate).await?;
        assert_eq!(files, expected_files, "wrong files for {predicate}");
        assert_eq!(
            collect_i64_values(&batches)?,
            expected_values,
            "wrong rows for {predicate}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn timestamp_arithmetic_and_fixed_transforms_select_expected_files_and_rows() -> TestResult {
    let tmp = TempDir::new()?;
    let table = Arc::new(create_utc_pruning_table(&tmp).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;
    let cases = [
        (
            "ts + interval '1 minute' = '1970-01-01T00:02:00Z'",
            vec!["time-target.parquet"],
            vec![60_000],
        ),
        (
            "ts - interval '1 minute' >= '1970-01-01T00:01:00Z'",
            vec!["time-after.parquet"],
            vec![120_000, 179_999],
        ),
        (
            "to_timestamp('1970-01-01T00:02:00Z') <= ts + interval '1 minute'",
            vec!["time-target.parquet", "time-after.parquet"],
            vec![60_000, 119_999, 120_000, 179_999],
        ),
        (
            "ts + interval '1 month' = '1970-02-01T00:01:00Z'",
            vec!["time-target.parquet"],
            vec![60_000],
        ),
        (
            "ts + interval '1 millisecond' <= '1970-01-01T00:02:00Z'",
            vec!["time-before.parquet", "time-target.parquet"],
            vec![0, 59_999, 60_000, 119_999],
        ),
        (
            "ts - ts < interval '1 second'",
            UTC_PRUNING_FILES.to_vec(),
            vec![0, 59_999, 60_000, 119_999, 120_000, 179_999],
        ),
        (
            "to_unixtime(ts) < 60",
            vec!["time-before.parquet"],
            vec![0, 59_999],
        ),
        (
            "to_unixtime(ts) < '60'",
            vec!["time-before.parquet"],
            vec![0, 59_999],
        ),
        (
            "date_trunc('minute', ts) = '1970-01-01T00:01:00Z'",
            vec!["time-target.parquet"],
            vec![60_000, 119_999],
        ),
        (
            "date_bin(interval '1 minute', ts) = '1970-01-01T00:01:00Z'",
            vec!["time-target.parquet"],
            vec![60_000, 119_999],
        ),
        (
            "date_bin(interval '1 minute', ts, to_timestamp('1970-01-01T00:00:30Z')) = '1970-01-01T00:01:30Z'",
            vec!["time-target.parquet", "time-after.parquet"],
            vec![119_999, 120_000],
        ),
        (
            "date_trunc('minute', ts) < '1970-01-01T00:01:30Z'",
            vec!["time-before.parquet", "time-target.parquet"],
            vec![0, 59_999, 60_000, 119_999],
        ),
    ];

    for (predicate, expected_files, expected_values) in cases {
        let (files, batches) = run_timestamp_query(&ctx, predicate).await?;
        assert_eq!(files, expected_files, "wrong files for {predicate}");
        assert_eq!(
            collect_i64_values(&batches)?,
            expected_values,
            "wrong rows for {predicate}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn overridden_builtin_name_does_not_enable_timestamp_pruning() -> TestResult {
    let tmp = TempDir::new()?;
    let table = Arc::new(create_utc_pruning_table(&tmp).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;
    let udf = create_udf(
        "to_unixtime",
        vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| {
            Ok(match &args[0] {
                ColumnarValue::Array(array) => {
                    ColumnarValue::Array(Arc::new(Int64Array::from(vec![1; array.len()])))
                }
                ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            })
        }),
    );
    ctx.register_udf(udf);

    let predicate = "to_unixtime(ts) = 1";
    let (files, batches) = run_timestamp_query(&ctx, predicate).await?;
    assert_eq!(files, UTC_PRUNING_FILES, "wrong files for {predicate}");
    assert_eq!(
        collect_i64_values(&batches)?,
        vec![0, 59_999, 60_000, 119_999, 120_000, 179_999],
        "wrong rows for {predicate}"
    );
    Ok(())
}

#[tokio::test]
async fn timestamp_calendar_date_transforms_select_expected_files_and_rows() -> TestResult {
    const FILES: &[&str] = &[
        "calendar-before.parquet",
        "calendar-target.parquet",
        "calendar-after.parquet",
    ];
    let tmp = TempDir::new()?;
    let table = create_zoned_pruning_table(
        &tmp,
        "UTC",
        &[
            (
                FILES[0],
                "2024-01-07T00:00:00Z",
                "2024-01-07T23:59:59.999Z",
                10.0,
            ),
            (
                FILES[1],
                "2024-01-08T00:00:00Z",
                "2024-01-08T23:59:59.999Z",
                20.0,
            ),
            (
                FILES[2],
                "2024-01-09T00:00:00Z",
                "2024-01-09T23:59:59.999Z",
                30.0,
            ),
        ],
    )
    .await?;
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::new(table))?;
    let expected_values = vec![
        ts_millis("2024-01-08T00:00:00Z"),
        ts_millis("2024-01-08T23:59:59.999Z"),
    ];

    for predicate in [
        "to_date(CAST(ts AS VARCHAR)) = '2024-01-08'",
        "date_trunc('day', ts) = '2024-01-08T00:00:00Z'",
    ] {
        let (files, batches) = run_timestamp_query(&ctx, predicate).await?;
        assert_eq!(files, vec![FILES[1]], "wrong files for {predicate}");
        assert_eq!(
            collect_i64_values(&batches)?,
            expected_values,
            "wrong rows for {predicate}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn timestamp_iana_dst_transforms_select_expected_files_and_rows() -> TestResult {
    const FILES: &[&str] = &[
        "dst-before-day.parquet",
        "dst-day-start.parquet",
        "dst-before-jump.parquet",
        "dst-after-jump.parquet",
        "dst-after-day.parquet",
        "dst-mixed-interval-source.parquet",
    ];
    let tmp = TempDir::new()?;
    let table = create_zoned_pruning_table(
        &tmp,
        "America/New_York",
        &[
            (
                FILES[0],
                "2024-03-10T04:00:00Z",
                "2024-03-10T04:59:59.999Z",
                10.0,
            ),
            (
                FILES[1],
                "2024-03-10T05:00:00Z",
                "2024-03-10T05:59:59.999Z",
                20.0,
            ),
            (
                FILES[2],
                "2024-03-10T06:00:00Z",
                "2024-03-10T06:59:59.999Z",
                30.0,
            ),
            (
                FILES[3],
                "2024-03-10T07:00:00Z",
                "2024-03-10T07:59:59.999Z",
                40.0,
            ),
            (
                FILES[4],
                "2024-03-11T04:00:00Z",
                "2024-03-11T04:59:59.999Z",
                50.0,
            ),
            (
                FILES[5],
                "2024-03-09T06:30:00Z",
                "2024-03-09T06:30:00.001Z",
                60.0,
            ),
        ],
    )
    .await?;
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::new(table))?;
    let cases = [
        (
            "date_trunc('day', ts) = '2024-03-10T00:00:00-05:00'",
            vec![FILES[1], FILES[2], FILES[3]],
            vec![
                ts_millis("2024-03-10T05:00:00Z"),
                ts_millis("2024-03-10T05:59:59.999Z"),
                ts_millis("2024-03-10T06:00:00Z"),
                ts_millis("2024-03-10T06:59:59.999Z"),
                ts_millis("2024-03-10T07:00:00Z"),
                ts_millis("2024-03-10T07:59:59.999Z"),
            ],
        ),
        (
            "date_trunc('hour', ts) = '2024-03-10T01:00:00-05:00'",
            vec![FILES[2]],
            vec![
                ts_millis("2024-03-10T06:00:00Z"),
                ts_millis("2024-03-10T06:59:59.999Z"),
            ],
        ),
        (
            "date_bin(interval '2 hours', ts) = '2024-03-10T01:00:00-05:00'",
            vec![FILES[2], FILES[3]],
            vec![
                ts_millis("2024-03-10T06:00:00Z"),
                ts_millis("2024-03-10T06:59:59.999Z"),
                ts_millis("2024-03-10T07:00:00Z"),
                ts_millis("2024-03-10T07:59:59.999Z"),
            ],
        ),
        (
            "ts + interval '1 day' = '2024-03-11T01:00:00-04:00'",
            vec![FILES[2]],
            vec![ts_millis("2024-03-10T06:00:00Z")],
        ),
    ];

    for (predicate, expected_files, expected_values) in cases {
        let (files, batches) = run_timestamp_query(&ctx, predicate).await?;
        assert_eq!(files, expected_files, "wrong files for {predicate}");
        assert_eq!(
            collect_i64_values(&batches)?,
            expected_values,
            "wrong rows for {predicate}"
        );
    }

    let predicate = "(ts + interval '1 day') + interval '1 hour' = \
                     '2024-03-10T03:30:00-04:00'";
    let (files, batches) = run_timestamp_query(&ctx, predicate).await?;
    assert!(
        files.iter().any(|file| file == FILES[5]),
        "matching file was pruned for {predicate}"
    );
    assert_eq!(
        collect_i64_values(&batches)?,
        vec![ts_millis("2024-03-09T06:30:00Z")],
        "wrong rows for {predicate}"
    );
    Ok(())
}

#[tokio::test]
async fn int64_queries_prune_planned_files_and_return_exact_rows() -> TestResult {
    const FILES: &[&str] = &[
        "int-a.parquet",
        "int-b.parquet",
        "int-c.parquet",
        "int-d.parquet",
        "int-e.parquet",
    ];
    let tmp = TempDir::new()?;
    let table = Arc::new(create_int64_table(&tmp).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;
    let cases = vec![
        (
            None,
            FILES.to_vec(),
            vec![i64::MIN, -10, -1, 0, 1, 10, 20, i64::MAX],
        ),
        (
            Some("idx < 0"),
            vec!["int-a.parquet", "int-b.parquet"],
            vec![i64::MIN, -10, -1],
        ),
        (
            Some("0 > idx"),
            vec!["int-a.parquet", "int-b.parquet"],
            vec![i64::MIN, -10, -1],
        ),
        (
            Some("idx <= -10"),
            vec!["int-a.parquet"],
            vec![i64::MIN, -10],
        ),
        (
            Some("-10 >= idx"),
            vec!["int-a.parquet"],
            vec![i64::MIN, -10],
        ),
        (Some("idx > 10"), vec!["int-e.parquet"], vec![20, i64::MAX]),
        (Some("10 < idx"), vec!["int-e.parquet"], vec![20, i64::MAX]),
        (Some("idx >= 20"), vec!["int-e.parquet"], vec![20, i64::MAX]),
        (Some("20 <= idx"), vec!["int-e.parquet"], vec![20, i64::MAX]),
        (Some("idx = 0"), vec!["int-c.parquet"], vec![0]),
        (Some("0 = idx"), vec!["int-c.parquet"], vec![0]),
        (
            Some("idx != 0"),
            vec![
                "int-a.parquet",
                "int-b.parquet",
                "int-d.parquet",
                "int-e.parquet",
            ],
            vec![i64::MIN, -10, -1, 1, 10, 20, i64::MAX],
        ),
        (
            Some("0 != idx"),
            vec![
                "int-a.parquet",
                "int-b.parquet",
                "int-d.parquet",
                "int-e.parquet",
            ],
            vec![i64::MIN, -10, -1, 1, 10, 20, i64::MAX],
        ),
        (
            Some("idx BETWEEN -1 AND 1"),
            vec!["int-b.parquet", "int-c.parquet", "int-d.parquet"],
            vec![-1, 0, 1],
        ),
        (
            Some("idx NOT BETWEEN -1 AND 1"),
            vec!["int-a.parquet", "int-d.parquet", "int-e.parquet"],
            vec![i64::MIN, -10, 10, 20, i64::MAX],
        ),
        (
            Some("idx IN (-1, 20)"),
            vec!["int-b.parquet", "int-e.parquet"],
            vec![-1, 20],
        ),
        (
            Some("idx NOT IN (-1, 20)"),
            vec![
                "int-a.parquet",
                "int-c.parquet",
                "int-d.parquet",
                "int-e.parquet",
            ],
            vec![i64::MIN, -10, 0, 1, 10, i64::MAX],
        ),
        (
            Some("idx >= 0 AND idx <= 1"),
            vec!["int-c.parquet", "int-d.parquet"],
            vec![0, 1],
        ),
        (
            Some("idx < -10 OR idx > 10"),
            vec!["int-a.parquet", "int-e.parquet"],
            vec![i64::MIN, 20, i64::MAX],
        ),
        (
            Some("NOT (idx < 1)"),
            vec!["int-d.parquet", "int-e.parquet"],
            vec![1, 10, 20, i64::MAX],
        ),
        (Some("idx > 9223372036854775807"), vec![], vec![]),
        (Some("tag = 'zero'"), FILES.to_vec(), vec![0]),
        (
            Some("idx = 0 AND tag = 'zero'"),
            vec!["int-c.parquet"],
            vec![0],
        ),
        (Some("idx = 0 OR tag = 'never'"), FILES.to_vec(), vec![0]),
        (
            Some("idx + CAST(char_length(tag) AS BIGINT) = 4"),
            FILES.to_vec(),
            vec![-1, 0],
        ),
    ];

    for (predicate, expected_files, expected_values) in cases {
        let (plan, batches) = run_numeric_query(&ctx, predicate).await?;
        assert_planned_files(&plan, FILES, &expected_files);
        assert_eq!(
            collect_i64_values(&batches)?,
            expected_values,
            "wrong results for predicate {predicate:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn uint64_queries_prune_planned_files_without_signed_narrowing() -> TestResult {
    const FILES: &[&str] = &[
        "uint-a.parquet",
        "uint-b.parquet",
        "uint-c.parquet",
        "uint-d.parquet",
    ];
    let tmp = TempDir::new()?;
    let table = Arc::new(create_uint64_table(&tmp).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;
    let signed_max = i64::MAX as u64;
    let signed_max_sql = "CAST('9223372036854775807' AS BIGINT UNSIGNED)";
    let above_signed_max_sql = "CAST('9223372036854775808' AS BIGINT UNSIGNED)";
    let unsigned_max_sql = "CAST('18446744073709551615' AS BIGINT UNSIGNED)";
    let cases = vec![
        (
            None,
            FILES.to_vec(),
            vec![0, signed_max, signed_max + 1, signed_max + 2, u64::MAX],
        ),
        (
            Some(format!("idx < {signed_max_sql}")),
            vec!["uint-a.parquet"],
            vec![0],
        ),
        (
            Some(format!("idx <= {signed_max_sql}")),
            vec!["uint-a.parquet", "uint-b.parquet"],
            vec![0, signed_max],
        ),
        (
            Some(format!("idx > {signed_max_sql}")),
            vec!["uint-c.parquet", "uint-d.parquet"],
            vec![signed_max + 1, signed_max + 2, u64::MAX],
        ),
        (
            Some(format!("idx >= {above_signed_max_sql}")),
            vec!["uint-c.parquet", "uint-d.parquet"],
            vec![signed_max + 1, signed_max + 2, u64::MAX],
        ),
        (
            Some(format!("idx = {unsigned_max_sql}")),
            vec!["uint-d.parquet"],
            vec![u64::MAX],
        ),
        (
            Some(format!("idx != {unsigned_max_sql}")),
            vec!["uint-a.parquet", "uint-b.parquet", "uint-c.parquet"],
            vec![0, signed_max, signed_max + 1, signed_max + 2],
        ),
        (
            Some(format!(
                "idx BETWEEN {signed_max_sql} AND {above_signed_max_sql}"
            )),
            vec!["uint-b.parquet", "uint-c.parquet"],
            vec![signed_max, signed_max + 1],
        ),
        (
            Some(format!(
                "idx NOT BETWEEN {signed_max_sql} AND {above_signed_max_sql}"
            )),
            vec!["uint-a.parquet", "uint-c.parquet", "uint-d.parquet"],
            vec![0, signed_max + 2, u64::MAX],
        ),
        (
            Some(format!("idx IN (0, {unsigned_max_sql})")),
            vec!["uint-a.parquet", "uint-d.parquet"],
            vec![0, u64::MAX],
        ),
        (
            Some(format!("idx NOT IN (0, {unsigned_max_sql})")),
            vec!["uint-b.parquet", "uint-c.parquet"],
            vec![signed_max, signed_max + 1, signed_max + 2],
        ),
        (
            Some(format!(
                "idx > {signed_max_sql} AND idx < {unsigned_max_sql}"
            )),
            vec!["uint-c.parquet"],
            vec![signed_max + 1, signed_max + 2],
        ),
        (
            Some(format!("idx = 0 OR idx = {above_signed_max_sql}")),
            vec!["uint-a.parquet", "uint-c.parquet"],
            vec![0, signed_max + 1],
        ),
        (
            Some(format!("NOT (idx < {above_signed_max_sql})")),
            vec!["uint-c.parquet", "uint-d.parquet"],
            vec![signed_max + 1, signed_max + 2, u64::MAX],
        ),
        (Some(format!("idx > {unsigned_max_sql}")), vec![], vec![]),
    ];

    for (predicate, expected_files, expected_values) in cases {
        let predicate = predicate.as_deref();
        let (plan, batches) = run_numeric_query(&ctx, predicate).await?;
        assert_planned_files(&plan, FILES, &expected_files);
        assert_eq!(
            collect_u64_values(&batches)?,
            expected_values,
            "wrong results for predicate {predicate:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn numeric_pruning_precedes_missing_file_size_lookup() -> TestResult {
    const FILES: &[&str] = &[
        "int-a.parquet",
        "int-b.parquet",
        "int-c.parquet",
        "int-d.parquet",
        "int-e.parquet",
    ];
    let tmp = TempDir::new()?;
    let table = create_int64_table(&tmp).await?;
    drop(table);

    remove_committed_file_size(tmp.path(), 2).await?;
    tokio::fs::remove_file(tmp.path().join("data/int-a.parquet")).await?;

    let table = Arc::new(TimeSeriesTable::open(TableLocation::local(tmp.path())).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, table)?;
    let (plan, batches) = run_numeric_query(&ctx, Some("idx >= 20")).await?;

    assert_planned_files(&plan, FILES, &["int-e.parquet"]);
    assert_eq!(collect_i64_values(&batches)?, vec![20, i64::MAX]);
    Ok(())
}
