//! Integration tests for the DataFusion table provider.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, Float64Builder, Int64Array, StringArray, StringBuilder, TimestampMillisecondArray,
    TimestampMillisecondBuilder, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};
use datafusion::catalog::TableProvider;
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tempfile::TempDir;
use timeseries_table_core::storage::TableLocation;
use timeseries_table_core::time_series_table::TimeSeriesTable;
use timeseries_table_core::transaction_log::logical_schema::{
    LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit,
};
use timeseries_table_core::transaction_log::{
    FileFormat, LogAction, SegmentId, SegmentMeta, TableMeta, TimeBucket, TimeIndexSpec,
    TransactionLogStore,
};
use timeseries_table_datafusion::TsTableProvider;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone)]
struct TestRow {
    ts_millis: i64,
    symbol: &'static str,
    price: Option<f64>,
}

fn make_index_spec() -> TimeIndexSpec {
    TimeIndexSpec {
        timestamp_column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        bucket: TimeBucket::Minutes(1),
        timezone: None,
    }
}

fn make_table_meta(price_nullable: bool) -> Result<TableMeta, Box<dyn std::error::Error>> {
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
            nullable: price_nullable,
        },
    ])?;

    Ok(TableMeta::new_time_series_with_schema(
        make_index_spec(),
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

fn make_rows(start: i64, count: usize, symbol: &'static str, price_base: f64) -> Vec<TestRow> {
    (0..count)
        .map(|idx| TestRow {
            ts_millis: start + idx as i64,
            symbol,
            price: Some(price_base + idx as f64),
        })
        .collect()
}

fn minutes_to_millis(minutes: i64) -> i64 {
    minutes * 60_000
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
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let schema = Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
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

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(ts_builder.finish()),
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

async fn create_two_segment_table(tmp: &TempDir) -> TestResult<TimeSeriesTable> {
    let mut table = create_table(tmp, false).await?;

    let rows_a = make_rows(minutes_to_millis(1), 5, "A", 10.0);
    let rows_b = make_rows(minutes_to_millis(3), 5, "A", 20.0);

    write_segment(tmp.path(), "data/seg-a.parquet", &rows_a, false)?;
    table
        .append_parquet_segment("data/seg-a.parquet", "ts")
        .await?;

    write_segment(tmp.path(), "data/seg-b.parquet", &rows_b, false)?;
    table
        .append_parquet_segment("data/seg-b.parquet", "ts")
        .await?;

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
    table.append_parquet_segment(rel_path, "ts").await?;

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
    let mut lines = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        // EXPLAIN output is not guaranteed to keep a stable column order across versions.
        // Prefer the "plan" column (or any column containing "plan") rather than guessing.
        let col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "plan")
            .or_else(|| {
                schema
                    .fields()
                    .iter()
                    .position(|f| f.name().contains("plan"))
            })
            .unwrap_or(0);
        let col = batch.column(col_idx);
        let strings = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            format!(
                "expected EXPLAIN output to be a string array (column {})",
                schema.field(col_idx).name()
            )
        })?;
        for row in 0..strings.len() {
            if strings.is_valid(row) {
                lines.push(strings.value(row).to_string());
            }
        }
    }
    Ok(lines.join("\n"))
}

fn pretty_batches(batches: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error>> {
    use arrow::util::pretty::pretty_format_batches;
    Ok(pretty_format_batches(batches)?.to_string())
}

fn find_data_source_exec(plan: &dyn ExecutionPlan) -> Option<&DataSourceExec> {
    if let Some(exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
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

macro_rules! assert_plan_contains {
    ($plan_text:expr, $pretty:expr, $needle:expr) => {
        assert!(
            $plan_text.contains($needle),
            "expected {needle} in plan:\n{plan_text}\n\npretty:\n{pretty}",
            needle = $needle,
            plan_text = $plan_text,
            pretty = $pretty
        );
    };
}

macro_rules! assert_plan_not_contains {
    ($plan_text:expr, $pretty:expr, $needle:expr) => {
        assert!(
            !$plan_text.contains($needle),
            "unexpected {needle} in plan:\n{plan_text}\n\npretty:\n{pretty}",
            needle = $needle,
            plan_text = $plan_text,
            pretty = $pretty
        );
    };
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

fn collect_ts_values(batches: &[RecordBatch]) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
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
                        return Err("unexpected null timestamp".into());
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
                        return Err("unexpected null timestamp".into());
                    }
                    out.push(arr.value(idx));
                }
            }
            other => return Err(format!("unexpected ts type {other:?}").into()),
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
    let values = collect_ts_values(&batches)?;
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
    let _table = TimeSeriesTable::create(location.clone(), meta).await?;

    let rows = make_rows(minutes_to_millis(1), 5, "A", 10.0);
    let rel_path = "data/seg-missing-size.parquet";
    write_segment(tmp.path(), rel_path, &rows, false)?;

    let ts_min = Utc
        .timestamp_millis_opt(minutes_to_millis(1))
        .single()
        .unwrap();
    let ts_max = Utc
        .timestamp_millis_opt(minutes_to_millis(1) + 4)
        .single()
        .unwrap();

    let seg = SegmentMeta {
        segment_id: SegmentId("seg-missing-size".to_string()),
        path: rel_path.to_string(),
        format: FileFormat::Parquet,
        ts_min,
        ts_max,
        row_count: rows.len() as u64,
        file_size: None,
        coverage_path: None,
    };

    let log = TransactionLogStore::new(location.clone());
    log.commit_with_expected_version(1, vec![LogAction::AddSegment(seg)])
        .await?;

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
    writer
        .append_parquet_segment("data/seg-a.parquet", "ts")
        .await?;

    let provider_table = Arc::new(TimeSeriesTable::open(location.clone()).await?);
    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&provider_table))?;

    let initial_batches = collect_batches(&ctx, "SELECT COUNT(*) FROM t").await?;
    let initial_count = scalar_u64(&initial_batches)?;
    assert_eq!(initial_count, 5);

    let rows_b = make_rows(minutes_to_millis(3), 5, "A", 20.0);
    write_segment(tmp.path(), "data/seg-b.parquet", &rows_b, false)?;
    writer
        .append_parquet_segment("data/seg-b.parquet", "ts")
        .await?;

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
        .set_max_row_group_size(5)
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
async fn prunes_files_on_time_filter_explain() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(
        &ctx,
        "EXPLAIN SELECT * FROM t \
         WHERE ts >= '1970-01-01T00:03:00Z' AND ts < '1970-01-01T00:03:01Z'",
    )
    .await?;
    let plan_text = explain_plan_text(&batches)?;
    let pretty = pretty_batches(&batches)?;

    assert_plan_contains!(plan_text, pretty, "seg-b.parquet");
    assert_plan_not_contains!(plan_text, pretty, "seg-a.parquet");
    Ok(())
}

#[tokio::test]
async fn does_not_prune_on_unrecognized_predicate_explain() -> TestResult {
    let tmp = TempDir::new()?;
    let table = create_two_segment_table(&tmp).await?;
    let table = Arc::new(table);

    let ctx = SessionContext::new();
    let _provider = register_provider(&ctx, Arc::clone(&table))?;

    let batches = collect_batches(&ctx, "EXPLAIN SELECT * FROM t WHERE symbol = 'A'").await?;
    let plan_text = explain_plan_text(&batches)?;
    let pretty = pretty_batches(&batches)?;

    assert_plan_contains!(plan_text, pretty, "seg-a.parquet");
    assert_plan_contains!(plan_text, pretty, "seg-b.parquet");
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
