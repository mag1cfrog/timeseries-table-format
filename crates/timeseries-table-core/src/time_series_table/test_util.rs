use super::*;
use crate::transaction_log::table_metadata::{LogicalDataType, LogicalTimestampUnit};
use crate::transaction_log::{
    LogicalField, LogicalSchema, TableKind, TableMeta, TimeBucket, TimeIndexSpec,
};
use arrow::array::{
    Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit as ArrowTimeUnit};
use arrow_array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use chrono::{DateTime, TimeZone, Utc};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub(crate) type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Clone)]
pub(crate) struct TestRow {
    pub(crate) ts_millis: i64,
    pub(crate) symbol: &'static str,
    pub(crate) price: f64,
}

pub(crate) fn make_table_meta_with_unit(unit: LogicalTimestampUnit) -> TableMeta {
    let index = TimeIndexSpec {
        timestamp_column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        bucket: TimeBucket::Minutes(1),
        timezone: None,
    };

    let logical_schema = LogicalSchema::new(vec![
        LogicalField {
            name: "ts".to_string(),
            data_type: LogicalDataType::Timestamp {
                unit,
                timezone: None,
            },
            nullable: true,
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
    ])
    .expect("valid logical schema");

    TableMeta {
        kind: TableKind::TimeSeries(index),
        logical_schema: Some(logical_schema),
        created_at: utc_datetime(2025, 1, 1, 0, 0, 0),
        format_version: 1,
        entity_identity: None,
    }
}

pub(crate) fn write_test_parquet(
    path: &Path,
    include_symbol: bool,
    price_as_int: bool,
    rows: &[TestRow],
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut fields: Vec<Arc<Type>> = Vec::new();

    let ts_field = Type::primitive_type_builder("ts", PhysicalType::INT64)
        .with_repetition(Repetition::REQUIRED)
        .with_logical_type(Some(LogicalType::Timestamp {
            is_adjusted_to_u_t_c: true,
            unit: TimeUnit::MILLIS,
        }))
        .build()?;
    fields.push(Arc::new(ts_field));

    if include_symbol {
        let symbol_field = Type::primitive_type_builder("symbol", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::String))
            .build()?;
        fields.push(Arc::new(symbol_field));
    }

    let price_builder = if price_as_int {
        Type::primitive_type_builder("price", PhysicalType::INT64)
    } else {
        Type::primitive_type_builder("price", PhysicalType::DOUBLE)
    };
    let price_field = price_builder
        .with_repetition(Repetition::REQUIRED)
        .build()?;
    fields.push(Arc::new(price_field));

    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()?,
    );

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

    let ts_values: Vec<i64> = rows.iter().map(|r| r.ts_millis).collect();
    let symbol_values: Vec<ByteArray> = rows
        .iter()
        .map(|r| ByteArray::from(r.symbol.as_bytes()))
        .collect();
    let price_f64: Vec<f64> = rows.iter().map(|r| r.price).collect();
    let price_i64: Vec<i64> = rows.iter().map(|r| r.price as i64).collect();

    let mut row_group_writer = writer.next_row_group()?;
    let mut col_idx = 0;
    while let Some(mut col_writer) = row_group_writer.next_column()? {
        let mut cw = col_writer.untyped();
        match (include_symbol, price_as_int, col_idx) {
            (true, _, 0) | (false, _, 0) => match &mut cw {
                ColumnWriter::Int64ColumnWriter(w) => {
                    w.write_batch(&ts_values, None, None)?;
                }
                _ => return Err("unexpected writer for ts".into()),
            },
            (true, _, 1) => match &mut cw {
                ColumnWriter::ByteArrayColumnWriter(w) => {
                    w.write_batch(&symbol_values, None, None)?;
                }
                _ => return Err("unexpected writer for symbol".into()),
            },
            (true, false, 2) | (false, false, 1) => match &mut cw {
                ColumnWriter::DoubleColumnWriter(w) => {
                    w.write_batch(&price_f64, None, None)?;
                }
                _ => return Err("unexpected writer for price f64".into()),
            },
            (true, true, 2) | (false, true, 1) => match &mut cw {
                ColumnWriter::Int64ColumnWriter(w) => {
                    w.write_batch(&price_i64, None, None)?;
                }
                _ => return Err("unexpected writer for price i64".into()),
            },
            _ => return Err("unexpected column writer ordering".into()),
        }
        col_writer.close()?;
        col_idx += 1;
    }
    row_group_writer.close()?;
    writer.close()?;

    Ok(())
}

pub(crate) fn write_arrow_parquet_with_unit(
    path: &Path,
    unit: ArrowTimeUnit,
    ts: &[Option<i64>],
    symbols: &[&str],
    prices: &[f64],
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(ts.len(), symbols.len());
    assert_eq!(symbols.len(), prices.len());

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let schema = Schema::new(vec![
        Field::new("ts", DataType::Timestamp(unit, None), true),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]);

    let ts_array: Arc<dyn Array> = match unit {
        ArrowTimeUnit::Second => {
            let mut b = TimestampSecondBuilder::with_capacity(ts.len());
            for v in ts {
                match v {
                    Some(val) => b.append_value(*val),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ArrowTimeUnit::Millisecond => {
            let mut b = TimestampMillisecondBuilder::with_capacity(ts.len());
            for v in ts {
                match v {
                    Some(val) => b.append_value(*val),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ArrowTimeUnit::Microsecond => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(ts.len());
            for v in ts {
                match v {
                    Some(val) => b.append_value(*val),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        ArrowTimeUnit::Nanosecond => {
            let mut b = TimestampNanosecondBuilder::with_capacity(ts.len());
            for v in ts {
                match v {
                    Some(val) => b.append_value(*val),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    };

    let mut sym_builder =
        StringBuilder::with_capacity(symbols.len(), symbols.iter().map(|s| s.len()).sum());
    for s in symbols {
        sym_builder.append_value(s);
    }
    let sym_array = Arc::new(sym_builder.finish()) as Arc<dyn Array>;

    let mut price_builder = Float64Builder::with_capacity(prices.len());
    for p in prices {
        price_builder.append_value(*p);
    }
    let price_array = Arc::new(price_builder.finish()) as Arc<dyn Array>;

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![ts_array, sym_array, price_array],
    )?;

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub(crate) fn write_arrow_parquet_int_time(
    path: &Path,
    ts: &[i64],
    symbols: &[&str],
    prices: &[f64],
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(ts.len(), symbols.len());
    assert_eq!(symbols.len(), prices.len());

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let schema = Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]);

    let mut ts_builder = Int64Builder::with_capacity(ts.len());
    for v in ts {
        ts_builder.append_value(*v);
    }
    let ts_array = Arc::new(ts_builder.finish()) as Arc<dyn Array>;

    let mut sym_builder =
        StringBuilder::with_capacity(symbols.len(), symbols.iter().map(|s| s.len()).sum());
    for s in symbols {
        sym_builder.append_value(s);
    }
    let sym_array = Arc::new(sym_builder.finish()) as Arc<dyn Array>;

    let mut price_builder = Float64Builder::with_capacity(prices.len());
    for p in prices {
        price_builder.append_value(*p);
    }
    let price_array = Arc::new(price_builder.finish()) as Arc<dyn Array>;

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![ts_array, sym_array, price_array],
    )?;

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub(crate) fn write_parquet_without_time_column(
    path: &Path,
    symbols: &[&str],
    prices: &[f64],
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(symbols.len(), prices.len());

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let schema = Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]);

    let mut sym_builder =
        StringBuilder::with_capacity(symbols.len(), symbols.iter().map(|s| s.len()).sum());
    for s in symbols {
        sym_builder.append_value(s);
    }
    let sym_array = Arc::new(sym_builder.finish()) as Arc<dyn Array>;

    let mut price_builder = Float64Builder::with_capacity(prices.len());
    for p in prices {
        price_builder.append_value(*p);
    }
    let price_array = Arc::new(price_builder.finish()) as Arc<dyn Array>;

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![sym_array, price_array])?;

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub(crate) fn utc_datetime(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .expect("valid UTC timestamp")
}

pub(crate) fn make_basic_table_meta() -> TableMeta {
    let index = TimeIndexSpec {
        timestamp_column: "ts".to_string(),
        entity_columns: vec!["symbol".to_string()],
        bucket: TimeBucket::Minutes(1),
        timezone: None,
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
    ])
    .expect("valid logical schema");

    TableMeta {
        kind: TableKind::TimeSeries(index),
        logical_schema: Some(logical_schema),
        created_at: utc_datetime(2025, 1, 1, 0, 0, 0),
        format_version: 1,
        entity_identity: None,
    }
}

pub(crate) async fn collect_scan_rows(
    table: &TimeSeriesTable,
    ts_start: DateTime<Utc>,
    ts_end: DateTime<Utc>,
) -> Result<Vec<(i64, String, f64)>, TableError> {
    let mut stream = table.scan_range(ts_start, ts_end).await?;
    let mut rows = Vec::new();

    while let Some(batch_res) = stream.next().await {
        let batch = batch_res?;

        let ts_idx = batch.schema().index_of("ts").expect("ts column");
        let sym_idx = batch.schema().index_of("symbol").expect("symbol column");
        let price_idx = batch.schema().index_of("price").expect("price column");

        let ts_arr = batch.column(ts_idx);
        let ts_values: Vec<i64> = match ts_arr.data_type() {
            DataType::Timestamp(ArrowTimeUnit::Second, _) => batch
                .column(ts_idx)
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("ts as seconds")
                .iter()
                .map(|v| v.expect("non-null ts"))
                .collect(),
            DataType::Timestamp(ArrowTimeUnit::Millisecond, _) => batch
                .column(ts_idx)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("ts as millis")
                .iter()
                .map(|v| v.expect("non-null ts"))
                .collect(),
            DataType::Timestamp(ArrowTimeUnit::Microsecond, _) => batch
                .column(ts_idx)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("ts as micros")
                .iter()
                .map(|v| v.expect("non-null ts"))
                .collect(),
            DataType::Timestamp(ArrowTimeUnit::Nanosecond, _) => batch
                .column(ts_idx)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts as nanos")
                .iter()
                .map(|v| v.expect("non-null ts"))
                .collect(),
            other => panic!("unexpected time type: {other:?}"),
        };
        let sym_arr = batch
            .column(sym_idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("symbol as utf8");
        let price_arr = batch
            .column(price_idx)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("price as f64");

        for ((ts, symbol), price) in ts_values
            .iter()
            .zip(sym_arr.iter())
            .zip(price_arr.values().iter())
        {
            let symbol = symbol.expect("symbol as utf8");
            let price = *price;
            rows.push((*ts, symbol.to_string(), price));
        }
    }

    Ok(rows)
}
