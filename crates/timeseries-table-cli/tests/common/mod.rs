use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::array::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub fn write_parquet_rows(path: &Path, rows: usize) -> TestResult {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut ts_builder = TimestampMillisecondBuilder::with_capacity(rows);
    let mut sym_builder = StringBuilder::new();
    let mut price_builder = Float64Builder::with_capacity(rows);
    let mut volume_builder = Int64Builder::with_capacity(rows);
    let mut trade_builder = BooleanBuilder::with_capacity(rows);
    let mut venue_builder = StringBuilder::new();
    let mut payload_builder = BinaryBuilder::new();

    let mut seed = 0xBAD_5EED_u64;
    let base_ts = 1_700_000_000_000i64;
    for i in 0..rows {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let rnd = seed;
        let ts = base_ts + (i as i64) * 1_000;
        let symbol = "SYM1".to_string();
        let price = 100.0 + (rnd % 10_000) as f64 / 100.0;
        let volume = 1_000 + (rnd % 5_000) as i64;
        let is_trade = i % 2 == 0;
        let venue = if i % 5 == 0 {
            None
        } else {
            Some(format!("X{}", (rnd % 7) + 1))
        };
        let payload = vec![i as u8, (i.wrapping_mul(3)) as u8];

        ts_builder.append_value(ts);
        sym_builder.append_value(symbol);
        price_builder.append_value(price);
        volume_builder.append_value(volume);
        trade_builder.append_value(is_trade);
        match venue {
            Some(v) => venue_builder.append_value(v),
            None => venue_builder.append_null(),
        }
        payload_builder.append_value(payload);
    }

    let schema = Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Int64, false),
        Field::new("is_trade", DataType::Boolean, false),
        Field::new("venue", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(ts_builder.finish()) as _,
            Arc::new(sym_builder.finish()),
            Arc::new(price_builder.finish()),
            Arc::new(volume_builder.finish()),
            Arc::new(trade_builder.finish()),
            Arc::new(venue_builder.finish()),
            Arc::new(payload_builder.finish()),
        ],
    )?;

    let file = std::fs::File::create(path)?;
    let props = parquet::file::properties::WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub fn sanitize_identifier(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }

    if out.is_empty() {
        return "t".to_string();
    }

    if out
        .chars()
        .next()
        .map(|ch| !ch.is_ascii_alphabetic())
        .unwrap_or(false)
    {
        out = format!("t_{out}");
    }

    out.make_ascii_lowercase();
    out
}

pub fn table_root(tmp: &tempfile::TempDir, name: &str) -> PathBuf {
    tmp.path().join(name)
}
