use std::path::Path;

use bytes::Bytes;
use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::file::metadata::FileMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::ColumnDescPtr;
use snafu::Backtrace;

use crate::storage::{TableLocation, read_all_bytes};
use crate::transaction_log::logical_schema::{
    LogicalDataType, LogicalField, LogicalSchema, LogicalSchemaError, LogicalTimestampUnit,
};
use crate::transaction_log::segments::{SegmentMetaError, SegmentResult, map_storage_error};

fn map_parquet_col_to_logical_type(
    column: &str,
    physical: PhysicalType,
    logical: Option<&LogicalType>,
    fixed_len_byte_array_len: Option<i32>,
) -> Result<LogicalDataType, LogicalSchemaError> {
    // First: look at logical annotation when present
    if let Some(logical) = logical {
        match logical {
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c: _,
                unit,
            } => {
                let unit = match unit {
                    TimeUnit::MILLIS => LogicalTimestampUnit::Millis,
                    TimeUnit::MICROS => LogicalTimestampUnit::Micros,
                    TimeUnit::NANOS => LogicalTimestampUnit::Nanos,
                };

                // No capture of timezone for now.
                return Ok(LogicalDataType::Timestamp {
                    unit,
                    timezone: None,
                });
            }
            LogicalType::String => {
                // Semantically a UTF-8 string, even though it's BYTE_ARRAY underneath
                return Ok(LogicalDataType::Utf8);
            }
            LogicalType::Map | LogicalType::List | LogicalType::Enum => {
                // For now, treat “complex” logical types as Other – v0.1 doesn’t need to fully support them.
                return Ok(LogicalDataType::Other(format!("parquet::{logical:?}")));
            }
            LogicalType::Decimal { scale, precision } => {
                return Ok(LogicalDataType::Decimal {
                    precision: *precision,
                    scale: *scale,
                });
            }

            _ => {}
        }
    }
    // Second: fall back to physical type when no (or unsupported) logical annotation
    Ok(match physical {
        PhysicalType::BOOLEAN => LogicalDataType::Bool,
        PhysicalType::INT32 => LogicalDataType::Int32,
        PhysicalType::INT64 => LogicalDataType::Int64,
        PhysicalType::FLOAT => LogicalDataType::Float32,
        PhysicalType::DOUBLE => LogicalDataType::Float64,
        PhysicalType::BYTE_ARRAY => LogicalDataType::Binary,
        PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            let byte_width = fixed_len_byte_array_len.ok_or_else(|| {
                LogicalSchemaError::FixedBinaryMissingLength {
                    column: column.to_string(),
                }
            })?;
            if byte_width <= 0 {
                return Err(LogicalSchemaError::FixedBinaryInvalidWidthInSchema {
                    column: column.to_string(),
                    byte_width,
                });
            }
            LogicalDataType::FixedBinary { byte_width }
        }
        PhysicalType::INT96 => LogicalDataType::Int96,
    })
}

fn column_nullable(desc: &ColumnDescPtr) -> bool {
    match desc.self_type().get_basic_info().repetition() {
        Repetition::REQUIRED => false,
        Repetition::OPTIONAL | Repetition::REPEATED => true,
    }
}

fn logical_schema_from_parquet(meta: &FileMetaData) -> Result<LogicalSchema, LogicalSchemaError> {
    let descr = meta.schema_descr();
    let mut cols = Vec::with_capacity(descr.num_columns());

    for col in descr.columns() {
        let name = col.path().string();

        let physical = col.physical_type();
        let logical = col.logical_type_ref();

        let fixed_len_byte_array_len = if physical == PhysicalType::FIXED_LEN_BYTE_ARRAY {
            Some(col.type_length())
        } else {
            None
        };
        let data_type =
            map_parquet_col_to_logical_type(&name, physical, logical, fixed_len_byte_array_len)?;

        let nullable = column_nullable(col);
        cols.push(LogicalField {
            name,
            data_type,
            nullable,
        });
    }

    LogicalSchema::new(cols)
}

/// Build a `LogicalSchema` from Parquet bytes.
///
/// This mirrors `logical_schema_from_parquet_location` but consumes a provided
/// `Bytes` buffer. The caller supplies the relative path (for error context)
/// and the full Parquet file contents. On success it returns a `LogicalSchema`
/// derived from the Parquet physical/logical types; otherwise it returns a
/// `SegmentMetaError` with contextual path information.
pub fn logical_schema_from_parquet_bytes(
    rel_path: &Path,
    data: Bytes,
) -> SegmentResult<LogicalSchema> {
    let path_str = rel_path.display().to_string();

    // Parse Parquet metadata from the in-memory buffer.
    let reader =
        SerializedFileReader::new(data).map_err(|source| SegmentMetaError::ParquetRead {
            path: path_str.clone(),
            source,
            backtrace: Backtrace::capture(),
        })?;

    let file_meta = reader.metadata().file_metadata();

    // Map Parquet physical/logical types into our LogicalSchema representation.
    logical_schema_from_parquet(file_meta).map_err(|source| {
        SegmentMetaError::LogicalSchemaInvalid {
            path: path_str,
            source,
        }
    })
}

/// Reads the Parquet file at `rel_path` from `location` and returns the inferred logical schema.
pub async fn logical_schema_from_parquet_location(
    location: &TableLocation,
    rel_path: &Path,
) -> SegmentResult<LogicalSchema> {
    let bytes = read_all_bytes(location, rel_path)
        .await
        .map_err(map_storage_error)?;

    logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::{LogicalType, Repetition, TimeUnit};
    use parquet::column::writer::ColumnWriter;
    use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    enum TestColumnValues {
        Bool(Vec<bool>),
        Int32(Vec<i32>),
        Int64(Vec<i64>),
        Float32(Vec<f32>),
        Float64(Vec<f64>),
        Int96(Vec<Int96>),
    }

    fn write_single_column_parquet(
        path: &Path,
        column_name: &str,
        physical: PhysicalType,
        values: TestColumnValues,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let col = Arc::new(
            Type::primitive_type_builder(column_name, physical)
                .with_repetition(Repetition::REQUIRED)
                .build()?,
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let mut row_group_writer = writer.next_row_group()?;
        let mut values = Some(values);
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            let values = values.take().ok_or("unexpected extra column")?;
            match (col_writer.untyped(), values) {
                (ColumnWriter::BoolColumnWriter(typed), TestColumnValues::Bool(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                (ColumnWriter::Int32ColumnWriter(typed), TestColumnValues::Int32(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                (ColumnWriter::Int64ColumnWriter(typed), TestColumnValues::Int64(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                (ColumnWriter::FloatColumnWriter(typed), TestColumnValues::Float32(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                (ColumnWriter::DoubleColumnWriter(typed), TestColumnValues::Float64(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                (ColumnWriter::Int96ColumnWriter(typed), TestColumnValues::Int96(v)) => {
                    typed.write_batch(&v, None, None)?;
                }
                _ => return Err("unexpected column writer type".into()),
            }
            col_writer.close()?;
        }
        row_group_writer.close()?;
        writer.close()?;
        Ok(())
    }

    fn assert_logical_schema_for_physical(
        file_name: &str,
        physical: PhysicalType,
        expected: LogicalDataType,
        values: TestColumnValues,
    ) -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new(file_name);
        let abs = tmp.path().join(rel_path);

        write_single_column_parquet(&abs, "col", physical, values)?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "col");
        assert_eq!(cols[0].data_type, expected);
        assert!(!cols[0].nullable);
        Ok(())
    }

    fn write_fixed_len_byte_array_file(
        path: &Path,
        column_name: &str,
        byte_width: i32,
        values: &[Vec<u8>],
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_fixed_len_byte_array_file_with_logical(path, column_name, byte_width, None, values)
    }

    fn write_fixed_len_byte_array_file_with_logical(
        path: &Path,
        column_name: &str,
        byte_width: i32,
        logical: Option<LogicalType>,
        values: &[Vec<u8>],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        for value in values {
            if value.len() != byte_width as usize {
                return Err(format!(
                    "fixed-len value size {} != expected {}",
                    value.len(),
                    byte_width
                )
                .into());
            }
        }

        let mut builder =
            Type::primitive_type_builder(column_name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .with_length(byte_width);
        if let Some(logical) = logical {
            builder = builder.with_logical_type(Some(logical));
        }
        let col = Arc::new(builder.build()?);
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let fixed_values: Vec<FixedLenByteArray> = values
            .iter()
            .cloned()
            .map(FixedLenByteArray::from)
            .collect();

        let mut row_group_writer = writer.next_row_group()?;
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            match col_writer.untyped() {
                ColumnWriter::FixedLenByteArrayColumnWriter(typed) => {
                    typed.write_batch(&fixed_values, None, None)?;
                }
                _ => return Err("unexpected column writer type".into()),
            }
            col_writer.close()?;
        }
        row_group_writer.close()?;
        writer.close()?;
        Ok(())
    }

    fn write_byte_array_file(
        path: &Path,
        column_name: &str,
        values: &[&[u8]],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        write_byte_array_file_with_logical(path, column_name, None, values)
    }

    fn write_byte_array_file_with_logical(
        path: &Path,
        column_name: &str,
        logical: Option<LogicalType>,
        values: &[&[u8]],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut builder = Type::primitive_type_builder(column_name, PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED);
        if let Some(logical) = logical {
            builder = builder.with_logical_type(Some(logical));
        }
        let col = Arc::new(builder.build()?);
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let byte_values: Vec<ByteArray> = values.iter().map(|v| ByteArray::from(*v)).collect();

        let mut row_group_writer = writer.next_row_group()?;
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            match col_writer.untyped() {
                ColumnWriter::ByteArrayColumnWriter(typed) => {
                    typed.write_batch(&byte_values, None, None)?;
                }
                _ => return Err("unexpected column writer type".into()),
            }
            col_writer.close()?;
        }
        row_group_writer.close()?;
        writer.close()?;
        Ok(())
    }

    fn write_decimal_int64_file(
        path: &Path,
        column_name: &str,
        precision: i32,
        scale: i32,
        values: &[i64],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let col = Arc::new(
            Type::primitive_type_builder(column_name, PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::Decimal { scale, precision }))
                .with_precision(precision)
                .with_scale(scale)
                .build()?,
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let mut row_group_writer = writer.next_row_group()?;
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            match col_writer.untyped() {
                ColumnWriter::Int64ColumnWriter(typed) => {
                    typed.write_batch(values, None, None)?;
                }
                _ => return Err("unexpected column writer type".into()),
            }
            col_writer.close()?;
        }
        row_group_writer.close()?;
        writer.close()?;
        Ok(())
    }

    fn write_schema_only_parquet(
        path: &Path,
        schema: Arc<Type>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;
        writer.close()?;
        Ok(())
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_timestamp_units() {
        let cases = vec![
            (TimeUnit::MILLIS, LogicalTimestampUnit::Millis),
            (TimeUnit::MICROS, LogicalTimestampUnit::Micros),
            (TimeUnit::NANOS, LogicalTimestampUnit::Nanos),
        ];

        for (unit, expected_unit) in cases {
            let logical = LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit,
            };

            let mapped =
                map_parquet_col_to_logical_type("ts", PhysicalType::INT64, Some(&logical), None)
                    .unwrap();
            assert_eq!(
                mapped,
                LogicalDataType::Timestamp {
                    unit: expected_unit,
                    timezone: None,
                }
            );
        }
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_string_logical() {
        let mapped = map_parquet_col_to_logical_type(
            "text",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::String),
            None,
        )
        .unwrap();
        assert_eq!(mapped, LogicalDataType::Utf8);
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_complex_logical_to_other() {
        let map_type = map_parquet_col_to_logical_type(
            "map",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::Map),
            None,
        )
        .unwrap();
        assert_eq!(map_type, LogicalDataType::Other("parquet::Map".to_string()));

        let list_type = map_parquet_col_to_logical_type(
            "list",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::List),
            None,
        )
        .unwrap();
        assert_eq!(
            list_type,
            LogicalDataType::Other("parquet::List".to_string())
        );

        let enum_type = map_parquet_col_to_logical_type(
            "enum",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::Enum),
            None,
        )
        .unwrap();
        assert_eq!(
            enum_type,
            LogicalDataType::Other("parquet::Enum".to_string())
        );
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_decimal() {
        let decimal = LogicalType::Decimal {
            scale: 2,
            precision: 10,
        };
        let decimal_type =
            map_parquet_col_to_logical_type("dec", PhysicalType::INT64, Some(&decimal), None)
                .unwrap();
        assert_eq!(
            decimal_type,
            LogicalDataType::Decimal {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn map_parquet_col_to_logical_type_prefers_physical_for_unknown_logical() {
        let mapped = map_parquet_col_to_logical_type(
            "json",
            PhysicalType::INT64,
            Some(&LogicalType::Json),
            None,
        )
        .unwrap();
        assert_eq!(mapped, LogicalDataType::Int64);
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_physical_types_without_logical() {
        let cases = vec![
            (PhysicalType::BOOLEAN, LogicalDataType::Bool),
            (PhysicalType::INT32, LogicalDataType::Int32),
            (PhysicalType::INT64, LogicalDataType::Int64),
            (PhysicalType::FLOAT, LogicalDataType::Float32),
            (PhysicalType::DOUBLE, LogicalDataType::Float64),
            (PhysicalType::BYTE_ARRAY, LogicalDataType::Binary),
            (
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
                LogicalDataType::FixedBinary { byte_width: 16 },
            ),
            (PhysicalType::INT96, LogicalDataType::Int96),
        ];

        for (physical, expected) in cases {
            let fixed_len_byte_array_len = if physical == PhysicalType::FIXED_LEN_BYTE_ARRAY {
                Some(16)
            } else {
                None
            };
            let mapped =
                map_parquet_col_to_logical_type("col", physical, None, fixed_len_byte_array_len)
                    .unwrap();
            assert_eq!(mapped, expected);
        }
    }

    #[test]
    fn map_parquet_col_to_logical_type_requires_fixed_len_byte_array_length() {
        let err =
            map_parquet_col_to_logical_type("bin", PhysicalType::FIXED_LEN_BYTE_ARRAY, None, None)
                .unwrap_err();
        assert!(
            matches!(
                &err,
                LogicalSchemaError::FixedBinaryMissingLength { column } if column == "bin"
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn logical_schema_maps_fixed_len_byte_array_with_width() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/fixed-len.parquet");
        let abs = tmp.path().join(rel_path);

        write_fixed_len_byte_array_file(&abs, "bin", 4, &[vec![0, 1, 2, 3], vec![4, 5, 6, 7]])?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "bin");
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::FixedBinary { byte_width: 4 }
        );
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_byte_array_to_binary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/byte-array.parquet");
        let abs = tmp.path().join(rel_path);

        write_byte_array_file(&abs, "bin", &[b"a", b"bc"])?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "bin");
        assert_eq!(cols[0].data_type, LogicalDataType::Binary);
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_string_logical_to_utf8() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/byte-array-string.parquet");
        let abs = tmp.path().join(rel_path);

        write_byte_array_file_with_logical(
            &abs,
            "text",
            Some(LogicalType::String),
            &[b"alpha", b"beta"],
        )?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "text");
        assert_eq!(cols[0].data_type, LogicalDataType::Utf8);
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_enum_logical_to_other() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/byte-array-enum.parquet");
        let abs = tmp.path().join(rel_path);

        write_byte_array_file_with_logical(&abs, "kind", Some(LogicalType::Enum), &[b"A", b"B"])?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "kind");
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::Other("parquet::Enum".to_string())
        );
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_decimal_logical() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/decimal-int64.parquet");
        let abs = tmp.path().join(rel_path);

        write_decimal_int64_file(&abs, "dec", 10, 2, &[1234, 5678])?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "dec");
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_uuid_logical_to_fixed_binary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/uuid.parquet");
        let abs = tmp.path().join(rel_path);

        write_fixed_len_byte_array_file_with_logical(
            &abs,
            "uuid",
            16,
            Some(LogicalType::Uuid),
            &[vec![0; 16], vec![1; 16]],
        )?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "uuid");
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::FixedBinary { byte_width: 16 }
        );
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_float16_logical_to_fixed_binary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/float16.parquet");
        let abs = tmp.path().join(rel_path);

        write_fixed_len_byte_array_file_with_logical(
            &abs,
            "f16",
            2,
            Some(LogicalType::Float16),
            &[vec![0, 0], vec![0x3c, 0x00]],
        )?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "f16");
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::FixedBinary { byte_width: 2 }
        );
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_json_logical_to_binary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/json.parquet");
        let abs = tmp.path().join(rel_path);

        write_byte_array_file_with_logical(
            &abs,
            "json",
            Some(LogicalType::Json),
            &[br#"{"a":1}"#, br#"{"b":2}"#],
        )?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "json");
        assert_eq!(cols[0].data_type, LogicalDataType::Binary);
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_bson_logical_to_binary() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/bson.parquet");
        let abs = tmp.path().join(rel_path);

        write_byte_array_file_with_logical(
            &abs,
            "bson",
            Some(LogicalType::Bson),
            &[b"\x05\x00\x00\x00\x00", b"\x05\x00\x00\x00\x01"],
        )?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "bson");
        assert_eq!(cols[0].data_type, LogicalDataType::Binary);
        assert!(!cols[0].nullable);
        Ok(())
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_map_logical_to_other() {
        let mapped = map_parquet_col_to_logical_type(
            "map",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::Map),
            None,
        )
        .unwrap();
        assert_eq!(mapped, LogicalDataType::Other("parquet::Map".to_string()));
    }

    #[test]
    fn map_parquet_col_to_logical_type_maps_list_logical_to_other() {
        let mapped = map_parquet_col_to_logical_type(
            "list",
            PhysicalType::BYTE_ARRAY,
            Some(&LogicalType::List),
            None,
        )
        .unwrap();
        assert_eq!(mapped, LogicalDataType::Other("parquet::List".to_string()));
    }

    #[test]
    fn logical_schema_ignores_list_group_annotation() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/list-group.parquet");
        let abs = tmp.path().join(rel_path);

        let element = Type::primitive_type_builder("element", PhysicalType::INT32)
            .with_repetition(Repetition::REPEATED)
            .build()?;
        let list_group = Type::group_type_builder("list_field")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(element)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(list_group)])
                .build()?,
        );

        write_schema_only_parquet(&abs, schema)?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].data_type, LogicalDataType::Int32);
        Ok(())
    }

    #[test]
    fn logical_schema_ignores_map_group_annotation() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/map-group.parquet");
        let abs = tmp.path().join(rel_path);

        let key = Type::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let value = Type::primitive_type_builder("value", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()?;
        let key_value = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(key), Arc::new(value)])
            .build()?;
        let map_group = Type::group_type_builder("map_field")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(key_value)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(map_group)])
                .build()?,
        );

        write_schema_only_parquet(&abs, schema)?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].data_type, LogicalDataType::Binary);
        assert_eq!(cols[1].data_type, LogicalDataType::Int64);
        Ok(())
    }

    #[test]
    fn logical_schema_maps_bool() -> TestResult {
        assert_logical_schema_for_physical(
            "data/bool.parquet",
            PhysicalType::BOOLEAN,
            LogicalDataType::Bool,
            TestColumnValues::Bool(vec![true, false]),
        )
    }

    #[test]
    fn logical_schema_maps_int32() -> TestResult {
        assert_logical_schema_for_physical(
            "data/int32.parquet",
            PhysicalType::INT32,
            LogicalDataType::Int32,
            TestColumnValues::Int32(vec![1, 2, 3]),
        )
    }

    #[test]
    fn logical_schema_maps_int64() -> TestResult {
        assert_logical_schema_for_physical(
            "data/int64.parquet",
            PhysicalType::INT64,
            LogicalDataType::Int64,
            TestColumnValues::Int64(vec![1, 2, 3]),
        )
    }

    #[test]
    fn logical_schema_maps_float32() -> TestResult {
        assert_logical_schema_for_physical(
            "data/float32.parquet",
            PhysicalType::FLOAT,
            LogicalDataType::Float32,
            TestColumnValues::Float32(vec![1.25, 2.5]),
        )
    }

    #[test]
    fn logical_schema_maps_float64() -> TestResult {
        assert_logical_schema_for_physical(
            "data/float64.parquet",
            PhysicalType::DOUBLE,
            LogicalDataType::Float64,
            TestColumnValues::Float64(vec![1.25, 2.5]),
        )
    }

    #[test]
    fn logical_schema_maps_int96() -> TestResult {
        let v1 = Int96::from(vec![1, 2, 3]);
        let v2 = Int96::from(vec![4, 5, 6]);
        assert_logical_schema_for_physical(
            "data/int96.parquet",
            PhysicalType::INT96,
            LogicalDataType::Int96,
            TestColumnValues::Int96(vec![v1, v2]),
        )
    }
}
