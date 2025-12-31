use std::path::Path;

use bytes::Bytes;
use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::file::metadata::FileMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::Type;
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

fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}.{name}")
    }
}

fn rep_nullable(rep: Repetition) -> bool {
    matches!(rep, Repetition::OPTIONAL)
}

fn type_shape(t: &Type) -> String {
    let rep = t.get_basic_info().repetition();
    if t.is_group() {
        let children: Vec<String> = t
            .get_fields()
            .iter()
            .map(|c| {
                format!(
                    "{}:{:?}",
                    c.get_basic_info().name(),
                    c.get_basic_info().repetition()
                )
            })
            .collect();
        format!("group({rep:?}, children=[{}])", children.join(", "))
    } else {
        format!("primitive({rep:?}, {:?})", t.get_physical_type())
    }
}

fn parquet_primitive_to_logical_datatype(
    t: &Type,
    column_path: &str,
) -> Result<LogicalDataType, LogicalSchemaError> {
    let physical = t.get_physical_type();
    let logical = t.get_basic_info().logical_type_ref();

    let fixed_len = if physical == PhysicalType::FIXED_LEN_BYTE_ARRAY {
        match t {
            Type::PrimitiveType { type_length, .. } => Some(*type_length),
            _ => None,
        }
    } else {
        None
    };

    map_parquet_col_to_logical_type(column_path, physical, logical, fixed_len)
}

fn parquet_type_to_logical_datatype(
    t: &Type,
    path: &str,
) -> Result<LogicalDataType, LogicalSchemaError> {
    // If this group is annotated as LIST/MAP, preserve semantics.
    if let Some(logical) = t.get_basic_info().logical_type_ref() {
        match logical {
            LogicalType::List => return parse_parquet_list(t, path),
            LogicalType::Map => return parse_parquet_map(t, path),
            _ => {}
        }
    }

    // Otherwise: group => Struct, primitive => primitive mapping
    if t.is_group() {
        let children = t
            .get_fields()
            .iter()
            .map(|c| parquet_type_to_logical_field(c, path))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(LogicalDataType::Struct { fields: children })
    } else {
        parquet_primitive_to_logical_datatype(t, path)
    }
}

fn parse_parquet_list(t: &Type, column_path: &str) -> Result<LogicalDataType, LogicalSchemaError> {
    if !t.is_group() {
        return Err(LogicalSchemaError::UnsupportedParquetListEncoding {
            column_path: column_path.to_string(),
            details: format!(
                "LIST annotation on a primitive is unsupported; observed {}",
                type_shape(t)
            ),
        });
    }

    let outer = t.get_fields();
    if outer.len() != 1 {
        return Err(LogicalSchemaError::UnsupportedParquetListEncoding {
            column_path: column_path.to_string(),
            details: format!(
                "LIST group must have exactly 1 child, got {}; observed {}",
                outer.len(),
                type_shape(t)
            ),
        });
    }

    let repeated = &outer[0];
    if !matches!(repeated.get_basic_info().repetition(), Repetition::REPEATED) {
        return Err(LogicalSchemaError::UnsupportedParquetListEncoding {
            column_path: column_path.to_string(),
            details: format!(
                "LIST child must be REPEATED; observed {}",
                type_shape(repeated)
            ),
        });
    }

    // Canonical: repeated group with exactly 1 child => element
    // Tolerate: repeated primitive/group => treat that repeated node as the element
    let (elem_type, elem_nullable) = if repeated.is_group() && repeated.get_fields().len() == 1 {
        let elem = &repeated.get_fields()[0];
        (
            elem,
            matches!(elem.get_basic_info().repetition(), Repetition::OPTIONAL),
        )
    } else {
        (repeated, false)
    };

    let elem_dt = parquet_type_to_logical_datatype(elem_type, &format!("{column_path}.element"))?;

    Ok(LogicalDataType::List {
        elements: Box::new(LogicalField {
            name: "element".to_string(),
            data_type: elem_dt,
            nullable: elem_nullable,
        }),
    })
}

fn parse_parquet_map(t: &Type, column_path: &str) -> Result<LogicalDataType, LogicalSchemaError> {
    if !t.is_group() {
        return Err(LogicalSchemaError::UnsupportedParquetMapEncoding {
            column_path: column_path.to_string(),
            details: format!(
                "MAP annotation on a primitive is unsupported; observed {}",
                type_shape(t)
            ),
        });
    }

    let outer = t.get_fields();
    if outer.len() != 1 {
        return Err(LogicalSchemaError::UnsupportedParquetMapEncoding {
            column_path: column_path.to_string(),
            details: format!(
                "MAP group must have exactly 1 child, got {}; observed {}",
                outer.len(),
                type_shape(t)
            ),
        });
    }

    let kv = &outer[0];
    if !kv.is_group() || !matches!(kv.get_basic_info().repetition(), Repetition::REPEATED) {
        return Err(LogicalSchemaError::UnsupportedParquetMapEncoding {
            column_path: column_path.to_string(),
            details: format!(
                "MAP child must be a REPEATED group (key_value); observed {}",
                type_shape(kv)
            ),
        });
    }

    let kv_fields = kv.get_fields();
    if !(kv_fields.len() == 1 || kv_fields.len() == 2) {
        return Err(LogicalSchemaError::UnsupportedParquetMapEncoding {
            column_path: column_path.to_string(),
            details: format!(
                "key_value group must have 1 or 2 children, got {}; observed {}",
                kv_fields.len(),
                type_shape(kv)
            ),
        });
    }

    let key_t = &kv_fields[0];
    if !matches!(key_t.get_basic_info().repetition(), Repetition::REQUIRED) {
        return Err(LogicalSchemaError::InvalidMapKeyNullability {
            column_path: column_path.to_string(),
        });
    }

    let key_dt = parquet_type_to_logical_datatype(key_t, &format!("{column_path}.key"))?;

    let value_field = if kv_fields.len() == 2 {
        let val_t = &kv_fields[1];
        let val_nullable = matches!(val_t.get_basic_info().repetition(), Repetition::OPTIONAL);
        let val_dt = parquet_type_to_logical_datatype(val_t, &format!("{column_path}.value"))?;
        Some(Box::new(LogicalField {
            name: "value".to_string(),
            nullable: val_nullable,
            data_type: val_dt,
        }))
    } else {
        None // Keys-only map
    };

    Ok(LogicalDataType::Map {
        key: Box::new(LogicalField {
            name: "key".to_string(),
            data_type: key_dt,
            nullable: false,
        }),
        value: value_field,
        keys_sorted: false, // Parquet schema doesn't reliably carry this; conservative default
    })
}

fn parquet_type_to_logical_field(
    t: &Type,
    parent: &str,
) -> Result<LogicalField, LogicalSchemaError> {
    let info = t.get_basic_info();
    let name = info.name().to_string();
    let path = join_path(parent, &name);
    let rep = info.repetition();
    let nullable = rep_nullable(rep);

    // 1) List/MAP are group-level logical annotations
    if let Some(logical) = info.logical_type_ref() {
        match logical {
            LogicalType::List => {
                let dt = parse_parquet_list(t, &path)?;
                return Ok(LogicalField {
                    name,
                    data_type: dt,
                    nullable,
                });
            }
            LogicalType::Map => {
                let dt = parse_parquet_map(t, &path)?;
                return Ok(LogicalField {
                    name,
                    data_type: dt,
                    nullable,
                });
            }
            _ => {
                // Other logical types handled by primitive mapping below
            }
        }
    }

    // 2) Optional legacy rule: unannotated REPEATED => list of required elements
    if matches!(rep, Repetition::REPEATED) {
        let elem_dt = parquet_type_to_logical_datatype(t, &format!("{path}.element"))?;
        return Ok(LogicalField {
            name,
            data_type: LogicalDataType::List {
                elements: Box::new(LogicalField {
                    name: "element".to_string(),
                    data_type: elem_dt,
                    nullable: false,
                }),
            },
            nullable: false,
        });
    }

    // 3) Non-LIST/MAP groups => Struct
    if t.is_group() {
        let children = t
            .get_fields()
            .iter()
            .map(|c| parquet_type_to_logical_field(c, &path))
            .collect::<Result<Vec<_>, _>>()?;

        return Ok(LogicalField {
            name,
            data_type: LogicalDataType::Struct { fields: children },
            nullable,
        });
    }

    // 4) Primitive leaf
    let dt = parquet_primitive_to_logical_datatype(t, &path)?;
    Ok(LogicalField {
        name,
        data_type: dt,
        nullable,
    })
}

fn logical_schema_from_parquet(meta: &FileMetaData) -> Result<LogicalSchema, LogicalSchemaError> {
    let root = meta.schema_descr().root_schema_ptr(); // schema tree root (group)
    let fields = root
        .get_fields()
        .iter()
        .map(|t| parquet_type_to_logical_field(t, ""))
        .collect::<Result<Vec<_>, _>>()?;

    LogicalSchema::new(fields)
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

    fn assert_schema_err(
        rel_path: &Path,
        schema: Arc<Type>,
        expected: LogicalSchemaError,
    ) -> TestResult {
        let tmp = TempDir::new()?;
        let abs = tmp.path().join(rel_path);
        write_schema_only_parquet(&abs, schema)?;

        let bytes = std::fs::read(&abs)?;
        match logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes)) {
            Err(SegmentMetaError::LogicalSchemaInvalid { source, .. }) => {
                assert_eq!(source, expected);
                Ok(())
            }
            other => Err(format!("expected LogicalSchemaInvalid, got {other:?}").into()),
        }
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
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::List {
                elements: Box::new(LogicalField {
                    name: "element".to_string(),
                    data_type: LogicalDataType::Int32,
                    nullable: false,
                }),
            }
        );
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

        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::Map {
                key: Box::new(LogicalField {
                    name: "key".to_string(),
                    data_type: LogicalDataType::Binary,
                    nullable: false,
                }),
                value: Some(Box::new(LogicalField {
                    name: "value".to_string(),
                    data_type: LogicalDataType::Int64,
                    nullable: true,
                })),
                keys_sorted: false,
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_list_canonical_optional_element() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/list-canonical.parquet");
        let abs = tmp.path().join(rel_path);

        let element = Type::primitive_type_builder("element", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()?;
        let repeated = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(element)])
            .build()?;
        let list_group = Type::group_type_builder("values")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(repeated)])
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
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::List {
                elements: Box::new(LogicalField {
                    name: "element".to_string(),
                    data_type: LogicalDataType::Int64,
                    nullable: true,
                }),
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_list_tolerates_repeated_primitive_child() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/list-2-level.parquet");
        let abs = tmp.path().join(rel_path);

        let repeated = Type::primitive_type_builder("element", PhysicalType::INT32)
            .with_repetition(Repetition::REPEATED)
            .build()?;
        let list_group = Type::group_type_builder("values")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(repeated)])
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
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::List {
                elements: Box::new(LogicalField {
                    name: "element".to_string(),
                    data_type: LogicalDataType::Int32,
                    nullable: false,
                }),
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_list_nested_list_element_preserves_list() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/list-nested.parquet");
        let abs = tmp.path().join(rel_path);

        let inner_element = Type::primitive_type_builder("element", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let inner_repeated = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(inner_element)])
            .build()?;
        let inner_list = Type::group_type_builder("element")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(inner_repeated)])
            .build()?;

        let outer_repeated = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(inner_list)])
            .build()?;
        let outer_list = Type::group_type_builder("col")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(outer_repeated)])
            .build()?;

        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(outer_list)])
                .build()?,
        );

        write_schema_only_parquet(&abs, schema)?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::List {
                elements: Box::new(LogicalField {
                    name: "element".to_string(),
                    data_type: LogicalDataType::List {
                        elements: Box::new(LogicalField {
                            name: "element".to_string(),
                            data_type: LogicalDataType::Int32,
                            nullable: false,
                        }),
                    },
                    nullable: true,
                }),
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_list_element_map_preserves_map() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/list-map.parquet");
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
        let map_group = Type::group_type_builder("element")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(key_value)])
            .build()?;

        let repeated = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(map_group)])
            .build()?;
        let list_group = Type::group_type_builder("col")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(repeated)])
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
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::List {
                elements: Box::new(LogicalField {
                    name: "element".to_string(),
                    data_type: LogicalDataType::Map {
                        key: Box::new(LogicalField {
                            name: "key".to_string(),
                            data_type: LogicalDataType::Binary,
                            nullable: false,
                        }),
                        value: Some(Box::new(LogicalField {
                            name: "value".to_string(),
                            data_type: LogicalDataType::Int64,
                            nullable: true,
                        })),
                        keys_sorted: false,
                    },
                    nullable: true,
                }),
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_map_value_list_preserves_list() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/map-list.parquet");
        let abs = tmp.path().join(rel_path);

        let key = Type::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let list_elem = Type::primitive_type_builder("element", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let list_repeated = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(list_elem)])
            .build()?;
        let list_group = Type::group_type_builder("value")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(list_repeated)])
            .build()?;
        let key_value = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(key), Arc::new(list_group)])
            .build()?;
        let map_group = Type::group_type_builder("col")
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

        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::Map {
                key: Box::new(LogicalField {
                    name: "key".to_string(),
                    data_type: LogicalDataType::Binary,
                    nullable: false,
                }),
                value: Some(Box::new(LogicalField {
                    name: "value".to_string(),
                    data_type: LogicalDataType::List {
                        elements: Box::new(LogicalField {
                            name: "element".to_string(),
                            data_type: LogicalDataType::Int32,
                            nullable: false,
                        }),
                    },
                    nullable: true,
                })),
                keys_sorted: false,
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_struct_with_list_and_map_fields() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/struct-list-map.parquet");
        let abs = tmp.path().join(rel_path);

        let list_elem = Type::primitive_type_builder("element", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let list_repeated = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(list_elem)])
            .build()?;
        let list_group = Type::group_type_builder("nums")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(list_repeated)])
            .build()?;

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
        let map_group = Type::group_type_builder("attrs")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(key_value)])
            .build()?;

        let struct_group = Type::group_type_builder("s")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![Arc::new(list_group), Arc::new(map_group)])
            .build()?;

        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(struct_group)])
                .build()?,
        );

        write_schema_only_parquet(&abs, schema)?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::Struct {
                fields: vec![
                    LogicalField {
                        name: "nums".to_string(),
                        data_type: LogicalDataType::List {
                            elements: Box::new(LogicalField {
                                name: "element".to_string(),
                                data_type: LogicalDataType::Int32,
                                nullable: false,
                            }),
                        },
                        nullable: true,
                    },
                    LogicalField {
                        name: "attrs".to_string(),
                        data_type: LogicalDataType::Map {
                            key: Box::new(LogicalField {
                                name: "key".to_string(),
                                data_type: LogicalDataType::Binary,
                                nullable: false,
                            }),
                            value: Some(Box::new(LogicalField {
                                name: "value".to_string(),
                                data_type: LogicalDataType::Int64,
                                nullable: true,
                            })),
                            keys_sorted: false,
                        },
                        nullable: true,
                    },
                ],
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_map_nested_map_value_preserves_map() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/map-map.parquet");
        let abs = tmp.path().join(rel_path);

        let inner_key = Type::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let inner_value = Type::primitive_type_builder("value", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()?;
        let inner_kv = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(inner_key), Arc::new(inner_value)])
            .build()?;
        let inner_map = Type::group_type_builder("value")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(inner_kv)])
            .build()?;

        let outer_key = Type::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let outer_kv = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(outer_key), Arc::new(inner_map)])
            .build()?;
        let outer_map = Type::group_type_builder("col")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(outer_kv)])
            .build()?;

        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(outer_map)])
                .build()?,
        );

        write_schema_only_parquet(&abs, schema)?;

        let bytes = std::fs::read(&abs)?;
        let schema = logical_schema_from_parquet_bytes(rel_path, Bytes::from(bytes))?;
        let cols = schema.columns();

        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::Map {
                key: Box::new(LogicalField {
                    name: "key".to_string(),
                    data_type: LogicalDataType::Binary,
                    nullable: false,
                }),
                value: Some(Box::new(LogicalField {
                    name: "value".to_string(),
                    data_type: LogicalDataType::Map {
                        key: Box::new(LogicalField {
                            name: "key".to_string(),
                            data_type: LogicalDataType::Binary,
                            nullable: false,
                        }),
                        value: Some(Box::new(LogicalField {
                            name: "value".to_string(),
                            data_type: LogicalDataType::Int32,
                            nullable: true,
                        })),
                        keys_sorted: false,
                    },
                    nullable: true,
                })),
                keys_sorted: false,
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_list_struct_list_preserves_nested_list() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/list-struct-list.parquet");
        let abs = tmp.path().join(rel_path);

        let inner_elem = Type::primitive_type_builder("element", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let inner_repeated = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(inner_elem)])
            .build()?;
        let inner_list = Type::group_type_builder("nums")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(inner_repeated)])
            .build()?;

        let struct_group = Type::group_type_builder("element")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![Arc::new(inner_list)])
            .build()?;

        let repeated = Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(struct_group)])
            .build()?;
        let list_group = Type::group_type_builder("col")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(repeated)])
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
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::List {
                elements: Box::new(LogicalField {
                    name: "element".to_string(),
                    data_type: LogicalDataType::Struct {
                        fields: vec![LogicalField {
                            name: "nums".to_string(),
                            data_type: LogicalDataType::List {
                                elements: Box::new(LogicalField {
                                    name: "element".to_string(),
                                    data_type: LogicalDataType::Int32,
                                    nullable: false,
                                }),
                            },
                            nullable: true,
                        }],
                    },
                    nullable: true,
                }),
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_map_rejects_non_group_child() -> TestResult {
        let rel_path = Path::new("data/map-non-group-child.parquet");
        let child = Type::primitive_type_builder("key_value", PhysicalType::INT32)
            .with_repetition(Repetition::REPEATED)
            .build()?;
        let map_group = Type::group_type_builder("attrs")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(child)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(map_group)])
                .build()?,
        );

        assert_schema_err(
            rel_path,
            schema,
            LogicalSchemaError::UnsupportedParquetMapEncoding {
                column_path: "attrs".to_string(),
                details: "MAP child must be a REPEATED group (key_value); observed primitive(REPEATED, INT32)".to_string(),
            },
        )
    }

    #[test]
    fn logical_schema_list_rejects_list_on_primitive() -> TestResult {
        let list_primitive = Type::primitive_type_builder("values", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .build();

        let err = list_primitive.expect_err("expected parquet builder to reject LIST on primitive");
        assert!(
            err.to_string()
                .contains("List cannot be applied to a primitive type for field 'values'"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn logical_schema_list_rejects_multiple_children() -> TestResult {
        let rel_path = Path::new("data/list-multi-children.parquet");
        let child1 = Type::primitive_type_builder("a", PhysicalType::INT32)
            .with_repetition(Repetition::REPEATED)
            .build()?;
        let child2 = Type::primitive_type_builder("b", PhysicalType::INT32)
            .with_repetition(Repetition::REPEATED)
            .build()?;
        let list_group = Type::group_type_builder("values")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(child1), Arc::new(child2)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(list_group)])
                .build()?,
        );

        assert_schema_err(
            rel_path,
            schema,
            LogicalSchemaError::UnsupportedParquetListEncoding {
                column_path: "values".to_string(),
                details: "LIST group must have exactly 1 child, got 2; observed group(OPTIONAL, children=[a:REPEATED, b:REPEATED])".to_string(),
            },
        )
    }

    #[test]
    fn logical_schema_list_rejects_non_repeated_child() -> TestResult {
        let rel_path = Path::new("data/list-non-repeated.parquet");
        let child = Type::primitive_type_builder("element", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()?;
        let list_group = Type::group_type_builder("values")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![Arc::new(child)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(list_group)])
                .build()?,
        );

        assert_schema_err(
            rel_path,
            schema,
            LogicalSchemaError::UnsupportedParquetListEncoding {
                column_path: "values".to_string(),
                details: "LIST child must be REPEATED; observed primitive(OPTIONAL, INT32)"
                    .to_string(),
            },
        )
    }

    #[test]
    fn logical_schema_map_canonical() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/map-canonical.parquet");
        let abs = tmp.path().join(rel_path);

        let key = Type::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let value = Type::primitive_type_builder("value", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()?;
        let key_value = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(key), Arc::new(value)])
            .build()?;
        let map_group = Type::group_type_builder("attrs")
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

        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::Map {
                key: Box::new(LogicalField {
                    name: "key".to_string(),
                    data_type: LogicalDataType::Binary,
                    nullable: false,
                }),
                value: Some(Box::new(LogicalField {
                    name: "value".to_string(),
                    data_type: LogicalDataType::Int32,
                    nullable: true,
                })),
                keys_sorted: false,
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_map_keys_only() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/map-keys-only.parquet");
        let abs = tmp.path().join(rel_path);

        let key = Type::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let key_value = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(key)])
            .build()?;
        let map_group = Type::group_type_builder("attrs")
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

        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0].data_type,
            LogicalDataType::Map {
                key: Box::new(LogicalField {
                    name: "key".to_string(),
                    data_type: LogicalDataType::Binary,
                    nullable: false,
                }),
                value: None,
                keys_sorted: false,
            }
        );
        Ok(())
    }

    #[test]
    fn logical_schema_map_rejects_non_repeated_child() -> TestResult {
        let rel_path = Path::new("data/map-non-repeated.parquet");
        let key = Type::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let kv = Type::group_type_builder("key_value")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![Arc::new(key)])
            .build()?;
        let map_group = Type::group_type_builder("attrs")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(kv)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(map_group)])
                .build()?,
        );

        assert_schema_err(
            rel_path,
            schema,
            LogicalSchemaError::UnsupportedParquetMapEncoding {
                column_path: "attrs".to_string(),
                details:
                    "MAP child must be a REPEATED group (key_value); observed group(OPTIONAL, children=[key:REQUIRED])"
                        .to_string(),
            },
        )
    }

    #[test]
    fn logical_schema_map_rejects_kv_child_count() -> TestResult {
        let rel_path = Path::new("data/map-kv-child-count.parquet");
        let key = Type::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let v1 = Type::primitive_type_builder("v1", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()?;
        let v2 = Type::primitive_type_builder("v2", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()?;
        let kv = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(key), Arc::new(v1), Arc::new(v2)])
            .build()?;
        let map_group = Type::group_type_builder("attrs")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(kv)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(map_group)])
                .build()?,
        );

        assert_schema_err(
            rel_path,
            schema,
            LogicalSchemaError::UnsupportedParquetMapEncoding {
                column_path: "attrs".to_string(),
                details: "key_value group must have 1 or 2 children, got 3; observed group(REPEATED, children=[key:REQUIRED, v1:OPTIONAL, v2:OPTIONAL])"
                    .to_string(),
            },
        )
    }

    #[test]
    fn logical_schema_map_rejects_nullable_key() -> TestResult {
        let rel_path = Path::new("data/map-nullable-key.parquet");
        let key = Type::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::OPTIONAL)
            .build()?;
        let kv = Type::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(key)])
            .build()?;
        let map_group = Type::group_type_builder("attrs")
            .with_repetition(Repetition::OPTIONAL)
            .with_logical_type(Some(LogicalType::Map))
            .with_fields(vec![Arc::new(kv)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(map_group)])
                .build()?,
        );

        assert_schema_err(
            rel_path,
            schema,
            LogicalSchemaError::InvalidMapKeyNullability {
                column_path: "attrs".to_string(),
            },
        )
    }

    #[test]
    fn logical_schema_struct_rejects_empty_group() -> TestResult {
        let rel_path = Path::new("data/struct-empty.parquet");
        let empty_group = Type::group_type_builder("s")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(empty_group)])
                .build()?,
        );

        assert_schema_err(
            rel_path,
            schema,
            LogicalSchemaError::EmptyStruct {
                column_path: "s".to_string(),
            },
        )
    }

    #[test]
    fn logical_schema_struct_rejects_duplicate_fields() -> TestResult {
        let rel_path = Path::new("data/struct-duplicate.parquet");
        let a1 = Type::primitive_type_builder("a", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let a2 = Type::primitive_type_builder("a", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        let group = Type::group_type_builder("s")
            .with_repetition(Repetition::OPTIONAL)
            .with_fields(vec![Arc::new(a1), Arc::new(a2)])
            .build()?;
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(group)])
                .build()?,
        );

        assert_schema_err(
            rel_path,
            schema,
            LogicalSchemaError::DuplicatedFieldName {
                column_path: "s".to_string(),
                field: "a".to_string(),
            },
        )
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
