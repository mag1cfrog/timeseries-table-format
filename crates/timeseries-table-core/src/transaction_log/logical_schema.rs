//! Logical schema definitions and validation for table metadata.
//!
//! This module models logical fields and data types stored in the transaction
//! log, along with validation and conversion to Arrow schemas.
use std::{collections::HashSet, fmt, sync::Arc};

use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};

use serde::{Deserialize, Serialize};
use snafu::prelude::*;

/// Units for logical timestamps recorded in the table metadata.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogicalTimestampUnit {
    /// Millisecond precision timestamps.
    Millis,
    /// Microsecond precision timestamps.
    Micros,
    /// Nanosecond precision timestamps.
    Nanos,
}

impl LogicalTimestampUnit {
    fn to_arrow_time_unit(self) -> TimeUnit {
        match self {
            LogicalTimestampUnit::Millis => TimeUnit::Millisecond,
            LogicalTimestampUnit::Micros => TimeUnit::Microsecond,
            LogicalTimestampUnit::Nanos => TimeUnit::Nanosecond,
        }
    }
}

impl fmt::Display for LogicalTimestampUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalTimestampUnit::Millis => write!(f, "ms"),
            LogicalTimestampUnit::Micros => write!(f, "us"),
            LogicalTimestampUnit::Nanos => write!(f, "ns"),
        }
    }
}

/// Logical column definition in a schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalField {
    /// Column name as stored in the schema.
    pub name: String,
    /// Logical data type for the column.
    pub data_type: LogicalDataType,
    /// Whether the column allows null values.
    pub nullable: bool,
}

impl LogicalField {
    fn to_arrow_field_ref(&self, path: &str) -> Result<FieldRef, SchemaConvertError> {
        let dt = self.data_type.to_arrow_datatype(path)?;
        Ok(Arc::new(Field::new(self.name.clone(), dt, self.nullable)))
    }
}

impl fmt::Display for LogicalField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.nullable {
            write!(f, "{}?: {}", self.name, self.data_type)
        } else {
            write!(f, "{}: {}", self.name, self.data_type)
        }
    }
}

fn join_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else {
        format!("{parent}.{child}")
    }
}

/// Logical data types that can be stored in the table schema metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogicalDataType {
    /// Boolean value.
    Bool,
    /// 32-bit signed integer.
    Int32,
    /// 64-bit signed integer.
    Int64,
    /// 32-bit floating point.
    Float32,
    /// 64-bit floating point.
    Float64,
    /// Variable-length binary data.
    Binary,
    /// Fixed-length binary data.
    FixedBinary {
        /// Fixed byte width for each value (in bytes).
        byte_width: i32,
    },
    /// UTF-8 encoded string.
    Utf8,
    /// Legacy 96-bit integer (primarily for Parquet compatibility).
    Int96,

    /// Timestamp value with a precision unit and optional timezone.
    Timestamp {
        /// Timestamp precision unit (millis, micros, nanos).
        unit: LogicalTimestampUnit,
        /// Optional IANA timezone identifier.
        timezone: Option<String>, // keep Option for future TZ support
    },

    /// Fixed-precision decimal value with declared precision and scale.
    Decimal {
        /// Total number of decimal digits (both sides of the decimal point).
        precision: i32,
        /// Number of digits to the right of the decimal point.
        scale: i32,
    },

    /// Struct with named child fields.
    Struct {
        /// Ordered set of child fields for the struct.
        fields: Vec<LogicalField>,
    },

    /// List (array) with a single element field definition.
    List {
        /// Element field definition for list items.
        elements: Box<LogicalField>,
    },

    /// Map with key/value field definitions.
    /// If `value` is None, this represents Parquet MAP "keys-only" semantics (set of keys).
    Map {
        /// Key field definition (must be non-nullable for Arrow compatibility).
        key: Box<LogicalField>,
        /// Value field definition.
        value: Option<Box<LogicalField>>,
        /// Whether entries are sorted by key.
        keys_sorted: bool,
    },

    /// Catch-all logical data type referenced by name.
    Other(String),
}

impl LogicalDataType {
    fn to_arrow_datatype(&self, column: &str) -> Result<DataType, SchemaConvertError> {
        Ok(match self {
            LogicalDataType::Bool => DataType::Boolean,
            LogicalDataType::Int32 => DataType::Int32,
            LogicalDataType::Int64 => DataType::Int64,
            LogicalDataType::Float32 => DataType::Float32,
            LogicalDataType::Float64 => DataType::Float64,
            LogicalDataType::Binary => DataType::Binary,
            LogicalDataType::Utf8 => DataType::Utf8,

            LogicalDataType::FixedBinary { byte_width } => {
                if *byte_width <= 0 {
                    return Err(SchemaConvertError::FixedBinaryInvalidWidth {
                        column: column.to_string(),
                        byte_width: *byte_width,
                    });
                }
                DataType::FixedSizeBinary(*byte_width)
            }

            LogicalDataType::Timestamp { unit, timezone } => {
                let tz: Option<Arc<str>> = timezone.as_ref().map(|s| Arc::<str>::from(s.as_str()));
                DataType::Timestamp(unit.to_arrow_time_unit(), tz)
            }

            LogicalDataType::Int96 => {
                return Err(SchemaConvertError::Int96Unsupported {
                    column: column.to_string(),
                });
            }

            LogicalDataType::Decimal { precision, scale } => {
                let precision = *precision;
                let scale = *scale;
                if precision <= 0 {
                    return Err(SchemaConvertError::DecimalInvalid {
                        column: column.to_string(),
                        precision,
                        scale,
                        details: "precision must be > 0".to_string(),
                    });
                }
                if scale < 0 {
                    return Err(SchemaConvertError::DecimalInvalid {
                        column: column.to_string(),
                        precision,
                        scale,
                        details: "scale must be >= 0".to_string(),
                    });
                }
                if scale > precision {
                    return Err(SchemaConvertError::DecimalInvalid {
                        column: column.to_string(),
                        precision,
                        scale,
                        details: "scale must be <= precision".to_string(),
                    });
                }

                if precision <= 38 {
                    DataType::Decimal128(precision as u8, scale as i8)
                } else if precision <= 76 {
                    DataType::Decimal256(precision as u8, scale as i8)
                } else {
                    return Err(SchemaConvertError::DecimalInvalid {
                        column: column.to_string(),
                        precision,
                        scale,
                        details: "precision exceeds Arrow maximum (76 digits)".to_string(),
                    });
                }
            }

            LogicalDataType::Struct { fields } => {
                let mut arrow_children: Vec<FieldRef> = Vec::with_capacity(fields.len());
                for f in fields {
                    let child_path = join_path(column, &f.name);
                    arrow_children.push(f.to_arrow_field_ref(&child_path)?);
                }
                DataType::Struct(Fields::from(arrow_children))
            }

            LogicalDataType::List { elements } => {
                let child_path = join_path(column, &elements.name);
                let element_field = elements.to_arrow_field_ref(&child_path)?;
                DataType::List(element_field)
            }

            LogicalDataType::Map {
                key,
                value,
                keys_sorted,
            } => {
                if key.nullable {
                    return Err(SchemaConvertError::MapKeyMustBeNonNull {
                        column: column.to_string(),
                    });
                }

                // Canonical Arrow Map field names are "entries", "key", "value"
                let key_path = format!("{column}.key");
                let val_path = format!("{column}.value");

                let key_dt = key.data_type.to_arrow_datatype(&key_path)?;

                let (val_dt, val_nullable) = match value.as_deref() {
                    Some(v) => (v.data_type.to_arrow_datatype(&val_path)?, v.nullable),
                    None => (DataType::Null, true),
                };

                let key_field: FieldRef = Arc::new(Field::new("key", key_dt, false));
                let val_field: FieldRef = Arc::new(Field::new("value", val_dt, val_nullable));

                let entries_dt = DataType::Struct(Fields::from(vec![key_field, val_field]));
                let entries_field: FieldRef = Arc::new(Field::new("entries", entries_dt, false));

                DataType::Map(entries_field, *keys_sorted)
            }

            LogicalDataType::Other(name) => {
                return Err(SchemaConvertError::OtherTypeUnsupported {
                    column: column.to_string(),
                    name: name.clone(),
                });
            }
        })
    }
}

impl fmt::Display for LogicalDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalDataType::Bool => write!(f, "bool"),
            LogicalDataType::Int32 => write!(f, "int32"),
            LogicalDataType::Int64 => write!(f, "int64"),
            LogicalDataType::Float32 => write!(f, "float32"),
            LogicalDataType::Float64 => write!(f, "float64"),
            LogicalDataType::Binary => write!(f, "binary"),
            LogicalDataType::FixedBinary { byte_width } => write!(f, "fixed_binary[{byte_width}]"),
            LogicalDataType::Utf8 => write!(f, "utf8"),
            LogicalDataType::Int96 => write!(f, "int96"),

            LogicalDataType::Timestamp { unit, timezone } => match timezone {
                Some(tz) => write!(f, "timestamp[{}]({})", unit, tz),
                None => write!(f, "timestamp[{}]", unit),
            },

            LogicalDataType::Decimal { precision, scale } => {
                write!(f, "decimal(precision={precision}, scale={scale})")
            }

            LogicalDataType::Struct { fields } => {
                write!(f, "Struct{{")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field)?;
                }
                write!(f, "}}")
            }

            LogicalDataType::List { elements } => {
                write!(f, "List<{}>", elements)
            }

            LogicalDataType::Map {
                key,
                value,
                keys_sorted,
            } => match value.as_deref() {
                Some(v) => write!(f, "Map<{}, {}, keys_sorted={}>", key, v, keys_sorted),
                None => write!(
                    f,
                    "Map<{}, value=omitted, keys_sorted={}>",
                    key, keys_sorted
                ),
            },

            LogicalDataType::Other(s) => write!(f, "{s}"),
        }
    }
}

/// Logical schema metadata describing the ordered collection of logical columns.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalSchema {
    /// All logical columns that compose the schema in their defined order.
    columns: Vec<LogicalField>,
}

impl LogicalSchema {
    /// Convert this logical schema to an owned Arrow [`Schema`].
    ///
    /// Fails if any column uses a logical type that cannot be represented in
    /// Arrow (see [`SchemaConvertError`]).
    pub fn to_arrow_schema(&self) -> Result<Schema, SchemaConvertError> {
        let mut fields = Vec::with_capacity(self.columns.len());
        for c in &self.columns {
            let fref = c.to_arrow_field_ref(&c.name)?;
            fields.push(fref.as_ref().clone());
        }

        Ok(Schema::new(fields))
    }

    /// Convert this logical schema to a shared Arrow [`SchemaRef`].
    ///
    /// This is a convenience wrapper around [`Self::to_arrow_schema`].
    pub fn to_arrow_schema_ref(&self) -> Result<SchemaRef, SchemaConvertError> {
        Ok(Arc::new(self.to_arrow_schema()?))
    }
}

/// Errors that can occur while constructing or validating a logical schema.
#[derive(Debug, Clone, Snafu, PartialEq, Eq)]
pub enum LogicalSchemaError {
    /// Duplicate column names are not allowed.
    #[snafu(display("Duplicate column name: {column}"))]
    DuplicateColumn {
        /// The duplicate column name.
        column: String,
    },

    /// FixedBinary columns must include a positive byte width.
    #[snafu(display(
        "invalid FixedBinary byte_width for column '{column}': {byte_width} (must be > 0)"
    ))]
    FixedBinaryInvalidWidthInSchema {
        /// Column name that failed validation.
        column: String,
        /// Declared byte width.
        byte_width: i32,
    },

    /// Parquet FIXED_LEN_BYTE_ARRAY columns must include a type_length.
    #[snafu(display(
        "FIXED_LEN_BYTE_ARRAY column '{column}' missing type_length in Parquet schema"
    ))]
    FixedBinaryMissingLength {
        /// Column name that failed validation.
        column: String,
    },

    /// Duplicate field names within a struct are not allowed.
    #[snafu(display("Duplicate field name: column={column_path}, field={field}"))]
    DuplicatedFieldName {
        /// Column path for the struct that contains the duplicate field.
        column_path: String,
        /// Duplicate field name.
        field: String,
    },

    /// Map key fields must be non-nullable in schema validation.
    #[snafu(display("Invalid Map Key: map key should not be null for column={column_path}"))]
    InvalidMapKeyNullability {
        /// Column path for the map with an invalid key nullability.
        column_path: String,
    },

    /// Struct fields must be non-empty.
    #[snafu(display("Struct must have at least one field: column={column_path}"))]
    EmptyStruct {
        /// Column path for the empty struct.
        column_path: String,
    },

    /// List element fields must have a non-empty name.
    #[snafu(display("List element field name must be non-empty: column={column_path}"))]
    ListElementNameEmpty {
        /// Column path for the list with an empty element name.
        column_path: String,
    },

    /// Struct fields must have a non-empty name.
    #[snafu(display("Struct field name must be non-empty: column={column_path}, field={field}"))]
    StructFieldNameEmpty {
        /// Column path for the struct with an empty field name.
        column_path: String,
        /// Empty field name.
        field: String,
    },

    /// Parquet LIST encoding does not match the supported layout.
    #[snafu(display("Unsupported Parquet LIST encoding: column={column_path}, details={details}"))]
    UnsupportedParquetListEncoding {
        /// Column path for the list with unsupported encoding.
        column_path: String,
        /// Details describing why the LIST encoding is unsupported.
        details: String,
    },

    /// Parquet MAP encoding does not match the supported layout.
    #[snafu(display("Unsupported Parquet MAP encoding: column={column_path}, details={details}"))]
    UnsupportedParquetMapEncoding {
        /// Column path for the map with unsupported encoding.
        column_path: String,
        /// Details describing why the MAP encoding is unsupported.
        details: String,
    },
}

impl LogicalSchema {
    /// Construct a validated logical schema (rejects duplicate column names).
    pub fn new(columns: Vec<LogicalField>) -> Result<Self, LogicalSchemaError> {
        let mut seen = HashSet::new();
        for col in &columns {
            if !seen.insert(col.name.clone()) {
                return DuplicateColumnSnafu {
                    column: col.name.clone(),
                }
                .fail();
            }
            validate_field(col, &col.name)?;
        }

        Ok(Self { columns })
    }

    /// Borrow the logical columns.
    pub fn columns(&self) -> &[LogicalField] {
        &self.columns
    }
}

fn validate_field(field: &LogicalField, path: &str) -> Result<(), LogicalSchemaError> {
    validate_dtype(&field.data_type, path)
}

fn validate_dtype(dt: &LogicalDataType, path: &str) -> Result<(), LogicalSchemaError> {
    match dt {
        LogicalDataType::FixedBinary { byte_width } => {
            if *byte_width <= 0 {
                return Err(LogicalSchemaError::FixedBinaryInvalidWidthInSchema {
                    column: path.to_string(),
                    byte_width: *byte_width,
                });
            }
            Ok(())
        }

        LogicalDataType::Struct { fields } => {
            if fields.is_empty() {
                return Err(LogicalSchemaError::EmptyStruct {
                    column_path: path.to_string(),
                });
            }

            let mut seen = HashSet::with_capacity(fields.len());
            for child in fields {
                if child.name.trim().is_empty() {
                    return Err(LogicalSchemaError::StructFieldNameEmpty {
                        column_path: path.to_string(),
                        field: child.name.clone(),
                    });
                }

                if !seen.insert(child.name.clone()) {
                    return Err(LogicalSchemaError::DuplicatedFieldName {
                        column_path: path.to_string(),
                        field: child.name.clone(),
                    });
                }
                let child_path = format!("{}.{}", path, child.name);
                validate_field(child, &child_path)?;
            }
            Ok(())
        }

        LogicalDataType::List { elements } => {
            if elements.name.trim().is_empty() {
                return Err(LogicalSchemaError::ListElementNameEmpty {
                    column_path: path.to_string(),
                });
            }
            let child_path = format!("{}.{}", path, elements.name);
            validate_field(elements, &child_path)
        }

        LogicalDataType::Map { key, value, .. } => {
            if key.nullable {
                return Err(LogicalSchemaError::InvalidMapKeyNullability {
                    column_path: path.to_string(),
                });
            }
            validate_field(key, &format!("{}.key", path))?;
            if let Some(v) = value.as_deref() {
                validate_field(v, &format!("{}.value", path))?;
            }

            Ok(())
        }

        _ => Ok(()),
    }
}

/// Errors encountered while converting between logical schema representations.
#[derive(Debug, Snafu)]
pub enum SchemaConvertError {
    /// The logical type is not supported by the target representation.
    #[snafu(display("unsupported logical type for column '{column}': {type_name} ({details})"))]
    UnsupportedLogicalType {
        /// Column name that failed conversion.
        column: String,
        /// High-level type name (for diagnostics).
        type_name: String,
        /// Additional details describing why it is unsupported.
        details: String,
    },

    /// FixedBinary fields must declare a positive byte width.
    #[snafu(display(
        "invalid FixedBinary byte_width for column '{column}': {byte_width} (must be > 0)"
    ))]
    FixedBinaryInvalidWidth {
        /// Column name that failed validation.
        column: String,
        /// Declared byte width.
        byte_width: i32,
    },

    /// Int96 is rejected for now to avoid legacy timestamp ambiguity.
    #[snafu(display("Int96 is not supported in v0.1 for column '{column}'"))]
    Int96Unsupported {
        /// Column name that failed conversion.
        column: String,
    },

    /// Catch-all "Other" types are not accepted in v0.1.
    #[snafu(display("Other type '{name}' is not supported in v0.1 for column '{column}'"))]
    OtherTypeUnsupported {
        /// Column name that failed conversion.
        column: String,
        /// Type name reported by the source.
        name: String,
    },

    /// Decimal precision/scale is out of supported bounds for Arrow conversion.
    #[snafu(display(
        "invalid decimal definition for column '{column}': precision={precision}, scale={scale} ({details})"
    ))]
    DecimalInvalid {
        /// Column name that failed conversion.
        column: String,
        /// Declared total precision.
        precision: i32,
        /// Declared scale (digits to the right of the decimal point).
        scale: i32,
        /// Human-readable details describing the constraint violation.
        details: String,
    },

    /// Map keys must be non-nullable when converting to Arrow.
    #[snafu(display("map key must be non-nullable for column '{column}'"))]
    MapKeyMustBeNonNull {
        /// Column name that failed conversion.
        column: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_logical_schema_all_supported() -> LogicalSchema {
        LogicalSchema::new(vec![
            LogicalField {
                name: "flag".to_string(),
                data_type: LogicalDataType::Bool,
                nullable: false,
            },
            LogicalField {
                name: "i32".to_string(),
                data_type: LogicalDataType::Int32,
                nullable: false,
            },
            LogicalField {
                name: "i64".to_string(),
                data_type: LogicalDataType::Int64,
                nullable: true,
            },
            LogicalField {
                name: "f32".to_string(),
                data_type: LogicalDataType::Float32,
                nullable: false,
            },
            LogicalField {
                name: "f64".to_string(),
                data_type: LogicalDataType::Float64,
                nullable: true,
            },
            LogicalField {
                name: "text".to_string(),
                data_type: LogicalDataType::Utf8,
                nullable: true,
            },
            LogicalField {
                name: "bytes".to_string(),
                data_type: LogicalDataType::Binary,
                nullable: true,
            },
            LogicalField {
                name: "fixed".to_string(),
                data_type: LogicalDataType::FixedBinary { byte_width: 16 },
                nullable: false,
            },
            LogicalField {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Micros,
                    timezone: Some("UTC".to_string()),
                },
                nullable: false,
            },
        ])
        .expect("valid logical schema")
    }

    #[test]
    fn logical_schema_to_arrow_schema_happy_path() {
        let logical = sample_logical_schema_all_supported();
        let schema = logical.to_arrow_schema().expect("arrow schema conversion");

        let expected = Schema::new(vec![
            Field::new("flag", DataType::Boolean, false),
            Field::new("i32", DataType::Int32, false),
            Field::new("i64", DataType::Int64, true),
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("bytes", DataType::Binary, true),
            Field::new("fixed", DataType::FixedSizeBinary(16), false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC"))),
                false,
            ),
        ]);

        assert_eq!(schema, expected);
    }

    #[test]
    fn logical_schema_rejects_fixed_binary_invalid_width() {
        for width in [0, -1] {
            let logical = LogicalSchema::new(vec![LogicalField {
                name: "bad_fixed".to_string(),
                data_type: LogicalDataType::FixedBinary { byte_width: width },
                nullable: false,
            }])
            .expect("valid schema structure");

            let err = logical.to_arrow_schema().unwrap_err();
            assert!(
                matches!(
                    &err,
                    SchemaConvertError::FixedBinaryInvalidWidth {
                        column,
                        byte_width
                    } if column == "bad_fixed" && *byte_width == width
                ),
                "unexpected error: {err:?}"
            );
        }
    }

    #[test]
    fn logical_schema_rejects_int96() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "legacy_ts".to_string(),
            data_type: LogicalDataType::Int96,
            nullable: false,
        }])
        .expect("valid schema structure");

        let err = logical.to_arrow_schema().unwrap_err();
        assert!(
            matches!(
                &err,
                SchemaConvertError::Int96Unsupported { column } if column == "legacy_ts"
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn logical_schema_rejects_other_type() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "opaque".to_string(),
            data_type: LogicalDataType::Other("parquet::Map".to_string()),
            nullable: true,
        }])
        .expect("valid schema structure");

        let err = logical.to_arrow_schema().unwrap_err();
        assert!(
            matches!(
                &err,
                SchemaConvertError::OtherTypeUnsupported { column, name }
                    if column == "opaque" && name == "parquet::Map"
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn logical_schema_timestamp_without_timezone() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "ts".to_string(),
            data_type: LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Millis,
                timezone: None,
            },
            nullable: false,
        }])
        .expect("valid schema structure");

        let schema = logical.to_arrow_schema().expect("arrow schema conversion");
        let expected = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]);
        assert_eq!(schema, expected);
    }

    #[test]
    fn logical_schema_decimal_conversion_bounds() {
        let valid_128 = LogicalSchema::new(vec![LogicalField {
            name: "dec128".to_string(),
            data_type: LogicalDataType::Decimal {
                precision: 38,
                scale: 10,
            },
            nullable: false,
        }])
        .expect("valid schema structure");
        let schema = valid_128
            .to_arrow_schema()
            .expect("arrow schema conversion");
        assert_eq!(
            schema,
            Schema::new(vec![Field::new(
                "dec128",
                DataType::Decimal128(38, 10),
                false
            )])
        );

        let valid_256 = LogicalSchema::new(vec![LogicalField {
            name: "dec256".to_string(),
            data_type: LogicalDataType::Decimal {
                precision: 76,
                scale: 5,
            },
            nullable: false,
        }])
        .expect("valid schema structure");
        let schema = valid_256
            .to_arrow_schema()
            .expect("arrow schema conversion");
        assert_eq!(
            schema,
            Schema::new(vec![Field::new(
                "dec256",
                DataType::Decimal256(76, 5),
                false
            )])
        );

        let invalid = LogicalSchema::new(vec![LogicalField {
            name: "dec_too_large".to_string(),
            data_type: LogicalDataType::Decimal {
                precision: 77,
                scale: 0,
            },
            nullable: false,
        }])
        .expect("valid schema structure");
        let err = invalid.to_arrow_schema().unwrap_err();
        assert!(
            matches!(
                &err,
                SchemaConvertError::DecimalInvalid { column, precision, scale, .. }
                    if column == "dec_too_large" && *precision == 77 && *scale == 0
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn logical_schema_decimal_validation_errors() {
        let cases = vec![
            ("dec_precision_zero", 0, 0, "precision must be > 0"),
            ("dec_scale_negative", 10, -1, "scale must be >= 0"),
            ("dec_scale_gt_precision", 4, 5, "scale must be <= precision"),
        ];

        for (name, precision, scale, details_substr) in cases {
            let logical = LogicalSchema::new(vec![LogicalField {
                name: name.to_string(),
                data_type: LogicalDataType::Decimal { precision, scale },
                nullable: false,
            }])
            .expect("valid schema structure");

            let err = logical.to_arrow_schema().unwrap_err();
            assert!(
                matches!(
                    &err,
                    SchemaConvertError::DecimalInvalid { column, precision: p, scale: s, details }
                        if column == name && *p == precision && *s == scale && details.contains(details_substr)
                ),
                "unexpected error: {err:?}"
            );
        }
    }

    #[test]
    fn logical_schema_fixed_binary_json_roundtrip() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "fixed".to_string(),
            data_type: LogicalDataType::FixedBinary { byte_width: 8 },
            nullable: false,
        }])
        .expect("valid schema structure");

        let json = serde_json::to_string(&logical).unwrap();
        let back: LogicalSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(back, logical);
    }

    #[test]
    fn logical_schema_decimal_json_roundtrip() {
        let logical = LogicalSchema::new(vec![LogicalField {
            name: "amount".to_string(),
            data_type: LogicalDataType::Decimal {
                precision: 18,
                scale: 4,
            },
            nullable: true,
        }])
        .expect("valid schema structure");

        let json = serde_json::to_string(&logical).unwrap();
        let back: LogicalSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(back, logical);
    }
}
