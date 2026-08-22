//! Per-append normalization into the registered Arrow schema.

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::RecordBatch,
    compute::cast,
    datatypes::{DataType, Schema, SchemaRef},
    error::ArrowError,
};

use crate::metadata::{
    logical_schema::LogicalSchema,
    schema_compat::{SchemaCompatibilityError, SchemaResult},
};

/// Validated column mapping from one append source into its output schema.
pub(super) struct AppendSchemaNormalizer {
    output_schema: SchemaRef,
    incoming_column_indices: Vec<usize>,
}

impl AppendSchemaNormalizer {
    /// Preserve an incoming schema when the table has no registered schema yet.
    pub(super) fn without_conversion(incoming_schema: SchemaRef) -> Self {
        Self {
            incoming_column_indices: (0..incoming_schema.fields().len()).collect(),
            output_schema: incoming_schema,
        }
    }

    /// Validate and map an incoming schema into the registered table schema.
    pub(super) fn for_registered_schema(
        incoming_schema: &Schema,
        registered_schema: &LogicalSchema,
    ) -> SchemaResult<Self> {
        let output_schema = registered_schema
            .to_arrow_schema_ref()
            .map_err(|source| SchemaCompatibilityError::RegisteredSchemaConversion { source })?;
        let mut incoming_by_name = HashMap::with_capacity(incoming_schema.fields().len());

        for (index, field) in incoming_schema.fields().iter().enumerate() {
            if incoming_by_name
                .insert(field.name().as_str(), index)
                .is_some()
            {
                return Err(SchemaCompatibilityError::DuplicateIncomingColumn {
                    column: field.name().clone(),
                });
            }
        }

        let mut incoming_column_indices = Vec::with_capacity(output_schema.fields().len());
        for table_field in output_schema.fields() {
            let incoming_index = incoming_by_name
                .get(table_field.name().as_str())
                .ok_or_else(|| SchemaCompatibilityError::MissingIncomingColumn {
                    column: table_field.name().clone(),
                })?;
            let incoming_field = &incoming_schema.fields()[*incoming_index];

            if table_field.is_nullable() != incoming_field.is_nullable() {
                return Err(SchemaCompatibilityError::IncomingNullabilityMismatch {
                    column: table_field.name().clone(),
                    table_nullable: table_field.is_nullable(),
                    incoming_nullable: incoming_field.is_nullable(),
                });
            }

            if table_field.data_type() != incoming_field.data_type()
                && !is_allowlisted_widening(incoming_field.data_type(), table_field.data_type())
            {
                return Err(SchemaCompatibilityError::IncomingTypeMismatch {
                    column: table_field.name().clone(),
                    table_type: table_field.data_type().clone(),
                    incoming_type: incoming_field.data_type().clone(),
                });
            }

            incoming_column_indices.push(*incoming_index);
        }

        if let Some(field) = incoming_schema
            .fields()
            .iter()
            .find(|field| output_schema.index_of(field.name()).is_err())
        {
            return Err(SchemaCompatibilityError::ExtraIncomingColumn {
                column: field.name().clone(),
            });
        }

        Ok(Self {
            output_schema,
            incoming_column_indices,
        })
    }

    pub(super) fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Reorder and widen one batch without retaining another input batch.
    pub(super) fn normalize_batch(
        &self,
        incoming_batch: &RecordBatch,
    ) -> Result<RecordBatch, ArrowError> {
        let columns = self
            .incoming_column_indices
            .iter()
            .zip(self.output_schema.fields())
            .map(|(incoming_index, output_field)| {
                let incoming = incoming_batch.column(*incoming_index);
                if incoming.data_type() == output_field.data_type() {
                    Ok(Arc::clone(incoming))
                } else {
                    cast(incoming, output_field.data_type())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
    }
}

fn is_allowlisted_widening(incoming: &DataType, table: &DataType) -> bool {
    matches!(
        (incoming, table),
        (DataType::Int8, DataType::Int32 | DataType::Int64)
            | (DataType::Int16, DataType::Int32 | DataType::Int64)
            | (DataType::Int32, DataType::Int64)
            | (
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32,
                DataType::UInt64
            )
            | (DataType::Float32, DataType::Float64)
    )
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{
            Array, ArrayRef, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
            Int64Array, StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::{Field, Fields},
    };

    use super::*;
    use crate::metadata::logical_schema::{LogicalDataType, LogicalField};

    struct WideningCase {
        incoming_type: DataType,
        table_type: LogicalDataType,
        incoming: ArrayRef,
        expected: ArrayRef,
    }

    fn field(name: &str, data_type: LogicalDataType, nullable: bool) -> LogicalField {
        LogicalField {
            name: name.to_string(),
            data_type,
            nullable,
        }
    }

    fn widening_cases() -> Vec<WideningCase> {
        vec![
            WideningCase {
                incoming_type: DataType::Int8,
                table_type: LogicalDataType::Int32,
                incoming: Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])),
                expected: Arc::new(Int32Array::from(vec![
                    Some(i32::from(i8::MIN)),
                    None,
                    Some(i32::from(i8::MAX)),
                ])),
            },
            WideningCase {
                incoming_type: DataType::Int8,
                table_type: LogicalDataType::Int64,
                incoming: Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])),
                expected: Arc::new(Int64Array::from(vec![
                    Some(i64::from(i8::MIN)),
                    None,
                    Some(i64::from(i8::MAX)),
                ])),
            },
            WideningCase {
                incoming_type: DataType::Int16,
                table_type: LogicalDataType::Int32,
                incoming: Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])),
                expected: Arc::new(Int32Array::from(vec![
                    Some(i32::from(i16::MIN)),
                    None,
                    Some(i32::from(i16::MAX)),
                ])),
            },
            WideningCase {
                incoming_type: DataType::Int16,
                table_type: LogicalDataType::Int64,
                incoming: Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])),
                expected: Arc::new(Int64Array::from(vec![
                    Some(i64::from(i16::MIN)),
                    None,
                    Some(i64::from(i16::MAX)),
                ])),
            },
            WideningCase {
                incoming_type: DataType::Int32,
                table_type: LogicalDataType::Int64,
                incoming: Arc::new(Int32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)])),
                expected: Arc::new(Int64Array::from(vec![
                    Some(i64::from(i32::MIN)),
                    None,
                    Some(i64::from(i32::MAX)),
                ])),
            },
            WideningCase {
                incoming_type: DataType::UInt8,
                table_type: LogicalDataType::UInt64,
                incoming: Arc::new(UInt8Array::from(vec![Some(u8::MIN), None, Some(u8::MAX)])),
                expected: Arc::new(UInt64Array::from(vec![
                    Some(u64::from(u8::MIN)),
                    None,
                    Some(u64::from(u8::MAX)),
                ])),
            },
            WideningCase {
                incoming_type: DataType::UInt16,
                table_type: LogicalDataType::UInt64,
                incoming: Arc::new(UInt16Array::from(vec![
                    Some(u16::MIN),
                    None,
                    Some(u16::MAX),
                ])),
                expected: Arc::new(UInt64Array::from(vec![
                    Some(u64::from(u16::MIN)),
                    None,
                    Some(u64::from(u16::MAX)),
                ])),
            },
            WideningCase {
                incoming_type: DataType::UInt32,
                table_type: LogicalDataType::UInt64,
                incoming: Arc::new(UInt32Array::from(vec![
                    Some(u32::MIN),
                    None,
                    Some(u32::MAX),
                ])),
                expected: Arc::new(UInt64Array::from(vec![
                    Some(u64::from(u32::MIN)),
                    None,
                    Some(u64::from(u32::MAX)),
                ])),
            },
            WideningCase {
                incoming_type: DataType::Float32,
                table_type: LogicalDataType::Float64,
                incoming: Arc::new(Float32Array::from(vec![
                    Some(f32::MIN),
                    None,
                    Some(f32::MAX),
                ])),
                expected: Arc::new(Float64Array::from(vec![
                    Some(f64::from(f32::MIN)),
                    None,
                    Some(f64::from(f32::MAX)),
                ])),
            },
        ]
    }

    #[test]
    fn normalizes_every_allowlisted_pair_with_boundaries_and_nulls() {
        for case in widening_cases() {
            let registered =
                LogicalSchema::new(vec![field("value", case.table_type, true)]).unwrap();
            let incoming_schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                case.incoming_type,
                true,
            )]));
            let batch =
                RecordBatch::try_new(Arc::clone(&incoming_schema), vec![case.incoming]).unwrap();
            let normalizer = AppendSchemaNormalizer::for_registered_schema(
                incoming_schema.as_ref(),
                &registered,
            )
            .unwrap();

            let normalized = normalizer.normalize_batch(&batch).unwrap();

            assert_eq!(
                normalized.schema(),
                registered.to_arrow_schema_ref().unwrap()
            );
            assert_eq!(normalized.column(0).to_data(), case.expected.to_data());
        }
    }

    #[test]
    fn reorders_columns_into_registered_order_and_ignores_metadata() {
        let registered = LogicalSchema::new(vec![
            field("first", LogicalDataType::Int64, false),
            field("second", LogicalDataType::Float64, true),
        ])
        .unwrap();
        let incoming_schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("second", DataType::Float32, true).with_metadata(HashMap::from([(
                    "field".to_string(),
                    "ignored".to_string(),
                )])),
                Field::new("first", DataType::Int32, false),
            ],
            HashMap::from([("schema".to_string(), "ignored".to_string())]),
        ));
        let second: ArrayRef = Arc::new(Float32Array::from(vec![Some(1.5), None]));
        let first: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let batch = RecordBatch::try_new(
            Arc::clone(&incoming_schema),
            vec![Arc::clone(&second), Arc::clone(&first)],
        )
        .unwrap();

        let normalizer =
            AppendSchemaNormalizer::for_registered_schema(incoming_schema.as_ref(), &registered)
                .unwrap();
        let normalized = normalizer.normalize_batch(&batch).unwrap();

        assert_eq!(
            normalized.schema(),
            registered.to_arrow_schema_ref().unwrap()
        );
        assert_eq!(
            normalized.column(0).to_data(),
            Int64Array::from(vec![1, 2]).to_data()
        );
        assert_eq!(
            normalized.column(1).to_data(),
            Float64Array::from(vec![Some(1.5), None]).to_data()
        );
    }

    #[test]
    fn accepts_exact_nested_types_without_recursive_widening() {
        let child = Arc::new(Field::new("child", DataType::Int32, true));
        let nested_type = DataType::Struct(Fields::from(vec![Arc::clone(&child)]));
        let registered = LogicalSchema::new(vec![field(
            "nested",
            LogicalDataType::Struct {
                fields: vec![field("child", LogicalDataType::Int32, true)],
            },
            true,
        )])
        .unwrap();
        let incoming_schema = Arc::new(Schema::new(vec![Field::new("nested", nested_type, true)]));
        let nested: ArrayRef = Arc::new(StructArray::from(vec![(
            child,
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
        )]));
        let batch =
            RecordBatch::try_new(Arc::clone(&incoming_schema), vec![Arc::clone(&nested)]).unwrap();
        let normalizer =
            AppendSchemaNormalizer::for_registered_schema(incoming_schema.as_ref(), &registered)
                .unwrap();

        let normalized = normalizer.normalize_batch(&batch).unwrap();

        assert!(Arc::ptr_eq(normalized.column(0), &nested));
    }

    #[test]
    fn rejects_non_allowlisted_type_pairs() {
        let cases = [
            (DataType::Int64, LogicalDataType::Int32),
            (DataType::Int32, LogicalDataType::UInt64),
            (DataType::UInt32, LogicalDataType::Int64),
            (DataType::Int32, LogicalDataType::Float64),
            (DataType::Float64, LogicalDataType::Float32),
            (
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                LogicalDataType::Timestamp {
                    unit: crate::metadata::logical_schema::LogicalTimestampUnit::Micros,
                    timezone: None,
                },
            ),
            (
                DataType::Decimal128(10, 2),
                LogicalDataType::Decimal {
                    precision: 12,
                    scale: 2,
                },
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                LogicalDataType::Utf8,
            ),
            (
                DataType::List(Arc::new(Field::new("item", DataType::Int8, true))),
                LogicalDataType::List {
                    elements: Box::new(field("item", LogicalDataType::Int32, true)),
                },
            ),
        ];

        for (incoming_type, table_type) in cases {
            let registered = LogicalSchema::new(vec![field("value", table_type, true)]).unwrap();
            let incoming = Schema::new(vec![Field::new("value", incoming_type.clone(), true)]);

            assert!(matches!(
                AppendSchemaNormalizer::for_registered_schema(&incoming, &registered),
                Err(SchemaCompatibilityError::IncomingTypeMismatch {
                    column,
                    incoming_type: actual,
                    ..
                }) if column == "value" && actual == incoming_type
            ));
        }
    }

    #[test]
    fn rejects_missing_extra_duplicate_and_nullability_changes() {
        let registered = LogicalSchema::new(vec![
            field("first", LogicalDataType::Int64, false),
            field("second", LogicalDataType::Int64, false),
        ])
        .unwrap();
        let missing = Schema::new(vec![Field::new("first", DataType::Int64, false)]);
        assert!(matches!(
            AppendSchemaNormalizer::for_registered_schema(&missing, &registered),
            Err(SchemaCompatibilityError::MissingIncomingColumn { column })
                if column == "second"
        ));

        let extra = Schema::new(vec![
            Field::new("first", DataType::Int64, false),
            Field::new("second", DataType::Int64, false),
            Field::new("third", DataType::Int64, false),
        ]);
        assert!(matches!(
            AppendSchemaNormalizer::for_registered_schema(&extra, &registered),
            Err(SchemaCompatibilityError::ExtraIncomingColumn { column })
                if column == "third"
        ));

        let duplicate = Schema::new(vec![
            Field::new("first", DataType::Int64, false),
            Field::new("first", DataType::Int64, false),
        ]);
        assert!(matches!(
            AppendSchemaNormalizer::for_registered_schema(&duplicate, &registered),
            Err(SchemaCompatibilityError::DuplicateIncomingColumn { column })
                if column == "first"
        ));

        let nullable = Schema::new(vec![
            Field::new("first", DataType::Int64, true),
            Field::new("second", DataType::Int64, false),
        ]);
        assert!(matches!(
            AppendSchemaNormalizer::for_registered_schema(&nullable, &registered),
            Err(SchemaCompatibilityError::IncomingNullabilityMismatch {
                column,
                table_nullable: false,
                incoming_nullable: true,
            }) if column == "first"
        ));
    }
}
