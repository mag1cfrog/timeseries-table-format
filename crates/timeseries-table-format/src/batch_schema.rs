//! Per-source alignment into a snapshot's canonical Arrow schema.
//!
//! Appends alone may use the established scalar widening allowlist. Historical
//! files must have exact physical types. Nullable omissions are feature-gated;
//! neither path may synthesize ordered-index or entity columns.

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{RecordBatch, new_null_array},
    compute::cast,
    datatypes::{DataType, Schema, SchemaRef},
    error::ArrowError,
};

use crate::metadata::{
    index::IndexSpec,
    logical_schema::LogicalSchema,
    schema_compat::{SchemaCompatibilityError, SchemaResult},
    table::TableMeta,
};

/// Internal policy, selected only after the caller's protocol compatibility gate.
/// This does not advertise support for the reserved feature.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SchemaPolicy {
    Strict,
    AddNullable,
}

impl SchemaPolicy {
    pub(crate) fn for_table(meta: &TableMeta) -> Self {
        if meta
            .required_reader_features()
            .contains("schema_add_columns")
        {
            Self::AddNullable
        } else {
            Self::Strict
        }
    }
}

/// Validated mapping reused across all batches from one declared source schema.
pub(crate) struct BatchSchemaNormalizer {
    output_schema: SchemaRef,
    incoming_column_indices: Vec<Option<usize>>,
}

impl BatchSchemaNormalizer {
    /// Preserve an incoming schema when the table has no registered schema yet.
    pub(crate) fn without_conversion(incoming_schema: SchemaRef) -> Self {
        Self {
            incoming_column_indices: (0..incoming_schema.fields().len()).map(Some).collect(),
            output_schema: incoming_schema,
        }
    }

    /// Validate and map an incoming schema into the registered table schema.
    pub(crate) fn for_append(
        incoming_schema: &Schema,
        registered_schema: &LogicalSchema,
        index: &IndexSpec,
        policy: SchemaPolicy,
    ) -> SchemaResult<Self> {
        Self::build(incoming_schema, registered_schema, index, policy, true)
    }

    /// Historical data is reordered and null-filled, never cast. Called only
    /// for snapshots that select the nullable-column policy.
    pub(crate) fn for_segment(
        incoming_schema: &Schema,
        registered_schema: &LogicalSchema,
        index: &IndexSpec,
    ) -> SchemaResult<Self> {
        Self::build(
            incoming_schema,
            registered_schema,
            index,
            SchemaPolicy::AddNullable,
            false,
        )
    }

    fn build(
        incoming_schema: &Schema,
        registered_schema: &LogicalSchema,
        index: &IndexSpec,
        policy: SchemaPolicy,
        allow_widening: bool,
    ) -> SchemaResult<Self> {
        let output_schema = registered_schema.to_arrow_schema_ref().map_err(|source| {
            SchemaCompatibilityError::RegisteredSchemaConversion {
                source: Box::new(source),
            }
        })?;
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
            let Some(&incoming_index) = incoming_by_name.get(table_field.name().as_str()) else {
                if policy == SchemaPolicy::AddNullable
                    && table_field.is_nullable()
                    && table_field.name() != &index.column
                    && !index.entity_columns.contains(table_field.name())
                {
                    incoming_column_indices.push(None);
                    continue;
                }
                return Err(SchemaCompatibilityError::MissingIncomingColumn {
                    column: table_field.name().clone(),
                });
            };
            let incoming_field = &incoming_schema.fields()[incoming_index];

            if table_field.is_nullable() != incoming_field.is_nullable() {
                return Err(SchemaCompatibilityError::IncomingNullabilityMismatch {
                    column: table_field.name().clone(),
                    table_nullable: table_field.is_nullable(),
                    incoming_nullable: incoming_field.is_nullable(),
                });
            }

            if table_field.data_type() != incoming_field.data_type()
                && !(allow_widening
                    && is_allowlisted_widening(incoming_field.data_type(), table_field.data_type()))
            {
                return Err(SchemaCompatibilityError::IncomingTypeMismatch {
                    column: table_field.name().clone(),
                    table_type: table_field.data_type().clone(),
                    incoming_type: incoming_field.data_type().clone(),
                });
            }

            incoming_column_indices.push(Some(incoming_index));
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

    pub(crate) fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Align one batch without retaining it. Exact arrays are shared; absent
    /// nested fields get parent nulls via Arrow's typed-null constructor.
    /// The caller must validate each batch against the declared source schema.
    pub(crate) fn normalize_batch(
        &self,
        incoming_batch: &RecordBatch,
    ) -> Result<RecordBatch, ArrowError> {
        let columns = self
            .incoming_column_indices
            .iter()
            .zip(self.output_schema.fields())
            .map(|(incoming_index, output_field)| {
                let Some(incoming_index) = incoming_index else {
                    return Ok(new_null_array(
                        output_field.data_type(),
                        incoming_batch.num_rows(),
                    ));
                };
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

    #[test]
    fn evolved_alignment_null_fills_whole_fields_and_shares_exact_arrays() {
        let child = Arc::new(Field::new("item", DataType::Int32, false));
        let missing_types = vec![
            DataType::Boolean,
            DataType::UInt64,
            DataType::Decimal128(12, 2),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            DataType::Struct(vec![child.clone()].into()),
            DataType::List(child),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
        ];
        let mut fields = vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("entity", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
        ];
        fields.extend(
            missing_types
                .iter()
                .enumerate()
                .map(|(i, t)| Field::new(format!("missing_{i}"), t.clone(), true)),
        );
        let canonical = LogicalSchema::try_from_arrow_schema(&Schema::new(fields)).unwrap();
        let incoming = RecordBatch::try_from_iter_with_nullable([
            (
                "value",
                Arc::new(Float64Array::from(vec![Some(2.5), None])) as ArrayRef,
                true,
            ),
            (
                "entity",
                Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
                true,
            ),
            (
                "ts",
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                true,
            ),
        ])
        .unwrap();
        for historical in [false, true] {
            let normalizer = if historical {
                BatchSchemaNormalizer::for_segment(&incoming.schema(), &canonical, &test_index())
            } else {
                BatchSchemaNormalizer::for_append(
                    &incoming.schema(),
                    &canonical,
                    &test_index(),
                    SchemaPolicy::AddNullable,
                )
            }
            .unwrap();
            for batch in [&incoming, &incoming.slice(0, 1)] {
                let output = normalizer.normalize_batch(batch).unwrap();
                assert_eq!(output.schema(), canonical.to_arrow_schema_ref().unwrap());
                for (out, src) in [(0, 2), (1, 1), (2, 0)] {
                    assert!(Arc::ptr_eq(output.column(out), batch.column(src)));
                }
                for (column, data_type) in output.columns()[3..].iter().zip(&missing_types) {
                    assert_eq!(column.data_type(), data_type);
                    assert_eq!(column.null_count(), batch.num_rows());
                    assert!((0..batch.num_rows()).all(|row| column.is_null(row)));
                }
            }
        }
        assert!(matches!(
            strict_normalizer(&incoming.schema(), &canonical),
            Err(SchemaCompatibilityError::MissingIncomingColumn { .. })
        ));
    }

    #[test]
    fn evolved_alignment_rejects_key_omission_and_incompatible_supplied_fields() {
        let fields = vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("entity", DataType::Int64, true),
            Field::new("required", DataType::Int64, false),
            Field::new("optional", DataType::Int64, true),
        ];
        let canonical = LogicalSchema::try_from_arrow_schema(&Schema::new(fields.clone())).unwrap();
        for missing in 0..3 {
            let incoming = Schema::new(
                fields
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != missing)
                    .map(|(_, f)| f.clone())
                    .collect::<Vec<_>>(),
            );
            for historical in [false, true] {
                let result = if historical {
                    BatchSchemaNormalizer::for_segment(&incoming, &canonical, &test_index())
                } else {
                    BatchSchemaNormalizer::for_append(
                        &incoming,
                        &canonical,
                        &test_index(),
                        SchemaPolicy::AddNullable,
                    )
                };
                assert!(
                    matches!(result, Err(SchemaCompatibilityError::MissingIncomingColumn { column }) if column == *fields[missing].name())
                );
            }
        }
        for replacement in [
            Field::new("optional", DataType::Float64, true),
            Field::new("optional", DataType::Int64, false),
            Field::new("Optional", DataType::Int64, true),
            Field::new(" optional ", DataType::Int64, true),
            Field::new("entity", DataType::Int64, true),
        ] {
            let mut incoming = fields[..3].to_vec();
            incoming.push(replacement);
            let incoming = Schema::new(incoming);
            assert!(
                BatchSchemaNormalizer::for_append(
                    &incoming,
                    &canonical,
                    &test_index(),
                    SchemaPolicy::AddNullable
                )
                .is_err()
            );
            assert!(
                BatchSchemaNormalizer::for_segment(&incoming, &canonical, &test_index()).is_err()
            );
        }
        let mut narrowed = fields[..3].to_vec();
        narrowed.push(Field::new("optional", DataType::Int32, true));
        let incoming = RecordBatch::try_new(
            Arc::new(Schema::new(narrowed)),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![3])),
                Arc::new(Int32Array::from(vec![Some(i32::MAX)])),
            ],
        )
        .unwrap();
        let normalizer = BatchSchemaNormalizer::for_append(
            &incoming.schema(),
            &canonical,
            &test_index(),
            SchemaPolicy::AddNullable,
        )
        .unwrap();
        let output = normalizer.normalize_batch(&incoming).unwrap();
        assert_eq!(
            output
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            i64::from(i32::MAX)
        );
        assert!(matches!(
            BatchSchemaNormalizer::for_segment(&incoming.schema(), &canonical, &test_index()),
            Err(SchemaCompatibilityError::IncomingTypeMismatch { .. })
        ));
    }

    fn test_index() -> IndexSpec {
        IndexSpec {
            column: "ts".into(),
            entity_columns: vec!["entity".into()],
            kind: crate::metadata::index::IndexKind::Int64 {
                index_granularity: std::num::NonZeroU64::new(1).unwrap(),
            },
        }
    }

    #[test]
    fn evolved_alignment_treats_top_level_names_literally() {
        let incoming = RecordBatch::try_from_iter_with_nullable([
            ("a.b", Arc::new(Int64Array::from(vec![3])) as ArrayRef, true),
            (" a ", Arc::new(Int64Array::from(vec![4])) as ArrayRef, true),
            ("A", Arc::new(Int64Array::from(vec![2])) as ArrayRef, true),
            ("a", Arc::new(Int64Array::from(vec![1])) as ArrayRef, true),
            ("ts", Arc::new(Int64Array::from(vec![0])) as ArrayRef, true),
            (
                "entity",
                Arc::new(Int64Array::from(vec![10])) as ArrayRef,
                true,
            ),
        ])
        .unwrap();
        let canonical = LogicalSchema::new(
            ["ts", "entity", "a", "A", "a.b", " a "]
                .into_iter()
                .map(|name| field(name, LogicalDataType::Int64, true))
                .collect(),
        )
        .unwrap();
        let output =
            BatchSchemaNormalizer::for_segment(&incoming.schema(), &canonical, &test_index())
                .unwrap()
                .normalize_batch(&incoming)
                .unwrap();
        for (column, expected) in output.columns().iter().zip([0, 10, 1, 2, 3, 4]) {
            assert_eq!(
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }

    fn strict_normalizer(
        incoming: &Schema,
        registered: &LogicalSchema,
    ) -> SchemaResult<BatchSchemaNormalizer> {
        BatchSchemaNormalizer::for_append(incoming, registered, &test_index(), SchemaPolicy::Strict)
    }

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
            let normalizer = strict_normalizer(incoming_schema.as_ref(), &registered).unwrap();

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

        let normalizer = strict_normalizer(incoming_schema.as_ref(), &registered).unwrap();
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
        let normalizer = strict_normalizer(incoming_schema.as_ref(), &registered).unwrap();

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
                strict_normalizer(&incoming, &registered),
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
            strict_normalizer(&missing, &registered),
            Err(SchemaCompatibilityError::MissingIncomingColumn { column })
                if column == "second"
        ));

        let extra = Schema::new(vec![
            Field::new("first", DataType::Int64, false),
            Field::new("second", DataType::Int64, false),
            Field::new("third", DataType::Int64, false),
        ]);
        assert!(matches!(
            strict_normalizer(&extra, &registered),
            Err(SchemaCompatibilityError::ExtraIncomingColumn { column })
                if column == "third"
        ));

        let duplicate = Schema::new(vec![
            Field::new("first", DataType::Int64, false),
            Field::new("first", DataType::Int64, false),
        ]);
        assert!(matches!(
            strict_normalizer(&duplicate, &registered),
            Err(SchemaCompatibilityError::DuplicateIncomingColumn { column })
                if column == "first"
        ));

        let nullable = Schema::new(vec![
            Field::new("first", DataType::Int64, true),
            Field::new("second", DataType::Int64, false),
        ]);
        assert!(matches!(
            strict_normalizer(&nullable, &registered),
            Err(SchemaCompatibilityError::IncomingNullabilityMismatch {
                column,
                table_nullable: false,
                incoming_nullable: true,
            }) if column == "first"
        ));
    }
}
