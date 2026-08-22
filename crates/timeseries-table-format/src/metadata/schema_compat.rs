//! Schema compatibility helpers (pure metadata).
//!
//! v0.1 rule: **no schema evolution**.
//! Every appended segment must have a [`LogicalSchema`] that matches the table's
//! canonical schema exactly.

use std::collections::HashMap;

use snafu::prelude::*;

use crate::{
    coverage::{EntityIdentity, EntityValue},
    metadata::{
        logical_schema::{LogicalDataType, LogicalField, LogicalSchema, LogicalSchemaError},
        table_metadata::{IndexKind, IndexSpec, TableMeta},
    },
};

/// Errors raised when a segment's schema is not compatible with the table.
#[derive(Debug, Snafu)]
pub enum SchemaCompatibilityError {
    /// The table does not yet have a canonical logical schema.
    ///
    /// Many call sites (like append) may choose to *not* use this and
    /// instead adopt the first segment's schema, but we keep the error
    /// available for operations that require a fixed schema.
    #[snafu(display("Table has no logical_schema; v0.1 cannot append without a canonical schema"))]
    MissingTableSchema,

    /// The segment is missing a column that exists in the table schema.
    #[snafu(display("Segment schema is missing required column {column}"))]
    MissingColumn {
        /// The name of the missing column.
        column: String,
    },

    /// The logical schema does not contain the registered index column.
    #[snafu(display("Schema is missing registered index column {column}"))]
    MissingIndexColumn {
        /// Registered index column name.
        column: String,
    },

    /// The logical schema does not contain a configured entity column.
    #[snafu(display("Schema is missing configured entity column {column}"))]
    MissingEntityColumn {
        /// Configured entity column name.
        column: String,
    },

    /// A configured entity column has an unsupported logical type.
    #[snafu(display(
        "Entity column {column} has unsupported logical type {actual}; expected utf8, int32, int64, or uint64"
    ))]
    UnsupportedEntityColumnType {
        /// Configured entity column name.
        column: String,
        /// Unsupported logical type.
        actual: LogicalDataType,
    },

    /// A persisted single-entity identity has the wrong component count.
    #[snafu(display(
        "Entity identity has {actual} components, but the table configures {expected} entity columns"
    ))]
    EntityIdentityArityMismatch {
        /// Configured entity column count.
        expected: usize,
        /// Persisted identity component count.
        actual: usize,
    },

    /// A persisted entity component has the wrong scalar type.
    #[snafu(display(
        "Entity identity component for column {column} has type {actual}; expected {expected}"
    ))]
    EntityIdentityTypeMismatch {
        /// Configured entity column name.
        column: String,
        /// Logical type required by the table schema.
        expected: LogicalDataType,
        /// Persisted scalar type.
        actual: &'static str,
    },

    /// The segment has an extra column that does not exist in the table schema.
    #[snafu(display("Segment schema has extra column {column} not present in table schema"))]
    ExtraColumn {
        /// The name of the extra column.
        column: String,
    },

    /// Column exists in both schemas, but the logical type / nullability differ.
    #[snafu(display(
        "Type mismatch for column {column}: table has {table_type}, segment has {segment_type}"
    ))]
    TypeMismatch {
        /// The name of the column with mismatched type.
        column: String,
        /// The type in the table schema.
        table_type: LogicalDataType,
        /// The type in the segment schema.
        segment_type: LogicalDataType,
    },

    /// Specialized version of TypeMismatch for the ordered index column.
    #[snafu(display(
        "Index column {column} has incompatible type: table has {table_type}, \
         segment has {segment_type}"
    ))]
    IndexColumnTypeMismatch {
        /// The name of the ordered index column.
        column: String,
        /// The type in the table schema.
        table_type: LogicalDataType,
        /// The type in the segment schema.
        segment_type: LogicalDataType,
    },

    /// The registered index kind disagrees with the logical schema.
    #[snafu(display(
        "Index column {column} has incompatible logical type: expected {expected}, found {actual}"
    ))]
    IndexKindMismatch {
        /// Registered index column name.
        column: String,
        /// Expected ordered domain.
        expected: &'static str,
        /// Logical type found in the schema.
        actual: LogicalDataType,
    },

    /// Logical schema construction or validation failed.
    #[snafu(display("Logical schema is invalid: {source}"))]
    LogicalSchema {
        /// The underlying logical schema error.
        #[snafu(source)]
        source: LogicalSchemaError,
    },
}

/// A convenience type alias for results of schema compatibility operations.
pub type SchemaResult<T> = Result<T, SchemaCompatibilityError>;

/// Convenience helper if you want to require a schema to be present.
pub fn require_table_schema(meta: &TableMeta) -> SchemaResult<&LogicalSchema> {
    match &meta.logical_schema {
        Some(schema) => Ok(schema),
        None => MissingTableSchemaSnafu.fail(),
    }
}

fn columns_by_name(schema: &LogicalSchema) -> HashMap<&str, &LogicalField> {
    schema
        .columns()
        .iter()
        .map(|col| (col.name.as_str(), col))
        .collect()
}

/// Validate the registered ordered index and entity columns against a logical schema.
///
/// # Errors
/// Returns [`SchemaCompatibilityError::MissingIndexColumn`] when the column is
/// absent and [`SchemaCompatibilityError::IndexKindMismatch`] when its logical
/// type does not match the registered domain. Missing or unsupported entity
/// columns return their corresponding typed errors.
pub fn ensure_index_spec_matches_schema(
    schema: &LogicalSchema,
    index: &IndexSpec,
) -> SchemaResult<()> {
    let field = schema
        .columns()
        .iter()
        .find(|field| field.name == index.column)
        .ok_or_else(|| SchemaCompatibilityError::MissingIndexColumn {
            column: index.column.clone(),
        })?;

    let matches = matches!(
        (&index.kind, &field.data_type),
        (
            IndexKind::Timestamp { .. },
            LogicalDataType::Timestamp { .. }
        ) | (IndexKind::Int64 { .. }, LogicalDataType::Int64)
            | (IndexKind::UInt64 { .. }, LogicalDataType::UInt64)
    );

    if !matches {
        return Err(SchemaCompatibilityError::IndexKindMismatch {
            column: index.column.clone(),
            expected: index.kind.name(),
            actual: field.data_type.clone(),
        });
    }

    for column in &index.entity_columns {
        let field = schema
            .columns()
            .iter()
            .find(|field| field.name == *column)
            .ok_or_else(|| SchemaCompatibilityError::MissingEntityColumn {
                column: column.clone(),
            })?;
        if !matches!(
            field.data_type,
            LogicalDataType::Utf8
                | LogicalDataType::Int32
                | LogicalDataType::Int64
                | LogicalDataType::UInt64
        ) {
            return Err(SchemaCompatibilityError::UnsupportedEntityColumnType {
                column: column.clone(),
                actual: field.data_type.clone(),
            });
        }
    }

    Ok(())
}

/// Validate a persisted identity against configured entity-column types.
///
/// # Errors
/// Returns an arity or component-type mismatch when the identity cannot belong
/// to the supplied table schema and index specification.
pub fn ensure_entity_identity_matches_schema(
    schema: &LogicalSchema,
    index: &IndexSpec,
    identity: &EntityIdentity,
) -> SchemaResult<()> {
    if identity.components().len() != index.entity_columns.len() {
        return Err(SchemaCompatibilityError::EntityIdentityArityMismatch {
            expected: index.entity_columns.len(),
            actual: identity.components().len(),
        });
    }

    for (column, value) in index.entity_columns.iter().zip(identity.components()) {
        let field = schema
            .columns()
            .iter()
            .find(|field| field.name == *column)
            .ok_or_else(|| SchemaCompatibilityError::MissingEntityColumn {
                column: column.clone(),
            })?;
        let matches = matches!(
            (&field.data_type, value),
            (LogicalDataType::Utf8, EntityValue::Utf8(_))
                | (LogicalDataType::Int32, EntityValue::Int32(_))
                | (LogicalDataType::Int64, EntityValue::Int64(_))
                | (LogicalDataType::UInt64, EntityValue::UInt64(_))
        );
        if !matches {
            let actual = match value {
                EntityValue::Utf8(_) => "utf8",
                EntityValue::Int32(_) => "int32",
                EntityValue::Int64(_) => "int64",
                EntityValue::UInt64(_) => "uint64",
            };
            return Err(SchemaCompatibilityError::EntityIdentityTypeMismatch {
                column: column.clone(),
                expected: field.data_type.clone(),
                actual,
            });
        }
    }

    Ok(())
}

/// Enforce the v0.1 "no schema evolution" rule by field name.
///
/// - Every table column must appear in the segment schema.
/// - No extra columns may appear in the segment schema.
/// - For every column, logical type and nullability must match exactly.
/// - Top-level column order may differ.
/// - If the mismatch is on the ordered index column, use a specific error.
pub fn ensure_schema_fields_match_by_name(
    table_schema: &LogicalSchema,
    segment_schema: &LogicalSchema,
    index: &IndexSpec,
) -> SchemaResult<()> {
    let index_col_name = index.column.as_str();

    let table_cols = columns_by_name(table_schema);
    let seg_cols = columns_by_name(segment_schema);

    for (name, table_field) in &table_cols {
        let seg_field =
            seg_cols
                .get(name)
                .ok_or_else(|| SchemaCompatibilityError::MissingColumn {
                    column: (*name).to_string(),
                })?;

        if table_field.data_type != seg_field.data_type
            || table_field.nullable != seg_field.nullable
        {
            let err = if *name == index_col_name {
                SchemaCompatibilityError::IndexColumnTypeMismatch {
                    column: (*name).to_string(),
                    table_type: table_field.data_type.clone(),
                    segment_type: seg_field.data_type.clone(),
                }
            } else {
                SchemaCompatibilityError::TypeMismatch {
                    column: (*name).to_string(),
                    table_type: table_field.data_type.clone(),
                    segment_type: seg_field.data_type.clone(),
                }
            };
            return Err(err);
        }
    }

    for name in seg_cols.keys() {
        if !table_cols.contains_key(name) {
            return Err(SchemaCompatibilityError::ExtraColumn {
                column: (*name).to_string(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::metadata::{
        logical_schema::{LogicalSchema, LogicalTimestampUnit},
        table_metadata::TimeBucket,
    };

    fn schema(data_type: LogicalDataType) -> LogicalSchema {
        LogicalSchema::new(vec![LogicalField {
            name: "idx".to_string(),
            data_type,
            nullable: false,
        }])
        .unwrap()
    }

    fn index(kind: IndexKind) -> IndexSpec {
        IndexSpec {
            column: "idx".to_string(),
            entity_columns: Vec::new(),
            kind,
        }
    }

    fn schema_with_entities(entity_types: Vec<LogicalDataType>) -> LogicalSchema {
        let mut fields = vec![LogicalField {
            name: "idx".to_string(),
            data_type: LogicalDataType::Int64,
            nullable: false,
        }];
        fields.extend(
            entity_types
                .into_iter()
                .enumerate()
                .map(|(position, data_type)| LogicalField {
                    name: format!("entity_{position}"),
                    data_type,
                    nullable: false,
                }),
        );
        LogicalSchema::new(fields).unwrap()
    }

    fn entity_index(count: usize) -> IndexSpec {
        IndexSpec {
            column: "idx".to_string(),
            entity_columns: (0..count)
                .map(|position| format!("entity_{position}"))
                .collect(),
            kind: IndexKind::Int64 {
                bucket_width: NonZeroU64::new(1).unwrap(),
            },
        }
    }

    #[test]
    fn ordered_index_schema_validation_accepts_each_exact_domain() {
        let cases = [
            (
                index(IndexKind::Timestamp {
                    bucket: TimeBucket::Seconds(1),
                    timezone: None,
                }),
                schema(LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Nanos,
                    timezone: Some("UTC".to_string()),
                }),
            ),
            (
                index(IndexKind::Int64 {
                    bucket_width: NonZeroU64::new(1).unwrap(),
                }),
                schema(LogicalDataType::Int64),
            ),
            (
                index(IndexKind::UInt64 {
                    bucket_width: NonZeroU64::new(1).unwrap(),
                }),
                schema(LogicalDataType::UInt64),
            ),
        ];

        for (index, schema) in cases {
            ensure_index_spec_matches_schema(&schema, &index).unwrap();
        }
    }

    #[test]
    fn ordered_index_schema_validation_rejects_missing_and_wrong_domains() {
        let unsigned = index(IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        });
        let missing = LogicalSchema::new(vec![LogicalField {
            name: "other".to_string(),
            data_type: LogicalDataType::UInt64,
            nullable: false,
        }])
        .unwrap();

        assert!(matches!(
            ensure_index_spec_matches_schema(&missing, &unsigned),
            Err(SchemaCompatibilityError::MissingIndexColumn { .. })
        ));
        assert!(matches!(
            ensure_index_spec_matches_schema(&schema(LogicalDataType::Int64), &unsigned),
            Err(SchemaCompatibilityError::IndexKindMismatch {
                expected: "uint64",
                actual: LogicalDataType::Int64,
                ..
            })
        ));
    }

    #[test]
    fn entity_schema_validation_accepts_only_supported_types() {
        let supported = vec![
            LogicalDataType::Utf8,
            LogicalDataType::Int32,
            LogicalDataType::Int64,
            LogicalDataType::UInt64,
        ];
        ensure_index_spec_matches_schema(&schema_with_entities(supported), &entity_index(4))
            .unwrap();

        let missing = ensure_index_spec_matches_schema(
            &schema_with_entities(vec![LogicalDataType::Utf8]),
            &entity_index(2),
        )
        .unwrap_err();
        assert!(matches!(
            missing,
            SchemaCompatibilityError::MissingEntityColumn { column }
                if column == "entity_1"
        ));

        let unsupported = ensure_index_spec_matches_schema(
            &schema_with_entities(vec![LogicalDataType::Bool]),
            &entity_index(1),
        )
        .unwrap_err();
        assert!(matches!(
            unsupported,
            SchemaCompatibilityError::UnsupportedEntityColumnType {
                column,
                actual: LogicalDataType::Bool,
            } if column == "entity_0"
        ));
    }

    #[test]
    fn persisted_entity_identity_must_match_schema_types_and_arity() {
        let schema = schema_with_entities(vec![
            LogicalDataType::Utf8,
            LogicalDataType::Int32,
            LogicalDataType::Int64,
            LogicalDataType::UInt64,
        ]);
        let index = entity_index(4);
        let identity = EntityIdentity::try_new(vec![
            EntityValue::from("device"),
            EntityValue::Int32(-1),
            EntityValue::Int64(i64::MIN),
            EntityValue::UInt64(u64::MAX),
        ])
        .unwrap();
        ensure_entity_identity_matches_schema(&schema, &index, &identity).unwrap();

        let wrong_type = EntityIdentity::try_new(vec![
            EntityValue::from("device"),
            EntityValue::UInt64(1),
            EntityValue::Int64(2),
            EntityValue::UInt64(3),
        ])
        .unwrap();
        assert!(matches!(
            ensure_entity_identity_matches_schema(&schema, &index, &wrong_type),
            Err(SchemaCompatibilityError::EntityIdentityTypeMismatch {
                column,
                expected: LogicalDataType::Int32,
                actual: "uint64",
            }) if column == "entity_1"
        ));

        let too_short = EntityIdentity::try_new(vec![EntityValue::from("device")]).unwrap();
        assert!(matches!(
            ensure_entity_identity_matches_schema(&schema, &index, &too_short),
            Err(SchemaCompatibilityError::EntityIdentityArityMismatch {
                expected: 4,
                actual: 1,
            })
        ));
    }
}
