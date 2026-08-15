//! Schema compatibility helpers (pure metadata).
//!
//! v0.1 rule: **no schema evolution**.
//! Every appended segment must have a [`LogicalSchema`] that matches the table's
//! canonical schema exactly.

use std::collections::HashMap;

use snafu::prelude::*;

use crate::metadata::{
    logical_schema::{LogicalDataType, LogicalField, LogicalSchema, LogicalSchemaError},
    table_metadata::{IndexKind, IndexSpec, TableMeta},
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

/// Validate the registered ordered index against a logical schema.
///
/// # Errors
/// Returns [`SchemaCompatibilityError::MissingIndexColumn`] when the column is
/// absent and [`SchemaCompatibilityError::IndexKindMismatch`] when its logical
/// type does not match the registered domain.
pub fn ensure_index_matches_schema(schema: &LogicalSchema, index: &IndexSpec) -> SchemaResult<()> {
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

    if matches {
        Ok(())
    } else {
        Err(SchemaCompatibilityError::IndexKindMismatch {
            column: index.column.clone(),
            expected: index.kind.name(),
            actual: field.data_type.clone(),
        })
    }
}

/// Enforce the v0.1 "no schema evolution" rule.
///
/// - Every table column must appear in the segment schema.
/// - No extra columns may appear in the segment schema.
/// - For every column, logical type and nullability must match exactly.
/// - If the mismatch is on the ordered index column, use a specific error.
pub fn ensure_schema_exact_match(
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
            ensure_index_matches_schema(&schema, &index).unwrap();
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
            ensure_index_matches_schema(&missing, &unsigned),
            Err(SchemaCompatibilityError::MissingIndexColumn { .. })
        ));
        assert!(matches!(
            ensure_index_matches_schema(&schema(LogicalDataType::Int64), &unsigned),
            Err(SchemaCompatibilityError::IndexKindMismatch {
                expected: "uint64",
                actual: LogicalDataType::Int64,
                ..
            })
        ));
    }
}
