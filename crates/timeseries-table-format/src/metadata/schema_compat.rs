//! Schema compatibility helpers (pure metadata).
//!
//! v0.1 rule: **no schema evolution**.
//! Every appended segment must have a [`LogicalSchema`] that matches the table's
//! canonical schema exactly.

use std::collections::HashMap;

use snafu::prelude::*;

use crate::metadata::{
    logical_schema::{LogicalDataType, LogicalField, LogicalSchema, LogicalSchemaError},
    table_metadata::{TableMeta, TimeIndexSpec},
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

    /// Specialised version of TypeMismatch for the time index column.
    #[snafu(display(
        "Time index column {column} has incompatible type: table has {table_type}, \
         segment has {segment_type}"
    ))]
    TimeIndexTypeMismatch {
        /// The name of the time index column.
        column: String,
        /// The type in the table schema.
        table_type: LogicalDataType,
        /// The type in the segment schema.
        segment_type: LogicalDataType,
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

/// Enforce the v0.1 "no schema evolution" rule.
///
/// - Every table column must appear in the segment schema.
/// - No extra columns may appear in the segment schema.
/// - For every column, logical type and nullability must match exactly.
/// - If the mismatch is on the time index column (from `index`), we use a
///   more specific `TimeIndexTypeMismatch` error.
pub fn ensure_schema_exact_match(
    table_schema: &LogicalSchema,
    segment_schema: &LogicalSchema,
    index: &TimeIndexSpec,
) -> SchemaResult<()> {
    let time_col_name = index.timestamp_column.as_str();

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
            let err = if *name == time_col_name {
                SchemaCompatibilityError::TimeIndexTypeMismatch {
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
