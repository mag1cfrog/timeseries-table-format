//! Helpers for enforcing schema compatibility between a table and new segments.
//!
//! v0.1 rule: **no schema evolution**.
//! Every appended segment must have a LogicalSchema that matches the table's
//! LogicalSchema exactly:
//! - same column set (order-insensitive in v0.1)
//! - same `data_type` string (including timestamp unit/tz encoding)
//! - same `nullable` flag.

use std::collections::HashMap;

use snafu::prelude::*;

use crate::transaction_log::{
    LogicalField, LogicalSchema, LogicalSchemaError, TableMeta, TimeIndexSpec,
    table_metadata::LogicalDataType,
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

/// Convenience helper if you *want* to require a schema to be present.
///
/// For `append_parquet_segment_with_id` / `_and_bytes` we likely won't use this
/// directly; instead we'll treat `logical_schema: None` as "adopt schema from
/// first segment".
pub fn require_table_schema(meta: &TableMeta) -> SchemaResult<&LogicalSchema> {
    match &meta.logical_schema {
        Some(schema) => Ok(schema),
        None => MissingTableSchemaSnafu.fail(),
    }
}

// ---- core comparison helpers ----
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
///
/// This function is intentionally *name-based* (not order-based): we don't
/// currently care about column order for scanning, only about names + types.
/// If you later want to enforce order as well, you can extend this helper.
pub fn ensure_schema_exact_match(
    table_schema: &LogicalSchema,
    segment_schema: &LogicalSchema,
    index: &TimeIndexSpec,
) -> SchemaResult<()> {
    let time_col_name = index.timestamp_column.as_str();

    let table_cols = columns_by_name(table_schema);
    let segment_cols = columns_by_name(segment_schema);

    // 1) Check for missing columns and type/nullability mismatches in *one* pass.
    for (name, tcol) in &table_cols {
        match segment_cols.get(name) {
            None => {
                return MissingColumnSnafu {
                    column: (*name).to_string(),
                }
                .fail();
            }
            Some(scol) => {
                if tcol.data_type != scol.data_type || tcol.nullable != scol.nullable {
                    let column = (*name).to_string();

                    if *name == time_col_name {
                        return TimeIndexTypeMismatchSnafu {
                            column,
                            table_type: tcol.data_type.clone(),
                            segment_type: scol.data_type.clone(),
                        }
                        .fail();
                    } else {
                        return TypeMismatchSnafu {
                            column,
                            table_type: tcol.data_type.clone(),
                            segment_type: scol.data_type.clone(),
                        }
                        .fail();
                    }
                }
            }
        }
    }

    // 2) Check for extra columns in the segment schema.
    for name in segment_cols.keys() {
        if !table_cols.contains_key(name) {
            return ExtraColumnSnafu {
                column: (*name).to_string(),
            }
            .fail();
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_log::{TimeBucket, table_metadata::LogicalTimestampUnit};

    fn schema(cols: Vec<(&str, LogicalDataType, bool)>) -> LogicalSchema {
        LogicalSchema::new(
            cols.into_iter()
                .map(|(name, dtype, nullable)| LogicalField {
                    name: name.to_string(),
                    data_type: dtype,
                    nullable,
                })
                .collect(),
        )
        .expect("valid logical schema")
    }

    fn index(ts_col: &str) -> TimeIndexSpec {
        TimeIndexSpec {
            timestamp_column: ts_col.to_string(),
            entity_columns: vec![],
            bucket: TimeBucket::Seconds(1),
            timezone: None,
        }
    }

    #[test]
    fn schemas_match_ok() {
        let table = schema(vec![
            (
                "ts",
                LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Nanos,
                    timezone: None,
                },
                false,
            ),
            ("price", LogicalDataType::Float64, false),
        ]);
        let seg = schema(vec![
            ("price", LogicalDataType::Float64, false),
            (
                "ts",
                LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Nanos,
                    timezone: None,
                },
                false,
            ),
        ]);

        ensure_schema_exact_match(&table, &seg, &index("ts")).expect("schemas should match");
    }

    #[test]
    fn missing_column_errors() {
        let table = schema(vec![
            (
                "ts",
                LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Nanos,
                    timezone: None,
                },
                false,
            ),
            ("price", LogicalDataType::Float64, false),
        ]);
        let seg = schema(vec![(
            "ts",
            LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Nanos,
                timezone: None,
            },
            false,
        )]);

        let err = ensure_schema_exact_match(&table, &seg, &index("ts")).unwrap_err();
        assert!(
            matches!(err, SchemaCompatibilityError::MissingColumn { column } if column == "price")
        );
    }

    #[test]
    fn extra_column_errors() {
        let table = schema(vec![(
            "ts",
            LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Nanos,
                timezone: None,
            },
            false,
        )]);
        let seg = schema(vec![
            (
                "ts",
                LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Nanos,
                    timezone: None,
                },
                false,
            ),
            ("price", LogicalDataType::Float64, false),
        ]);

        let err = ensure_schema_exact_match(&table, &seg, &index("ts")).unwrap_err();
        assert!(
            matches!(err, SchemaCompatibilityError::ExtraColumn { column } if column == "price")
        );
    }

    #[test]
    fn type_mismatch_errors() {
        let table = schema(vec![("price", LogicalDataType::Float64, false)]);
        let seg = schema(vec![("price", LogicalDataType::Int64, false)]);

        let err = ensure_schema_exact_match(&table, &seg, &index("ts")).unwrap_err();
        assert!(
            matches!(err, SchemaCompatibilityError::TypeMismatch { column, .. } if column == "price")
        );
    }

    #[test]
    fn nullability_mismatch_errors() {
        let table = schema(vec![("price", LogicalDataType::Float64, false)]);
        let seg = schema(vec![("price", LogicalDataType::Float64, true)]);

        let err = ensure_schema_exact_match(&table, &seg, &index("ts")).unwrap_err();
        assert!(
            matches!(err, SchemaCompatibilityError::TypeMismatch { column, .. } if column == "price")
        );
    }

    #[test]
    fn time_index_type_mismatch_errors() {
        let table = schema(vec![(
            "ts",
            LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Micros,
                timezone: None,
            },
            false,
        )]);
        let seg = schema(vec![(
            "ts",
            LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Millis,
                timezone: None,
            },
            false,
        )]);

        let err = ensure_schema_exact_match(&table, &seg, &index("ts")).unwrap_err();
        assert!(
            matches!(err, SchemaCompatibilityError::TimeIndexTypeMismatch { column, .. } if column == "ts")
        );
    }

    #[test]
    fn require_table_schema_errors_when_missing() {
        let meta = TableMeta {
            kind: crate::transaction_log::TableKind::Generic,
            logical_schema: None,
            created_at: chrono::Utc::now(),
            format_version: 1,
            entity_identity: None,
        };

        let err = require_table_schema(&meta).unwrap_err();
        assert!(matches!(err, SchemaCompatibilityError::MissingTableSchema));
    }

    #[test]
    fn empty_schemas_match() {
        let table = schema(vec![]);
        let seg = schema(vec![]);

        ensure_schema_exact_match(&table, &seg, &index("ts")).expect("empty schemas should match");
    }

    #[test]
    fn column_names_are_case_sensitive() {
        let table = schema(vec![("Price", LogicalDataType::Float64, false)]);
        let seg = schema(vec![("price", LogicalDataType::Float64, false)]);

        let err = ensure_schema_exact_match(&table, &seg, &index("ts")).unwrap_err();
        assert!(
            matches!(err, SchemaCompatibilityError::MissingColumn { column } if column == "Price")
        );
    }

    #[test]
    fn require_table_schema_returns_schema_when_present() {
        let schema = schema(vec![(
            "ts",
            LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Nanos,
                timezone: None,
            },
            false,
        )]);
        let meta = TableMeta {
            kind: crate::transaction_log::TableKind::Generic,
            logical_schema: Some(schema.clone()),
            created_at: chrono::Utc::now(),
            format_version: 1,
            entity_identity: None,
        };

        let result = require_table_schema(&meta).expect("should return schema");
        assert_eq!(result, &schema);
    }
}
