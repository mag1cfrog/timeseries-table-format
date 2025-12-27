//! Helpers for reading and validating entity identity metadata from a segment.

use std::{collections::BTreeMap, path::Path};

use arrow::{datatypes::DataType, error::ArrowError};
use arrow_array::{Array, ArrayRef, LargeStringArray, StringArray};
use bytes::Bytes;
use parquet::{
    arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder},
    errors::ParquetError,
};
use snafu::prelude::*;

use crate::storage::StorageError;

/// Mapping of entity attribute names to their normalized string values.
pub type EntityIdentity = BTreeMap<String, String>;

#[derive(Debug, Snafu)]
/// Errors returned while extracting an entity identity from a segment.
pub enum SegmentEntityIdentityError {
    /// Failed to read from the underlying storage layer.
    #[snafu(display("Storage error while reading {path}: {source}"))]
    Storage {
        /// Path of the segment or file being read.
        path: String,
        /// Storage error produced by the backend.
        #[snafu(backtrace)]
        source: StorageError,
    },

    /// Failed to decode a Parquet file.
    #[snafu(display("Parquet read error for {path}: {source}"))]
    ParquetRead {
        /// Path of the Parquet file being read.
        path: String,
        /// Parquet error emitted by the reader.
        source: ParquetError,
    },

    /// Failed to decode Arrow data from the Parquet reader.
    #[snafu(display("Arrow read error for {path}: {source}"))]
    ArrowRead {
        /// Path of the Parquet file being read.
        path: String,
        /// Arrow error emitted while decoding batches.
        source: ArrowError,
    },

    /// Requested entity column is missing from the segment schema.
    #[snafu(display("Entity column not found in {path}: {column}"))]
    EntityColumnNotFound {
        /// Path of the segment that was inspected.
        path: String,
        /// Column name that was expected.
        column: String,
    },

    /// Entity column type is not supported for identity extraction.
    #[snafu(display("Unsupported entity column type in {path}: {column} has {datatype}"))]
    EntityColumnUnsupportedType {
        /// Path of the segment that was inspected.
        path: String,
        /// Column name that had the unsupported type.
        column: String,
        /// Human-readable representation of the column's data type.
        datatype: String,
    },

    /// Entity column contains null values that are not allowed.
    #[snafu(display("Entity column contains nulls in {path}: {column}"))]
    EntityColumnHasNull {
        /// Path of the segment that was inspected.
        path: String,
        /// Column name that contained nulls.
        column: String,
    },

    /// Entity column contains more than one distinct value.
    #[snafu(display(
        "Entity column has multiple values in {path}: {column} (first={first}, other={other})"
    ))]
    EntityColumnMultipleValues {
        /// Path of the segment that was inspected.
        path: String,
        /// Column name that contained multiple values.
        column: String,
        /// First value observed in the column.
        first: String,
        /// Additional, conflicting value observed in the column.
        other: String,
    },

    /// Entity column has no values, which indicates an empty segment.
    #[snafu(display("Entity column has no values (empty segment) in {path}: {column}"))]
    EntityColumnEmpty {
        /// Path of the segment that was inspected.
        path: String,
        /// Column name that had no values.
        column: String,
    },
}

fn feed_entity_column(
    path_str: &str,
    col_name: &str,
    array: &ArrayRef,
    pinned: &mut Option<String>,
) -> Result<(), SegmentEntityIdentityError> {
    // v0.1: allow Utf8 + LargeUtf8
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SegmentEntityIdentityError::EntityColumnUnsupportedType {
                    path: path_str.to_string(),
                    column: col_name.to_string(),
                    datatype: array.data_type().to_string(),
                })?;

            if arr.null_count() > 0 {
                return Err(SegmentEntityIdentityError::EntityColumnHasNull {
                    path: path_str.to_string(),
                    column: col_name.to_string(),
                });
            }

            for row in 0..arr.len() {
                let v = arr.value(row);
                match pinned.as_deref() {
                    None => *pinned = Some(v.to_string()),
                    Some(first) if first == v => {}
                    Some(first) => {
                        return Err(SegmentEntityIdentityError::EntityColumnMultipleValues {
                            path: path_str.to_string(),
                            column: col_name.to_string(),
                            first: first.to_string(),
                            other: v.to_string(),
                        });
                    }
                }
            }

            Ok(())
        }

        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| SegmentEntityIdentityError::EntityColumnUnsupportedType {
                    path: path_str.to_string(),
                    column: col_name.to_string(),
                    datatype: array.data_type().to_string(),
                })?;

            if arr.null_count() > 0 {
                return Err(SegmentEntityIdentityError::EntityColumnHasNull {
                    path: path_str.to_string(),
                    column: col_name.to_string(),
                });
            }

            for row in 0..arr.len() {
                let v = arr.value(row);
                match pinned.as_deref() {
                    None => *pinned = Some(v.to_string()),
                    Some(first) if first == v => {}
                    Some(first) => {
                        return Err(SegmentEntityIdentityError::EntityColumnMultipleValues {
                            path: path_str.to_string(),
                            column: col_name.to_string(),
                            first: first.to_string(),
                            other: v.to_string(),
                        });
                    }
                }
            }

            Ok(())
        }

        other => Err(SegmentEntityIdentityError::EntityColumnUnsupportedType {
            path: path_str.to_string(),
            column: col_name.to_string(),
            datatype: other.to_string(),
        }),
    }
}

/// Extracts entity identity values from a Parquet payload.
///
/// Reads only the requested `entity_columns` from `parquet_bytes`, validating
/// that each column exists, is string-typed, contains no nulls, and is constant
/// across the segment. Returns an empty map when `entity_columns` is empty.
pub fn segment_entity_identity_from_parquet_bytes(
    parquet_bytes: Bytes,
    rel_path: &Path,
    entity_columns: &[String],
) -> Result<EntityIdentity, SegmentEntityIdentityError> {
    let path_str = rel_path.display().to_string();

    if entity_columns.is_empty() {
        return Ok(EntityIdentity::new());
    }

    let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes).map_err(|source| {
        SegmentEntityIdentityError::ParquetRead {
            path: path_str.clone(),
            source,
        }
    })?;

    let arrow_schema = builder.schema();

    // validate columns exist up-front (nicer error)
    for c in entity_columns {
        if arrow_schema.index_of(c).is_err() {
            return Err(SegmentEntityIdentityError::EntityColumnNotFound {
                path: path_str.clone(),
                column: c.clone(),
            });
        }
    }

    // project just entity columns
    let cols_as_str: Vec<&str> = entity_columns.iter().map(|s| s.as_str()).collect();
    let mask = ProjectionMask::columns(builder.parquet_schema(), cols_as_str);
    let reader = builder.with_projection(mask).build().map_err(|source| {
        SegmentEntityIdentityError::ParquetRead {
            path: path_str.clone(),
            source,
        }
    })?;

    // one "pinned" value per column
    let mut pinned = vec![None; entity_columns.len()];

    for batch_res in reader {
        let batch = batch_res.map_err(|source| SegmentEntityIdentityError::ArrowRead {
            path: path_str.clone(),
            source,
        })?;

        let batch_schema = batch.schema();

        for (i, col_name) in entity_columns.iter().enumerate() {
            let idx = batch_schema.index_of(col_name).map_err(|_| {
                SegmentEntityIdentityError::EntityColumnNotFound {
                    path: path_str.clone(),
                    column: col_name.clone(),
                }
            })?;

            let col = batch.column(idx);
            feed_entity_column(&path_str, col_name, col, &mut pinned[i])?;
        }
    }

    let mut out = EntityIdentity::new();
    for (col, v) in entity_columns.iter().zip(pinned.into_iter()) {
        let Some(v) = v else {
            return Err(SegmentEntityIdentityError::EntityColumnEmpty {
                path: path_str.clone(),
                column: col.clone(),
            });
        };
        out.insert(col.clone(), v);
    }

    Ok(out)
}
