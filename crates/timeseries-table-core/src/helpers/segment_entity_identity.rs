//! Helpers for reading and validating entity identity metadata from a segment.

use std::{collections::BTreeMap, path::Path, vec};

use arrow::{
    datatypes::{DataType, Schema},
    error::ArrowError,
};
use arrow_array::{Array, ArrayRef, LargeStringArray, StringArray};
use bytes::Bytes;
use parquet::{
    arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder},
    errors::ParquetError,
    file::metadata::ParquetMetaData,
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

fn try_entity_identity_from_stats(
    meta: &ParquetMetaData,
    rel_path: &str,
    entity_columns: &[String],
    arrow_schema: &Schema,
) -> Result<Option<EntityIdentity>, SegmentEntityIdentityError> {
    if entity_columns.is_empty() {
        return Ok(Some(EntityIdentity::new()));
    }

    // If the whole file is empty, treat as empty segment for identity purposes.
    if meta.file_metadata().num_rows() == 0 {
        // pick the first column for an actionable error
        return Err(SegmentEntityIdentityError::EntityColumnEmpty {
            path: rel_path.to_string(),
            column: entity_columns[0].clone(),
        });
    }

    // Map entity column name -> parquet leaf column index
    let schema_descr = meta.file_metadata().schema_descr();
    let mut parquet_col_idxs = Vec::with_capacity(entity_columns.len());

    for col_name in entity_columns {
        // Ensure Arrow type is supported up-front (so stats bytes -> UTF-8 makes sense)
        let dt = arrow_schema
            .field_with_name(col_name)
            .map_err(|_| SegmentEntityIdentityError::EntityColumnNotFound {
                path: rel_path.to_string(),
                column: col_name.clone(),
            })?
            .data_type();

        match dt {
            DataType::Utf8 | DataType::LargeUtf8 => {}
            other => {
                return Err(SegmentEntityIdentityError::EntityColumnUnsupportedType {
                    path: rel_path.to_string(),
                    column: col_name.clone(),
                    datatype: other.to_string(),
                });
            }
        }

        let idx = schema_descr
            .columns()
            .iter()
            .position(|c| c.path().string() == *col_name)
            .ok_or_else(|| SegmentEntityIdentityError::EntityColumnNotFound {
                path: rel_path.to_string(),
                column: col_name.clone(),
            })?;

        parquet_col_idxs.push(idx);
    }

    let mut pinned = vec![None; entity_columns.len()];

    for rg in meta.row_groups() {
        for (i, (col_name, &col_idx)) in entity_columns
            .iter()
            .zip(parquet_col_idxs.iter())
            .enumerate()
        {
            let col_chunk = rg.column(col_idx);

            let Some(stats) = col_chunk.statistics() else {
                // No stats => can't fast-path
                return Ok(None);
            };

            // null_count is optional; if missing we must not assume 0 => can't fast-path safely
            // if present and > 0, that's a definite error.
            match stats.null_count_opt() {
                Some(0) => {}
                Some(_) => {
                    return Err(SegmentEntityIdentityError::EntityColumnHasNull {
                        path: rel_path.to_string(),
                        column: col_name.clone(),
                    });
                }
                None => return Ok(None),
            }

            // If distinct_count is present and != 1, we can fail immediately.
            if let Some(d) = stats.distinct_count_opt()
                && d != 1
            {
                // Try to give a helpful first/other if we can; else generic.
                let (first, other) = match (stats.min_bytes_opt(), stats.max_bytes_opt()) {
                    (Some(minb), Some(maxb)) => {
                        let a = std::str::from_utf8(minb)
                            .unwrap_or("<non-utf8>")
                            .to_string();
                        let b = std::str::from_utf8(maxb)
                            .unwrap_or("<non-utf8>")
                            .to_string();
                        (a, b)
                    }
                    _ => ("<unknown>".to_string(), "<unknown>".to_string()),
                };

                return Err(SegmentEntityIdentityError::EntityColumnMultipleValues {
                    path: rel_path.to_string(),
                    column: col_name.clone(),
                    first,
                    other,
                });
            }

            // For strings, Parquet may store compact bounds; require "exact" to trust equality/inequality.
            // If not exact, fallback to scan.
            if !stats.min_is_exact() || !stats.max_is_exact() {
                return Ok(None);
            }

            let (Some(minb), Some(maxb)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) else {
                // Missing min/max => can't derive the value
                return Ok(None);
            };

            // If exact and min != max, then there are definitely multiple values.
            if minb != maxb {
                let first = std::str::from_utf8(minb)
                    .map_err(
                        |_| SegmentEntityIdentityError::EntityColumnUnsupportedType {
                            path: rel_path.to_string(),
                            column: col_name.clone(),
                            datatype: "non-utf8 bytes".to_string(),
                        },
                    )?
                    .to_string();
                let other = std::str::from_utf8(maxb)
                    .map_err(
                        |_| SegmentEntityIdentityError::EntityColumnUnsupportedType {
                            path: rel_path.to_string(),
                            column: col_name.clone(),
                            datatype: "non-utf8 bytes".to_string(),
                        },
                    )?
                    .to_string();

                return Err(SegmentEntityIdentityError::EntityColumnMultipleValues {
                    path: rel_path.to_string(),
                    column: col_name.clone(),
                    first,
                    other,
                });
            }

            // min == max and exact => constant value for this row group
            let v = std::str::from_utf8(minb).map_err(|_| {
                SegmentEntityIdentityError::EntityColumnUnsupportedType {
                    path: rel_path.to_string(),
                    column: col_name.clone(),
                    datatype: "non-utf8 bytes".to_string(),
                }
            })?;

            match pinned[i].as_deref() {
                None => pinned[i] = Some(v.to_string()),
                Some(first) if first == v => {}
                Some(first) => {
                    // Different constants across row groups => multiple values in the segment
                    return Err(SegmentEntityIdentityError::EntityColumnMultipleValues {
                        path: rel_path.to_string(),
                        column: col_name.clone(),
                        first: first.to_string(),
                        other: v.to_string(),
                    });
                }
            }
        }
    }

    // Ensure we actually found a value per column
    let mut out = EntityIdentity::new();
    for (col, v) in entity_columns.iter().zip(pinned.into_iter()) {
        let Some(v) = v else {
            return Err(SegmentEntityIdentityError::EntityColumnEmpty {
                path: rel_path.to_string(),
                column: col.clone(),
            });
        };
        out.insert(col.clone(), v);
    }

    Ok(Some(out))
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

    // stats fast-path (no batch decode)
    if let Some(identity) = try_entity_identity_from_stats(
        builder.metadata(),
        &path_str,
        entity_columns,
        arrow_schema.as_ref(),
    )? {
        return Ok(identity);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Cursor, path::Path, sync::Arc};

    use arrow::{
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use arrow_array::{ArrayRef, Int32Array, LargeStringArray, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    fn make_batch(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(schema, columns).expect("record batch")
    }

    fn parquet_bytes_from_batches(
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        props: WriterProperties,
    ) -> Vec<u8> {
        let cursor = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(cursor, schema, Some(props)).expect("arrow writer");
        for batch in batches {
            writer.write(&batch).expect("write batch");
        }
        let cursor = writer.into_inner().expect("finalize parquet");
        cursor.into_inner()
    }

    fn identity_from_bytes(
        bytes: Vec<u8>,
        entity_columns: &[String],
    ) -> Result<EntityIdentity, SegmentEntityIdentityError> {
        segment_entity_identity_from_parquet_bytes(
            Bytes::from(bytes),
            Path::new("segment.parquet"),
            entity_columns,
        )
    }

    fn string_array(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn large_string_array(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(LargeStringArray::from(values.to_vec()))
    }

    #[test]
    fn identity_empty_columns_returns_empty() {
        let identity = segment_entity_identity_from_parquet_bytes(
            Bytes::from_static(b"not parquet"),
            Path::new("segment.parquet"),
            &[],
        )
        .expect("empty columns");
        assert!(identity.is_empty());
    }

    #[test]
    fn identity_happy_path_utf8() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![
                string_array(&[Some("alpha"), Some("alpha")]),
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ],
        );
        let bytes = parquet_bytes_from_batches(
            Arc::clone(&schema),
            vec![batch],
            WriterProperties::builder().build(),
        );

        let identity = identity_from_bytes(bytes, &[String::from("entity")]).expect("identity");
        assert_eq!(identity.get("entity").map(String::as_str), Some("alpha"));
    }

    #[test]
    fn identity_happy_path_large_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::LargeUtf8,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![large_string_array(&[Some("alpha"), Some("alpha")])],
        );
        let bytes = parquet_bytes_from_batches(
            Arc::clone(&schema),
            vec![batch],
            WriterProperties::builder().build(),
        );

        let identity = identity_from_bytes(bytes, &[String::from("entity")]).expect("identity");
        assert_eq!(identity.get("entity").map(String::as_str), Some("alpha"));
    }

    #[test]
    fn identity_missing_column_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("alpha")])],
        );
        let bytes = parquet_bytes_from_batches(
            Arc::clone(&schema),
            vec![batch],
            WriterProperties::builder().build(),
        );

        let err = identity_from_bytes(bytes, &[String::from("missing")]).unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnNotFound { .. }
        ));
    }

    #[test]
    fn identity_unsupported_type_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Int32,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef],
        );
        let bytes = parquet_bytes_from_batches(
            Arc::clone(&schema),
            vec![batch],
            WriterProperties::builder().build(),
        );

        let err = identity_from_bytes(bytes, &[String::from("entity")]).unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnUnsupportedType { .. }
        ));
    }

    #[test]
    fn identity_column_has_null_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            true,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), None])],
        );
        let bytes = parquet_bytes_from_batches(
            Arc::clone(&schema),
            vec![batch],
            WriterProperties::builder().build(),
        );

        let err = identity_from_bytes(bytes, &[String::from("entity")]).unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnHasNull { .. }
        ));
    }

    #[test]
    fn identity_multiple_values_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("beta")])],
        );
        let bytes = parquet_bytes_from_batches(
            Arc::clone(&schema),
            vec![batch],
            WriterProperties::builder().build(),
        );

        let err = identity_from_bytes(bytes, &[String::from("entity")]).unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnMultipleValues { .. }
        ));
    }

    #[test]
    fn identity_empty_segment_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let batch = make_batch(Arc::clone(&schema), vec![string_array(&[])]);
        let bytes = parquet_bytes_from_batches(
            Arc::clone(&schema),
            vec![batch],
            WriterProperties::builder().build(),
        );

        let err = identity_from_bytes(bytes, &[String::from("entity")]).unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnEmpty { .. }
        ));
    }

    #[test]
    fn identity_fallback_scan_success() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("alpha")])],
        );
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let bytes = parquet_bytes_from_batches(Arc::clone(&schema), vec![batch], props);

        let identity = identity_from_bytes(bytes, &[String::from("entity")]).expect("identity");
        assert_eq!(identity.get("entity").map(String::as_str), Some("alpha"));
    }

    #[test]
    fn identity_fallback_scan_nulls_return_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            true,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), None])],
        );
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let bytes = parquet_bytes_from_batches(Arc::clone(&schema), vec![batch], props);

        let err = identity_from_bytes(bytes, &[String::from("entity")]).unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnHasNull { .. }
        ));
    }

    #[test]
    fn identity_fallback_scan_multiple_values_return_error() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("beta")])],
        );
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let bytes = parquet_bytes_from_batches(Arc::clone(&schema), vec![batch], props);

        let err = identity_from_bytes(bytes, &[String::from("entity")]).unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnMultipleValues { .. }
        ));
    }

    #[test]
    fn identity_arrow_read_error_on_invalid_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("alpha")])],
        );
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let mut bytes = parquet_bytes_from_batches(Arc::clone(&schema), vec![batch], props);

        let needle = b"alpha";
        let pos = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("needle in parquet data");
        bytes[pos] = 0xFF;

        let err = identity_from_bytes(bytes, &[String::from("entity")]).unwrap_err();
        assert!(matches!(err, SegmentEntityIdentityError::ArrowRead { .. }));
    }

    #[test]
    fn identity_parquet_read_error_on_invalid_bytes() {
        let err = segment_entity_identity_from_parquet_bytes(
            Bytes::from_static(b"not parquet"),
            Path::new("segment.parquet"),
            &[String::from("entity")],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::ParquetRead { .. }
        ));
    }
}
