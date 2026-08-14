//! Helpers for reading and validating entity identity metadata from a segment.

use std::{collections::BTreeMap, path::Path};

use arrow::datatypes::{DataType, Schema};
use arrow_array::{Array, ArrayRef, LargeStringArray, StringArray};
use futures::{Stream, StreamExt};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::ParquetRecordBatchStreamBuilder,
    },
    errors::ParquetError,
    file::metadata::ParquetMetaData,
};
use snafu::prelude::*;
use tokio::task::JoinSet;

use crate::storage::{StorageError, TableLocation, open_local_file};

use super::{INSPECTION_BATCH_SIZE, resolve_rg_settings};

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

struct EntityScanPlan {
    pinned: Vec<Option<String>>,
    row_groups_to_scan: Vec<usize>,
}

fn merge_entity_value(
    path: &str,
    column: &str,
    pinned: &mut Option<String>,
    value: &str,
) -> Result<(), SegmentEntityIdentityError> {
    match pinned.as_deref() {
        None => *pinned = Some(value.to_string()),
        Some(first) if first == value => {}
        Some(first) => {
            return Err(SegmentEntityIdentityError::EntityColumnMultipleValues {
                path: path.to_string(),
                column: column.to_string(),
                first: first.to_string(),
                other: value.to_string(),
            });
        }
    }
    Ok(())
}

fn constant_entity_value_from_stats(
    stats: &parquet::file::statistics::Statistics,
    path: &str,
    column: &str,
    column_is_required: bool,
) -> Result<Option<String>, SegmentEntityIdentityError> {
    match stats.null_count_opt() {
        Some(0) => {}
        Some(_) => {
            return Err(SegmentEntityIdentityError::EntityColumnHasNull {
                path: path.to_string(),
                column: column.to_string(),
            });
        }
        None => return Ok(None),
    }

    // arrow-rs reports Some(0) when a Parquet null count was omitted. A
    // required column cannot contain nulls by schema; an optional column must
    // therefore be scanned even when this API reports zero.
    if !column_is_required {
        return Ok(None);
    }

    if let Some(distinct) = stats.distinct_count_opt()
        && distinct != 1
    {
        let first = stats
            .min_bytes_opt()
            .and_then(|value| std::str::from_utf8(value).ok())
            .unwrap_or("<unknown>");
        let other = stats
            .max_bytes_opt()
            .and_then(|value| std::str::from_utf8(value).ok())
            .unwrap_or("<unknown>");
        return Err(SegmentEntityIdentityError::EntityColumnMultipleValues {
            path: path.to_string(),
            column: column.to_string(),
            first: first.to_string(),
            other: other.to_string(),
        });
    }

    if !stats.min_is_exact() || !stats.max_is_exact() {
        return Ok(None);
    }

    let (Some(min), Some(max)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) else {
        return Ok(None);
    };
    let min = std::str::from_utf8(min).map_err(|_| {
        SegmentEntityIdentityError::EntityColumnUnsupportedType {
            path: path.to_string(),
            column: column.to_string(),
            datatype: "non-utf8 bytes".to_string(),
        }
    })?;
    let max = std::str::from_utf8(max).map_err(|_| {
        SegmentEntityIdentityError::EntityColumnUnsupportedType {
            path: path.to_string(),
            column: column.to_string(),
            datatype: "non-utf8 bytes".to_string(),
        }
    })?;

    if min != max {
        return Err(SegmentEntityIdentityError::EntityColumnMultipleValues {
            path: path.to_string(),
            column: column.to_string(),
            first: min.to_string(),
            other: max.to_string(),
        });
    }

    Ok(Some(min.to_string()))
}

fn plan_entity_scan(
    meta: &ParquetMetaData,
    rel_path: &str,
    entity_columns: &[String],
    arrow_schema: &Schema,
) -> Result<EntityScanPlan, SegmentEntityIdentityError> {
    if meta.file_metadata().num_rows() == 0 {
        return Err(SegmentEntityIdentityError::EntityColumnEmpty {
            path: rel_path.to_string(),
            column: entity_columns[0].clone(),
        });
    }

    let schema_descr = meta.file_metadata().schema_descr();
    let mut parquet_col_idxs = Vec::with_capacity(entity_columns.len());
    let mut column_is_required = Vec::with_capacity(entity_columns.len());

    for col_name in entity_columns {
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
        column_is_required.push(schema_descr.column(idx).max_def_level() == 0);
    }

    let mut pinned = vec![None; entity_columns.len()];
    let mut row_groups_to_scan = Vec::new();

    for (row_group_index, row_group) in meta.row_groups().iter().enumerate() {
        if row_group.num_rows() == 0 {
            continue;
        }

        let mut group_values = Vec::with_capacity(entity_columns.len());
        let mut requires_scan = false;
        for ((col_name, &col_idx), &column_is_required) in entity_columns
            .iter()
            .zip(&parquet_col_idxs)
            .zip(&column_is_required)
        {
            let value = match row_group.column(col_idx).statistics() {
                Some(stats) => {
                    constant_entity_value_from_stats(stats, rel_path, col_name, column_is_required)?
                }
                None => None,
            };
            requires_scan |= value.is_none();
            group_values.push(value);
        }

        if requires_scan {
            row_groups_to_scan.push(row_group_index);
            continue;
        }

        for ((col_name, pinned), value) in entity_columns.iter().zip(&mut pinned).zip(group_values)
        {
            if let Some(value) = value {
                merge_entity_value(rel_path, col_name, pinned, &value)?;
            }
        }
    }

    Ok(EntityScanPlan {
        pinned,
        row_groups_to_scan,
    })
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
                merge_entity_value(path_str, col_name, pinned, arr.value(row))?;
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
                merge_entity_value(path_str, col_name, pinned, arr.value(row))?;
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

async fn scan_entity_batches(
    path: &str,
    entity_columns: &[String],
    mut reader: impl Stream<
        Item = Result<arrow::record_batch::RecordBatch, parquet::errors::ParquetError>,
    > + Unpin,
) -> Result<Vec<Option<String>>, SegmentEntityIdentityError> {
    let mut pinned = vec![None; entity_columns.len()];

    while let Some(batch) = reader.next().await {
        let batch = batch.map_err(|source| SegmentEntityIdentityError::ParquetRead {
            path: path.to_string(),
            source,
        })?;

        for (column, pinned) in entity_columns.iter().zip(&mut pinned) {
            let array = batch.column_by_name(column).ok_or_else(|| {
                SegmentEntityIdentityError::EntityColumnNotFound {
                    path: path.to_string(),
                    column: column.clone(),
                }
            })?;
            feed_entity_column(path, column, array, pinned)?;
        }

        tokio::task::yield_now().await;
    }

    Ok(pinned)
}

async fn scan_entity_row_groups(
    location: &TableLocation,
    rel_path: &Path,
    path: &str,
    entity_columns: &[String],
    metadata: ArrowReaderMetadata,
    row_groups: Vec<usize>,
) -> Result<Vec<Option<String>>, SegmentEntityIdentityError> {
    let columns = entity_columns.iter().map(String::as_str);
    let mask = ProjectionMask::columns(metadata.parquet_schema(), columns);
    let (max_tasks, row_groups_per_task) = resolve_rg_settings(row_groups.len());
    let chunks = row_groups
        .chunks(row_groups_per_task)
        .map(<[usize]>::to_vec)
        .collect::<Vec<_>>();
    debug_assert!(chunks.len() <= max_tasks);

    let mut tasks = JoinSet::new();
    for chunk in chunks {
        let location = location.clone();
        let rel_path = rel_path.to_path_buf();
        let path = path.to_string();
        let entity_columns = entity_columns.to_vec();
        let metadata = metadata.clone();
        let mask = mask.clone();

        tasks.spawn(async move {
            let file = open_local_file(location.as_ref(), &rel_path)
                .await
                .map_err(|source| SegmentEntityIdentityError::Storage {
                    path: path.clone(),
                    source,
                })?;
            let reader = ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata)
                .with_projection(mask)
                .with_row_groups(chunk)
                .with_batch_size(INSPECTION_BATCH_SIZE)
                .build()
                .map_err(|source| SegmentEntityIdentityError::ParquetRead {
                    path: path.clone(),
                    source,
                })?;
            scan_entity_batches(&path, &entity_columns, reader).await
        });
    }

    let mut pinned = vec![None; entity_columns.len()];
    while let Some(result) = tasks.join_next().await {
        let task_values = result.map_err(|source| SegmentEntityIdentityError::ParquetRead {
            path: path.to_string(),
            source: ParquetError::General(format!("row-group scan task failed: {source}")),
        })??;
        for ((column, pinned), value) in entity_columns.iter().zip(&mut pinned).zip(task_values) {
            if let Some(value) = value {
                merge_entity_value(path, column, pinned, &value)?;
            }
        }
    }

    Ok(pinned)
}

/// Extract entity identity values directly from a local Parquet file.
pub(crate) async fn segment_entity_identity_from_parquet(
    location: &TableLocation,
    rel_path: &Path,
    entity_columns: &[String],
) -> Result<EntityIdentity, SegmentEntityIdentityError> {
    let path = rel_path.display().to_string();

    if entity_columns.is_empty() {
        return Ok(EntityIdentity::new());
    }

    let mut file = open_local_file(location.as_ref(), rel_path)
        .await
        .map_err(|source| SegmentEntityIdentityError::Storage {
            path: path.clone(),
            source,
        })?;
    let metadata = ArrowReaderMetadata::load_async(&mut file, ArrowReaderOptions::default())
        .await
        .map_err(|source| SegmentEntityIdentityError::ParquetRead {
            path: path.clone(),
            source,
        })?;
    let EntityScanPlan {
        mut pinned,
        row_groups_to_scan,
    } = plan_entity_scan(
        metadata.metadata(),
        &path,
        entity_columns,
        metadata.schema(),
    )?;
    drop(file);

    if !row_groups_to_scan.is_empty() {
        let scanned = scan_entity_row_groups(
            location,
            rel_path,
            &path,
            entity_columns,
            metadata,
            row_groups_to_scan,
        )
        .await?;
        for ((column, pinned), value) in entity_columns.iter().zip(&mut pinned).zip(scanned) {
            if let Some(value) = value {
                merge_entity_value(&path, column, pinned, &value)?;
            }
        }
    }

    let mut out = EntityIdentity::new();
    for (col, v) in entity_columns.iter().zip(pinned) {
        let Some(v) = v else {
            return Err(SegmentEntityIdentityError::EntityColumnEmpty {
                path: path.clone(),
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
    use std::{
        fs::{File, OpenOptions},
        io::{Read, Seek, SeekFrom, Write},
        sync::Arc,
    };

    use arrow::{
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use arrow_array::{
        ArrayRef, Int32Array, LargeStringArray, StringArray, builder::BinaryBuilder,
    };
    use parquet::{
        arrow::ArrowWriter,
        basic::Compression,
        file::{
            metadata::ParquetMetaDataWriter,
            properties::{EnabledStatistics, WriterProperties},
            reader::{FileReader, SerializedFileReader},
        },
    };
    use tempfile::TempDir;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn make_batch(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(schema, columns).expect("record batch")
    }

    fn write_parquet_batches(
        path: &Path,
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
        props: WriterProperties,
    ) -> TestResult {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(props))?;
        for batch in batches {
            writer.write(&batch)?;
            writer.flush()?;
        }
        writer.close()?;
        Ok(())
    }

    async fn identity_from_file(
        tmp: &TempDir,
        rel_path: &Path,
        entity_columns: &[String],
    ) -> Result<EntityIdentity, SegmentEntityIdentityError> {
        segment_entity_identity_from_parquet(
            &TableLocation::local(tmp.path()),
            rel_path,
            entity_columns,
        )
        .await
    }

    fn string_array(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn large_string_array(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(LargeStringArray::from(values.to_vec()))
    }

    fn clear_column_statistics(
        path: &Path,
        row_group_index: usize,
        column_index: usize,
    ) -> TestResult {
        let reader = SerializedFileReader::new(File::open(path)?)?;
        let mut metadata = reader.metadata().clone().into_builder();
        let mut row_groups = metadata.take_row_groups();
        let mut row_group = row_groups[row_group_index].clone().into_builder();
        let mut columns = row_group.take_columns();
        columns[column_index] = columns[column_index]
            .clone()
            .into_builder()
            .clear_statistics()
            .build()?;
        row_groups[row_group_index] = row_group.set_column_metadata(columns).build()?;
        let metadata = metadata
            .set_row_groups(row_groups)
            .set_column_index(None)
            .set_offset_index(None)
            .build();
        drop(reader);

        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let file_len = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::End(-8))?;
        let mut footer = [0; 8];
        file.read_exact(&mut footer)?;
        assert_eq!(&footer[4..], b"PAR1");
        let metadata_len = u32::from_le_bytes(footer[..4].try_into()?) as u64;
        let metadata_start = file_len - metadata_len - 8;
        file.set_len(metadata_start)?;
        file.seek(SeekFrom::Start(metadata_start))?;
        ParquetMetaDataWriter::new(&mut file, &metadata).finish()?;
        file.flush()?;
        Ok(())
    }

    #[tokio::test]
    async fn empty_entity_columns_return_without_opening_the_file() {
        let identity = segment_entity_identity_from_parquet(
            &TableLocation::local("missing-table"),
            Path::new("not-parquet"),
            &[],
        )
        .await
        .expect("empty columns");
        assert!(identity.is_empty());
    }

    #[tokio::test]
    async fn exact_utf8_statistics_produce_identity() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/utf8.parquet");
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
        write_parquet_batches(
            &tmp.path().join(rel_path),
            Arc::clone(&schema),
            vec![batch],
            WriterProperties::builder().build(),
        )?;

        let identity = identity_from_file(&tmp, rel_path, &[String::from("entity")]).await?;
        assert_eq!(identity.get("entity").map(String::as_str), Some("alpha"));
        Ok(())
    }

    #[tokio::test]
    async fn exact_large_utf8_statistics_produce_identity() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/large_utf8.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::LargeUtf8,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![large_string_array(&[Some("alpha"), Some("alpha")])],
        );
        write_parquet_batches(
            &tmp.path().join(rel_path),
            Arc::clone(&schema),
            vec![batch],
            WriterProperties::builder().build(),
        )?;

        let identity = identity_from_file(&tmp, rel_path, &[String::from("entity")]).await?;
        assert_eq!(identity.get("entity").map(String::as_str), Some("alpha"));
        Ok(())
    }

    #[tokio::test]
    async fn multiple_entity_columns_are_extracted() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/multiple_columns.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("site", DataType::Utf8, false),
            Field::new("sensor", DataType::Utf8, false),
        ]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![
                string_array(&[Some("west"), Some("west")]),
                string_array(&[Some("temperature"), Some("temperature")]),
            ],
        );
        write_parquet_batches(
            &tmp.path().join(rel_path),
            schema,
            vec![batch],
            WriterProperties::builder().build(),
        )?;

        let identity = identity_from_file(
            &tmp,
            rel_path,
            &[String::from("site"), String::from("sensor")],
        )
        .await?;
        assert_eq!(identity.get("site").map(String::as_str), Some("west"));
        assert_eq!(
            identity.get("sensor").map(String::as_str),
            Some("temperature")
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_column_returns_path_and_column() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/missing_column.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("alpha")])],
        );
        write_parquet_batches(
            &tmp.path().join(rel_path),
            schema,
            vec![batch],
            WriterProperties::builder().build(),
        )?;

        let err = identity_from_file(&tmp, rel_path, &[String::from("missing")])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnNotFound { path, column }
                if path == "data/missing_column.parquet" && column == "missing"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_entity_type_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/unsupported.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Int32,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef],
        );
        write_parquet_batches(
            &tmp.path().join(rel_path),
            schema,
            vec![batch],
            WriterProperties::builder().build(),
        )?;

        let err = identity_from_file(&tmp, rel_path, &[String::from("entity")])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnUnsupportedType { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn statistics_reject_nulls() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/null_stats.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            true,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), None])],
        );
        write_parquet_batches(
            &tmp.path().join(rel_path),
            schema,
            vec![batch],
            WriterProperties::builder().build(),
        )?;

        let err = identity_from_file(&tmp, rel_path, &[String::from("entity")])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnHasNull { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn optional_column_scans_when_zero_null_count_is_ambiguous() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/optional.parquet");
        let abs_path = tmp.path().join(rel_path);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            true,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("alpha")])],
        );
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        write_parquet_batches(&abs_path, schema, vec![batch], props)?;

        let reader = SerializedFileReader::new(File::open(&abs_path)?)?;
        let data_page = reader.metadata().row_group(0).column(0).data_page_offset() as u64;
        drop(reader);
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&abs_path)
            .await?;
        file.seek(SeekFrom::Start(data_page)).await?;
        file.write_all(&[0xFF; 16]).await?;
        file.flush().await?;
        drop(file);

        let err = identity_from_file(&tmp, rel_path, &[String::from("entity")])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::ParquetRead { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn statistics_reject_multiple_values() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/multiple_stats.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let batch = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("beta")])],
        );
        write_parquet_batches(
            &tmp.path().join(rel_path),
            schema,
            vec![batch],
            WriterProperties::builder().build(),
        )?;

        let err = identity_from_file(&tmp, rel_path, &[String::from("entity")])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnMultipleValues { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn empty_segment_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/empty.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let batch = make_batch(Arc::clone(&schema), vec![string_array(&[])]);
        write_parquet_batches(
            &tmp.path().join(rel_path),
            schema,
            vec![batch],
            WriterProperties::builder().build(),
        )?;

        let err = identity_from_file(&tmp, rel_path, &[String::from("entity")])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnEmpty { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn missing_statistics_scan_only_the_affected_row_group() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/partial_stats.parquet");
        let abs_path = tmp.path().join(rel_path);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let first = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("alpha")])],
        );
        let second = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("alpha")])],
        );
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        write_parquet_batches(&abs_path, schema, vec![first, second], props)?;
        clear_column_statistics(&abs_path, 1, 0)?;

        let reader = SerializedFileReader::new(File::open(&abs_path)?)?;
        let first_page = reader.metadata().row_group(0).column(0).data_page_offset() as u64;
        drop(reader);
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&abs_path)
            .await?;
        file.seek(SeekFrom::Start(first_page)).await?;
        file.write_all(&[0xFF; 16]).await?;
        file.flush().await?;
        drop(file);

        let identity = identity_from_file(&tmp, rel_path, &[String::from("entity")]).await?;
        assert_eq!(identity.get("entity").map(String::as_str), Some("alpha"));
        Ok(())
    }

    #[tokio::test]
    async fn statistics_and_fallback_scan_reject_conflicting_values() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/hybrid_conflict.parquet");
        let abs_path = tmp.path().join(rel_path);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let first = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("alpha"), Some("alpha")])],
        );
        let second = make_batch(
            Arc::clone(&schema),
            vec![string_array(&[Some("beta"), Some("beta")])],
        );
        write_parquet_batches(
            &abs_path,
            schema,
            vec![first, second],
            WriterProperties::builder().build(),
        )?;
        clear_column_statistics(&abs_path, 1, 0)?;

        let err = identity_from_file(&tmp, rel_path, &[String::from("entity")])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnMultipleValues { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn fallback_scan_rejects_nulls() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/null_scan.parquet");
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
        write_parquet_batches(&tmp.path().join(rel_path), schema, vec![batch], props)?;

        let err = identity_from_file(&tmp, rel_path, &[String::from("entity")])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnHasNull { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn fallback_scan_rejects_multiple_values() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/multiple_scan.parquet");
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
        write_parquet_batches(&tmp.path().join(rel_path), schema, vec![batch], props)?;

        let err = identity_from_file(&tmp, rel_path, &[String::from("entity")])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::EntityColumnMultipleValues { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn fallback_scan_handles_multiple_bounded_batches() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/multiple_batches.parquet");
        let row_count = INSPECTION_BATCH_SIZE * 2 + 17;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "entity",
            DataType::Utf8,
            false,
        )]));
        let values = StringArray::from_iter_values(std::iter::repeat_n("alpha", row_count));
        let batch = make_batch(Arc::clone(&schema), vec![Arc::new(values)]);
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        write_parquet_batches(&tmp.path().join(rel_path), schema, vec![batch], props)?;

        let identity = identity_from_file(&tmp, rel_path, &[String::from("entity")]).await?;
        assert_eq!(identity.get("entity").map(String::as_str), Some("alpha"));
        Ok(())
    }

    #[tokio::test]
    async fn fallback_scan_ignores_large_unprojected_payload() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/large_payload.parquet");
        let abs_path = tmp.path().join(rel_path);
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("payload", DataType::Binary, false),
        ]));
        let mut payloads = BinaryBuilder::with_capacity(4, 4 * 1024 * 1024);
        let payload = vec![0xA5; 1024 * 1024];
        for _ in 0..4 {
            payloads.append_value(&payload);
        }
        let batch = make_batch(
            Arc::clone(&schema),
            vec![
                string_array(&[Some("alpha"), Some("alpha"), Some("alpha"), Some("alpha")]),
                Arc::new(payloads.finish()),
            ],
        );
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        write_parquet_batches(&abs_path, schema, vec![batch], props)?;

        let reader = SerializedFileReader::new(File::open(&abs_path)?)?;
        let payload_page = reader.metadata().row_group(0).column(1).data_page_offset() as u64;
        drop(reader);
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&abs_path)
            .await?;
        file.seek(SeekFrom::Start(payload_page)).await?;
        file.write_all(&[0xFF; 32]).await?;
        file.flush().await?;
        drop(file);

        assert!(tokio::fs::metadata(&abs_path).await?.len() > 4 * 1024 * 1024);
        let identity = identity_from_file(&tmp, rel_path, &[String::from("entity")]).await?;
        assert_eq!(identity.get("entity").map(String::as_str), Some("alpha"));
        Ok(())
    }

    #[tokio::test]
    async fn invalid_parquet_returns_read_error() -> TestResult {
        let tmp = TempDir::new()?;
        let rel_path = Path::new("data/invalid.parquet");
        tokio::fs::create_dir_all(tmp.path().join("data")).await?;
        tokio::fs::write(tmp.path().join(rel_path), b"not parquet").await?;

        let err = identity_from_file(&tmp, rel_path, &[String::from("entity")])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            SegmentEntityIdentityError::ParquetRead { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn missing_file_returns_storage_error() -> TestResult {
        let tmp = TempDir::new()?;
        let err = identity_from_file(
            &tmp,
            Path::new("data/missing.parquet"),
            &[String::from("entity")],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SegmentEntityIdentityError::Storage { .. }));
        Ok(())
    }
}
