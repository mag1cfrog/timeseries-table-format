//! Parquet segment metadata derivation.
//!
//! This module extracts per-segment metadata (index bounds, row count, etc.)
//! from stored Parquet segments through the storage abstraction.

use std::path::Path;

use chrono::{DateTime, TimeZone, Utc};
use futures::{Stream, StreamExt};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    async_reader::ParquetRecordBatchStreamBuilder,
};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;

use snafu::Backtrace;
use tokio::task::JoinSet;

use crate::metadata::segments::ParquetIndexColumnError;
use crate::metadata::table_metadata::{IndexKind, IndexSpec, IndexValue};
use crate::storage::{TableLocation, file_size, open_parquet_reader};
use crate::transaction_log::segments::{SegmentError, SegmentMetaError, SegmentResult};
use crate::transaction_log::{FileFormat, SegmentEntityLayout, SegmentMeta};

use super::schema::{ParquetIndexKind, ParquetTimestampUnit, validate_parquet_index};

#[derive(Debug, Clone, Copy)]
enum TimestampUnit {
    Seconds,
    Millis,
    Micros,
    Nanos,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IndexBounds {
    min: IndexValue,
    max: IndexValue,
}

struct IndexStatsPlan {
    bounds: Option<IndexBounds>,
    row_groups_to_scan: Vec<usize>,
}

fn compare_index_values(
    path: &str,
    left: &IndexValue,
    right: &IndexValue,
) -> Result<std::cmp::Ordering, SegmentMetaError> {
    left.compare(right)
        .map_err(|source| SegmentMetaError::InvalidIndexBounds {
            path: path.to_string(),
            source,
        })
}

fn observe_index_value(
    path: &str,
    bounds: &mut Option<IndexBounds>,
    value: IndexValue,
) -> Result<(), SegmentMetaError> {
    match bounds {
        Some(bounds) => {
            if compare_index_values(path, &value, &bounds.min)?.is_lt() {
                bounds.min = value.clone();
            }
            if compare_index_values(path, &value, &bounds.max)?.is_gt() {
                bounds.max = value;
            }
        }
        None => {
            *bounds = Some(IndexBounds {
                min: value.clone(),
                max: value,
            });
        }
    }
    Ok(())
}

fn merge_index_bounds(
    path: &str,
    current: &mut Option<IndexBounds>,
    next: IndexBounds,
) -> Result<(), SegmentMetaError> {
    observe_index_value(path, current, next.min)?;
    observe_index_value(path, current, next.max)
}

fn timestamp_unit(unit: ParquetTimestampUnit) -> TimestampUnit {
    match unit {
        ParquetTimestampUnit::Millis => TimestampUnit::Millis,
        ParquetTimestampUnit::Micros => TimestampUnit::Micros,
        ParquetTimestampUnit::Nanos => TimestampUnit::Nanos,
    }
}

fn index_value_from_physical(
    path: &str,
    column: &str,
    kind: ParquetIndexKind,
    raw: i64,
) -> Result<IndexValue, SegmentMetaError> {
    match kind {
        ParquetIndexKind::Timestamp(unit) => {
            ts_from_i64(path, column, timestamp_unit(unit), raw).map(IndexValue::Timestamp)
        }
        ParquetIndexKind::Int64 => Ok(IndexValue::Int64(raw)),
        ParquetIndexKind::UInt64 => Ok(IndexValue::UInt64(raw as u64)),
    }
}

/// Use exact row-group statistics and identify only the row groups that require scanning.
fn plan_index_scan(
    path: &str,
    column: &str,
    column_index: usize,
    kind: ParquetIndexKind,
    metadata: &ParquetMetaData,
) -> Result<IndexStatsPlan, SegmentMetaError> {
    let mut bounds = None;
    let mut row_groups_to_scan = Vec::new();

    for (row_group_index, rg) in metadata.row_groups().iter().enumerate() {
        if rg.num_rows() == 0 {
            continue;
        }
        let col_meta = rg.column(column_index);

        let stats = match col_meta.statistics() {
            Some(s) => s,
            None => {
                row_groups_to_scan.push(row_group_index);
                continue;
            }
        };

        let (raw_min, raw_max) = match stats {
            Statistics::Int64(stats) if stats.min_is_exact() && stats.max_is_exact() => {
                match (stats.min_opt(), stats.max_opt()) {
                    (Some(min), Some(max)) => (*min, *max),
                    _ => {
                        row_groups_to_scan.push(row_group_index);
                        continue;
                    }
                }
            }
            Statistics::Int64(_) => {
                row_groups_to_scan.push(row_group_index);
                continue;
            }
            stats => {
                return Err(SegmentMetaError::ParquetStatsShape {
                    path: path.to_string(),
                    column: column.to_string(),
                    detail: format!("expected INT64 statistics, got {:?}", stats.physical_type()),
                });
            }
        };

        let group_bounds = IndexBounds {
            min: index_value_from_physical(path, column, kind, raw_min)?,
            max: index_value_from_physical(path, column, kind, raw_max)?,
        };
        if compare_index_values(path, &group_bounds.min, &group_bounds.max)?.is_gt() {
            return Err(SegmentMetaError::ParquetStatsShape {
                path: path.to_string(),
                column: column.to_string(),
                detail: format!(
                    "{} statistics minimum {} exceeds maximum {}",
                    group_bounds.min.kind_name(),
                    group_bounds.min,
                    group_bounds.max
                ),
            });
        }

        merge_index_bounds(path, &mut bounds, group_bounds)?;
    }

    Ok(IndexStatsPlan {
        bounds,
        row_groups_to_scan,
    })
}

use super::{INSPECTION_BATCH_SIZE, resolve_rg_settings};

fn arrow_index_error(path: &str, index: &IndexSpec, observed_type: String) -> SegmentMetaError {
    SegmentMetaError::OrderedIndexColumn {
        source: ParquetIndexColumnError {
            path: path.to_string(),
            column: index.column.clone(),
            expected_domain: index.kind.name(),
            observed_type,
        },
    }
}

fn observe_primitive_array<T, F>(
    path: &str,
    bounds: &mut Option<IndexBounds>,
    array: &arrow_array::PrimitiveArray<T>,
    mut to_value: F,
) -> Result<(), SegmentMetaError>
where
    T: arrow_array::types::ArrowPrimitiveType,
    F: FnMut(T::Native) -> Result<IndexValue, SegmentMetaError>,
{
    use arrow_array::Array;

    if array.null_count() == 0 {
        for &raw in array.values() {
            observe_index_value(path, bounds, to_value(raw)?)?;
        }
    } else {
        for raw in array.iter().flatten() {
            observe_index_value(path, bounds, to_value(raw)?)?;
        }
    }
    Ok(())
}

async fn scan_index_batches(
    path: &str,
    index: &IndexSpec,
    mut reader: impl Stream<
        Item = Result<arrow::record_batch::RecordBatch, parquet::errors::ParquetError>,
    > + Unpin,
) -> SegmentResult<(Option<IndexBounds>, u64)> {
    use arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
    use arrow_array::{
        Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
    };

    let mut bounds = None;
    let mut scanned_rows: u64 = 0;

    while let Some(batch_res) = reader.next().await {
        let batch = batch_res.map_err(|source| SegmentMetaError::ParquetRead {
            path: path.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })?;
        scanned_rows = scanned_rows.saturating_add(batch.num_rows() as u64);

        let col = batch.column(0);
        match (&index.kind, col.data_type()) {
            (IndexKind::Timestamp { .. }, DataType::Timestamp(unit, _)) => {
                let unit = match unit {
                    ArrowTimeUnit::Second => TimestampUnit::Seconds,
                    ArrowTimeUnit::Millisecond => TimestampUnit::Millis,
                    ArrowTimeUnit::Microsecond => TimestampUnit::Micros,
                    ArrowTimeUnit::Nanosecond => TimestampUnit::Nanos,
                };
                macro_rules! scan_timestamp_array {
                    ($array_type:ty) => {{
                        let array =
                            col.as_any().downcast_ref::<$array_type>().ok_or_else(|| {
                                arrow_index_error(path, index, format!("Arrow {}", col.data_type()))
                            })?;
                        observe_primitive_array(path, &mut bounds, array, |raw| {
                            ts_from_i64(path, &index.column, unit, raw).map(IndexValue::Timestamp)
                        })?;
                    }};
                }
                match unit {
                    TimestampUnit::Seconds => scan_timestamp_array!(TimestampSecondArray),
                    TimestampUnit::Millis => scan_timestamp_array!(TimestampMillisecondArray),
                    TimestampUnit::Micros => scan_timestamp_array!(TimestampMicrosecondArray),
                    TimestampUnit::Nanos => scan_timestamp_array!(TimestampNanosecondArray),
                }
            }
            (IndexKind::Int64 { .. }, DataType::Int64) => {
                let array = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    arrow_index_error(path, index, format!("Arrow {}", col.data_type()))
                })?;
                observe_primitive_array(path, &mut bounds, array, |raw| {
                    Ok(IndexValue::Int64(raw))
                })?;
            }
            (IndexKind::UInt64 { .. }, DataType::UInt64) => {
                let array = col.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                    arrow_index_error(path, index, format!("Arrow {}", col.data_type()))
                })?;
                observe_primitive_array(path, &mut bounds, array, |raw| {
                    Ok(IndexValue::UInt64(raw))
                })?;
            }
            other => {
                return Err(arrow_index_error(path, index, format!("Arrow {other:?}")).into());
            }
        }

        tokio::task::yield_now().await;
    }

    Ok((bounds, scanned_rows))
}

async fn scan_index_row_groups(
    location: &TableLocation,
    rel_path: &Path,
    path: &str,
    index: &IndexSpec,
    metadata: ArrowReaderMetadata,
    row_groups: Vec<usize>,
) -> SegmentResult<(Option<IndexBounds>, u64)> {
    let mask = ProjectionMask::columns(metadata.parquet_schema(), [index.column.as_str()]);
    let (max_tasks, row_groups_per_task) = resolve_rg_settings(row_groups.len());
    let chunks = row_groups
        .chunks(row_groups_per_task)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>();
    debug_assert!(chunks.len() <= max_tasks);

    let mut tasks = JoinSet::new();
    for chunk in chunks {
        let location = location.clone();
        let rel_path = rel_path.to_path_buf();
        let path = path.to_string();
        let index = index.clone();
        let metadata = metadata.clone();
        let mask = mask.clone();

        tasks.spawn(async move {
            let file = open_parquet_reader(location.as_ref(), &rel_path)
                .await
                .map_err(SegmentError::from)?;
            let reader = ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata)
                .with_projection(mask)
                .with_row_groups(chunk)
                .with_batch_size(INSPECTION_BATCH_SIZE)
                .build()
                .map_err(|source| SegmentMetaError::ParquetRead {
                    path: path.clone(),
                    source,
                    backtrace: Backtrace::capture(),
                })?;

            scan_index_batches(&path, &index, reader).await
        });
    }

    let mut bounds = None;
    let mut scanned_rows: u64 = 0;
    while let Some(result) = tasks.join_next().await {
        let (task_bounds, rows) = result.map_err(|source| SegmentMetaError::RowGroupTask {
            path: path.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })??;
        if let Some(task_bounds) = task_bounds {
            merge_index_bounds(path, &mut bounds, task_bounds)?;
        }
        scanned_rows = scanned_rows.saturating_add(rows);
    }

    Ok((bounds, scanned_rows))
}

fn ts_from_i64(
    path: &str,
    column: &str,
    unit: TimestampUnit,
    value: i64,
) -> Result<DateTime<Utc>, SegmentMetaError> {
    let dt_opt = match unit {
        TimestampUnit::Seconds => Utc.timestamp_opt(value, 0),
        TimestampUnit::Millis => Utc.timestamp_millis_opt(value),
        TimestampUnit::Micros => Utc.timestamp_micros(value),
        TimestampUnit::Nanos => {
            let secs = value.div_euclid(1_000_000_000);
            let nanos = value.rem_euclid(1_000_000_000) as u32;
            Utc.timestamp_opt(secs, nanos)
        }
    };

    dt_opt
        .single()
        .ok_or_else(|| SegmentMetaError::ParquetStatsShape {
            path: path.to_string(),
            column: column.to_string(),
            detail: format!("timestamp value {value} out of chrono range"),
        })
}

/// Profiling details collected while building `SegmentMeta`.
#[derive(Debug, Clone)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct SegmentMetaReport {
    /// Number of row groups reported by Parquet metadata.
    pub(crate) row_groups: usize,
    /// Total row count from file metadata.
    pub(crate) row_count: u64,
    /// True if no ordered-index rows needed to be scanned.
    pub(crate) used_stats: bool,
    /// Number of rows scanned during fallback (0 if stats were used).
    pub(crate) scanned_rows: u64,
}

/// Build segment metadata and profiling data from a stored Parquet segment.
pub(crate) async fn segment_meta_from_parquet(
    location: &TableLocation,
    rel_path: &Path,
    index: &IndexSpec,
) -> SegmentResult<(SegmentMeta, SegmentMetaReport)> {
    let path_str = rel_path.display().to_string();
    let file_size = file_size(location.as_ref(), rel_path)
        .await
        .map_err(SegmentError::from)?;
    if file_size < 8 {
        return Err(SegmentMetaError::TooShort {
            path: path_str.clone(),
        }
        .into());
    }

    let mut file = open_parquet_reader(location.as_ref(), rel_path)
        .await
        .map_err(SegmentError::from)?;

    let metadata = ArrowReaderMetadata::load_async(&mut file, ArrowReaderOptions::default())
        .await
        .map_err(|source| SegmentMetaError::ParquetRead {
            path: path_str.clone(),
            source,
            backtrace: Backtrace::capture(),
        })?;

    let (row_count, row_groups, stats_plan) =
        {
            let parquet_metadata = metadata.metadata();
            let file_meta = parquet_metadata.file_metadata();
            let row_count: u64 = file_meta.num_rows().try_into().map_err(|_| {
                SegmentMetaError::ParquetStatsShape {
                    path: path_str.clone(),
                    column: index.column.clone(),
                    detail: format!("negative row count {}", file_meta.num_rows()),
                }
            })?;
            let row_groups = parquet_metadata.num_row_groups();
            let schema = file_meta.schema_descr();
            let validated = validate_parquet_index(&path_str, schema, index)
                .map_err(|source| SegmentMetaError::OrderedIndexColumn { source })?;
            let stats_plan = plan_index_scan(
                &path_str,
                &index.column,
                validated.leaf_index,
                validated.kind,
                parquet_metadata,
            )?;
            (row_count, row_groups, stats_plan)
        };
    drop(file);

    let IndexStatsPlan {
        mut bounds,
        row_groups_to_scan,
    } = stats_plan;
    let used_stats = row_groups_to_scan.is_empty();
    let scanned_rows = if used_stats {
        0
    } else {
        let (scanned_bounds, scanned_rows) = scan_index_row_groups(
            location,
            rel_path,
            &path_str,
            index,
            metadata,
            row_groups_to_scan,
        )
        .await?;
        if let Some(scanned_bounds) = scanned_bounds {
            merge_index_bounds(&path_str, &mut bounds, scanned_bounds)?;
        }
        scanned_rows
    };
    let bounds = bounds.ok_or_else(|| SegmentMetaError::NoObservedIndexValue {
        path: path_str.clone(),
        column: index.column.clone(),
        expected_domain: index.kind.name(),
    })?;

    let meta_out = SegmentMeta {
        path: path_str,
        format: FileFormat::Parquet,
        // Entity-aware append replaces this after deriving exact coverage.
        entity_layout: SegmentEntityLayout::NotApplicable,
        index_min: bounds.min,
        index_max: bounds.max,
        row_count,
        file_size: Some(file_size),
        coverage_path: None,
    };

    let report = SegmentMetaReport {
        row_groups,
        row_count,
        used_stats,
        scanned_rows,
    };

    Ok((meta_out, report))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::table_metadata::{IndexKind, IndexSpec, IndexValue, TimeIndexGranularity};
    use crate::transaction_log::segments::SegmentError;
    use arrow::array::{
        ArrayRef, BinaryBuilder, Int64Array, TimestampMillisecondArray, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, LogicalType, Repetition, TimeUnit, Type as PhysicalType};
    use parquet::column::writer::ColumnWriter;
    use parquet::errors::ParquetError;
    use parquet::file::metadata::{
        ColumnChunkMetaData, FileMetaData, ParquetMetaDataWriter, RowGroupMetaData,
    };
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::{SchemaDescriptor, Type};
    use snafu::ErrorCompat;
    use std::error::Error as _;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::num::NonZeroU64;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::fs;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn timestamp_index(column: &str) -> IndexSpec {
        IndexSpec {
            column: column.to_string(),
            entity_columns: Vec::new(),
            kind: IndexKind::Timestamp {
                index_granularity: TimeIndexGranularity::Seconds(1),
                timezone: None,
            },
        }
    }

    fn int64_index(column: &str) -> IndexSpec {
        IndexSpec {
            column: column.to_string(),
            entity_columns: Vec::new(),
            kind: IndexKind::Int64 {
                index_granularity: NonZeroU64::MIN,
            },
        }
    }

    fn uint64_index(column: &str) -> IndexSpec {
        IndexSpec {
            column: column.to_string(),
            entity_columns: Vec::new(),
            kind: IndexKind::UInt64 {
                index_granularity: NonZeroU64::MIN,
            },
        }
    }

    fn timestamp(value: &IndexValue) -> &DateTime<Utc> {
        match value {
            IndexValue::Timestamp(value) => value,
            other => panic!("expected timestamp bound, found {other}"),
        }
    }

    fn int64(value: &IndexValue) -> i64 {
        match value {
            IndexValue::Int64(value) => *value,
            other => panic!("expected int64 bound, found {other}"),
        }
    }

    fn uint64(value: &IndexValue) -> u64 {
        match value {
            IndexValue::UInt64(value) => *value,
            other => panic!("expected uint64 bound, found {other}"),
        }
    }

    fn write_parquet_file(
        path: &Path,
        time_column: &str,
        logical: Option<&str>,
        physical: PhysicalType,
        values: &[i64],
        stats_enabled: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_parquet_row_groups(
            path,
            time_column,
            logical,
            physical,
            &[values],
            stats_enabled,
        )
    }

    fn write_parquet_row_groups(
        path: &Path,
        time_column: &str,
        logical: Option<&str>,
        physical: PhysicalType,
        row_groups: &[&[i64]],
        stats_enabled: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut builder = Type::primitive_type_builder(time_column, physical)
            .with_repetition(Repetition::REQUIRED);

        if let Some(l) = logical {
            let lt = match l {
                "TIMESTAMP_MILLIS" => LogicalType::timestamp(true, TimeUnit::MILLIS),
                "TIMESTAMP_MICROS" => LogicalType::timestamp(true, TimeUnit::MICROS),
                "TIMESTAMP_NANOS" => LogicalType::timestamp(true, TimeUnit::NANOS),
                "INT_64" => LogicalType::integer(64, true),
                "UINT_64" => LogicalType::integer(64, false),
                other => return Err(format!("unsupported logical for test: {other}").into()),
            };
            builder = builder.with_logical_type(Some(lt));
        }

        let col = Arc::new(builder.build()?);
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );

        let props = WriterProperties::builder()
            .set_offset_index_disabled(true)
            .set_column_index_truncate_length(None);
        let props = if stats_enabled {
            props.build()
        } else {
            props
                .set_statistics_enabled(EnabledStatistics::None)
                .build()
        };

        let file = File::create(path)?;
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        for values in row_groups {
            let mut row_group_writer = writer.next_row_group()?;
            while let Some(mut col_writer) = row_group_writer.next_column()? {
                match col_writer.untyped() {
                    ColumnWriter::Int64ColumnWriter(typed) => {
                        typed.write_batch(values, None, None)?;
                    }
                    ColumnWriter::Int32ColumnWriter(typed) => {
                        let downcast: Vec<i32> = values.iter().map(|v| *v as i32).collect();
                        typed.write_batch(&downcast, None, None)?;
                    }
                    _ => return Err("unexpected column writer type".into()),
                }
                col_writer.close()?;
            }
            row_group_writer.close()?;
        }
        writer.close()?;
        Ok(())
    }

    fn write_arrow_timestamp_file(
        path: &Path,
        timestamps: &[Option<i64>],
        payload_size: Option<usize>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_arrow_index_file(
            path,
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
            payload_size,
            false,
        )
    }

    fn write_arrow_index_file(
        path: &Path,
        column: &str,
        data_type: DataType,
        index_values: ArrayRef,
        payload_size: Option<usize>,
        stats_enabled: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let row_count = index_values.len();
        let mut fields = vec![Field::new(column, data_type, true)];
        let mut arrays = vec![index_values];

        if let Some(payload_size) = payload_size {
            fields.push(Field::new("payload", DataType::Binary, false));
            let payload = vec![0xA5; payload_size];
            let mut payloads = BinaryBuilder::with_capacity(row_count, row_count * payload_size);
            for _ in 0..row_count {
                payloads.append_value(&payload);
            }
            arrays.push(Arc::new(payloads.finish()));
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false);
        let props = if stats_enabled {
            props.build()
        } else {
            props
                .set_statistics_enabled(EnabledStatistics::None)
                .build()
        };
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn write_arrow_int64_file(
        path: &Path,
        column: &str,
        values: &[Option<i64>],
        stats_enabled: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_arrow_index_file(
            path,
            column,
            DataType::Int64,
            Arc::new(Int64Array::from(values.to_vec())),
            None,
            stats_enabled,
        )
    }

    fn write_arrow_uint64_file(
        path: &Path,
        column: &str,
        values: &[Option<u64>],
        stats_enabled: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_arrow_index_file(
            path,
            column,
            DataType::UInt64,
            Arc::new(UInt64Array::from(values.to_vec())),
            None,
            stats_enabled,
        )
    }

    fn clear_index_statistics(
        path: &Path,
        row_group_index: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reader = SerializedFileReader::new(File::open(path)?)?;
        let mut metadata = reader.metadata().clone().into_builder();
        let mut row_groups = metadata.take_row_groups();
        let mut row_group = row_groups[row_group_index].clone().into_builder();
        let mut columns = row_group.take_columns();
        columns[0] = columns[0]
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

    fn metadata_with_int64_statistics(
        stats: Statistics,
    ) -> Result<ParquetMetaData, Box<dyn std::error::Error>> {
        let column = Arc::new(
            Type::primitive_type_builder("ts", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::timestamp(true, TimeUnit::MILLIS)))
                .build()?,
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![column])
                .build()?,
        );
        let schema = Arc::new(SchemaDescriptor::new(schema));
        let column = ColumnChunkMetaData::builder(schema.column(0))
            .set_num_values(2)
            .set_statistics(stats)
            .build()?;
        let row_group = RowGroupMetaData::builder(Arc::clone(&schema))
            .set_num_rows(2)
            .add_column_metadata(column)
            .build()?;
        let file = FileMetaData::new(1, 2, None, None, schema, None);
        Ok(ParquetMetaData::new(file, vec![row_group]))
    }

    #[test]
    fn ts_from_i64_out_of_range_is_error() {
        let err = ts_from_i64("path", "ts", TimestampUnit::Millis, i64::MAX).unwrap_err();
        assert!(matches!(err, SegmentMetaError::ParquetStatsShape { .. }));
    }

    #[test]
    fn inexact_statistics_force_fallback() -> TestResult {
        let stats = match Statistics::int64(Some(10), Some(20), None, Some(0), false) {
            Statistics::Int64(stats) => Statistics::Int64(stats.with_min_is_exact(false)),
            _ => unreachable!(),
        };
        let metadata = metadata_with_int64_statistics(stats)?;

        for kind in [
            ParquetIndexKind::Timestamp(ParquetTimestampUnit::Millis),
            ParquetIndexKind::Int64,
            ParquetIndexKind::UInt64,
        ] {
            let plan = plan_index_scan("path", "index", 0, kind, &metadata)?;
            assert_eq!(plan.bounds, None);
            assert_eq!(plan.row_groups_to_scan, vec![0]);
        }
        Ok(())
    }

    #[test]
    fn inverted_statistics_are_rejected() -> TestResult {
        for (kind, min, max, domain) in [
            (
                ParquetIndexKind::Timestamp(ParquetTimestampUnit::Millis),
                20,
                10,
                "timestamp",
            ),
            (ParquetIndexKind::Int64, 20, 10, "int64"),
            (ParquetIndexKind::UInt64, -1, 0, "uint64"),
        ] {
            let stats = Statistics::int64(Some(min), Some(max), None, Some(0), false);
            let metadata = metadata_with_int64_statistics(stats)?;
            let err = plan_index_scan("data/inverted.parquet", "index", 0, kind, &metadata)
                .err()
                .expect("inverted statistics must fail");

            assert!(matches!(
                err,
                SegmentMetaError::ParquetStatsShape {
                    path,
                    column,
                    detail,
                } if path == "data/inverted.parquet"
                    && column == "index"
                    && detail.contains(&format!("{domain} statistics minimum"))
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn segment_meta_happy_path_uses_stats() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/ts.parquet");
        let abs = tmp.path().join(rel_path);

        write_parquet_file(
            &abs,
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &[10, 20, 30],
            true,
        )?;

        let (meta, report) =
            segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await?;

        assert_eq!(timestamp(&meta.index_min).timestamp_millis(), 10);
        assert_eq!(timestamp(&meta.index_max).timestamp_millis(), 30);
        assert_eq!(meta.row_count, 3);
        let len = fs::metadata(&abs).await?.len();
        assert_eq!(meta.file_size, Some(len));
        assert_eq!(report.row_groups, 1);
        assert_eq!(report.row_count, 3);
        assert!(report.used_stats);
        assert_eq!(report.scanned_rows, 0);
        Ok(())
    }

    #[tokio::test]
    async fn segment_meta_preserves_int64_extremes_in_stats_and_fallback() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let values = [i64::MIN, 0, i64::MAX];

        let stats_path = Path::new("data/int64-stats.parquet");
        write_parquet_file(
            &tmp.path().join(stats_path),
            "index",
            Some("INT_64"),
            PhysicalType::INT64,
            &values,
            true,
        )?;
        let (stats_meta, stats_report) =
            segment_meta_from_parquet(&location, stats_path, &int64_index("index")).await?;
        assert_eq!(int64(&stats_meta.index_min), i64::MIN);
        assert_eq!(int64(&stats_meta.index_max), i64::MAX);
        assert!(stats_report.used_stats);
        assert_eq!(stats_report.scanned_rows, 0);

        let fallback_path = Path::new("data/int64-fallback.parquet");
        write_arrow_int64_file(
            &tmp.path().join(fallback_path),
            "index",
            &values.map(Some),
            false,
        )?;
        let (fallback_meta, fallback_report) =
            segment_meta_from_parquet(&location, fallback_path, &int64_index("index")).await?;
        assert_eq!(int64(&fallback_meta.index_min), i64::MIN);
        assert_eq!(int64(&fallback_meta.index_max), i64::MAX);
        assert!(!fallback_report.used_stats);
        assert_eq!(fallback_report.scanned_rows, values.len() as u64);
        Ok(())
    }

    #[tokio::test]
    async fn segment_meta_preserves_uint64_extremes_in_stats_and_fallback() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let values = [0, i64::MAX as u64 + 1, u64::MAX];

        for (name, stats_enabled) in [("stats", true), ("fallback", false)] {
            let rel_path = PathBuf::from(format!("data/uint64-{name}.parquet"));
            write_arrow_uint64_file(
                &tmp.path().join(&rel_path),
                "index",
                &values.map(Some),
                stats_enabled,
            )?;

            let (meta, report) =
                segment_meta_from_parquet(&location, &rel_path, &uint64_index("index")).await?;
            assert_eq!(uint64(&meta.index_min), 0);
            assert_eq!(uint64(&meta.index_max), u64::MAX);
            assert_eq!(report.used_stats, stats_enabled);
            assert_eq!(
                report.scanned_rows,
                if stats_enabled {
                    0
                } else {
                    values.len() as u64
                }
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn segment_meta_combines_out_of_order_row_group_statistics() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/multiple_stats.parquet");
        let row_groups: [&[i64]; 3] = [&[100, 200], &[-50, 0], &[25, 400]];

        write_parquet_row_groups(
            &tmp.path().join(rel_path),
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &row_groups,
            true,
        )?;

        let (meta, report) =
            segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await?;

        assert_eq!(timestamp(&meta.index_min).timestamp_millis(), -50);
        assert_eq!(timestamp(&meta.index_max).timestamp_millis(), 400);
        assert_eq!(meta.row_count, 6);
        assert_eq!(report.row_groups, 3);
        assert!(report.used_stats);
        assert_eq!(report.scanned_rows, 0);
        Ok(())
    }

    #[tokio::test]
    async fn segment_meta_falls_back_to_scan_when_stats_missing() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/no_stats.parquet");
        let abs = tmp.path().join(rel_path);

        write_parquet_file(
            &abs,
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &[5, 7],
            false,
        )?;

        let (meta, report) =
            segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await?;

        assert_eq!(timestamp(&meta.index_min).timestamp_millis(), 5);
        assert_eq!(timestamp(&meta.index_max).timestamp_millis(), 7);
        assert_eq!(meta.row_count, 2);
        assert!(!report.used_stats);
        assert_eq!(report.scanned_rows, 2);
        Ok(())
    }

    #[tokio::test]
    async fn fallback_scans_only_the_row_group_missing_statistics() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/partial_stats.parquet");
        let abs = tmp.path().join(rel_path);
        let row_groups: [&[i64]; 3] = [&[10, 20], &[-100, 300], &[30, 40]];

        write_parquet_row_groups(
            &abs,
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &row_groups,
            true,
        )?;
        clear_index_statistics(&abs, 1)?;

        let (meta, report) =
            segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await?;

        assert_eq!(timestamp(&meta.index_min).timestamp_millis(), -100);
        assert_eq!(timestamp(&meta.index_max).timestamp_millis(), 300);
        assert_eq!(meta.row_count, 6);
        assert_eq!(report.row_groups, 3);
        assert!(!report.used_stats);
        assert_eq!(report.scanned_rows, 2);
        Ok(())
    }

    #[tokio::test]
    async fn int64_fallback_scans_only_row_groups_without_usable_stats() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/int64-partial-stats.parquet");
        let abs = tmp.path().join(rel_path);
        let row_groups: [&[i64]; 3] = [&[10, 20], &[i64::MIN, i64::MAX], &[30, 40]];

        write_parquet_row_groups(
            &abs,
            "index",
            Some("INT_64"),
            PhysicalType::INT64,
            &row_groups,
            true,
        )?;
        clear_index_statistics(&abs, 1)?;

        let (meta, report) =
            segment_meta_from_parquet(&location, rel_path, &int64_index("index")).await?;
        assert_eq!(int64(&meta.index_min), i64::MIN);
        assert_eq!(int64(&meta.index_max), i64::MAX);
        assert!(!report.used_stats);
        assert_eq!(report.scanned_rows, 2);
        Ok(())
    }

    #[tokio::test]
    async fn empty_row_group_does_not_force_fallback() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/incomplete_stats.parquet");
        let row_groups: [&[i64]; 3] = [&[10, 20], &[], &[-5, 30]];

        write_parquet_row_groups(
            &tmp.path().join(rel_path),
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &row_groups,
            true,
        )?;

        let (meta, report) =
            segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await?;

        assert_eq!(timestamp(&meta.index_min).timestamp_millis(), -5);
        assert_eq!(timestamp(&meta.index_max).timestamp_millis(), 30);
        assert_eq!(meta.row_count, 4);
        assert_eq!(report.row_groups, 3);
        assert!(report.used_stats);
        assert_eq!(report.scanned_rows, 0);
        Ok(())
    }

    #[tokio::test]
    async fn fallback_scans_multiple_bounded_batches() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/multiple_batches.parquet");
        let row_count = INSPECTION_BATCH_SIZE * 2 + 17;
        let values = (0..row_count as i64).rev().collect::<Vec<_>>();

        write_parquet_file(
            &tmp.path().join(rel_path),
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &values,
            false,
        )?;

        let (meta, report) =
            segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await?;

        assert_eq!(timestamp(&meta.index_min).timestamp_millis(), 0);
        assert_eq!(
            timestamp(&meta.index_max).timestamp_millis(),
            row_count as i64 - 1
        );
        assert_eq!(meta.row_count, row_count as u64);
        assert!(!report.used_stats);
        assert_eq!(report.scanned_rows, row_count as u64);
        Ok(())
    }

    #[tokio::test]
    async fn fallback_ignores_nulls_but_counts_scanned_rows() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/nulls.parquet");

        write_arrow_timestamp_file(
            &tmp.path().join(rel_path),
            &[None, Some(20), None, Some(-10), None],
            None,
        )?;

        let (meta, report) =
            segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await?;

        assert_eq!(timestamp(&meta.index_min).timestamp_millis(), -10);
        assert_eq!(timestamp(&meta.index_max).timestamp_millis(), 20);
        assert_eq!(meta.row_count, 5);
        assert!(!report.used_stats);
        assert_eq!(report.scanned_rows, 5);
        Ok(())
    }

    #[tokio::test]
    async fn fallback_rejects_an_all_null_timestamp_column() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/all_null.parquet");

        write_arrow_timestamp_file(&tmp.path().join(rel_path), &[None, None, None], None)?;

        let result = segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await;

        assert!(matches!(
            result,
            Err(SegmentError::Metadata {
                source: SegmentMetaError::NoObservedIndexValue {
                    path,
                    column,
                    expected_domain: "timestamp"
                }
            }) if path == "data/all_null.parquet" && column == "ts"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn fallback_rejects_all_null_integer_indexes() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let signed_path = Path::new("data/all-null-int64.parquet");
        write_arrow_int64_file(&tmp.path().join(signed_path), "index", &[None, None], false)?;
        let signed = segment_meta_from_parquet(&location, signed_path, &int64_index("index")).await;
        assert!(matches!(
            signed,
            Err(SegmentError::Metadata {
                source: SegmentMetaError::NoObservedIndexValue {
                    expected_domain: "int64",
                    ..
                }
            })
        ));

        let unsigned_path = Path::new("data/all-null-uint64.parquet");
        write_arrow_uint64_file(
            &tmp.path().join(unsigned_path),
            "index",
            &[None, None],
            false,
        )?;
        let unsigned =
            segment_meta_from_parquet(&location, unsigned_path, &uint64_index("index")).await;
        assert!(matches!(
            unsigned,
            Err(SegmentError::Metadata {
                source: SegmentMetaError::NoObservedIndexValue {
                    expected_domain: "uint64",
                    ..
                }
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn fallback_ignores_a_large_unprojected_payload_column() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let plain_path = Path::new("data/timestamps_only.parquet");
        let payload_path = Path::new("data/large_payload.parquet");
        let timestamps = [Some(30), Some(-20), Some(10), Some(50)];

        write_arrow_timestamp_file(&tmp.path().join(plain_path), &timestamps, None)?;
        write_arrow_timestamp_file(
            &tmp.path().join(payload_path),
            &timestamps,
            Some(1024 * 1024),
        )?;

        let mut payload_file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path().join(payload_path))
            .await?;
        let payload_metadata =
            ArrowReaderMetadata::load_async(&mut payload_file, ArrowReaderOptions::default())
                .await?;
        let payload_index = payload_metadata
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .position(|column| column.path().string() == "payload")
            .expect("payload column");
        let payload_column = payload_metadata
            .metadata()
            .row_group(0)
            .column(payload_index);
        payload_file
            .seek(std::io::SeekFrom::Start(
                payload_column.data_page_offset() as u64
            ))
            .await?;
        payload_file.write_all(&[0xFF; 32]).await?;
        payload_file.flush().await?;
        drop(payload_file);

        let (plain, plain_report) =
            segment_meta_from_parquet(&location, plain_path, &timestamp_index("ts")).await?;
        let (with_payload, payload_report) =
            segment_meta_from_parquet(&location, payload_path, &timestamp_index("ts")).await?;

        assert!(with_payload.file_size.unwrap() > 4 * 1024 * 1024);
        assert_eq!(with_payload.index_min, plain.index_min);
        assert_eq!(with_payload.index_max, plain.index_max);
        assert_eq!(with_payload.row_count, plain.row_count);
        assert!(!plain_report.used_stats);
        assert!(!payload_report.used_stats);
        assert_eq!(payload_report.scanned_rows, timestamps.len() as u64);
        Ok(())
    }

    #[tokio::test]
    async fn segment_meta_errors_when_no_rows_and_no_stats() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/empty.parquet");
        let abs = tmp.path().join(rel_path);

        write_parquet_file(
            &abs,
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &[],
            false,
        )?;

        let result = segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await;

        assert!(matches!(
            result,
            Err(SegmentError::Metadata {
                source: SegmentMetaError::NoObservedIndexValue { .. }
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn segment_meta_supports_micro_and_nano_units() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        // Micros
        let rel_micro = Path::new("data/micro.parquet");
        let abs_micro = tmp.path().join(rel_micro);
        write_parquet_file(
            &abs_micro,
            "ts",
            Some("TIMESTAMP_MICROS"),
            PhysicalType::INT64,
            &[1_000, 2_000],
            true,
        )?;

        let (meta_micro, _) =
            segment_meta_from_parquet(&location, rel_micro, &timestamp_index("ts")).await?;
        assert_eq!(
            timestamp(&meta_micro.index_min)
                .timestamp_nanos_opt()
                .map(|n| n / 1_000),
            Some(1_000)
        );
        assert_eq!(
            timestamp(&meta_micro.index_max)
                .timestamp_nanos_opt()
                .map(|n| n / 1_000),
            Some(2_000)
        );

        // Nanos
        let rel_nano = Path::new("data/nano.parquet");
        let abs_nano = tmp.path().join(rel_nano);
        write_parquet_file(
            &abs_nano,
            "ts",
            Some("TIMESTAMP_NANOS"),
            PhysicalType::INT64,
            &[3_000, 9_000],
            true,
        )?;

        let (meta_nano, _) =
            segment_meta_from_parquet(&location, rel_nano, &timestamp_index("ts")).await?;
        assert_eq!(
            timestamp(&meta_nano.index_min).timestamp_nanos_opt(),
            Some(3_000)
        );
        assert_eq!(
            timestamp(&meta_nano.index_max).timestamp_nanos_opt(),
            Some(9_000)
        );

        Ok(())
    }

    #[tokio::test]
    async fn missing_time_column_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/no_time.parquet");
        let abs = tmp.path().join(rel_path);

        // Write a parquet file with a different column name.
        write_parquet_file(
            &abs,
            "other",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &[1, 2],
            true,
        )?;

        let result = segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await;

        assert!(matches!(
            result,
            Err(SegmentError::Metadata {
                source: SegmentMetaError::OrderedIndexColumn {
                    source: ParquetIndexColumnError {
                        path,
                        column,
                        expected_domain: "timestamp",
                        observed_type,
                    }
                }
            }) if path == "data/no_time.parquet"
                && column == "ts"
                && observed_type == "missing"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_time_type_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/unsupported_time.parquet");
        let abs = tmp.path().join(rel_path);

        // INT32 with timestamp logical is unsupported.
        write_parquet_file(&abs, "ts", None, PhysicalType::INT32, &[1, 2], true)?;

        let result = segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await;

        assert!(matches!(
            result,
            Err(SegmentError::Metadata {
                source: SegmentMetaError::OrderedIndexColumn {
                    source: ParquetIndexColumnError {
                        path,
                        column,
                        expected_domain: "timestamp",
                        observed_type,
                    }
                }
            }) if path == "data/unsupported_time.parquet"
                && column == "ts"
                && observed_type.contains("INT32")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn integer_index_validation_rejects_signed_unsigned_mismatches() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let signed_path = Path::new("data/signed.parquet");
        write_arrow_int64_file(&tmp.path().join(signed_path), "index", &[Some(1)], true)?;
        let signed_as_unsigned =
            segment_meta_from_parquet(&location, signed_path, &uint64_index("index")).await;
        assert!(matches!(
            signed_as_unsigned,
            Err(SegmentError::Metadata {
                source: SegmentMetaError::OrderedIndexColumn {
                    source: ParquetIndexColumnError {
                        path,
                        column,
                        expected_domain: "uint64",
                        observed_type,
                    }
                }
            }) if path == "data/signed.parquet"
                && column == "index"
                && observed_type.contains("logical=None")
        ));

        let unsigned_path = Path::new("data/unsigned.parquet");
        write_arrow_uint64_file(&tmp.path().join(unsigned_path), "index", &[Some(1)], true)?;
        let unsigned_as_signed =
            segment_meta_from_parquet(&location, unsigned_path, &int64_index("index")).await;
        assert!(matches!(
            unsigned_as_signed,
            Err(SegmentError::Metadata {
                source: SegmentMetaError::OrderedIndexColumn {
                    source: ParquetIndexColumnError {
                        path,
                        column,
                        expected_domain: "int64",
                        observed_type,
                    }
                }
            }) if path == "data/unsigned.parquet"
                && column == "index"
                && observed_type.contains("is_signed: false")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn out_of_range_timestamp_has_path_and_column_context() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/out_of_range.parquet");
        let abs = tmp.path().join(rel_path);

        write_parquet_file(
            &abs,
            "ts",
            Some("TIMESTAMP_MILLIS"),
            PhysicalType::INT64,
            &[i64::MAX],
            true,
        )?;

        let result = segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await;

        assert!(matches!(
            result,
            Err(SegmentError::Metadata {
                source: SegmentMetaError::ParquetStatsShape { path, column, .. }
            }) if path == "data/out_of_range.parquet" && column == "ts"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn bad_parquet_file_returns_parquet_read_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/corrupt.parquet");
        let abs = tmp.path().join(rel_path);

        // Valid magic bytes but invalid body so the parquet reader fails.
        tokio::fs::create_dir_all(abs.parent().unwrap()).await?;
        tokio::fs::write(&abs, b"PAR1PAR1garbage").await?;

        let error = segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts"))
            .await
            .expect_err("corrupt Parquet must fail");

        let segment_backtrace = ErrorCompat::backtrace(&error).expect("segment backtrace");
        let metadata = error
            .source()
            .and_then(|source| source.downcast_ref::<SegmentMetaError>())
            .expect("segment metadata source");
        let metadata_backtrace = ErrorCompat::backtrace(metadata).expect("metadata backtrace");
        metadata
            .source()
            .and_then(|source| source.downcast_ref::<ParquetError>())
            .expect("Parquet source");

        assert!(matches!(
            &error,
            SegmentError::Metadata {
                source: SegmentMetaError::ParquetRead { path, .. }
            } if path == "data/corrupt.parquet"
        ));
        assert!(std::ptr::eq(segment_backtrace, metadata_backtrace));
        Ok(())
    }

    #[tokio::test]
    async fn too_short_file_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/short.parquet");
        let abs = tmp.path().join(rel_path);
        tokio::fs::create_dir_all(abs.parent().unwrap()).await?;
        tokio::fs::write(&abs, b"short").await?;

        let result = segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await;

        assert!(matches!(
            result,
            Err(SegmentError::Metadata {
                source: SegmentMetaError::TooShort { .. }
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn missing_file_returns_storage_error_with_path_context() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("data/missing.parquet");

        let result = segment_meta_from_parquet(&location, rel_path, &timestamp_index("ts")).await;

        assert!(matches!(
            result,
            Err(SegmentError::MissingFile { path, .. }) if path == "data/missing.parquet"
        ));
        Ok(())
    }
}
