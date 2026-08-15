//! Parquet segment metadata derivation.
//!
//! This module extracts per-segment metadata (index bounds, row count, etc.)
//! directly from local Parquet files.

use std::path::Path;

use chrono::{DateTime, TimeZone, Utc};
use futures::{Stream, StreamExt};
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    async_reader::ParquetRecordBatchStreamBuilder,
};
use parquet::basic::{LogicalType, TimeUnit, Type as PhysicalType};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;

use snafu::Backtrace;
use tokio::task::JoinSet;

use crate::metadata::time_column::TimeColumnError;
use crate::storage::{TableLocation, file_size, open_parquet_reader};
use crate::transaction_log::segments::{SegmentMetaError, SegmentResult, map_storage_error};
use crate::transaction_log::{FileFormat, SegmentMeta};

struct TimestampStatsPlan {
    min_max: Option<(i64, i64)>,
    row_groups_to_scan: Vec<usize>,
}

fn merge_min_max(current: &mut Option<(i64, i64)>, next: (i64, i64)) {
    *current = Some(match *current {
        Some((min, max)) => (min.min(next.0), max.max(next.1)),
        None => next,
    });
}

/// Use exact row-group statistics and identify only the row groups that require scanning.
fn plan_timestamp_scan(
    path: &str,
    column: &str,
    time_idx: usize,
    metadata: &ParquetMetaData,
) -> Result<TimestampStatsPlan, SegmentMetaError> {
    let mut min_max = None;
    let mut row_groups_to_scan = Vec::new();

    for (row_group_index, rg) in metadata.row_groups().iter().enumerate() {
        if rg.num_rows() == 0 {
            continue;
        }
        let col_meta = rg.column(time_idx);

        let stats = match col_meta.statistics() {
            Some(s) => s,
            None => {
                row_groups_to_scan.push(row_group_index);
                continue;
            }
        };

        let (group_min, group_max) = match stats {
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

        if group_min > group_max {
            return Err(SegmentMetaError::ParquetStatsShape {
                path: path.to_string(),
                column: column.to_string(),
                detail: format!(
                    "timestamp statistics minimum {group_min} exceeds maximum {group_max}"
                ),
            });
        }

        merge_min_max(&mut min_max, (group_min, group_max));
    }

    Ok(TimestampStatsPlan {
        min_max,
        row_groups_to_scan,
    })
}

use super::{INSPECTION_BATCH_SIZE, resolve_rg_settings};

async fn scan_timestamp_batches(
    path: &str,
    time_column: &str,
    mut reader: impl Stream<
        Item = Result<arrow::record_batch::RecordBatch, parquet::errors::ParquetError>,
    > + Unpin,
) -> SegmentResult<(Option<(i64, i64)>, u64)> {
    use arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
    use arrow_array::{
        Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };

    let mut min_max = None;
    let mut scanned_rows: u64 = 0;

    macro_rules! scan_arr {
        ($arr:ty, $col:expr) => {{
            let arr = $col.as_any().downcast_ref::<$arr>().ok_or_else(|| {
                SegmentMetaError::TimeColumn {
                    path: path.to_string(),
                    source: TimeColumnError::UnsupportedArrowType {
                        column: time_column.to_string(),
                        datatype: $col.data_type().to_string(),
                    },
                }
            })?;

            if arr.null_count() == 0 {
                for &v in arr.values() {
                    merge_min_max(&mut min_max, (v, v));
                }
            } else {
                for v in arr.iter().flatten() {
                    merge_min_max(&mut min_max, (v, v));
                }
            }
        }};
    }

    while let Some(batch_res) = reader.next().await {
        let batch = batch_res.map_err(|source| SegmentMetaError::ParquetRead {
            path: path.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })?;
        scanned_rows = scanned_rows.saturating_add(batch.num_rows() as u64);

        let col = batch.column(0);
        match col.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                ArrowTimeUnit::Second => scan_arr!(TimestampSecondArray, col),
                ArrowTimeUnit::Millisecond => scan_arr!(TimestampMillisecondArray, col),
                ArrowTimeUnit::Microsecond => scan_arr!(TimestampMicrosecondArray, col),
                ArrowTimeUnit::Nanosecond => scan_arr!(TimestampNanosecondArray, col),
            },
            other => {
                return Err(SegmentMetaError::TimeColumn {
                    path: path.to_string(),
                    source: TimeColumnError::UnsupportedArrowType {
                        column: time_column.to_string(),
                        datatype: other.to_string(),
                    },
                }
                .into());
            }
        }

        tokio::task::yield_now().await;
    }

    Ok((min_max, scanned_rows))
}

async fn scan_timestamp_row_groups(
    location: &TableLocation,
    rel_path: &Path,
    path: &str,
    time_column: &str,
    metadata: ArrowReaderMetadata,
    row_groups: Vec<usize>,
) -> SegmentResult<(Option<(i64, i64)>, u64)> {
    let mask = ProjectionMask::columns(metadata.parquet_schema(), [time_column]);
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
        let time_column = time_column.to_string();
        let metadata = metadata.clone();
        let mask = mask.clone();

        tasks.spawn(async move {
            let file = open_parquet_reader(location.as_ref(), &rel_path)
                .await
                .map_err(map_storage_error)?;
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

            scan_timestamp_batches(&path, &time_column, reader).await
        });
    }

    let mut min_max = None;
    let mut scanned_rows: u64 = 0;
    while let Some(result) = tasks.join_next().await {
        let (task_min_max, rows) = result.map_err(|source| SegmentMetaError::ParquetRead {
            path: path.to_string(),
            source: parquet::errors::ParquetError::General(format!(
                "row-group scan task failed: {source}"
            )),
            backtrace: Backtrace::capture(),
        })??;
        if let Some(task_min_max) = task_min_max {
            merge_min_max(&mut min_max, task_min_max);
        }
        scanned_rows = scanned_rows.saturating_add(rows);
    }

    Ok((min_max, scanned_rows))
}

/// Internal enum to capture which Parquet timestamp unit we selected.
#[derive(Debug, Clone, Copy)]
enum TimestampUnit {
    Millis,
    Micros,
    Nanos,
}

fn choose_timestamp_unit_from_logical(
    column: &str,
    physical: PhysicalType,
    logical: Option<&LogicalType>,
) -> Result<TimestampUnit, TimeColumnError> {
    if physical != PhysicalType::INT64 {
        return Err(TimeColumnError::UnsupportedParquetType {
            column: column.to_string(),
            physical: format!("{physical:?}"),
            logical: format!("{logical:?}"),
        });
    }

    match logical {
        Some(LogicalType::Timestamp { unit, .. }) => match unit {
            TimeUnit::MILLIS => Ok(TimestampUnit::Millis),
            TimeUnit::MICROS => Ok(TimestampUnit::Micros),
            TimeUnit::NANOS => Ok(TimestampUnit::Nanos),
        },
        other => Err(TimeColumnError::UnsupportedParquetType {
            column: column.to_string(),
            physical: format!("{physical:?}"),
            logical: format!("{other:?}"),
        }),
    }
}

fn ts_from_i64(
    path: &str,
    column: &str,
    unit: TimestampUnit,
    value: i64,
) -> Result<DateTime<Utc>, SegmentMetaError> {
    let dt_opt = match unit {
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
pub(crate) struct SegmentMetaReport {
    /// Number of row groups reported by Parquet metadata.
    pub(crate) row_groups: usize,
    /// Total row count from file metadata.
    pub(crate) row_count: u64,
    /// True if no timestamp rows needed to be scanned.
    pub(crate) used_stats: bool,
    /// Number of rows scanned during fallback (0 if stats were used).
    pub(crate) scanned_rows: u64,
}

/// Build segment metadata and profiling data directly from a local Parquet file.
pub(crate) async fn segment_meta_from_parquet(
    location: &TableLocation,
    rel_path: &Path,
    time_column: &str,
) -> SegmentResult<(SegmentMeta, SegmentMetaReport)> {
    let path_str = rel_path.display().to_string();
    let file_size = file_size(location.as_ref(), rel_path)
        .await
        .map_err(map_storage_error)?;
    if file_size < 8 {
        return Err(SegmentMetaError::TooShort {
            path: path_str.clone(),
        }
        .into());
    }

    let mut file = open_parquet_reader(location.as_ref(), rel_path)
        .await
        .map_err(map_storage_error)?;

    let metadata = ArrowReaderMetadata::load_async(&mut file, ArrowReaderOptions::default())
        .await
        .map_err(|source| SegmentMetaError::ParquetRead {
            path: path_str.clone(),
            source,
            backtrace: Backtrace::capture(),
        })?;

    let (row_count, row_groups, unit, stats_plan) = {
        let parquet_metadata = metadata.metadata();
        let file_meta = parquet_metadata.file_metadata();
        let row_count: u64 =
            file_meta
                .num_rows()
                .try_into()
                .map_err(|_| SegmentMetaError::ParquetStatsShape {
                    path: path_str.clone(),
                    column: time_column.to_string(),
                    detail: format!("negative row count {}", file_meta.num_rows()),
                })?;
        let row_groups = parquet_metadata.num_row_groups();
        let schema = file_meta.schema_descr();
        let time_idx = schema
            .columns()
            .iter()
            .position(|column| column.path().string() == time_column)
            .ok_or_else(|| SegmentMetaError::TimeColumn {
                path: path_str.clone(),
                source: TimeColumnError::Missing {
                    column: time_column.to_string(),
                },
            })?;
        let column = schema.column(time_idx);
        let unit = choose_timestamp_unit_from_logical(
            time_column,
            column.physical_type(),
            column.logical_type_ref(),
        )
        .map_err(|source| SegmentMetaError::TimeColumn {
            path: path_str.clone(),
            source,
        })?;
        let stats_plan = plan_timestamp_scan(&path_str, time_column, time_idx, parquet_metadata)?;
        (row_count, row_groups, unit, stats_plan)
    };
    drop(file);

    let TimestampStatsPlan {
        mut min_max,
        row_groups_to_scan,
    } = stats_plan;
    let used_stats = row_groups_to_scan.is_empty();
    let scanned_rows = if used_stats {
        0
    } else {
        let (scanned_min_max, scanned_rows) = scan_timestamp_row_groups(
            location,
            rel_path,
            &path_str,
            time_column,
            metadata,
            row_groups_to_scan,
        )
        .await?;
        if let Some(scanned_min_max) = scanned_min_max {
            merge_min_max(&mut min_max, scanned_min_max);
        }
        scanned_rows
    };
    let (ts_min_raw, ts_max_raw) =
        min_max.ok_or_else(|| SegmentMetaError::ParquetStatsMissing {
            path: path_str.clone(),
            column: time_column.to_string(),
        })?;

    // Convert raw i64 timestamps to DateTime<Utc> using the chosen unit.
    let ts_min = ts_from_i64(&path_str, time_column, unit, ts_min_raw)?;
    let ts_max = ts_from_i64(&path_str, time_column, unit, ts_max_raw)?;

    let meta_out = SegmentMeta {
        path: path_str,
        format: FileFormat::Parquet,
        index_min: ts_min.into(),
        index_max: ts_max.into(),
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
    use crate::metadata::table_metadata::IndexValue;
    use crate::transaction_log::segments::{SegmentError, SegmentIoError};
    use arrow::array::{ArrayRef, BinaryBuilder, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, LogicalType, Repetition, TimeUnit};
    use parquet::column::writer::ColumnWriter;
    use parquet::file::metadata::{
        ColumnChunkMetaData, FileMetaData, ParquetMetaDataWriter, RowGroupMetaData,
    };
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::{SchemaDescriptor, Type};
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::fs;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn timestamp(value: &IndexValue) -> &DateTime<Utc> {
        match value {
            IndexValue::Timestamp(value) => value,
            other => panic!("expected timestamp bound, found {other}"),
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
                "TIMESTAMP_MILLIS" => LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: TimeUnit::MILLIS,
                },
                "TIMESTAMP_MICROS" => LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: TimeUnit::MICROS,
                },
                "TIMESTAMP_NANOS" => LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: TimeUnit::NANOS,
                },
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
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut fields = vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        )];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(TimestampMillisecondArray::from(
            timestamps.to_vec(),
        ))];

        if let Some(payload_size) = payload_size {
            fields.push(Field::new("payload", DataType::Binary, false));
            let payload = vec![0xA5; payload_size];
            let mut payloads =
                BinaryBuilder::with_capacity(timestamps.len(), timestamps.len() * payload_size);
            for _ in timestamps {
                payloads.append_value(&payload);
            }
            arrays.push(Arc::new(payloads.finish()));
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn clear_timestamp_statistics(
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

    fn metadata_with_timestamp_statistics(
        stats: Statistics,
    ) -> Result<ParquetMetaData, Box<dyn std::error::Error>> {
        let column = Arc::new(
            Type::primitive_type_builder("ts", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: TimeUnit::MILLIS,
                }))
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
    fn choose_timestamp_unit_rejects_wrong_logical() {
        // No logical type (None) should fail
        let err = choose_timestamp_unit_from_logical("ts", PhysicalType::INT64, None).unwrap_err();
        assert!(matches!(
            err,
            TimeColumnError::UnsupportedParquetType { .. }
        ));
    }

    #[test]
    fn choose_timestamp_unit_rejects_wrong_physical() {
        let lt = LogicalType::Timestamp {
            is_adjusted_to_u_t_c: true,
            unit: TimeUnit::MILLIS,
        };
        let err =
            choose_timestamp_unit_from_logical("ts", PhysicalType::INT32, Some(&lt)).unwrap_err();
        assert!(matches!(
            err,
            TimeColumnError::UnsupportedParquetType { .. }
        ));
    }

    #[test]
    fn inexact_statistics_force_fallback() -> TestResult {
        let stats = match Statistics::int64(Some(10), Some(20), None, Some(0), false) {
            Statistics::Int64(stats) => Statistics::Int64(stats.with_min_is_exact(false)),
            _ => unreachable!(),
        };
        let metadata = metadata_with_timestamp_statistics(stats)?;

        let plan = plan_timestamp_scan("path", "ts", 0, &metadata)?;
        assert_eq!(plan.min_max, None);
        assert_eq!(plan.row_groups_to_scan, vec![0]);
        Ok(())
    }

    #[test]
    fn inverted_statistics_are_rejected() -> TestResult {
        let stats = Statistics::int64(Some(20), Some(10), None, Some(0), false);
        let metadata = metadata_with_timestamp_statistics(stats)?;

        let err = plan_timestamp_scan("data/inverted.parquet", "ts", 0, &metadata)
            .err()
            .expect("inverted statistics must fail");

        assert!(matches!(
            err,
            SegmentMetaError::ParquetStatsShape {
                path,
                column,
                detail,
            } if path == "data/inverted.parquet"
                && column == "ts"
                && detail == "timestamp statistics minimum 20 exceeds maximum 10"
        ));
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

        let (meta, report) = segment_meta_from_parquet(&location, rel_path, "ts").await?;

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

        let (meta, report) = segment_meta_from_parquet(&location, rel_path, "ts").await?;

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

        let (meta, report) = segment_meta_from_parquet(&location, rel_path, "ts").await?;

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
        clear_timestamp_statistics(&abs, 1)?;

        let (meta, report) = segment_meta_from_parquet(&location, rel_path, "ts").await?;

        assert_eq!(timestamp(&meta.index_min).timestamp_millis(), -100);
        assert_eq!(timestamp(&meta.index_max).timestamp_millis(), 300);
        assert_eq!(meta.row_count, 6);
        assert_eq!(report.row_groups, 3);
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

        let (meta, report) = segment_meta_from_parquet(&location, rel_path, "ts").await?;

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

        let (meta, report) = segment_meta_from_parquet(&location, rel_path, "ts").await?;

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

        let (meta, report) = segment_meta_from_parquet(&location, rel_path, "ts").await?;

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

        let result = segment_meta_from_parquet(&location, rel_path, "ts").await;

        assert!(matches!(
            result,
            Err(SegmentError::Meta {
                source: SegmentMetaError::ParquetStatsMissing { path, column }
            }) if path == "data/all_null.parquet" && column == "ts"
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

        let (plain, plain_report) = segment_meta_from_parquet(&location, plain_path, "ts").await?;
        let (with_payload, payload_report) =
            segment_meta_from_parquet(&location, payload_path, "ts").await?;

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

        let result = segment_meta_from_parquet(&location, rel_path, "ts").await;

        assert!(matches!(
            result,
            Err(SegmentError::Meta {
                source: SegmentMetaError::ParquetStatsMissing { .. }
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

        let (meta_micro, _) = segment_meta_from_parquet(&location, rel_micro, "ts").await?;
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

        let (meta_nano, _) = segment_meta_from_parquet(&location, rel_nano, "ts").await?;
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

        let result = segment_meta_from_parquet(&location, rel_path, "ts").await;

        assert!(matches!(
            result,
            Err(SegmentError::Meta {
                source: SegmentMetaError::TimeColumn {
                    source: TimeColumnError::Missing { .. },
                    ..
                }
            })
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

        let result = segment_meta_from_parquet(&location, rel_path, "ts").await;

        assert!(matches!(
            result,
            Err(SegmentError::Meta {
                source: SegmentMetaError::TimeColumn {
                    source: TimeColumnError::UnsupportedParquetType { .. },
                    ..
                }
            })
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

        let result = segment_meta_from_parquet(&location, rel_path, "ts").await;

        assert!(matches!(
            result,
            Err(SegmentError::Meta {
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

        let result = segment_meta_from_parquet(&location, rel_path, "ts").await;

        assert!(matches!(
            result,
            Err(SegmentError::Meta {
                source: SegmentMetaError::ParquetRead { .. }
            })
        ));
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

        let result = segment_meta_from_parquet(&location, rel_path, "ts").await;

        assert!(matches!(
            result,
            Err(SegmentError::Meta {
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

        let result = segment_meta_from_parquet(&location, rel_path, "ts").await;

        assert!(matches!(
            result,
            Err(SegmentError::Io {
                source: SegmentIoError::MissingFile { path, .. }
            }) if path == "data/missing.parquet"
        ));
        Ok(())
    }
}
