//! Range scan implementation for `TimeSeriesTable`.
//!
//! This module wires the public `scan_range` entry point to the underlying
//! segment metadata and Parquet readers:
//! - Pick candidate segments whose `[ts_min, ts_max]` intersects the requested
//!   half-open window `[ts_start, ts_end)`.
//! - Stream those segments in timestamp order with Parquet's native async,
//!   file-backed reader.
//! - Filter each batch by the time column with half-open semantics, converting
//!   the requested bounds to the column’s Arrow timestamp unit while preserving
//!   timezone metadata.
//!
//! The filtering path uses Arrow’s scalar comparison kernels to avoid
//! allocating full-length bound arrays, and treats null timestamp values as
//! “drop row” via `filter_record_batch`. The implementation assumes v0.1
//! invariants (non-overlapping segments) so chronological ordering is a simple
//! sort by `ts_min`.
use std::path::Path;

use arrow::array::Scalar;
use arrow::array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::{boolean as boolean_kernels, cmp as cmp_kernels};
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, TimeUnit};
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, future};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use snafu::prelude::*;

use crate::metadata::segments::SegmentMeta;
use crate::storage::{self, TableLocation};
use crate::transaction_log::TableState;

use super::error::{InvalidRangeSnafu, StorageSnafu, TableError};
use super::{TimeSeriesScan, TimeSeriesTable};
use crate::metadata::segments::cmp_segment_meta_by_time;

const SCAN_BATCH_SIZE: usize = 8_192;

fn segments_for_range(
    state: &TableState,
    ts_start: DateTime<Utc>,
    ts_end: DateTime<Utc>,
) -> Vec<SegmentMeta> {
    state
        .segments
        .values()
        .filter(|seg| {
            // half-open query [ts_start, ts_end)
            // intersection with segment's [ts_min, ts_max] (closed) is:
            // seg.ts_max >= ts_start && seg.ts_min < ts_end
            seg.ts_max >= ts_start && seg.ts_min < ts_end
        })
        .cloned()
        .collect()
}

/// Helper macro to filter a `RecordBatch` by a timestamp column for a
/// half-open time range `[start, end)`.
///
/// This macro is used by `read_segment_range` for all supported timestamp
/// units (`second`, `millisecond`, `microsecond`, `nanosecond`) and
/// encapsulates three non-obvious choices:
///
/// 1. **Half-open semantics**:
///    Rows are kept iff `start_bound <= ts < end_bound`, where
///    `start_bound`/`end_bound` are already converted to the same integer
///    time unit as the column (via `to_bounds_i64`).
///
/// 2. **Timezone preservation**:
///    Arrow timestamps carry an optional timezone in their `DataType`
///    (`Timestamp(unit, Option<tz>)`). The comparison kernels require the
///    types (including timezone) of both operands to match. To avoid
///    mismatches, we:
///       - read the timezone from the actual column’s `DataType`,
///       - build 1-element timestamp arrays for `start` and `end` with
///         the same unit and timezone,
///       - wrap those arrays as `Scalar<Timestamp…Array>`.
///
///    This ensures `ts_arr` and the scalar bounds have identical
///    `DataType`, so the Arrow `gt_eq` / `lt` kernels accept them.
///
/// 3. **Scalar-based vectorization (no full-length bound arrays)**:
///    Arrow’s compute kernels accept `Datum` operands, which can be
///    either arrays or scalars. When one side is a scalar, the kernel
///    *broadcasts* the single value across the length of the array without
///    materializing a repeated column. Using `Scalar::new` over a
///    1-element array gives us:
///       - vectorized, element-wise comparison over the whole batch, and
///       - minimal extra allocation (two tiny 1-element arrays),
///         instead of allocating full-length `[start; len]` / `[end; len]`
///         arrays.
///
/// The resulting `BooleanArray` mask is then passed to
/// `filter_record_batch`, which drops nulls in the mask (null => “do not
/// keep row”), matching the intended `null -> false` semantics for the
/// time column.
///
/// This macro returns the non-empty filtered batch, or `None` when every row
/// was filtered out.
macro_rules! filter_ts_batch {
    ($array_ty: ty,
    $batch:expr,
    $ts_idx:expr,
    $start_bound:expr,
    $end_bound:expr,
    $path:expr,
    $time_col:expr,
    $ts_field:expr
) => {{
        // 1) Downcast the column to the concrete timestamp array type
        let col = $batch.column($ts_idx);
        let ts_arr = col.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
            TableError::UnsupportedTimeType {
                path: $path.to_string(),
                column: $time_col.to_string(),
                datatype: $ts_field.data_type().clone(),
            }
        })?;

        // 2) Extract timezone from the array's DataType to ensure that our scalar matches and comparisons are compatible
        let tz_opt = match ts_arr.data_type() {
            DataType::Timestamp(_, tz_opt) => tz_opt.clone(),
            _ => None,
        };

        // 3) Build *1-element* arrays for the bounds, with matching timezone,
        //    then wrap them as Scalars. Arrow's comparison kernels operate on
        //    `Datum` (array or scalar) and will broadcast these scalar bounds
        //    across the whole `ts_arr` without allocating full-length repeated
        //    arrays.
        let start_arr = <$array_ty>::from(vec![$start_bound]).with_timezone_opt(tz_opt.clone());
        let end_arr = <$array_ty>::from(vec![$end_bound]).with_timezone_opt(tz_opt);

        // Wrap them as scalars (no repeated buffers)
        let start_scalar = Scalar::new(start_arr);
        let end_scalar = Scalar::new(end_arr);

        // 4) Vectorized comparisons:
        // ge_mask = (ts >= start)
        // lt_mask = (ts < end)
        let ge_mask = cmp_kernels::gt_eq(ts_arr, &start_scalar).map_err(|source| {
            TableError::Arrow {
                path: $path.to_string(),
                source,
            }
        })?;
        let lt_mask = cmp_kernels::lt(ts_arr, &end_scalar).map_err(|source| {
            TableError::Arrow {
                path: $path.to_string(),
                source,
            }
        })?;

        // 5) Combine: keep rows where ts >= start AND ts < end
        let mask = boolean_kernels::and(&ge_mask, &lt_mask).map_err(|source| {
            TableError::Arrow {
                path: $path.to_string(),
                source,
            }
        })?;

        // Note on null semantics:
        // - If ts_arr[i] is null, both comparisons produce null in the mask.
        // Arrow's `filter_record_batch` treats null mask values as false,
        // excluding those rows from results.

        // 6) apply the mask to the whole batch
        let filtered = filter_record_batch(&$batch, &mask).map_err(|source| {
            TableError::Arrow {
                path: $path.to_string(),
                source,
            }
        })?;

        Ok::<_, TableError>((filtered.num_rows() > 0).then_some(filtered))
    }};
}

fn to_bounds_i64(
    field: &Field,
    path: &str,
    column: &str,
    ts_start: DateTime<Utc>,
    ts_end: DateTime<Utc>,
) -> Result<(i64, i64), TableError> {
    let ceil_bound = |dt: DateTime<Utc>, floor: i64, nanos_per_unit: u32| {
        if dt.timestamp_subsec_nanos().is_multiple_of(nanos_per_unit) {
            Ok(floor)
        } else {
            floor
                .checked_add(1)
                .ok_or_else(|| TableError::TimeConversionOverflow {
                    path: path.to_string(),
                    column: column.to_string(),
                    timestamp: dt,
                })
        }
    };
    let to_ns = |dt: DateTime<Utc>| {
        dt.timestamp()
            .checked_mul(1_000_000_000)
            .and_then(|secs| secs.checked_add(dt.timestamp_subsec_nanos() as i64))
            .ok_or_else(|| TableError::TimeConversionOverflow {
                path: path.to_string(),
                column: column.to_string(),
                timestamp: dt,
            })
    };

    match field.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => Ok((
            ceil_bound(ts_start, ts_start.timestamp(), 1_000_000_000)?,
            ceil_bound(ts_end, ts_end.timestamp(), 1_000_000_000)?,
        )),

        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok((
            ceil_bound(ts_start, ts_start.timestamp_millis(), 1_000_000)?,
            ceil_bound(ts_end, ts_end.timestamp_millis(), 1_000_000)?,
        )),

        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok((
            ceil_bound(ts_start, ts_start.timestamp_micros(), 1_000)?,
            ceil_bound(ts_end, ts_end.timestamp_micros(), 1_000)?,
        )),

        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok((to_ns(ts_start)?, to_ns(ts_end)?)),

        other => Err(TableError::UnsupportedTimeType {
            path: path.to_string(),
            column: column.to_string(),
            datatype: other.clone(),
        }),
    }
}

async fn segment_range_stream<T>(
    reader: T,
    path: String,
    time_column: &str,
    ts_start: DateTime<Utc>,
    ts_end: DateTime<Utc>,
) -> Result<TimeSeriesScan, TableError>
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|source| TableError::ParquetRead {
            path: path.clone(),
            source,
        })?;

    // Locate the time column and compute numeric bounds before moving the
    // builder into the directly-polled record-batch stream.
    let schema = builder.schema();
    let ts_idx = schema
        .index_of(time_column)
        .map_err(|_| TableError::MissingTimeColumn {
            path: path.clone(),
            column: time_column.to_string(),
        })?;
    let ts_field = schema.field(ts_idx).clone();
    let (start_bound, end_bound) = to_bounds_i64(&ts_field, &path, time_column, ts_start, ts_end)?;

    let reader = builder
        .with_batch_size(SCAN_BATCH_SIZE)
        .build()
        .map_err(|source| TableError::ParquetRead {
            path: path.clone(),
            source,
        })?;
    let time_column = time_column.to_string();

    let stream = reader
        .then(move |batch_res| {
            let path = path.clone();
            let time_column = time_column.clone();
            let ts_field = ts_field.clone();

            async move {
                let batch = batch_res.map_err(|source| TableError::ParquetRead {
                    path: path.clone(),
                    source,
                })?;

                let filtered = match ts_field.data_type() {
                    DataType::Timestamp(TimeUnit::Second, _) => filter_ts_batch!(
                        TimestampSecondArray,
                        batch,
                        ts_idx,
                        start_bound,
                        end_bound,
                        path,
                        time_column,
                        ts_field
                    )?,
                    DataType::Timestamp(TimeUnit::Millisecond, _) => filter_ts_batch!(
                        TimestampMillisecondArray,
                        batch,
                        ts_idx,
                        start_bound,
                        end_bound,
                        path,
                        time_column,
                        ts_field
                    )?,
                    DataType::Timestamp(TimeUnit::Microsecond, _) => filter_ts_batch!(
                        TimestampMicrosecondArray,
                        batch,
                        ts_idx,
                        start_bound,
                        end_bound,
                        path,
                        time_column,
                        ts_field
                    )?,
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => filter_ts_batch!(
                        TimestampNanosecondArray,
                        batch,
                        ts_idx,
                        start_bound,
                        end_bound,
                        path,
                        time_column,
                        ts_field
                    )?,
                    other => {
                        return Err(TableError::UnsupportedTimeType {
                            path,
                            column: time_column,
                            datatype: other.clone(),
                        });
                    }
                };

                if filtered.is_none() {
                    tokio::task::yield_now().await;
                }

                Ok(filtered)
            }
        })
        .try_filter_map(|batch| future::ready(Ok(batch)));

    Ok(Box::pin(stream))
}

async fn read_segment_range(
    location: &TableLocation,
    segment: &SegmentMeta,
    time_column: &str,
    ts_start: DateTime<Utc>,
    ts_end: DateTime<Utc>,
) -> Result<TimeSeriesScan, TableError> {
    let rel_path = Path::new(&segment.path);
    let reader = storage::open_parquet_reader(location.as_ref(), rel_path)
        .await
        .context(StorageSnafu)?;

    segment_range_stream(reader, segment.path.clone(), time_column, ts_start, ts_end).await
}

struct ScanState {
    candidates: std::vec::IntoIter<SegmentMeta>,
    current: Option<TimeSeriesScan>,
    location: TableLocation,
    time_column: String,
    ts_start: DateTime<Utc>,
    ts_end: DateTime<Utc>,
}

impl TimeSeriesTable {
    /// Scan the time-series table for record batches overlapping `[ts_start, ts_end)`,
    /// returning a stream of filtered batches from the segments covering that range.
    pub async fn scan_range(
        &self,
        ts_start: DateTime<Utc>,
        ts_end: DateTime<Utc>,
    ) -> Result<TimeSeriesScan, TableError> {
        if ts_start >= ts_end {
            return InvalidRangeSnafu {
                start: ts_start,
                end: ts_end,
            }
            .fail();
        }

        let ts_column = self.index.timestamp_column.clone();

        // 1) Pick candidate segments.
        let mut candidates = segments_for_range(&self.state, ts_start, ts_end);

        // 2) Sort deterministically by ts_min, ts_max, and path.
        candidates.sort_unstable_by(cmp_segment_meta_by_time);

        let state = ScanState {
            candidates: candidates.into_iter(),
            current: None,
            location: self.location().clone(),
            time_column: ts_column,
            ts_start,
            ts_end,
        };

        // Process one lazily opened segment stream at a time. `try_unfold`
        // drops its state after the first error, so later segments are not
        // opened after a terminal scan failure.
        let stream = futures::stream::try_unfold(state, |mut state| async move {
            loop {
                if let Some(current) = state.current.as_mut() {
                    match current.next().await {
                        Some(Ok(batch)) => return Ok(Some((batch, state))),
                        Some(Err(error)) => return Err(error),
                        None => state.current = None,
                    }
                }

                let Some(segment) = state.candidates.next() else {
                    return Ok(None);
                };
                state.current = Some(
                    read_segment_range(
                        &state.location,
                        &segment,
                        &state.time_column,
                        state.ts_start,
                        state.ts_end,
                    )
                    .await?,
                );
            }
        });

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::TableLocation;
    use crate::table::test_util::*;

    use crate::metadata::logical_schema::LogicalTimestampUnit;
    use crate::metadata::segments::FileFormat;

    use arrow::array::RecordBatch;
    use arrow::datatypes::{Schema, TimeUnit as ArrowTimeUnit};

    use chrono::{TimeZone, Utc};
    use futures::{FutureExt, StreamExt, future::BoxFuture};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ArrowReaderOptions;
    use parquet::errors::Result as ParquetResult;
    use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
    use parquet::file::properties::WriterProperties;

    use std::fs::File;
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tempfile::TempDir;

    struct TrackingReader {
        data: bytes::Bytes,
        read_calls: Arc<AtomicUsize>,
        bytes_read: Arc<AtomicUsize>,
        gate: Option<(usize, futures::channel::oneshot::Receiver<()>)>,
    }

    impl TrackingReader {
        fn new(data: bytes::Bytes) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
            let read_calls = Arc::new(AtomicUsize::new(0));
            let bytes_read = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    data,
                    read_calls: Arc::clone(&read_calls),
                    bytes_read: Arc::clone(&bytes_read),
                    gate: None,
                },
                read_calls,
                bytes_read,
            )
        }

        fn with_gate(
            data: bytes::Bytes,
            gated_call: usize,
        ) -> (
            Self,
            Arc<AtomicUsize>,
            Arc<AtomicUsize>,
            futures::channel::oneshot::Sender<()>,
        ) {
            let (mut reader, read_calls, bytes_read) = Self::new(data);
            let (release, gate) = futures::channel::oneshot::channel();
            reader.gate = Some((gated_call, gate));
            (reader, read_calls, bytes_read, release)
        }

        fn read_ranges(&self, ranges: Vec<Range<u64>>) -> (usize, Vec<bytes::Bytes>) {
            let call = self.read_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.bytes_read.fetch_add(
                ranges
                    .iter()
                    .map(|range| (range.end - range.start) as usize)
                    .sum::<usize>(),
                Ordering::SeqCst,
            );
            (
                call,
                ranges
                    .into_iter()
                    .map(|range| self.data.slice(range.start as usize..range.end as usize))
                    .collect(),
            )
        }
    }

    impl AsyncFileReader for TrackingReader {
        fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
            let (_, mut ranges) = self.read_ranges(vec![range]);
            futures::future::ready(Ok(ranges.pop().expect("one requested range"))).boxed()
        }

        fn get_byte_ranges(
            &mut self,
            ranges: Vec<Range<u64>>,
        ) -> BoxFuture<'_, ParquetResult<Vec<bytes::Bytes>>> {
            let (call, bytes) = self.read_ranges(ranges);
            let gate = self
                .gate
                .as_ref()
                .is_some_and(|(gated_call, _)| *gated_call == call)
                .then(|| self.gate.take().expect("configured gate").1);
            async move {
                if let Some(gate) = gate {
                    let _ = gate.await;
                }
                Ok(bytes)
            }
            .boxed()
        }

        fn get_metadata<'a>(
            &'a mut self,
            _options: Option<&'a ArrowReaderOptions>,
        ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
            let metadata = ParquetMetaDataReader::new()
                .parse_and_finish(&self.data)
                .map(Arc::new);
            futures::future::ready(metadata).boxed()
        }
    }

    fn write_multi_row_group_parquet(
        path: &Path,
        row_groups: &[&[i64]],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let primary_timezone: Arc<str> = Arc::from("UTC");
        let secondary_timezone: Arc<str> = Arc::from("+00:00");
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(primary_timezone.clone())),
                false,
            ),
            Field::new(
                "observed_at",
                DataType::Timestamp(ArrowTimeUnit::Microsecond, Some(secondary_timezone.clone())),
                false,
            ),
        ]));

        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(
            file,
            Arc::clone(&schema),
            Some(WriterProperties::builder().build()),
        )?;
        for values in row_groups {
            let ts = TimestampMillisecondArray::from(values.to_vec())
                .with_timezone_opt(Some(primary_timezone.clone()));
            let observed_at = TimestampMicrosecondArray::from(
                values.iter().map(|value| value * 1_000).collect::<Vec<_>>(),
            )
            .with_timezone_opt(Some(secondary_timezone.clone()));
            writer.write(&RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(ts), Arc::new(observed_at)],
            )?)?;
            writer.flush()?;
        }
        writer.close()?;
        Ok(())
    }

    #[tokio::test]
    async fn segment_stream_reads_on_demand_and_preserves_schema() -> TestResult {
        let tmp = TempDir::new()?;
        let path = tmp.path().join("multi-row-group.parquet");
        write_multi_row_group_parquet(&path, &[&[1_000], &[2_000], &[3_000]])?;
        let data = bytes::Bytes::from(std::fs::read(path)?);
        let file_size = data.len();

        let (reader, read_calls, bytes_read) = TrackingReader::new(data.clone());
        let mut stream = segment_range_stream(
            reader,
            "data/multi-row-group.parquet".to_string(),
            "ts",
            Utc.timestamp_millis_opt(0).single().unwrap(),
            Utc.timestamp_millis_opt(4_000).single().unwrap(),
        )
        .await?;

        assert_eq!(read_calls.load(Ordering::SeqCst), 0);
        let first = stream.next().await.transpose()?.expect("first batch");
        assert_eq!(read_calls.load(Ordering::SeqCst), 1);
        assert!(bytes_read.load(Ordering::SeqCst) < file_size);
        assert_eq!(
            first.schema().field(0).data_type(),
            &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(Arc::<str>::from("UTC")))
        );
        assert_eq!(
            first.schema().field(1).data_type(),
            &DataType::Timestamp(ArrowTimeUnit::Microsecond, Some(Arc::<str>::from("+00:00")))
        );

        tokio::task::yield_now().await;
        assert_eq!(read_calls.load(Ordering::SeqCst), 1);
        drop(stream);
        tokio::task::yield_now().await;
        assert_eq!(read_calls.load(Ordering::SeqCst), 1);

        let (reader, gated_calls, _, release) = TrackingReader::with_gate(data, 2);
        let mut stream = segment_range_stream(
            reader,
            "data/multi-row-group.parquet".to_string(),
            "ts",
            Utc.timestamp_millis_opt(0).single().unwrap(),
            Utc.timestamp_millis_opt(4_000).single().unwrap(),
        )
        .await?;
        let mut timestamps = Vec::new();
        let first = stream.next().await.transpose()?.expect("first batch");
        timestamps.extend(
            first
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("millisecond timestamp")
                .values()
                .iter()
                .copied(),
        );
        assert_eq!(gated_calls.load(Ordering::SeqCst), 1);

        let second = stream.next();
        futures::pin_mut!(second);
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(gated_calls.load(Ordering::SeqCst), 2);
        release.send(()).expect("release second row-group read");
        let second = second.await.transpose()?.expect("second batch");
        timestamps.extend(
            second
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("millisecond timestamp")
                .values()
                .iter()
                .copied(),
        );

        while let Some(batch) = stream.next().await.transpose()? {
            timestamps.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("millisecond timestamp")
                    .values()
                    .iter()
                    .copied(),
            );
        }
        assert_eq!(timestamps, vec![1_000, 2_000, 3_000]);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fully_filtered_segment_stream_yields_to_runtime() -> TestResult {
        let tmp = TempDir::new()?;
        let path = tmp.path().join("filtered-row-groups.parquet");
        write_multi_row_group_parquet(&path, &[&[1_000], &[2_000], &[3_000]])?;
        let data = bytes::Bytes::from(std::fs::read(path)?);
        let (reader, read_calls, _) = TrackingReader::new(data);
        let observer_ran = Arc::new(AtomicBool::new(false));
        let observer = tokio::spawn({
            let read_calls = Arc::clone(&read_calls);
            let observer_ran = Arc::clone(&observer_ran);
            async move {
                while read_calls.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
                observer_ran.store(true, Ordering::SeqCst);
            }
        });

        let mut stream = segment_range_stream(
            reader,
            "data/filtered-row-groups.parquet".to_string(),
            "ts",
            Utc.timestamp_millis_opt(10_000).single().unwrap(),
            Utc.timestamp_millis_opt(20_000).single().unwrap(),
        )
        .await?;
        assert!(stream.next().await.is_none());
        assert!(observer_ran.load(Ordering::SeqCst));
        observer.await?;
        Ok(())
    }

    #[tokio::test]
    async fn read_segment_range_errors_when_missing_time_column() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let rel = "data/no-ts.parquet";
        let path = tmp.path().join(rel);
        write_parquet_without_time_column(&path, &["A"], &[1.0])?;

        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 0),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        };

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 1, 0);

        let err = match read_segment_range(&location, &segment, "ts", start, end).await {
            Err(err) => err,
            Ok(_) => panic!("missing ts column should error"),
        };

        assert!(matches!(err, TableError::MissingTimeColumn { .. }));
        assert!(err.to_string().contains(rel));
        Ok(())
    }

    #[tokio::test]
    async fn read_segment_range_errors_on_unsupported_time_type() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let rel = "data/int-ts.parquet";
        let path = tmp.path().join(rel);
        let ts_vals = [1_000_i64, 2_000];
        write_arrow_parquet_int_time(&path, &ts_vals, &["A", "B"], &[1.0, 2.0])?;

        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 1),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 2),
            row_count: ts_vals.len() as u64,
            file_size: None,
            coverage_path: None,
        };

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 1, 0);

        let err = match read_segment_range(&location, &segment, "ts", start, end).await {
            Err(err) => err,
            Ok(_) => panic!("unsupported time type should error"),
        };

        assert!(matches!(err, TableError::UnsupportedTimeType { .. }));
        assert!(err.to_string().contains(rel));
        Ok(())
    }

    #[tokio::test]
    async fn read_segment_range_overflow_bounds_nanoseconds() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let rel = "data/nano-empty.parquet";
        let path = tmp.path().join(rel);
        write_arrow_parquet_with_unit(&path, ArrowTimeUnit::Nanosecond, &[], &[], &[])?;

        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 0),
            row_count: 0,
            file_size: None,
            coverage_path: None,
        };

        let huge = Utc
            .timestamp_opt(9_223_372_037, 0)
            .single()
            .expect("overflow ts");
        let err = match read_segment_range(&location, &segment, "ts", huge, huge).await {
            Err(err) => err,
            Ok(_) => panic!("overflow during bound conversion should error"),
        };

        assert!(matches!(err, TableError::TimeConversionOverflow { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_filters_and_orders_across_segments() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel1 = "data/seg-scan-1.parquet";
        let path1 = tmp.path().join(rel1);
        write_test_parquet(
            &path1,
            true,
            false,
            &[
                TestRow {
                    ts_millis: 1_000,
                    symbol: "A",
                    price: 10.0,
                },
                TestRow {
                    ts_millis: 2_000,
                    symbol: "A",
                    price: 20.0,
                },
            ],
        )?;

        let rel2 = "data/seg-scan-2.parquet";
        let path2 = tmp.path().join(rel2);
        write_test_parquet(
            &path2,
            true,
            false,
            &[
                TestRow {
                    ts_millis: 61_000,
                    symbol: "A",
                    price: 30.0,
                },
                TestRow {
                    ts_millis: 62_000,
                    symbol: "A",
                    price: 40.0,
                },
            ],
        )?;

        table.append_parquet_segment(rel1, "ts").await?;
        table.append_parquet_segment(rel2, "ts").await?;

        // Query spans both segments but excludes the last row of the second segment.
        let start = Utc.timestamp_millis_opt(1_500).single().expect("valid ts");
        let end = Utc.timestamp_millis_opt(61_500).single().expect("valid ts");

        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(
            rows,
            vec![
                (2_000, "A".to_string(), 20.0),
                (61_000, "A".to_string(), 30.0),
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn scan_range_exclusive_end_and_empty() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel = "data/seg-boundary.parquet";
        let path = tmp.path().join(rel);
        write_test_parquet(
            &path,
            true,
            false,
            &[
                TestRow {
                    ts_millis: 1_000,
                    symbol: "A",
                    price: 10.0,
                },
                TestRow {
                    ts_millis: 2_000,
                    symbol: "A",
                    price: 20.0,
                },
            ],
        )?;

        table.append_parquet_segment(rel, "ts").await?;

        let start = Utc.timestamp_millis_opt(1_000).single().expect("valid ts");
        let end = Utc.timestamp_millis_opt(2_000).single().expect("valid ts");
        let rows = collect_scan_rows(&table, start, end).await?;
        assert_eq!(rows, vec![(1_000, "A".to_string(), 10.0)]);

        let empty_start = Utc.timestamp_millis_opt(5_000).single().expect("valid ts");
        let empty_end = Utc.timestamp_millis_opt(6_000).single().expect("valid ts");
        let rows = collect_scan_rows(&table, empty_start, empty_end).await?;
        assert!(rows.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn scan_range_rejects_invalid_range() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let table = TimeSeriesTable::create(location, meta).await?;

        let start = Utc.timestamp_millis_opt(1_000).single().expect("valid ts");
        let end = start;

        let result = table.scan_range(start, end).await;

        assert!(matches!(result, Err(TableError::InvalidRange { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_supports_second_unit() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel = "data/seg-seconds.parquet";
        let path = tmp.path().join(rel);
        write_arrow_parquet_with_unit(
            &path,
            ArrowTimeUnit::Second,
            &[Some(1), Some(2), Some(3)],
            &["A", "A", "A"],
            &[1.0, 2.0, 3.0],
        )?;
        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: Utc.timestamp_opt(1, 0).single().unwrap(),
            ts_max: Utc.timestamp_opt(3, 0).single().unwrap(),
            row_count: 3,
            file_size: None,
            coverage_path: None,
        };
        table.state.segments.insert(segment.path.clone(), segment);

        let start = Utc.timestamp_millis_opt(1_500).single().unwrap();
        let end = Utc.timestamp_millis_opt(2_500).single().unwrap();
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(rows, vec![(2, "A".to_string(), 2.0)]);
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_supports_microsecond_unit() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_table_meta_with_unit(LogicalTimestampUnit::Micros);
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel = "data/seg-micros.parquet";
        let path = tmp.path().join(rel);
        write_arrow_parquet_with_unit(
            &path,
            ArrowTimeUnit::Microsecond,
            &[Some(1_000_000), Some(2_000_000), Some(3_000_000)],
            &["A", "A", "A"],
            &[1.0, 2.0, 3.0],
        )?;

        table.append_parquet_segment(rel, "ts").await?;

        let start = Utc
            .timestamp_opt(1, 500_000_000)
            .single()
            .expect("valid start");
        let end = Utc
            .timestamp_opt(2, 500_000_000)
            .single()
            .expect("valid end");
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(rows, vec![(2_000_000, "A".to_string(), 2.0)]);
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_supports_nanosecond_unit() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_table_meta_with_unit(LogicalTimestampUnit::Nanos);
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel = "data/seg-nanos.parquet";
        let path = tmp.path().join(rel);
        write_arrow_parquet_with_unit(
            &path,
            ArrowTimeUnit::Nanosecond,
            &[
                Some(1_000_000_000),
                Some(1_500_000_000),
                Some(2_000_000_000),
            ],
            &["A", "A", "A"],
            &[1.0, 2.0, 3.0],
        )?;

        table.append_parquet_segment(rel, "ts").await?;

        let start = Utc
            .timestamp_opt(1, 250_000_000)
            .single()
            .expect("valid start");
        let end = Utc
            .timestamp_opt(1, 750_000_000)
            .single()
            .expect("valid end");
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(rows, vec![(1_500_000_000, "A".to_string(), 2.0)]);
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_filters_null_timestamps() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_table_meta_with_unit(LogicalTimestampUnit::Millis);
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel = "data/seg-null-ts.parquet";
        let path = tmp.path().join(rel);
        write_arrow_parquet_with_unit(
            &path,
            ArrowTimeUnit::Millisecond,
            &[Some(1_000), None, Some(2_000)],
            &["A", "A", "A"],
            &[1.0, 2.0, 3.0],
        )?;

        table.append_parquet_segment(rel, "ts").await?;

        let start = Utc.timestamp_millis_opt(500).single().unwrap();
        let end = Utc.timestamp_millis_opt(2_500).single().unwrap();
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(
            rows,
            vec![(1_000, "A".to_string(), 1.0), (2_000, "A".to_string(), 3.0)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_empty_when_no_segments() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let table = TimeSeriesTable::create(location, meta).await?;

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 1, 0);

        let mut stream = table.scan_range(start, end).await?;
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_empty_for_zero_row_segment() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel = "data/seg-empty.parquet";
        let path = tmp.path().join(rel);
        write_arrow_parquet_with_unit(&path, ArrowTimeUnit::Millisecond, &[], &[], &[])?;

        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 0),
            row_count: 0,
            file_size: None,
            coverage_path: None,
        };

        table.state.segments.insert(segment.path.clone(), segment);

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 1, 0);

        let mut stream = table.scan_range(start, end).await?;
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_all_null_time_filtered_out() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_table_meta_with_unit(LogicalTimestampUnit::Millis);
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel = "data/seg-null-only.parquet";
        let path = tmp.path().join(rel);
        write_arrow_parquet_with_unit(
            &path,
            ArrowTimeUnit::Millisecond,
            &[None, None],
            &["A", "B"],
            &[1.0, 2.0],
        )?;

        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 1),
            row_count: 2,
            file_size: None,
            coverage_path: None,
        };

        table.state.segments.insert(segment.path.clone(), segment);

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 0, 5);

        let mut stream = table.scan_range(start, end).await?;
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_errors_on_missing_time_column_in_segment() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel = "data/seg-scan-no-ts.parquet";
        let path = tmp.path().join(rel);
        write_parquet_without_time_column(&path, &["A"], &[1.0])?;

        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2024, 1, 1, 0, 1, 0),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        };

        table.state.segments.insert(segment.path.clone(), segment);
        let unopened = SegmentMeta {
            path: "data/should-not-open.parquet".to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 1, 30),
            ts_max: utc_datetime(2024, 1, 1, 0, 1, 31),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        };
        table.state.segments.insert(unopened.path.clone(), unopened);

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 2, 0);

        let mut stream = table.scan_range(start, end).await?;
        let err = stream.next().await.expect("expected error from scan");

        assert!(matches!(err, Err(TableError::MissingTimeColumn { .. })));
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_errors_on_unsupported_time_type_segment() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel = "data/seg-scan-int-ts.parquet";
        let path = tmp.path().join(rel);
        write_arrow_parquet_int_time(&path, &[1_000], &["A"], &[1.0])?;

        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 1),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 1),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        };

        table.state.segments.insert(segment.path.clone(), segment);

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 1, 0);

        let mut stream = table.scan_range(start, end).await?;
        let err = stream.next().await.expect("expected error from scan");

        assert!(matches!(err, Err(TableError::UnsupportedTimeType { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_orders_segments_by_ts_min() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel_b = "data/seg-overlap-b.parquet";
        let path_b = tmp.path().join(rel_b);
        write_test_parquet(
            &path_b,
            true,
            false,
            &[TestRow {
                ts_millis: 120_000,
                symbol: "A",
                price: 2.0,
            }],
        )?;

        let rel_a = "data/seg-overlap-a.parquet";
        let path_a = tmp.path().join(rel_a);
        write_test_parquet(
            &path_a,
            true,
            false,
            &[TestRow {
                ts_millis: 60_000,
                symbol: "A",
                price: 1.0,
            }],
        )?;

        // Append in reverse ts_min order to exercise the segment comparator.
        table.append_parquet_segment(rel_b, "ts").await?;
        table.append_parquet_segment(rel_a, "ts").await?;

        let start = Utc.timestamp_millis_opt(50_000).single().unwrap();
        let end = Utc.timestamp_millis_opt(150_000).single().unwrap();
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(
            rows,
            vec![
                (60_000, "A".to_string(), 1.0),
                (120_000, "A".to_string(), 2.0)
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_skips_non_overlapping_segments() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel1 = "data/seg-early.parquet";
        let path1 = tmp.path().join(rel1);
        write_test_parquet(
            &path1,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 1.0,
            }],
        )?;

        let rel2 = "data/seg-late.parquet";
        let path2 = tmp.path().join(rel2);
        write_test_parquet(
            &path2,
            true,
            false,
            &[TestRow {
                ts_millis: 70_000,
                symbol: "A",
                price: 9.0,
            }],
        )?;

        table.append_parquet_segment(rel1, "ts").await?;
        table.append_parquet_segment(rel2, "ts").await?;

        let start = Utc.timestamp_millis_opt(1_500).single().unwrap();
        let end = Utc.timestamp_millis_opt(2_000).single().unwrap();
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(rows, Vec::new());
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_reports_missing_segment_path() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;
        let rel = "data/missing.parquet";
        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: Utc.timestamp_millis_opt(1_000).single().unwrap(),
            ts_max: Utc.timestamp_millis_opt(2_000).single().unwrap(),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        };
        table.state.segments.insert(segment.path.clone(), segment);

        let mut stream = table
            .scan_range(
                Utc.timestamp_millis_opt(0).single().unwrap(),
                Utc.timestamp_millis_opt(3_000).single().unwrap(),
            )
            .await?;
        let error = stream
            .next()
            .await
            .expect("missing segment error")
            .expect_err("missing segment should fail");

        assert!(matches!(error, TableError::Storage { .. }));
        assert!(error.to_string().contains(rel));
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_propagates_parquet_read_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel = "data/seg-corrupt.parquet";
        let path = tmp.path().join(rel);
        write_test_parquet(
            &path,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 1.0,
            }],
        )?;

        table.append_parquet_segment(rel, "ts").await?;

        // Corrupt the file after append so scan encounters a read failure.
        let f = std::fs::OpenOptions::new().write(true).open(&path)?;
        f.set_len(4)?;

        let start = Utc.timestamp_millis_opt(0).single().unwrap();
        let end = Utc.timestamp_millis_opt(2_000).single().unwrap();

        let mut stream = table.scan_range(start, end).await?;
        let err = stream
            .next()
            .await
            .expect("first item should be error")
            .expect_err("corrupt segment should fail");

        assert!(matches!(err, TableError::ParquetRead { .. }));
        assert!(err.to_string().contains(rel));
        Ok(())
    }
}
