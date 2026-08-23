//! Range scan implementation for `TimeSeriesTable`.
//!
//! This module wires the public `scan_range` entry point to the underlying
//! segment metadata and Parquet readers:
//! - Pick candidate segments whose ordered `index_min`/`index_max` intersects the requested
//!   half-open range `[start, end)`.
//! - Visit candidate segments deterministically with Parquet's native async,
//!   file-backed reader.
//! - Filter each batch by its ordered-index column with native Arrow scalar
//!   comparisons and half-open semantics.
//!
//! The filtering path uses Arrow's scalar comparison kernels to avoid
//! allocating full-length bound arrays, and treats null index values as
//! "drop row" via `filter_record_batch`. Input rows need not be ordered, and
//! the returned batches and rows have no ordering guarantee.
use std::{path::Path, pin::Pin};

use arrow::array::{Datum, Scalar};
use arrow::array::{
    Int64Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::{boolean as boolean_kernels, cmp as cmp_kernels};
use arrow::datatypes::{DataType, Field, TimeUnit};
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, future};
use parquet::{
    arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    errors::ParquetError,
};
use snafu::{Backtrace, prelude::*};

use crate::metadata::{
    segments::SegmentMeta,
    table_metadata::{IndexValue, IndexValueError, validate_index_range},
};
use crate::storage::{self, TableLocation};
use crate::table::error::ScanSnafu;
use crate::table::{TableError, TimeSeriesScan, TimeSeriesTable};
use crate::transaction_log::TableState;

const SCAN_BATCH_SIZE: usize = 8_192;

type SegmentScanStream =
    Pin<Box<dyn futures::Stream<Item = Result<RecordBatch, ScanError>> + Send>>;

/// Errors from planning or lazily executing a table scan.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum ScanError {
    /// The requested half-open ordered-index range is invalid.
    #[snafu(display("Invalid scan range: {source}"))]
    InvalidRange {
        /// Complete range validation error.
        source: IndexValueError,
        /// Backtrace captured at the scan planning boundary.
        backtrace: Box<Backtrace>,
    },

    /// Persisted segment bounds cannot be ordered in the table's index domain.
    #[snafu(display("Invalid persisted segment bounds while planning scan: {source}"))]
    InvalidSegmentBounds {
        /// Complete segment bounds error.
        source: IndexValueError,
        /// Backtrace captured at the scan planning boundary.
        backtrace: Box<Backtrace>,
    },

    /// Opening a segment from storage failed during lazy scan execution.
    #[snafu(display("Failed to open segment {path} during scan execution: {source}"))]
    Storage {
        /// Table-relative segment path.
        path: String,
        /// Complete storage error.
        #[snafu(source(from(storage::StorageError, Box::new)), backtrace)]
        source: Box<storage::StorageError>,
    },

    /// Reading Parquet metadata or batches failed.
    #[snafu(display(
        "Parquet error while {operation} for segment {path} during scan execution: {source}"
    ))]
    Parquet {
        /// Table-relative segment path.
        path: String,
        /// Parquet operation that failed.
        operation: &'static str,
        /// Complete Parquet error.
        #[snafu(source(from(ParquetError, Box::new)))]
        source: Box<ParquetError>,
        /// Backtrace captured at the scan boundary.
        backtrace: Box<Backtrace>,
    },

    /// An Arrow compute operation failed while filtering a batch.
    #[snafu(display(
        "Arrow error while {operation} for column {column} in segment {path} during scan execution: {source}"
    ))]
    Arrow {
        /// Table-relative segment path.
        path: String,
        /// Configured ordered-index column.
        column: String,
        /// Arrow operation that failed.
        operation: &'static str,
        /// Complete Arrow error.
        #[snafu(source(from(arrow::error::ArrowError, Box::new)))]
        source: Box<arrow::error::ArrowError>,
        /// Backtrace captured at the scan boundary.
        backtrace: Box<Backtrace>,
    },

    /// A segment is missing the configured ordered-index column.
    #[snafu(display(
        "Missing ordered-index column {column} in segment {path} during scan execution"
    ))]
    MissingIndexColumn {
        /// Table-relative segment path.
        path: String,
        /// Configured ordered-index column.
        column: String,
        /// Backtrace captured at the scan boundary.
        backtrace: Box<Backtrace>,
    },

    /// A segment's ordered-index Arrow type disagrees with the table index.
    #[snafu(display(
        "Ordered-index column {column} in segment {path} has Arrow type {datatype:?}, expected {expected}, during scan execution"
    ))]
    IndexColumnTypeMismatch {
        /// Table-relative segment path.
        path: String,
        /// Configured ordered-index column.
        column: String,
        /// Registered ordered-index domain.
        expected: &'static str,
        /// Arrow type found in the segment.
        datatype: Box<DataType>,
        /// Backtrace captured at the scan boundary.
        backtrace: Box<Backtrace>,
    },

    /// Converting a timestamp bound to the segment's unit would overflow `i64`.
    #[snafu(display(
        "Timestamp conversion overflow for column {column} in segment {path} during scan execution (value: {timestamp})"
    ))]
    TimeConversionOverflow {
        /// Table-relative segment path.
        path: String,
        /// Configured ordered-index column.
        column: String,
        /// Timestamp that could not be represented.
        timestamp: DateTime<Utc>,
        /// Backtrace captured at the scan boundary.
        backtrace: Box<Backtrace>,
    },
}

fn segments_for_range(
    state: &TableState,
    start: &IndexValue,
    end: &IndexValue,
) -> Result<Vec<SegmentMeta>, IndexValueError> {
    let mut candidates = Vec::new();
    for segment in state.segments_sorted_by_index()? {
        if !segment.index_max.compare(start)?.is_lt() && segment.index_min.compare(end)?.is_lt() {
            candidates.push(segment.clone());
        }
    }
    Ok(candidates)
}

/// Filter one batch with native scalar comparisons. Arrow broadcasts each
/// scalar bound without allocating a batch-sized bound column.
fn filter_index_batch(
    batch: RecordBatch,
    index_idx: usize,
    start: &dyn Datum,
    end: &dyn Datum,
    path: &str,
    index_column: &str,
) -> Result<Option<RecordBatch>, ScanError> {
    let index_array = batch.column(index_idx).as_ref();
    let ge_mask = cmp_kernels::gt_eq(&index_array, start).context(ArrowSnafu {
        path,
        column: index_column,
        operation: "comparing the lower bound",
    })?;
    let lt_mask = cmp_kernels::lt(&index_array, end).context(ArrowSnafu {
        path,
        column: index_column,
        operation: "comparing the upper bound",
    })?;
    let mask = boolean_kernels::and(&ge_mask, &lt_mask).context(ArrowSnafu {
        path,
        column: index_column,
        operation: "combining comparison masks",
    })?;
    let filtered = filter_record_batch(&batch, &mask).context(ArrowSnafu {
        path,
        column: index_column,
        operation: "filtering a record batch",
    })?;
    Ok((filtered.num_rows() > 0).then_some(filtered))
}

#[derive(Clone, Copy)]
enum ScanBounds {
    Timestamp(i64, i64),
    Int64(i64, i64),
    UInt64(u64, u64),
}

fn timestamp_bounds_for_field(
    field: &Field,
    path: &str,
    column: &str,
    ts_start: DateTime<Utc>,
    ts_end: DateTime<Utc>,
) -> Result<(i64, i64), ScanError> {
    let ceil_bound = |dt: DateTime<Utc>, floor: i64, nanos_per_unit: u32| {
        if dt.timestamp_subsec_nanos().is_multiple_of(nanos_per_unit) {
            Ok(floor)
        } else {
            floor.checked_add(1).context(TimeConversionOverflowSnafu {
                path,
                column,
                timestamp: dt,
            })
        }
    };
    let to_ns = |dt: DateTime<Utc>| {
        dt.timestamp()
            .checked_mul(1_000_000_000)
            .and_then(|secs| secs.checked_add(dt.timestamp_subsec_nanos() as i64))
            .context(TimeConversionOverflowSnafu {
                path,
                column,
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

        other => IndexColumnTypeMismatchSnafu {
            path,
            column,
            expected: "timestamp",
            datatype: other.clone(),
        }
        .fail(),
    }
}

async fn build_segment_scan_stream<S, E>(
    reader: impl AsyncFileReader + Unpin + 'static,
    path: String,
    index_column: &str,
    start: S,
    end: E,
) -> Result<SegmentScanStream, ScanError>
where
    S: Into<IndexValue>,
    E: Into<IndexValue>,
{
    let start = start.into();
    let end = end.into();
    let expected = start.kind_name();

    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .context(ParquetSnafu {
            path: &path,
            operation: "reading metadata",
        })?;

    // Locate the index column and compute native bounds before moving the
    // builder into the directly-polled record-batch stream.
    let schema = builder.schema();
    let index_idx = schema
        .index_of(index_column)
        .ok()
        .context(MissingIndexColumnSnafu {
            path: &path,
            column: index_column,
        })?;
    let index_field = schema.field(index_idx).clone();
    let bounds = match (&start, &end, index_field.data_type()) {
        (IndexValue::Timestamp(start), IndexValue::Timestamp(end), DataType::Timestamp(_, _)) => {
            let (start, end) =
                timestamp_bounds_for_field(&index_field, &path, index_column, *start, *end)?;
            ScanBounds::Timestamp(start, end)
        }
        (IndexValue::Int64(start), IndexValue::Int64(end), DataType::Int64) => {
            ScanBounds::Int64(*start, *end)
        }
        (IndexValue::UInt64(start), IndexValue::UInt64(end), DataType::UInt64) => {
            ScanBounds::UInt64(*start, *end)
        }
        _ => {
            return IndexColumnTypeMismatchSnafu {
                path,
                column: index_column,
                expected,
                datatype: index_field.data_type().clone(),
            }
            .fail();
        }
    };

    let reader = builder
        .with_batch_size(SCAN_BATCH_SIZE)
        .build()
        .context(ParquetSnafu {
            path: &path,
            operation: "building the batch stream",
        })?;
    let index_column = index_column.to_string();

    let stream = reader
        .then(move |batch_res| {
            let path = path.clone();
            let index_column = index_column.clone();
            let index_field = index_field.clone();

            async move {
                let batch = batch_res.context(ParquetSnafu {
                    path: &path,
                    operation: "reading a batch",
                })?;

                let filtered = match (bounds, index_field.data_type()) {
                    (ScanBounds::Timestamp(start, end), DataType::Timestamp(unit, timezone)) => {
                        macro_rules! filter_timestamp {
                            ($array_ty:ty) => {{
                                let start = Scalar::new(
                                    <$array_ty>::from(vec![start])
                                        .with_timezone_opt(timezone.clone()),
                                );
                                let end = Scalar::new(
                                    <$array_ty>::from(vec![end])
                                        .with_timezone_opt(timezone.clone()),
                                );
                                filter_index_batch(
                                    batch,
                                    index_idx,
                                    &start,
                                    &end,
                                    &path,
                                    &index_column,
                                )
                            }};
                        }
                        match unit {
                            TimeUnit::Second => filter_timestamp!(TimestampSecondArray)?,
                            TimeUnit::Millisecond => filter_timestamp!(TimestampMillisecondArray)?,
                            TimeUnit::Microsecond => filter_timestamp!(TimestampMicrosecondArray)?,
                            TimeUnit::Nanosecond => filter_timestamp!(TimestampNanosecondArray)?,
                        }
                    }
                    (ScanBounds::Int64(start, end), DataType::Int64) => {
                        let start = Scalar::new(Int64Array::from(vec![start]));
                        let end = Scalar::new(Int64Array::from(vec![end]));
                        filter_index_batch(batch, index_idx, &start, &end, &path, &index_column)?
                    }
                    (ScanBounds::UInt64(start, end), DataType::UInt64) => {
                        let start = Scalar::new(UInt64Array::from(vec![start]));
                        let end = Scalar::new(UInt64Array::from(vec![end]));
                        filter_index_batch(batch, index_idx, &start, &end, &path, &index_column)?
                    }
                    (_, datatype) => {
                        return IndexColumnTypeMismatchSnafu {
                            path,
                            column: index_column,
                            expected,
                            datatype: datatype.clone(),
                        }
                        .fail();
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

async fn open_segment_scan<S, E>(
    location: &TableLocation,
    segment: &SegmentMeta,
    index_column: &str,
    start: S,
    end: E,
) -> Result<SegmentScanStream, ScanError>
where
    S: Into<IndexValue>,
    E: Into<IndexValue>,
{
    let rel_path = Path::new(&segment.path);
    let reader = storage::open_parquet_reader(location.as_ref(), rel_path)
        .await
        .context(StorageSnafu {
            path: &segment.path,
        })?;

    build_segment_scan_stream(reader, segment.path.clone(), index_column, start, end).await
}

struct ScanState {
    candidates: std::vec::IntoIter<SegmentMeta>,
    current: Option<SegmentScanStream>,
    location: TableLocation,
    index_column: String,
    start: IndexValue,
    end: IndexValue,
}

impl TimeSeriesTable {
    fn build_scan_stream(
        &self,
        start: IndexValue,
        end: IndexValue,
    ) -> Result<
        impl futures::Stream<Item = Result<RecordBatch, ScanError>> + Send + 'static,
        ScanError,
    > {
        validate_index_range(&self.index.kind, &start, &end).context(InvalidRangeSnafu)?;

        // Pick candidate segments and sort them by index_min, index_max, and path.
        let candidates =
            segments_for_range(&self.state, &start, &end).context(InvalidSegmentBoundsSnafu)?;

        let state = ScanState {
            candidates: candidates.into_iter(),
            current: None,
            location: self.location().clone(),
            index_column: self.index.column.clone(),
            start,
            end,
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
                    open_segment_scan(
                        &state.location,
                        &segment,
                        &state.index_column,
                        state.start.clone(),
                        state.end.clone(),
                    )
                    .await?,
                );
            }
        });

        Ok(stream)
    }

    /// Scan the time-series table for record batches overlapping `[start, end)`,
    /// returning a stream of filtered batches from the segments covering that range.
    ///
    /// Input rows need not be ordered. The returned batches and rows have no
    /// ordering guarantee; callers that need ordered results must sort them.
    pub async fn scan_range<S, E>(&self, start: S, end: E) -> Result<TimeSeriesScan, TableError>
    where
        S: Into<IndexValue>,
        E: Into<IndexValue>,
    {
        let start = start.into();
        let end = end.into();
        let stream = self.build_scan_stream(start, end).context(ScanSnafu)?;
        Ok(Box::pin(
            stream.map_err(|source| TableError::Scan { source }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::TableLocation;
    use crate::table::test_util::*;

    use crate::metadata::logical_schema::LogicalTimestampUnit;
    use crate::metadata::segments::{FileFormat, SegmentEntityLayout};
    use crate::metadata::table_metadata::{IndexKind, IndexSpec, TableMeta};

    use arrow::array::ArrayRef;
    use arrow::datatypes::{Schema, TimeUnit as ArrowTimeUnit};

    use chrono::{TimeZone, Utc};
    use futures::{FutureExt, StreamExt, future::BoxFuture};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ArrowReaderOptions;
    use parquet::errors::Result as ParquetResult;
    use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
    use parquet::file::properties::WriterProperties;

    use snafu::ErrorCompat;
    use std::error::Error as _;
    use std::fs::File;
    use std::num::NonZeroU64;
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tempfile::TempDir;

    #[derive(Default)]
    struct TrackingStats {
        read_calls: AtomicUsize,
        bytes_read: AtomicUsize,
        dropped: AtomicBool,
    }

    struct TrackingReader {
        data: bytes::Bytes,
        stats: Arc<TrackingStats>,
        gate: Option<(usize, futures::channel::oneshot::Receiver<()>)>,
        fail_on_read_call: Option<usize>,
    }

    impl TrackingReader {
        fn new(data: bytes::Bytes) -> (Self, Arc<TrackingStats>) {
            let stats = Arc::new(TrackingStats::default());
            (
                Self {
                    data,
                    stats: Arc::clone(&stats),
                    gate: None,
                    fail_on_read_call: None,
                },
                stats,
            )
        }

        fn with_failure(data: bytes::Bytes, failed_call: usize) -> (Self, Arc<TrackingStats>) {
            let (mut reader, stats) = Self::new(data);
            reader.fail_on_read_call = Some(failed_call);
            (reader, stats)
        }

        fn with_gate(
            data: bytes::Bytes,
            gated_call: usize,
        ) -> (
            Self,
            Arc<TrackingStats>,
            futures::channel::oneshot::Sender<()>,
        ) {
            let (mut reader, stats) = Self::new(data);
            let (release, gate) = futures::channel::oneshot::channel();
            reader.gate = Some((gated_call, gate));
            (reader, stats, release)
        }

        fn read_ranges(&self, ranges: Vec<Range<u64>>) -> (usize, Vec<bytes::Bytes>) {
            let call = self.stats.read_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.stats.bytes_read.fetch_add(
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

    impl Drop for TrackingReader {
        fn drop(&mut self) {
            self.stats.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl AsyncFileReader for TrackingReader {
        fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
            let (call, mut ranges) = self.read_ranges(vec![range]);
            let result = if self.fail_on_read_call == Some(call) {
                Err(ParquetError::General(
                    "injected batch read failure".to_string(),
                ))
            } else {
                Ok(ranges.pop().expect("one requested range"))
            };
            futures::future::ready(result).boxed()
        }

        fn get_byte_ranges(
            &mut self,
            ranges: Vec<Range<u64>>,
        ) -> BoxFuture<'_, ParquetResult<Vec<bytes::Bytes>>> {
            let (call, bytes) = self.read_ranges(ranges);
            let fail = self.fail_on_read_call == Some(call);
            let gate = self
                .gate
                .as_ref()
                .is_some_and(|(gated_call, _)| *gated_call == call)
                .then(|| self.gate.take().expect("configured gate").1);
            async move {
                if let Some(gate) = gate {
                    let _ = gate.await;
                }
                if fail {
                    Err(ParquetError::General(
                        "injected batch read failure".to_string(),
                    ))
                } else {
                    Ok(bytes)
                }
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

    fn indexed_segment(path: &str, min: IndexValue, max: IndexValue) -> SegmentMeta {
        SegmentMeta {
            path: path.to_string(),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: min,
            index_max: max,
            row_count: 1,
            file_size: None,
            coverage_path: None,
        }
    }

    fn state_with_segments(segments: Vec<SegmentMeta>) -> TableState {
        TableState {
            version: 1,
            table_meta: make_basic_table_meta(),
            segments: segments
                .into_iter()
                .map(|segment| (segment.path.clone(), segment))
                .collect(),
            table_coverage: None,
        }
    }

    fn integer_table_meta(kind: IndexKind) -> TableMeta {
        TableMeta::new_time_series(IndexSpec {
            column: "ts".to_string(),
            entity_columns: Vec::new(),
            kind,
        })
    }

    fn write_index_parquet(path: &Path, values: ArrayRef) -> TestResult {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            values.data_type().clone(),
            values.null_count() > 0,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values])?;
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build();
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    async fn collect_i64_index(
        table: &TimeSeriesTable,
        start: i64,
        end: i64,
    ) -> Result<Vec<i64>, TableError> {
        let mut stream = table.scan_range(start, end).await?;
        let mut values = Vec::new();
        while let Some(batch) = stream.next().await.transpose()? {
            assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
            values.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 index")
                    .iter()
                    .flatten(),
            );
        }
        Ok(values)
    }

    async fn collect_u64_index(
        table: &TimeSeriesTable,
        start: u64,
        end: u64,
    ) -> Result<Vec<u64>, TableError> {
        let mut stream = table.scan_range(start, end).await?;
        let mut values = Vec::new();
        while let Some(batch) = stream.next().await.transpose()? {
            assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt64);
            values.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("uint64 index")
                    .iter()
                    .flatten(),
            );
        }
        Ok(values)
    }

    #[test]
    fn integer_candidates_preserve_half_open_order() -> Result<(), IndexValueError> {
        let signed = state_with_segments(vec![
            indexed_segment("before", (-10i64).into(), (-1i64).into()),
            indexed_segment("touch-start", (-5i64).into(), 0i64.into()),
            indexed_segment("same-b", 1i64.into(), 9i64.into()),
            indexed_segment("same-a", 1i64.into(), 9i64.into()),
            indexed_segment("at-end", 10i64.into(), 20i64.into()),
        ]);
        let signed_paths = segments_for_range(&signed, &0i64.into(), &10i64.into())?
            .into_iter()
            .map(|segment| segment.path)
            .collect::<Vec<_>>();
        assert_eq!(signed_paths, ["touch-start", "same-a", "same-b"]);

        let start = i64::MAX as u64 + 1;
        let unsigned = state_with_segments(vec![
            indexed_segment("below", 0u64.into(), (start - 1).into()),
            indexed_segment("touch-start", (start - 1).into(), start.into()),
            indexed_segment("inside", (start + 1).into(), (u64::MAX - 1).into()),
            indexed_segment("at-end", u64::MAX.into(), u64::MAX.into()),
        ]);
        let unsigned_paths = segments_for_range(&unsigned, &start.into(), &u64::MAX.into())?
            .into_iter()
            .map(|segment| segment.path)
            .collect::<Vec<_>>();
        assert_eq!(unsigned_paths, ["touch-start", "inside"]);
        Ok(())
    }

    #[test]
    fn candidate_selection_rejects_invalid_persisted_bounds() {
        let inverted =
            state_with_segments(vec![indexed_segment("inverted", 2i64.into(), 1i64.into())]);
        assert!(matches!(
            segments_for_range(&inverted, &0i64.into(), &3i64.into()),
            Err(IndexValueError::InvalidBounds { .. })
        ));

        let signed = state_with_segments(vec![indexed_segment("signed", 0i64.into(), 1i64.into())]);
        assert!(matches!(
            segments_for_range(&signed, &0u64.into(), &2u64.into()),
            Err(IndexValueError::DomainMismatch { .. })
        ));
    }

    #[test]
    fn arrow_filter_failures_preserve_the_typed_source() -> TestResult {
        let batch =
            RecordBatch::try_from_iter([("ts", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])?;
        let start = Scalar::new(UInt64Array::from(vec![0]));
        let end = Scalar::new(UInt64Array::from(vec![2]));

        let error = filter_index_batch(batch, 0, &start, &end, "data/segment.parquet", "ts")
            .expect_err("mismatched comparison types must fail");

        assert!(matches!(
            error.source(),
            Some(source) if source.downcast_ref::<Box<arrow::error::ArrowError>>().is_some()
        ));
        assert!(ErrorCompat::backtrace(&error).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_filters_signed_integer_boundaries_and_nulls() -> TestResult {
        let tmp = TempDir::new()?;
        let kind = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        let mut table =
            TimeSeriesTable::create(TableLocation::local(tmp.path()), integer_table_meta(kind))
                .await?;
        let rel = "data/int64-scan.parquet";
        write_index_parquet(
            &tmp.path().join(rel),
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(-2),
                None,
                Some(-1),
                Some(0),
                Some(1),
                Some(i64::MAX - 1),
                Some(i64::MAX),
            ])),
        )?;
        append_parquet_fixture(&mut table, rel).await?;

        assert_eq!(collect_i64_index(&table, -2, 2).await?, [-2, -1, 0, 1]);
        assert_eq!(
            collect_i64_index(&table, i64::MIN, i64::MIN + 1).await?,
            [i64::MIN]
        );
        assert_eq!(
            collect_i64_index(&table, i64::MAX - 1, i64::MAX).await?,
            [i64::MAX - 1]
        );
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_preserves_large_unsigned_values() -> TestResult {
        let tmp = TempDir::new()?;
        let kind = IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        let mut table =
            TimeSeriesTable::create(TableLocation::local(tmp.path()), integer_table_meta(kind))
                .await?;
        let rel = "data/uint64-scan.parquet";
        let start = i64::MAX as u64 + 1;
        write_index_parquet(
            &tmp.path().join(rel),
            Arc::new(UInt64Array::from(vec![
                Some(start - 1),
                Some(start),
                None,
                Some(start + 1),
                Some(u64::MAX - 1),
                Some(u64::MAX),
            ])),
        )?;
        append_parquet_fixture(&mut table, rel).await?;

        assert_eq!(
            collect_u64_index(&table, start, u64::MAX).await?,
            [start, start + 1, u64::MAX - 1]
        );
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_validates_typed_bounds_before_segment_checks() -> TestResult {
        let tmp = TempDir::new()?;
        let kind = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        let mut table =
            TimeSeriesTable::create(TableLocation::local(tmp.path()), integer_table_meta(kind))
                .await?;
        table.state.segments.insert(
            "missing.parquet".to_string(),
            indexed_segment("missing.parquet", 2i64.into(), 1i64.into()),
        );

        let equal = match table.scan_range(0i64, 0i64).await {
            Err(error) => error,
            Ok(_) => panic!("equal range must fail"),
        };
        let reversed = match table.scan_range(1i64, 0i64).await {
            Err(error) => error,
            Ok(_) => panic!("reversed range must fail"),
        };
        for error in [equal, reversed] {
            assert!(matches!(
                error,
                TableError::Scan {
                    source: ScanError::InvalidRange {
                        source: IndexValueError::InvalidRange { .. },
                        ..
                    }
                }
            ));
        }

        assert!(matches!(
            table.scan_range(0i64, 1u64).await,
            Err(TableError::Scan {
                source: ScanError::InvalidRange {
                    source: IndexValueError::KindMismatch {
                        expected: "int64",
                        actual: "uint64"
                    },
                    ..
                }
            })
        ));
        let start = Utc.timestamp_opt(0, 0).single().unwrap();
        let end = Utc.timestamp_opt(1, 0).single().unwrap();
        assert!(matches!(
            table.scan_range(start, end).await,
            Err(TableError::Scan {
                source: ScanError::InvalidRange {
                    source: IndexValueError::KindMismatch {
                        expected: "int64",
                        actual: "timestamp"
                    },
                    ..
                }
            })
        ));

        assert!(matches!(
            table.scan_range(0i64, 3i64).await,
            Err(TableError::Scan {
                source: ScanError::InvalidSegmentBounds {
                    source: IndexValueError::InvalidBounds { .. },
                    ..
                }
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_rejects_reversed_unsigned_range() -> TestResult {
        let tmp = TempDir::new()?;
        let kind = IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        let table =
            TimeSeriesTable::create(TableLocation::local(tmp.path()), integer_table_meta(kind))
                .await?;

        assert!(matches!(
            table.scan_range(2u64, 1u64).await,
            Err(TableError::Scan {
                source: ScanError::InvalidRange {
                    source: IndexValueError::InvalidRange { .. },
                    ..
                }
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn integer_segment_stream_is_directly_polled() -> TestResult {
        let tmp = TempDir::new()?;
        let path = tmp.path().join("int64-stream.parquet");
        write_index_parquet(
            &path,
            Arc::new(Int64Array::from(vec![Some(0), Some(1), Some(-1), None])),
        )?;
        let data = bytes::Bytes::from(std::fs::read(path)?);
        let (reader, stats) = TrackingReader::new(data.clone());
        let mut stream = build_segment_scan_stream(
            reader,
            "data/int64-stream.parquet".to_string(),
            "ts",
            0i64,
            2i64,
        )
        .await?;
        assert_eq!(stats.read_calls.load(Ordering::SeqCst), 0);

        let batch = stream.next().await.transpose()?.expect("filtered batch");
        assert_eq!(stats.read_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 index")
                .iter()
                .flatten()
                .collect::<Vec<_>>(),
            [0]
        );
        drop(stream);
        assert!(stats.dropped.load(Ordering::SeqCst));

        let (reader, stats, release) = TrackingReader::with_gate(data, 2);
        let mut stream = build_segment_scan_stream(
            reader,
            "data/int64-stream.parquet".to_string(),
            "ts",
            0i64,
            2i64,
        )
        .await?;
        assert_eq!(
            stream
                .next()
                .await
                .transpose()?
                .expect("first integer batch")
                .num_rows(),
            1
        );
        let second = stream.next();
        futures::pin_mut!(second);
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(stats.read_calls.load(Ordering::SeqCst), 2);
        release.send(()).expect("release second row-group read");
        assert_eq!(
            second
                .await
                .transpose()?
                .expect("second integer batch")
                .num_rows(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn segment_scan_preserves_lazy_batch_read_error() -> TestResult {
        let tmp = TempDir::new()?;
        let path = tmp.path().join("lazy-read-failure.parquet");
        write_index_parquet(&path, Arc::new(Int64Array::from(vec![0, 1])))?;
        let data = bytes::Bytes::from(std::fs::read(path)?);
        let (reader, stats) = TrackingReader::with_failure(data, 1);
        let mut stream = build_segment_scan_stream(
            reader,
            "data/lazy-read-failure.parquet".to_string(),
            "ts",
            0i64,
            2i64,
        )
        .await?;

        assert_eq!(stats.read_calls.load(Ordering::SeqCst), 0);
        let error = stream
            .next()
            .await
            .expect("lazy batch read")
            .expect_err("injected read must fail");
        assert!(matches!(
            &error,
            ScanError::Parquet {
                path,
                operation: "reading a batch",
                ..
            } if path == "data/lazy-read-failure.parquet"
        ));
        assert!(matches!(
            error.source(),
            Some(source) if source.downcast_ref::<Box<ParquetError>>().is_some()
        ));
        assert!(ErrorCompat::backtrace(&error).is_some());
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn integer_scan_skips_non_candidates_and_stops_after_error() -> TestResult {
        let tmp = TempDir::new()?;
        let kind = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        let mut table =
            TimeSeriesTable::create(TableLocation::local(tmp.path()), integer_table_meta(kind))
                .await?;
        let wrong_type = "data/wrong-type.parquet";
        write_index_parquet(
            &tmp.path().join(wrong_type),
            Arc::new(UInt64Array::from(vec![1, 2])),
        )?;
        for segment in [
            indexed_segment(
                "data/non-candidate.parquet",
                (-10i64).into(),
                (-5i64).into(),
            ),
            indexed_segment(wrong_type, 1i64.into(), 2i64.into()),
            indexed_segment("data/later.parquet", 3i64.into(), 4i64.into()),
        ] {
            table.state.segments.insert(segment.path.clone(), segment);
        }

        let mut empty = table.scan_range(-20i64, -15i64).await?;
        assert!(empty.next().await.is_none());

        let mut failed = table.scan_range(0i64, 10i64).await?;
        assert!(matches!(
            failed.next().await,
            Some(Err(TableError::Scan {
                source: ScanError::IndexColumnTypeMismatch { .. }
            }))
        ));
        assert!(failed.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_reports_registered_and_decoded_index_types() -> TestResult {
        let tmp = TempDir::new()?;
        let kind = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        let mut table =
            TimeSeriesTable::create(TableLocation::local(tmp.path()), integer_table_meta(kind))
                .await?;
        let rel = "data/wrong-index-type.parquet";
        write_index_parquet(
            &tmp.path().join(rel),
            Arc::new(UInt64Array::from(vec![1, 2])),
        )?;
        table.state.segments.insert(
            rel.to_string(),
            indexed_segment(rel, 1i64.into(), 2i64.into()),
        );

        let mut stream = table.scan_range(0i64, 3i64).await?;
        let error = stream
            .next()
            .await
            .expect("segment error")
            .expect_err("decoded type must match registered index");
        assert!(matches!(
            error,
            TableError::Scan {
                source: ScanError::IndexColumnTypeMismatch {
                    path,
                    column,
                    expected: "int64",
                    datatype,
                    ..
                }
            } if path == rel && column == "ts" && *datatype == DataType::UInt64
        ));
        assert!(stream.next().await.is_none());
        Ok(())
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

    #[test]
    fn timestamp_bounds_round_up_to_column_precision() {
        let timestamp = |seconds, nanos| Utc.timestamp_opt(seconds, nanos).single().unwrap();
        let cases = [
            (
                ArrowTimeUnit::Second,
                timestamp(1, 500_000_000),
                timestamp(2, 500_000_000),
                (2, 3),
            ),
            (
                ArrowTimeUnit::Second,
                timestamp(-2, 500_000_000),
                timestamp(-1, 500_000_000),
                (-1, 0),
            ),
            (
                ArrowTimeUnit::Second,
                timestamp(1, 0),
                timestamp(2, 0),
                (1, 2),
            ),
            (
                ArrowTimeUnit::Millisecond,
                timestamp(1, 500_000),
                timestamp(2, 500_000),
                (1_001, 2_001),
            ),
            (
                ArrowTimeUnit::Millisecond,
                timestamp(-1, 999_500_000),
                timestamp(0, 500_000),
                (0, 1),
            ),
            (
                ArrowTimeUnit::Millisecond,
                timestamp(1, 0),
                timestamp(2, 0),
                (1_000, 2_000),
            ),
            (
                ArrowTimeUnit::Microsecond,
                timestamp(1, 500),
                timestamp(2, 500),
                (1_000_001, 2_000_001),
            ),
            (
                ArrowTimeUnit::Microsecond,
                timestamp(-1, 999_999_500),
                timestamp(0, 500),
                (0, 1),
            ),
            (
                ArrowTimeUnit::Microsecond,
                timestamp(1, 0),
                timestamp(2, 0),
                (1_000_000, 2_000_000),
            ),
        ];

        for (unit, start, end, expected) in cases {
            let field = Field::new("ts", DataType::Timestamp(unit, None), false);
            assert_eq!(
                timestamp_bounds_for_field(&field, "data/segment.parquet", "ts", start, end)
                    .unwrap(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn segment_stream_reads_on_demand_and_preserves_schema() -> TestResult {
        let tmp = TempDir::new()?;
        let path = tmp.path().join("multi-row-group.parquet");
        write_multi_row_group_parquet(&path, &[&[1_000], &[2_000], &[3_000]])?;
        let data = bytes::Bytes::from(std::fs::read(path)?);
        let file_size = data.len();

        let (reader, stats) = TrackingReader::new(data.clone());
        let mut stream = build_segment_scan_stream(
            reader,
            "data/multi-row-group.parquet".to_string(),
            "ts",
            Utc.timestamp_millis_opt(0).single().unwrap(),
            Utc.timestamp_millis_opt(4_000).single().unwrap(),
        )
        .await?;

        assert_eq!(stats.read_calls.load(Ordering::SeqCst), 0);
        let first = stream.next().await.transpose()?.expect("first batch");
        assert_eq!(stats.read_calls.load(Ordering::SeqCst), 1);
        assert!(stats.bytes_read.load(Ordering::SeqCst) < file_size);
        assert_eq!(
            first.schema().field(0).data_type(),
            &DataType::Timestamp(ArrowTimeUnit::Millisecond, Some(Arc::<str>::from("UTC")))
        );
        assert_eq!(
            first.schema().field(1).data_type(),
            &DataType::Timestamp(ArrowTimeUnit::Microsecond, Some(Arc::<str>::from("+00:00")))
        );

        tokio::task::yield_now().await;
        assert_eq!(stats.read_calls.load(Ordering::SeqCst), 1);
        drop(stream);
        assert!(stats.dropped.load(Ordering::SeqCst));
        assert_eq!(stats.read_calls.load(Ordering::SeqCst), 1);

        let (reader, gated_stats, release) = TrackingReader::with_gate(data, 2);
        let mut stream = build_segment_scan_stream(
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
        assert_eq!(gated_stats.read_calls.load(Ordering::SeqCst), 1);

        let second = stream.next();
        futures::pin_mut!(second);
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(gated_stats.read_calls.load(Ordering::SeqCst), 2);
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
    async fn fully_filtered_segment_stream_yields_and_cancels() -> TestResult {
        let tmp = TempDir::new()?;
        let path = tmp.path().join("filtered-row-groups.parquet");
        write_multi_row_group_parquet(&path, &[&[1_000], &[2_000], &[3_000]])?;
        let data = bytes::Bytes::from(std::fs::read(path)?);
        let (reader, stats, release) = TrackingReader::with_gate(data, 2);

        let mut stream = build_segment_scan_stream(
            reader,
            "data/filtered-row-groups.parquet".to_string(),
            "ts",
            Utc.timestamp_millis_opt(10_000).single().unwrap(),
            Utc.timestamp_millis_opt(20_000).single().unwrap(),
        )
        .await?;

        {
            let next = stream.next();
            futures::pin_mut!(next);
            assert!(futures::poll!(&mut next).is_pending());
            assert_eq!(stats.read_calls.load(Ordering::SeqCst), 1);
            assert!(futures::poll!(&mut next).is_pending());
            assert_eq!(stats.read_calls.load(Ordering::SeqCst), 2);
        }

        drop(stream);
        assert!(stats.dropped.load(Ordering::SeqCst));
        assert!(release.send(()).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn open_segment_scan_errors_when_missing_time_column() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let rel = "data/no-ts.parquet";
        let path = tmp.path().join(rel);
        write_parquet_without_time_column(&path, &["A"], &[1.0])?;

        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: (utc_datetime(2024, 1, 1, 0, 0, 0)).into(),
            index_max: (utc_datetime(2024, 1, 1, 0, 0, 0)).into(),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        };

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 1, 0);

        let err = match open_segment_scan(&location, &segment, "ts", start, end).await {
            Err(err) => err,
            Ok(_) => panic!("missing ts column should error"),
        };

        assert!(matches!(err, ScanError::MissingIndexColumn { .. }));
        assert!(err.to_string().contains(rel));
        Ok(())
    }

    #[tokio::test]
    async fn open_segment_scan_errors_on_unsupported_time_type() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let rel = "data/int-ts.parquet";
        let path = tmp.path().join(rel);
        let ts_vals = [1_000_i64, 2_000];
        write_arrow_parquet_int_time(&path, &ts_vals, &["A", "B"], &[1.0, 2.0])?;

        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: (utc_datetime(2024, 1, 1, 0, 0, 1)).into(),
            index_max: (utc_datetime(2024, 1, 1, 0, 0, 2)).into(),
            row_count: ts_vals.len() as u64,
            file_size: None,
            coverage_path: None,
        };

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 1, 0);

        let err = match open_segment_scan(&location, &segment, "ts", start, end).await {
            Err(err) => err,
            Ok(_) => panic!("unsupported time type should error"),
        };

        assert!(matches!(err, ScanError::IndexColumnTypeMismatch { .. }));
        assert!(err.to_string().contains(rel));
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_reports_timestamp_conversion_overflow() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(
            location,
            make_table_meta_with_unit(LogicalTimestampUnit::Nanos),
        )
        .await?;

        let rel = "data/nano-empty.parquet";
        let path = tmp.path().join(rel);
        write_arrow_parquet_with_unit(&path, ArrowTimeUnit::Nanosecond, &[], &[], &[])?;

        let huge = Utc
            .timestamp_opt(9_223_372_037, 0)
            .single()
            .expect("overflow ts");
        let end = huge
            .checked_add_signed(chrono::Duration::seconds(1))
            .unwrap();

        let segment = SegmentMeta {
            path: rel.to_string(),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: huge.into(),
            index_max: huge.into(),
            row_count: 0,
            file_size: None,
            coverage_path: None,
        };
        table.state.segments.insert(segment.path.clone(), segment);

        let mut stream = table.scan_range(huge, end).await?;
        let error = stream
            .next()
            .await
            .expect("timestamp conversion error")
            .expect_err("overflow during bound conversion must fail");

        assert!(matches!(
            &error,
            TableError::Scan {
                source: ScanError::TimeConversionOverflow { .. }
            }
        ));
        assert!(error.to_string().contains(rel));
        assert!(ErrorCompat::backtrace(&error).is_some());
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_filters_across_segments() -> TestResult {
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
                    symbol: "B",
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
                    symbol: "B",
                    price: 40.0,
                },
            ],
        )?;

        append_parquet_fixture(&mut table, rel1).await?;
        append_parquet_fixture(&mut table, rel2).await?;

        // Query spans both segments but excludes the last row of the second segment.
        let start = Utc.timestamp_millis_opt(1_500).single().expect("valid ts");
        let end = Utc.timestamp_millis_opt(61_500).single().expect("valid ts");

        let mut rows = collect_scan_rows(&table, start, end).await?;
        rows.sort_by_key(|row| row.0);

        assert_eq!(
            rows,
            vec![
                (2_000, "B".to_string(), 20.0),
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
                    symbol: "B",
                    price: 20.0,
                },
            ],
        )?;

        append_parquet_fixture(&mut table, rel).await?;

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

        let error = match table.scan_range(start, end).await {
            Err(error) => error,
            Ok(_) => panic!("invalid range must fail"),
        };
        let scan_source = error
            .source()
            .and_then(|source| source.downcast_ref::<ScanError>())
            .expect("scan source");
        assert!(matches!(
            scan_source.source(),
            Some(source) if source.downcast_ref::<IndexValueError>().is_some()
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            ErrorCompat::backtrace(scan_source).expect("scan backtrace"),
        ));
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
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: (Utc.timestamp_opt(1, 0).single().unwrap()).into(),
            index_max: (Utc.timestamp_opt(3, 0).single().unwrap()).into(),
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
            &["A", "B", "C"],
            &[1.0, 2.0, 3.0],
        )?;

        append_parquet_fixture(&mut table, rel).await?;

        let start = Utc
            .timestamp_opt(1, 500_000_000)
            .single()
            .expect("valid start");
        let end = Utc
            .timestamp_opt(2, 500_000_000)
            .single()
            .expect("valid end");
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(rows, vec![(2_000_000, "B".to_string(), 2.0)]);
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
            &["A", "B", "C"],
            &[1.0, 2.0, 3.0],
        )?;

        append_parquet_fixture(&mut table, rel).await?;

        let start = Utc
            .timestamp_opt(1, 250_000_000)
            .single()
            .expect("valid start");
        let end = Utc
            .timestamp_opt(1, 750_000_000)
            .single()
            .expect("valid end");
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(rows, vec![(1_500_000_000, "B".to_string(), 2.0)]);
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
            &["A", "A", "B"],
            &[1.0, 2.0, 3.0],
        )?;

        append_parquet_fixture(&mut table, rel).await?;

        let start = Utc.timestamp_millis_opt(500).single().unwrap();
        let end = Utc.timestamp_millis_opt(2_500).single().unwrap();
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(
            rows,
            vec![(1_000, "A".to_string(), 1.0), (2_000, "B".to_string(), 3.0)]
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
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: (utc_datetime(2024, 1, 1, 0, 0, 0)).into(),
            index_max: (utc_datetime(2024, 1, 1, 0, 0, 0)).into(),
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
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: (utc_datetime(2024, 1, 1, 0, 0, 0)).into(),
            index_max: (utc_datetime(2024, 1, 1, 0, 0, 1)).into(),
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
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: (utc_datetime(2024, 1, 1, 0, 0, 0)).into(),
            index_max: (utc_datetime(2024, 1, 1, 0, 1, 0)).into(),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        };

        table.state.segments.insert(segment.path.clone(), segment);
        let unopened = SegmentMeta {
            path: "data/should-not-open.parquet".to_string(),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: (utc_datetime(2024, 1, 1, 0, 1, 30)).into(),
            index_max: (utc_datetime(2024, 1, 1, 0, 1, 31)).into(),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        };
        table.state.segments.insert(unopened.path.clone(), unopened);

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 2, 0);

        let mut stream = table.scan_range(start, end).await?;
        let err = stream.next().await.expect("expected error from scan");

        assert!(matches!(
            err,
            Err(TableError::Scan {
                source: ScanError::MissingIndexColumn { .. }
            })
        ));
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
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: (utc_datetime(2024, 1, 1, 0, 0, 1)).into(),
            index_max: (utc_datetime(2024, 1, 1, 0, 0, 1)).into(),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        };

        table.state.segments.insert(segment.path.clone(), segment);

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 1, 0);

        let mut stream = table.scan_range(start, end).await?;
        let err = stream.next().await.expect("expected error from scan");

        assert!(matches!(
            err,
            Err(TableError::Scan {
                source: ScanError::IndexColumnTypeMismatch { .. }
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_reads_segments_independent_of_append_order() -> TestResult {
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

        // Append in reverse index order to exercise segment discovery.
        append_parquet_fixture(&mut table, rel_b).await?;
        append_parquet_fixture(&mut table, rel_a).await?;

        let start = Utc.timestamp_millis_opt(50_000).single().unwrap();
        let end = Utc.timestamp_millis_opt(150_000).single().unwrap();
        let mut rows = collect_scan_rows(&table, start, end).await?;
        rows.sort_by_key(|row| row.0);

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

        append_parquet_fixture(&mut table, rel1).await?;
        append_parquet_fixture(&mut table, rel2).await?;

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
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: (Utc.timestamp_millis_opt(1_000).single().unwrap()).into(),
            index_max: (Utc.timestamp_millis_opt(2_000).single().unwrap()).into(),
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

        let scan_source = error
            .source()
            .and_then(|source| source.downcast_ref::<ScanError>())
            .expect("scan source");
        let storage_source = scan_source
            .source()
            .and_then(|source| source.downcast_ref::<Box<storage::StorageError>>())
            .map(Box::as_ref)
            .expect("storage source");
        assert!(matches!(
            storage_source,
            storage::StorageError::NotFound { .. }
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            ErrorCompat::backtrace(storage_source).expect("storage backtrace"),
        ));
        assert!(error.to_string().contains(rel));
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn scan_range_preserves_non_not_found_storage_error() -> TestResult {
        let tmp = TempDir::new()?;
        let kind = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        let mut table =
            TimeSeriesTable::create(TableLocation::local(tmp.path()), integer_table_meta(kind))
                .await?;
        let rel = "../outside.parquet";
        table.state.segments.insert(
            rel.to_string(),
            indexed_segment(rel, 0i64.into(), 1i64.into()),
        );

        let mut stream = table.scan_range(0i64, 2i64).await?;
        let error = stream
            .next()
            .await
            .expect("invalid storage path error")
            .expect_err("invalid storage path must fail");
        let scan_source = error
            .source()
            .and_then(|source| source.downcast_ref::<ScanError>())
            .expect("scan source");
        let storage_source = scan_source
            .source()
            .and_then(|source| source.downcast_ref::<Box<storage::StorageError>>())
            .map(Box::as_ref)
            .expect("storage source");

        assert!(matches!(
            storage_source,
            storage::StorageError::OtherIo { .. }
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            ErrorCompat::backtrace(storage_source).expect("storage backtrace"),
        ));
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

        append_parquet_fixture(&mut table, rel).await?;

        // Corrupt the file after append so scan encounters a read failure.
        let committed_path = table
            .state()
            .segments
            .values()
            .next()
            .expect("appended segment")
            .path
            .clone();
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(tmp.path().join(&committed_path))?;
        f.set_len(4)?;

        let start = Utc.timestamp_millis_opt(0).single().unwrap();
        let end = Utc.timestamp_millis_opt(2_000).single().unwrap();

        let mut stream = table.scan_range(start, end).await?;
        let err = stream
            .next()
            .await
            .expect("first item should be error")
            .expect_err("corrupt segment should fail");

        assert!(matches!(
            err,
            TableError::Scan {
                source: ScanError::Parquet { .. }
            }
        ));
        assert!(err.to_string().contains(&committed_path));
        Ok(())
    }
}
