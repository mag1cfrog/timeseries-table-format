//! Range scan implementation for `TimeSeriesTable`.
//!
//! This module wires the public `scan_range` entry point to the underlying
//! segment metadata and Parquet readers:
//! - Pick candidate segments whose `[ts_min, ts_max]` intersects the requested
//!   half-open window `[ts_start, ts_end)`.
//! - Stream those segments in timestamp order, reading Parquet bytes from
//!   storage and building `RecordBatch` readers over in-memory buffers.
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
    Array, RecordBatchReader, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::{boolean as boolean_kernels, cmp as cmp_kernels};
use arrow::datatypes::{Field, TimeUnit};
use arrow::{array::RecordBatch, datatypes::DataType};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use snafu::prelude::*;

use crate::helpers::segment_order::cmp_segment_meta_by_time;
use crate::{
    storage::{self, TableLocation},
    time_series_table::{
        TimeSeriesScan, TimeSeriesTable,
        error::{InvalidRangeSnafu, StorageSnafu, TableError},
    },
    transaction_log::{SegmentMeta, TableState},
};

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
/// This macro returns `Result<(), TableError>` so callers can use `?` for
/// error propagation inside the match over different timestamp units.
macro_rules! filter_ts_batch {
    ($array_ty: ty,
    $batch:expr,
    $ts_idx:expr,
    $start_bound:expr,
    $end_bound:expr,
    $time_col:expr,
    $ts_field:expr,
    $out:expr
) => {{
        // 1) Downcast the column to the concrete timestamp array type
        let col = $batch.column($ts_idx);
        let ts_arr = col.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
            TableError::UnsupportedTimeType {
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
        let ge_mask = cmp_kernels::gt_eq(ts_arr, &start_scalar)
            .map_err(|source| TableError::Arrow { source })?;
        let lt_mask =
            cmp_kernels::lt(ts_arr, &end_scalar).map_err(|source| TableError::Arrow { source })?;

        // 5) Combine: keep rows where ts >= start AND ts < end
        let mask = boolean_kernels::and(&ge_mask, &lt_mask)
            .map_err(|source| TableError::Arrow { source })?;

        // Note on null semantics:
        // - If ts_arr[i] is null, both comparisons produce null in the mask.
        // Arrow's `filter_record_batch` treats null mask values as false,
        // excluding those rows from results.

        // 6) apply the mask to the whole batch
        let filtered =
            filter_record_batch(&$batch, &mask).map_err(|source| TableError::Arrow { source })?;

        if filtered.num_rows() > 0 {
            $out.push(filtered);
        }

        Ok::<(), TableError>(())
    }};
}

fn to_bounds_i64(
    field: &Field,
    column: &str,
    ts_start: DateTime<Utc>,
    ts_end: DateTime<Utc>,
) -> Result<(i64, i64), TableError> {
    let to_ns = |dt: DateTime<Utc>| {
        dt.timestamp()
            .checked_mul(1_000_000_000)
            .and_then(|secs| secs.checked_add(dt.timestamp_subsec_nanos() as i64))
            .ok_or_else(|| TableError::TimeConversionOverflow {
                column: column.to_string(),
                timestamp: dt,
            })
    };

    match field.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => Ok((ts_start.timestamp(), ts_end.timestamp())),

        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok((ts_start.timestamp_millis(), ts_end.timestamp_millis()))
        }

        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok((ts_start.timestamp_micros(), ts_end.timestamp_micros()))
        }

        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok((to_ns(ts_start)?, to_ns(ts_end)?)),

        other => Err(TableError::UnsupportedTimeType {
            column: column.to_string(),
            datatype: other.clone(),
        }),
    }
}

async fn read_segment_range(
    location: &TableLocation,
    segment: &SegmentMeta,
    time_column: &str,
    ts_start: DateTime<Utc>,
    ts_end: DateTime<Utc>,
) -> Result<Vec<RecordBatch>, TableError> {
    let rel_path = Path::new(&segment.path);

    // 1) Use storage layer to get raw bytes.
    let bytes = storage::read_all_bytes(location, rel_path)
        .await
        .context(StorageSnafu)?;

    // 2) Build a reader over an in-memory cursor.
    let bytes = Bytes::from(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|source| TableError::ParquetRead { source })?;

    let reader = builder
        .build()
        .map_err(|source| TableError::ParquetRead { source })?;

    // 3) locate the time column and compute numeric bounds
    let schema = reader.schema();
    let ts_idx = schema
        .index_of(time_column)
        .map_err(|_| TableError::MissingTimeColumn {
            column: time_column.to_string(),
        })?;

    let ts_field = schema.field(ts_idx);

    let (start_bound, end_bound) = to_bounds_i64(ts_field, time_column, ts_start, ts_end)?;

    let mut out = Vec::new();

    // 4) iterate over batches and filter them
    for batch_res in reader {
        let batch = batch_res.map_err(|source| TableError::Arrow { source })?;

        match ts_field.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => {
                filter_ts_batch!(
                    TimestampSecondArray,
                    batch,
                    ts_idx,
                    start_bound,
                    end_bound,
                    time_column,
                    ts_field,
                    out
                )?;
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                filter_ts_batch!(
                    TimestampMillisecondArray,
                    batch,
                    ts_idx,
                    start_bound,
                    end_bound,
                    time_column,
                    ts_field,
                    out
                )?;
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                filter_ts_batch!(
                    TimestampMicrosecondArray,
                    batch,
                    ts_idx,
                    start_bound,
                    end_bound,
                    time_column,
                    ts_field,
                    out
                )?;
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                filter_ts_batch!(
                    TimestampNanosecondArray,
                    batch,
                    ts_idx,
                    start_bound,
                    end_bound,
                    time_column,
                    ts_field,
                    out
                )?;
            }
            other => {
                return Err(TableError::UnsupportedTimeType {
                    column: time_column.to_string(),
                    datatype: other.clone(),
                });
            }
        }
    }

    Ok(out)
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

        // 2) Sort by ts_min to ensure segments are processed in chronological order.
        //    In v0.1 we assume non-overlapping segments, so sorting guarantees scan order.
        //    Unstable is fine here; we only care about ordering by ts_min.
        candidates.sort_unstable_by(cmp_segment_meta_by_time);

        let location = self.location.clone();

        // 3) Build stream: for each segment, read + filter
        let stream = futures::stream::iter(candidates.into_iter())
            .then(move |seg| {
                let location = location.clone();
                let ts_column = ts_column.clone();

                async move {
                    let batches =
                        read_segment_range(&location, &seg, &ts_column, ts_start, ts_end).await?;

                    Ok::<_, TableError>(futures::stream::iter(
                        batches.into_iter().map(Ok::<_, TableError>),
                    ))
                }
            })
            .try_flatten();

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::TableLocation;
    use crate::time_series_table::test_util::*;

    use crate::transaction_log::segments::{FileFormat, SegmentId};
    use crate::transaction_log::table_metadata::LogicalTimestampUnit;

    use arrow::datatypes::TimeUnit as ArrowTimeUnit;

    use chrono::{TimeZone, Utc};
    use futures::StreamExt;

    use tempfile::TempDir;

    #[tokio::test]
    async fn read_segment_range_errors_when_missing_time_column() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let rel = "data/no-ts.parquet";
        let path = tmp.path().join(rel);
        write_parquet_without_time_column(&path, &["A"], &[1.0])?;

        let segment = SegmentMeta {
            segment_id: SegmentId("seg-no-ts".to_string()),
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 0),
            row_count: 1,
            coverage_path: None,
        };

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 1, 0);

        let err = read_segment_range(&location, &segment, "ts", start, end)
            .await
            .expect_err("missing ts column should error");

        assert!(matches!(err, TableError::MissingTimeColumn { .. }));
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
            segment_id: SegmentId("seg-int".to_string()),
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 1),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 2),
            row_count: ts_vals.len() as u64,
            coverage_path: None,
        };

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 1, 0);

        let err = read_segment_range(&location, &segment, "ts", start, end)
            .await
            .expect_err("unsupported time type should error");

        assert!(matches!(err, TableError::UnsupportedTimeType { .. }));
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
            segment_id: SegmentId("seg-nano-empty".to_string()),
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 0),
            row_count: 0,
            coverage_path: None,
        };

        let huge = Utc
            .timestamp_opt(9_223_372_037, 0)
            .single()
            .expect("overflow ts");
        let err = read_segment_range(&location, &segment, "ts", huge, huge)
            .await
            .expect_err("overflow during bound conversion should error");

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

        table
            .append_parquet_segment_with_id(SegmentId("seg-scan-1".to_string()), rel1, "ts")
            .await?;
        table
            .append_parquet_segment_with_id(SegmentId("seg-scan-2".to_string()), rel2, "ts")
            .await?;

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

        table
            .append_parquet_segment_with_id(SegmentId("seg-boundary".to_string()), rel, "ts")
            .await?;

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

        table
            .append_parquet_segment_with_id(SegmentId("seg-micros".to_string()), rel, "ts")
            .await?;

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

        table
            .append_parquet_segment_with_id(SegmentId("seg-nanos".to_string()), rel, "ts")
            .await?;

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

        table
            .append_parquet_segment_with_id(SegmentId("seg-null".to_string()), rel, "ts")
            .await?;

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
            segment_id: SegmentId("seg-empty".to_string()),
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 0),
            row_count: 0,
            coverage_path: None,
        };

        table
            .state
            .segments
            .insert(segment.segment_id.clone(), segment);

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
            segment_id: SegmentId("seg-null-only".to_string()),
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 1),
            row_count: 2,
            coverage_path: None,
        };

        table
            .state
            .segments
            .insert(segment.segment_id.clone(), segment);

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
            segment_id: SegmentId("seg-scan-no-ts".to_string()),
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2024, 1, 1, 0, 1, 0),
            row_count: 1,
            coverage_path: None,
        };

        table
            .state
            .segments
            .insert(segment.segment_id.clone(), segment);

        let start = utc_datetime(2024, 1, 1, 0, 0, 0);
        let end = utc_datetime(2024, 1, 1, 0, 2, 0);

        let mut stream = table.scan_range(start, end).await?;
        let err = stream.next().await.expect("expected error from scan");

        assert!(matches!(err, Err(TableError::MissingTimeColumn { .. })));
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
            segment_id: SegmentId("seg-scan-int".to_string()),
            path: rel.to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2024, 1, 1, 0, 0, 1),
            ts_max: utc_datetime(2024, 1, 1, 0, 0, 1),
            row_count: 1,
            coverage_path: None,
        };

        table
            .state
            .segments
            .insert(segment.segment_id.clone(), segment);

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

        // append in reverse ts_min order to ensure sort_by_key is exercised
        table
            .append_parquet_segment_with_id(SegmentId("seg-b".to_string()), rel_b, "ts")
            .await?;
        table
            .append_parquet_segment_with_id(SegmentId("seg-a".to_string()), rel_a, "ts")
            .await?;

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

        table
            .append_parquet_segment_with_id(SegmentId("seg-early".to_string()), rel1, "ts")
            .await?;
        table
            .append_parquet_segment_with_id(SegmentId("seg-late".to_string()), rel2, "ts")
            .await?;

        let start = Utc.timestamp_millis_opt(1_500).single().unwrap();
        let end = Utc.timestamp_millis_opt(2_000).single().unwrap();
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(rows, Vec::new());
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

        table
            .append_parquet_segment_with_id(SegmentId("seg-corrupt".to_string()), rel, "ts")
            .await?;

        // Corrupt the file after append so scan encounters a read failure.
        let f = std::fs::OpenOptions::new().write(true).open(&path)?;
        f.set_len(4)?;

        let start = Utc.timestamp_millis_opt(0).single().unwrap();
        let end = Utc.timestamp_millis_opt(2_000).single().unwrap();

        let mut stream = table.scan_range(start, end).await?;
        let err = stream.next().await.expect("first item should be error");

        assert!(matches!(err, Err(TableError::ParquetRead { .. })));
        Ok(())
    }
}
