//! High-level time-series table abstraction.
//!
//! This wraps the transaction log and table state into a single, user-facing
//! type that knows:
//! - where the table lives (`TableLocation`),
//! - how to talk to the transaction log (`TransactionLogStore`),
//! - what the current committed state is (`TableState`),
//! - and what the time index spec is (`TimeIndexSpec`).
//!
//! In v0.1 this is intentionally read-heavy and write-light:
//! - `open` reconstructs state from the transaction log,
//! - `create` bootstraps a fresh table with an initial metadata commit.
//!   Later issues will add append APIs, coverage, and scanning.

use std::{path::Path, pin::Pin};

use arrow::array::{
    Array, RecordBatchReader, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::{boolean as boolean_kernels, cmp as cmp_kernels};
use arrow::datatypes::{Field, TimeUnit};
use arrow::{array::RecordBatch, datatypes::DataType, error::ArrowError};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryStreamExt};
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, errors::ParquetError};
use snafu::prelude::*;

use crate::{
    helpers::{
        parquet::{logical_schema_from_parquet_location, segment_meta_from_parquet_location},
        schema::{SchemaCompatibilityError, ensure_schema_exact_match},
    },
    storage::{self, StorageError, TableLocation},
    transaction_log::{
        CommitError, LogAction, SegmentId, SegmentMeta, TableKind, TableMeta, TableState,
        TimeIndexSpec, TransactionLogStore, segments::SegmentMetaError,
    },
};

/// Errors from high-level time-series table operations.
///
/// Each variant carries enough context for callers to surface actionable
/// messages to users or implement retries where appropriate (for example,
/// conflicts on optimistic concurrency control).
#[derive(Debug, Snafu)]
pub enum TableError {
    /// Any error coming from the transaction log / commit machinery
    /// (for example, OCC conflicts, storage failures, or corrupt commits).
    #[snafu(display("Transaction log error: {source}"))]
    TransactionLog {
        /// Underlying transaction log / commit error.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// Attempting to open a table that has no commits at all (CURRENT == 0).
    #[snafu(display("Cannot open table with no commits (CURRENT version is 0)"))]
    EmptyTable,

    /// The underlying table is not a time-series table (TableKind mismatch).
    #[snafu(display("Table kind is {kind:?}, expected TableKind::TimeSeries"))]
    NotTimeSeries {
        /// The actual kind of the underlying table that was discovered.
        kind: TableKind,
    },

    /// Attempt to create a table where commits already exist (idempotency guard for create).
    #[snafu(display("Table already exists; current transaction log version is {current_version}"))]
    AlreadyExists {
        /// Current transaction log version that indicates the table already exists.
        current_version: u64,
    },

    /// Segment-level metadata / Parquet error during append (for example, missing time column, unsupported type, corrupt stats).
    #[snafu(display("Segment metadata error while appending: {source}"))]
    SegmentMeta {
        /// Underlying segment metadata error.
        #[snafu(source, backtrace)]
        source: SegmentMetaError,
    },

    /// Schema compatibility error when appending a segment with incompatible schema (no evolution allowed in v0.1).
    #[snafu(display("Schema compatibility error: {source}"))]
    SchemaCompatibility {
        /// Underlying schema compatibility error.
        #[snafu(source)]
        source: SchemaCompatibilityError,
    },

    /// Table has progressed past the initial metadata commit but still lacks
    /// a canonical logical schema (invariant violation for v0.1).
    #[snafu(display("Table has no logical_schema at version {version}; cannot append in v0.1"))]
    MissingCanonicalSchema {
        /// The transaction log version missing a canonical logical schema.
        version: u64,
    },

    /// Storage error while accessing table data (read/write failure at the storage layer).
    #[snafu(display("Storage error while accessing table data: {source}"))]
    Storage {
        /// Underlying storage error while reading or writing table data.
        source: StorageError,
    },

    /// Start and end timestamps must satisfy start < end when scanning.
    #[snafu(display("Invalid scan range: start={start}, end={end} (expect start < end)"))]
    InvalidRange {
        /// Inclusive/lower timestamp bound supplied by caller.
        start: DateTime<Utc>,
        /// Exclusive/upper timestamp bound supplied by caller.
        end: DateTime<Utc>,
    },

    /// Parquet read/IO error during scanning or schema extraction.
    #[snafu(display("Parquet read error: {source}"))]
    ParquetRead {
        /// Underlying Parquet error raised during read or schema extraction.
        source: ParquetError,
    },

    /// Arrow compute or conversion error while materializing or filtering batches.
    #[snafu(display("Arrow error while filtering batch: {source}"))]
    Arrow {
        /// Underlying Arrow error raised during batch conversion or filtering.
        source: ArrowError,
    },

    /// Segment is missing the configured time column required for scans.
    #[snafu(display("Missing time column {column} in segment"))]
    MissingTimeColumn {
        /// Name of the expected time column that was not found in the segment.
        column: String,
    },

    /// Time column exists but has an unsupported Arrow type for scanning.
    #[snafu(display("Unsupported time column {column} with type {datatype:?}"))]
    UnsupportedTimeType {
        /// Name of the time column with an unsupported type.
        column: String,
        /// Arrow data type encountered for the time column.
        datatype: DataType,
    },

    /// Converting a timestamp to the requested unit would overflow `i64`.
    #[snafu(display("Timestamp conversion overflow for column {column} (value: {timestamp})"))]
    TimeConversionOverflow {
        /// Name of the time column being converted.
        column: String,
        /// The timestamp value that could not be represented as i64 nanos.
        timestamp: DateTime<Utc>,
    },
}

/// Stream of Arrow RecordBatch values from a time-series scan.
pub type TimeSeriesScan = Pin<Box<dyn Stream<Item = Result<RecordBatch, TableError>> + Send>>;

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

        // 2) Build scalar arrays with matching timezone
        // Extract timezone from the actual array's data type to ensure comparison compatibility
        let tz = match ts_arr.data_type() {
            DataType::Timestamp(_, tz_opt) => tz_opt.clone(),
            _ => None,
        };

        let start_scalar =
            <$array_ty>::from(vec![$start_bound; ts_arr.len()]).with_timezone_opt(tz.clone());
        let end_scalar = <$array_ty>::from(vec![$end_bound; ts_arr.len()]).with_timezone_opt(tz);

        // 3) Vectorized comparisons:
        // ge_mask = (ts >= start)
        // lt_mask = (ts < end)
        let ge_mask = cmp_kernels::gt_eq(ts_arr, &start_scalar)
            .map_err(|source| TableError::Arrow { source })?;
        let lt_mask =
            cmp_kernels::lt(ts_arr, &end_scalar).map_err(|source| TableError::Arrow { source })?;

        // 4) Combine: keep rows where ts >= start AND ts < end
        let mask = boolean_kernels::and(&ge_mask, &lt_mask)
            .map_err(|source| TableError::Arrow { source })?;

        // Note on null semantics:
        // - If ts_arr[i] is null, both comparisons produce null in the mask.
        // - Arrow’s `filter`/`filter_record_batch` treat nulls in the mask
        //   as “do not keep this row”, which matches the
        //   `null -> false` behavior.

        // 5) apply the mask to the whole batch
        let filtered =
            filter_record_batch(&$batch, &mask).map_err(|source| TableError::Arrow { source })?;

        if filtered.num_rows() > 0 {
            $out.push(filtered);
        }

        Ok::<(), TableError>(())
    }};
}

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

/// High-level time-series table handle.
///
/// This is the main entry point for callers. It bundles:
/// - where the table is,
/// - how to talk to the transaction log,
/// - what the current committed state is,
/// - and the extracted time index spec.
#[derive(Debug)]
pub struct TimeSeriesTable {
    location: TableLocation,
    log: TransactionLogStore,
    state: TableState,
    index: TimeIndexSpec,
}

impl TimeSeriesTable {
    /// Return the current committed table state.
    pub fn state(&self) -> &TableState {
        &self.state
    }

    /// Return the time index specification for this table.
    pub fn index_spec(&self) -> &TimeIndexSpec {
        &self.index
    }

    /// Return the table location.
    pub fn location(&self) -> &TableLocation {
        &self.location
    }

    /// Return the transaction log store handle.
    pub fn log_store(&self) -> &TransactionLogStore {
        &self.log
    }

    /// Open an existing time-series table at the given location.
    ///
    /// Steps:
    /// - Build a `TransactionLogStore` for the location.
    /// - Rebuild `TableState` from the transaction log.
    /// - Reject empty tables (version == 0).
    /// - Require `TableKind::TimeSeries` and extract `TimeIndexSpec`.
    pub async fn open(location: TableLocation) -> Result<Self, TableError> {
        let log = TransactionLogStore::new(location.clone());

        // Early return for tables with no commits so we surface TableError::EmptyTable
        // instead of a lower-level corrupt state error.
        let current_version = log
            .load_current_version()
            .await
            .context(TransactionLogSnafu)?;

        if current_version == 0 {
            return EmptyTableSnafu.fail();
        }

        // Rebuild the snapshot of state from the log.
        let state = log
            .rebuild_table_state()
            .await
            .context(TransactionLogSnafu)?;

        // Extract the time index spec from TableMeta.kind.
        let index = match &state.table_meta.kind {
            TableKind::TimeSeries(spec) => spec.clone(),
            other => {
                return NotTimeSeriesSnafu {
                    kind: other.clone(),
                }
                .fail();
            }
        };

        Ok(Self {
            location,
            log,
            state,
            index,
        })
    }

    /// Create a new time-series table at the given location.
    ///
    /// This:
    /// - Requires `table_meta.kind` to be `TableKind::TimeSeries`,
    /// - Verifies that there are no existing commits (version must be 0),
    /// - Writes an initial commit with `UpdateTableMeta(table_meta.clone())`,
    /// - Returns a `TimeSeriesTable` with a fresh `TableState`.
    pub async fn create(
        location: TableLocation,
        table_meta: TableMeta,
    ) -> Result<Self, TableError> {
        // 1) Extract the time index spec from the provided metadata
        // and ensure this is actually a time-series table.
        let index = match &table_meta.kind {
            TableKind::TimeSeries(spec) => spec.clone(),
            other => {
                return NotTimeSeriesSnafu {
                    kind: other.clone(),
                }
                .fail();
            }
        };

        let log = TransactionLogStore::new(location.clone());

        // 2) Check that there are no existing commits. This keeps `create`
        // from silently appending to a pre-existing table.
        let current_version = log
            .load_current_version()
            .await
            .context(TransactionLogSnafu)?;

        if current_version != 0 {
            return AlreadyExistsSnafu { current_version }.fail();
        }

        // 3) Write the initial metadata commit at version 1.
        //
        // `TableMetaDelta` is an alias for `TableMeta` in v0.1, so we can
        // pass `table_meta.clone()` directly into `UpdateTableMeta`.
        let actions = vec![LogAction::UpdateTableMeta(table_meta.clone())];

        let new_version = log
            .commit_with_expected_version(0, actions)
            .await
            .context(TransactionLogSnafu)?;

        debug_assert_eq!(new_version, 1);

        // 4) Rebuild state from the log so that `state` is guaranteed to be
        //    consistent with what is on disk.
        let state = log
            .rebuild_table_state()
            .await
            .context(TransactionLogSnafu)?;
        Ok(Self {
            location,
            log,
            state,
            index,
        })
    }

    /// Append a new Parquet segment, registering it in the transaction log.
    ///
    /// v0.1 behavior:
    /// - Build SegmentMeta from the Parquet file (ts_min, ts_max, row_count).
    /// - Derive the segment logical schema from the Parquet file.
    /// - If the table has no logical_schema yet, adopt this segment schema
    ///   as canonical and write an UpdateTableMeta + AddSegment commit.
    /// - Otherwise, enforce "no schema evolution" via schema_helpers.
    /// - Commit with OCC on the current version.
    /// - Update in-memory TableState on success.
    ///
    /// v0.1: duplicates (same segment_id/path) are allowed; they’ll just be
    /// treated as distinct segments. We can tighten that later.
    pub async fn append_parquet_segment(
        &mut self,
        segment_id: SegmentId,
        relative_path: &str,
        time_column: &str,
    ) -> Result<u64, TableError> {
        let rel_path = Path::new(relative_path);

        // 1) Build SegmentMeta from Parquet (ts_min, ts_max, row_count, basic validation).
        let segment_meta =
            segment_meta_from_parquet_location(&self.location, rel_path, segment_id, time_column)
                .await
                .context(SegmentMetaSnafu)?;

        // 2) Derive the segment's LogicalSchema from the same Parquet file.
        let segment_schema = logical_schema_from_parquet_location(&self.location, rel_path)
            .await
            .context(SegmentMetaSnafu)?;

        let expected_version = self.state.version;
        let maybe_table_schema = self.state.table_meta.logical_schema.as_ref();
        // 3) Decide schema behavior based on both schema *and* version:
        //
        // - logical_schema == None && version == 1:
        //     first append after create() — adopt this segment’s schema.
        // - logical_schema == None && version != 1:
        //     table is in a bad state for v0.1 → error.
        // - logical_schema == Some(..):
        //     enforce “no schema evolution” via ensure_schema_exact_match.
        let (actions, new_table_meta) = match maybe_table_schema {
            None if expected_version == 1 => {
                // Bootstrap case: adopt this segment’s schema as canonical in the
                // same commit that adds the segment.
                let mut updated_meta = self.state.table_meta.clone();
                updated_meta.logical_schema = Some(segment_schema.clone());

                let actions = vec![
                    LogAction::UpdateTableMeta(updated_meta.clone()),
                    LogAction::AddSegment(segment_meta.clone()),
                ];

                (actions, Some(updated_meta))
            }

            None => {
                return MissingCanonicalSchemaSnafu {
                    version: expected_version,
                }
                .fail();
            }

            Some(table_schema) => {
                ensure_schema_exact_match(table_schema, &segment_schema, &self.index)
                    .context(SchemaCompatibilitySnafu)?;

                let actions = vec![LogAction::AddSegment(segment_meta.clone())];

                (actions, None)
            }
        };

        // 4) Commit with OCC. Conflict → CommitError::Conflict → TableError::TransactionLog.
        let new_version = self
            .log
            .commit_with_expected_version(expected_version, actions)
            .await
            .context(TransactionLogSnafu)?;

        // 5) Update in-memory state.
        self.state.version = new_version;

        if let Some(updated_meta) = new_table_meta {
            self.state.table_meta = updated_meta
        }

        self.state
            .segments
            .insert(segment_meta.segment_id.clone(), segment_meta);

        Ok(new_version)
    }

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
        candidates.sort_by_key(|seg| seg.ts_min);

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
    use crate::transaction_log::segments::{FileFormat, SegmentId, SegmentMetaError};
    use crate::transaction_log::table_metadata::{LogicalDataType, LogicalTimestampUnit};
    use crate::transaction_log::{
        CommitError, LogicalColumn, LogicalSchema, TableKind, TableMeta, TimeBucket, TimeIndexSpec,
        TransactionLogStore,
    };
    use arrow::array::{
        Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
    };
    use arrow::datatypes::{Schema, TimeUnit as ArrowTimeUnit};
    use chrono::{TimeZone, Utc};
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
    use parquet::column::writer::ColumnWriter;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[derive(Clone)]
    struct TestRow {
        ts_millis: i64,
        symbol: &'static str,
        price: f64,
    }

    fn make_table_meta_with_unit(unit: LogicalTimestampUnit) -> TableMeta {
        let index = TimeIndexSpec {
            timestamp_column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string()],
            bucket: TimeBucket::Minutes(1),
            timezone: None,
        };

        let logical_schema = LogicalSchema::new(vec![
            LogicalColumn {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit,
                    timezone: None,
                },
                nullable: true,
            },
            LogicalColumn {
                name: "symbol".to_string(),
                data_type: LogicalDataType::Utf8,
                nullable: false,
            },
            LogicalColumn {
                name: "price".to_string(),
                data_type: LogicalDataType::Float64,
                nullable: false,
            },
        ])
        .expect("valid logical schema");

        TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: Some(logical_schema),
            created_at: utc_datetime(2025, 1, 1, 0, 0, 0),
            format_version: 1,
        }
    }

    fn write_test_parquet(
        path: &Path,
        include_symbol: bool,
        price_as_int: bool,
        rows: &[TestRow],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut fields: Vec<Arc<Type>> = Vec::new();

        let ts_field = Type::primitive_type_builder("ts", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::MILLIS,
            }))
            .build()?;
        fields.push(Arc::new(ts_field));

        if include_symbol {
            let symbol_field = Type::primitive_type_builder("symbol", PhysicalType::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::String))
                .build()?;
            fields.push(Arc::new(symbol_field));
        }

        let price_builder = if price_as_int {
            Type::primitive_type_builder("price", PhysicalType::INT64)
        } else {
            Type::primitive_type_builder("price", PhysicalType::DOUBLE)
        };
        let price_field = price_builder
            .with_repetition(Repetition::REQUIRED)
            .build()?;
        fields.push(Arc::new(price_field));

        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(fields)
                .build()?,
        );

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

        let ts_values: Vec<i64> = rows.iter().map(|r| r.ts_millis).collect();
        let symbol_values: Vec<ByteArray> = rows
            .iter()
            .map(|r| ByteArray::from(r.symbol.as_bytes()))
            .collect();
        let price_f64: Vec<f64> = rows.iter().map(|r| r.price).collect();
        let price_i64: Vec<i64> = rows.iter().map(|r| r.price as i64).collect();

        let mut row_group_writer = writer.next_row_group()?;
        let mut col_idx = 0;
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            let mut cw = col_writer.untyped();
            match (include_symbol, price_as_int, col_idx) {
                (true, _, 0) | (false, _, 0) => match &mut cw {
                    ColumnWriter::Int64ColumnWriter(w) => {
                        w.write_batch(&ts_values, None, None)?;
                    }
                    _ => return Err("unexpected writer for ts".into()),
                },
                (true, _, 1) => match &mut cw {
                    ColumnWriter::ByteArrayColumnWriter(w) => {
                        w.write_batch(&symbol_values, None, None)?;
                    }
                    _ => return Err("unexpected writer for symbol".into()),
                },
                (true, false, 2) | (false, false, 1) => match &mut cw {
                    ColumnWriter::DoubleColumnWriter(w) => {
                        w.write_batch(&price_f64, None, None)?;
                    }
                    _ => return Err("unexpected writer for price f64".into()),
                },
                (true, true, 2) | (false, true, 1) => match &mut cw {
                    ColumnWriter::Int64ColumnWriter(w) => {
                        w.write_batch(&price_i64, None, None)?;
                    }
                    _ => return Err("unexpected writer for price i64".into()),
                },
                _ => return Err("unexpected column writer ordering".into()),
            }
            col_writer.close()?;
            col_idx += 1;
        }
        row_group_writer.close()?;
        writer.close()?;

        Ok(())
    }

    fn write_arrow_parquet_with_unit(
        path: &Path,
        unit: ArrowTimeUnit,
        ts: &[Option<i64>],
        symbols: &[&str],
        prices: &[f64],
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(ts.len(), symbols.len());
        assert_eq!(symbols.len(), prices.len());

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let schema = Schema::new(vec![
            Field::new("ts", DataType::Timestamp(unit, None), true),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]);

        let ts_array: Arc<dyn Array> = match unit {
            ArrowTimeUnit::Second => {
                let mut b = TimestampSecondBuilder::with_capacity(ts.len());
                for v in ts {
                    match v {
                        Some(val) => b.append_value(*val),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowTimeUnit::Millisecond => {
                let mut b = TimestampMillisecondBuilder::with_capacity(ts.len());
                for v in ts {
                    match v {
                        Some(val) => b.append_value(*val),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowTimeUnit::Microsecond => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(ts.len());
                for v in ts {
                    match v {
                        Some(val) => b.append_value(*val),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ArrowTimeUnit::Nanosecond => {
                let mut b = TimestampNanosecondBuilder::with_capacity(ts.len());
                for v in ts {
                    match v {
                        Some(val) => b.append_value(*val),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };

        let mut sym_builder =
            StringBuilder::with_capacity(symbols.len(), symbols.iter().map(|s| s.len()).sum());
        for s in symbols {
            sym_builder.append_value(s);
        }
        let sym_array = Arc::new(sym_builder.finish()) as Arc<dyn Array>;

        let mut price_builder = Float64Builder::with_capacity(prices.len());
        for p in prices {
            price_builder.append_value(*p);
        }
        let price_array = Arc::new(price_builder.finish()) as Arc<dyn Array>;

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![ts_array, sym_array, price_array],
        )?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }

    fn write_arrow_parquet_int_time(
        path: &Path,
        ts: &[i64],
        symbols: &[&str],
        prices: &[f64],
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(ts.len(), symbols.len());
        assert_eq!(symbols.len(), prices.len());

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let schema = Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]);

        let mut ts_builder = Int64Builder::with_capacity(ts.len());
        for v in ts {
            ts_builder.append_value(*v);
        }
        let ts_array = Arc::new(ts_builder.finish()) as Arc<dyn Array>;

        let mut sym_builder =
            StringBuilder::with_capacity(symbols.len(), symbols.iter().map(|s| s.len()).sum());
        for s in symbols {
            sym_builder.append_value(s);
        }
        let sym_array = Arc::new(sym_builder.finish()) as Arc<dyn Array>;

        let mut price_builder = Float64Builder::with_capacity(prices.len());
        for p in prices {
            price_builder.append_value(*p);
        }
        let price_array = Arc::new(price_builder.finish()) as Arc<dyn Array>;

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![ts_array, sym_array, price_array],
        )?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }

    fn write_parquet_without_time_column(
        path: &Path,
        symbols: &[&str],
        prices: &[f64],
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(symbols.len(), prices.len());

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let schema = Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]);

        let mut sym_builder =
            StringBuilder::with_capacity(symbols.len(), symbols.iter().map(|s| s.len()).sum());
        for s in symbols {
            sym_builder.append_value(s);
        }
        let sym_array = Arc::new(sym_builder.finish()) as Arc<dyn Array>;

        let mut price_builder = Float64Builder::with_capacity(prices.len());
        for p in prices {
            price_builder.append_value(*p);
        }
        let price_array = Arc::new(price_builder.finish()) as Arc<dyn Array>;

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![sym_array, price_array])?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }

    fn utc_datetime(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .expect("valid UTC timestamp")
    }

    fn make_basic_table_meta() -> TableMeta {
        let index = TimeIndexSpec {
            timestamp_column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string()],
            bucket: TimeBucket::Minutes(1),
            timezone: None,
        };

        let logical_schema = LogicalSchema::new(vec![
            LogicalColumn {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Millis,
                    timezone: None,
                },
                nullable: false,
            },
            LogicalColumn {
                name: "symbol".to_string(),
                data_type: LogicalDataType::Utf8,
                nullable: false,
            },
            LogicalColumn {
                name: "price".to_string(),
                data_type: LogicalDataType::Float64,
                nullable: false,
            },
        ])
        .expect("valid logical schema");

        TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: Some(logical_schema),
            created_at: utc_datetime(2025, 1, 1, 0, 0, 0),
            format_version: 1,
        }
    }

    async fn collect_scan_rows(
        table: &TimeSeriesTable,
        ts_start: DateTime<Utc>,
        ts_end: DateTime<Utc>,
    ) -> Result<Vec<(i64, String, f64)>, TableError> {
        let mut stream = table.scan_range(ts_start, ts_end).await?;
        let mut rows = Vec::new();

        while let Some(batch_res) = stream.next().await {
            let batch = batch_res?;

            let ts_idx = batch.schema().index_of("ts").expect("ts column");
            let sym_idx = batch.schema().index_of("symbol").expect("symbol column");
            let price_idx = batch.schema().index_of("price").expect("price column");

            let ts_arr = batch.column(ts_idx);
            let ts_values: Vec<i64> = match ts_arr.data_type() {
                DataType::Timestamp(ArrowTimeUnit::Second, _) => batch
                    .column(ts_idx)
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .expect("ts as seconds")
                    .iter()
                    .map(|v| v.expect("non-null ts"))
                    .collect(),
                DataType::Timestamp(ArrowTimeUnit::Millisecond, _) => batch
                    .column(ts_idx)
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .expect("ts as millis")
                    .iter()
                    .map(|v| v.expect("non-null ts"))
                    .collect(),
                DataType::Timestamp(ArrowTimeUnit::Microsecond, _) => batch
                    .column(ts_idx)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("ts as micros")
                    .iter()
                    .map(|v| v.expect("non-null ts"))
                    .collect(),
                DataType::Timestamp(ArrowTimeUnit::Nanosecond, _) => batch
                    .column(ts_idx)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("ts as nanos")
                    .iter()
                    .map(|v| v.expect("non-null ts"))
                    .collect(),
                other => panic!("unexpected time type: {other:?}"),
            };
            let sym_arr = batch
                .column(sym_idx)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("symbol as utf8");
            let price_arr = batch
                .column(price_idx)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("price as f64");

            for ((ts, symbol), price) in ts_values
                .iter()
                .zip(sym_arr.iter())
                .zip(price_arr.values().iter())
            {
                let symbol = symbol.expect("symbol as utf8");
                let price = *price;
                rows.push((*ts, symbol.to_string(), price));
            }
        }

        Ok(rows)
    }

    #[tokio::test]
    async fn create_initializes_log_and_state() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let meta = make_basic_table_meta();
        let table = TimeSeriesTable::create(location.clone(), meta).await?;

        // State should be at version 1 with no segments.
        assert_eq!(table.state().version, 1);
        assert!(table.state().segments.is_empty());

        // Verify that the log layout exists on disk.
        let root = match table.location() {
            TableLocation::Local(p) => p.clone(),
        };

        let log_dir = root.join(TransactionLogStore::LOG_DIR_NAME);
        assert!(log_dir.is_dir());

        let current_path = log_dir.join(TransactionLogStore::CURRENT_FILE_NAME);
        let current_contents = tokio::fs::read_to_string(&current_path).await?;
        assert_eq!(current_contents.trim(), "1");

        Ok(())
    }

    #[tokio::test]
    async fn open_round_trip_after_create() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let meta = make_basic_table_meta();
        let created = TimeSeriesTable::create(location.clone(), meta).await?;

        let reopened = TimeSeriesTable::open(location.clone()).await?;

        assert_eq!(created.state().version, reopened.state().version);
        assert_eq!(created.index_spec(), reopened.index_spec());
        Ok(())
    }

    #[tokio::test]
    async fn open_empty_root_errors() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        // There is no CURRENT and no commits, so opening should fail.
        let result = TimeSeriesTable::open(location).await;
        assert!(matches!(result, Err(TableError::EmptyTable)));
        Ok(())
    }

    #[tokio::test]
    async fn create_fails_if_table_already_exists() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let meta = make_basic_table_meta();
        let _first = TimeSeriesTable::create(location.clone(), meta.clone()).await?;

        // Second create should detect existing commits and fail.
        let result = TimeSeriesTable::create(location.clone(), meta).await;
        assert!(matches!(result, Err(TableError::AlreadyExists { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_missing_time_column_errors() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel = "data/seg-no-ts.parquet";
        let path = tmp.path().join(rel);
        write_parquet_without_time_column(&path, &["A"], &[1.0])?;

        let err = table
            .append_parquet_segment(SegmentId("seg-no-ts".to_string()), rel, "ts")
            .await
            .expect_err("expected missing time column");

        match err {
            TableError::SegmentMeta { source } => {
                assert!(matches!(source, SegmentMetaError::MissingTimeColumn { .. }));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_updates_state_and_log() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();

        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel_path = "data/seg1.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_test_parquet(
            &abs_path,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;

        let new_version = table
            .append_parquet_segment(SegmentId("seg-1".to_string()), rel_path, "ts")
            .await?;

        assert_eq!(new_version, 2);
        assert_eq!(table.state.version, 2);
        let seg = table
            .state
            .segments
            .get(&SegmentId("seg-1".to_string()))
            .expect("segment present");
        assert_eq!(seg.path, rel_path);
        assert_eq!(seg.row_count, 1);
        assert_eq!(seg.ts_min.timestamp_millis(), 1_000);
        assert_eq!(seg.ts_max.timestamp_millis(), 1_000);

        let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
        let commit_path = log_dir.join("0000000002.json");
        assert!(commit_path.is_file());
        let current =
            tokio::fs::read_to_string(log_dir.join(TransactionLogStore::CURRENT_FILE_NAME)).await?;
        assert_eq!(current.trim(), "2");

        let reopened = TimeSeriesTable::open(location).await?;
        assert!(
            reopened
                .state
                .segments
                .contains_key(&SegmentId("seg-1".to_string()))
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_adopts_schema_when_missing() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let index = TimeIndexSpec {
            timestamp_column: "ts".to_string(),
            entity_columns: vec![],
            bucket: TimeBucket::Minutes(1),
            timezone: None,
        };
        let meta = TableMeta {
            kind: TableKind::TimeSeries(index),
            logical_schema: None,
            created_at: utc_datetime(2025, 1, 1, 0, 0, 0),
            format_version: 1,
        };

        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel_path = "data/seg-adopt.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_test_parquet(
            &abs_path,
            true,
            false,
            &[TestRow {
                ts_millis: 5_000,
                symbol: "B",
                price: 20.0,
            }],
        )?;

        let new_version = table
            .append_parquet_segment(SegmentId("seg-adopt".to_string()), rel_path, "ts")
            .await?;

        assert_eq!(new_version, 2);
        let schema = table
            .state
            .table_meta
            .logical_schema
            .as_ref()
            .expect("schema adopted");
        let names: Vec<_> = schema.columns().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["ts", "symbol", "price"]);
        let ts_col = &schema.columns()[0];
        assert_eq!(
            ts_col.data_type,
            LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Millis,
                timezone: None,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_rejects_schema_mismatch() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location, meta).await?;

        let rel_path = "data/seg-missing-symbol.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_test_parquet(
            &abs_path,
            false,
            false,
            &[TestRow {
                ts_millis: 10_000,
                symbol: "C",
                price: 30.0,
            }],
        )?;

        let err = table
            .append_parquet_segment(SegmentId("seg-bad".to_string()), rel_path, "ts")
            .await
            .expect_err("expected schema mismatch");

        match err {
            TableError::SchemaCompatibility { source } => {
                assert!(matches!(
                    source,
                    crate::helpers::schema::SchemaCompatibilityError::MissingColumn { .. }
                ));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_allows_duplicate_id_and_path() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel_path = "data/dup.parquet";
        let abs_path = tmp.path().join(rel_path);

        write_test_parquet(
            &abs_path,
            true,
            false,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 10.0,
            }],
        )?;
        let v2 = table
            .append_parquet_segment(SegmentId("seg-dup".to_string()), rel_path, "ts")
            .await?;
        assert_eq!(v2, 2);
        assert_eq!(table.state.version, 2);
        assert_eq!(
            table
                .state
                .segments
                .get(&SegmentId("seg-dup".to_string()))
                .unwrap()
                .row_count,
            1
        );

        // Overwrite the same path with different content and append again with the same segment ID.
        write_test_parquet(
            &abs_path,
            true,
            false,
            &[
                TestRow {
                    ts_millis: 2_000,
                    symbol: "B",
                    price: 20.0,
                },
                TestRow {
                    ts_millis: 3_000,
                    symbol: "C",
                    price: 30.0,
                },
            ],
        )?;

        let v3 = table
            .append_parquet_segment(SegmentId("seg-dup".to_string()), rel_path, "ts")
            .await?;
        assert_eq!(v3, 3);
        assert_eq!(table.state.version, 3);

        let seg = table
            .state
            .segments
            .get(&SegmentId("seg-dup".to_string()))
            .expect("segment retained after duplicate append");
        assert_eq!(seg.row_count, 2);
        assert_eq!(seg.ts_min.timestamp_millis(), 2_000);
        assert_eq!(seg.ts_max.timestamp_millis(), 3_000);

        let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
        assert!(log_dir.join("0000000003.json").is_file());
        Ok(())
    }

    #[tokio::test]
    async fn append_parquet_segment_conflict_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let rel_path = "data/conflict.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_test_parquet(
            &abs_path,
            true,
            false,
            &[TestRow {
                ts_millis: 10_000,
                symbol: "X",
                price: 100.0,
            }],
        )?;

        // Simulate an external writer advancing CURRENT to version 2 while this handle is at version 1.
        table
            .log
            .commit_with_expected_version(1, vec![])
            .await
            .expect("external commit succeeds");

        let err = table
            .append_parquet_segment(SegmentId("seg-conflict".to_string()), rel_path, "ts")
            .await
            .expect_err("expected conflict due to stale version");

        match err {
            TableError::TransactionLog { source } => {
                assert!(matches!(
                    source,
                    CommitError::Conflict {
                        expected: 1,
                        found: 2,
                        ..
                    }
                ));
            }
            other => panic!("unexpected error: {other:?}"),
        }
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
                    ts_millis: 3_000,
                    symbol: "C",
                    price: 30.0,
                },
                TestRow {
                    ts_millis: 4_000,
                    symbol: "D",
                    price: 40.0,
                },
            ],
        )?;

        table
            .append_parquet_segment(SegmentId("seg-scan-1".to_string()), rel1, "ts")
            .await?;
        table
            .append_parquet_segment(SegmentId("seg-scan-2".to_string()), rel2, "ts")
            .await?;

        let start = Utc.timestamp_millis_opt(1_500).single().expect("valid ts");
        let end = Utc.timestamp_millis_opt(3_500).single().expect("valid ts");

        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(
            rows,
            vec![
                (2_000, "B".to_string(), 20.0),
                (3_000, "C".to_string(), 30.0),
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

        table
            .append_parquet_segment(SegmentId("seg-boundary".to_string()), rel, "ts")
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
    async fn read_segment_range_overflow_nanoseconds() -> TestResult {
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
        };

        let huge = Utc
            .timestamp_opt(9_223_372_037, 0)
            .single()
            .expect("overflow ts");
        let err = read_segment_range(&location, &segment, "ts", huge, huge)
            .await
            .expect_err("overflow should error");

        assert!(matches!(err, TableError::TimeConversionOverflow { .. }));
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

        table
            .append_parquet_segment(SegmentId("seg-micros".to_string()), rel, "ts")
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

        table
            .append_parquet_segment(SegmentId("seg-nanos".to_string()), rel, "ts")
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
            &["A", "B", "C"],
            &[1.0, 2.0, 3.0],
        )?;

        table
            .append_parquet_segment(SegmentId("seg-null".to_string()), rel, "ts")
            .await?;

        let start = Utc.timestamp_millis_opt(500).single().unwrap();
        let end = Utc.timestamp_millis_opt(2_500).single().unwrap();
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(
            rows,
            vec![(1_000, "A".to_string(), 1.0), (2_000, "C".to_string(), 3.0)]
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
    async fn scan_range_orders_overlapping_segments_by_ts_min() -> TestResult {
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
                ts_millis: 1_500,
                symbol: "B1",
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
                ts_millis: 1_000,
                symbol: "A1",
                price: 1.0,
            }],
        )?;

        // append in reverse ts_min order to ensure sort_by_key is exercised
        table
            .append_parquet_segment(SegmentId("seg-b".to_string()), rel_b, "ts")
            .await?;
        table
            .append_parquet_segment(SegmentId("seg-a".to_string()), rel_a, "ts")
            .await?;

        let start = Utc.timestamp_millis_opt(900).single().unwrap();
        let end = Utc.timestamp_millis_opt(2_000).single().unwrap();
        let rows = collect_scan_rows(&table, start, end).await?;

        assert_eq!(
            rows,
            vec![
                (1_000, "A1".to_string(), 1.0),
                (1_500, "B1".to_string(), 2.0)
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
                ts_millis: 10_000,
                symbol: "Z",
                price: 9.0,
            }],
        )?;

        table
            .append_parquet_segment(SegmentId("seg-early".to_string()), rel1, "ts")
            .await?;
        table
            .append_parquet_segment(SegmentId("seg-late".to_string()), rel2, "ts")
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
            .append_parquet_segment(SegmentId("seg-corrupt".to_string()), rel, "ts")
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
