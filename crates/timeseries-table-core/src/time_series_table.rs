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

use std::path::Path;

use snafu::prelude::*;

use crate::{
    helpers::{
        parquet::{logical_schema_from_parquet_location, segment_meta_from_parquet_location},
        schema::{SchemaCompatibilityError, ensure_schema_exact_match},
    },
    storage::TableLocation,
    transaction_log::{
        CommitError, LogAction, SegmentId, TableKind, TableMeta, TableState, TimeIndexSpec,
        TransactionLogStore, segments::SegmentMetaError,
    },
};

/// Errors from high-level time-series table operations.
#[derive(Debug, Snafu)]
pub enum TableError {
    /// Any error coming from the transaction log / commit machinery.
    #[snafu(display("Transaction log error: {source}"))]
    TransactionLog {
        /// Underlying transaction log / commit error.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// Attempting to open a table that has no commits at all.
    #[snafu(display("Cannot open table with no commits (CURRENT version is 0)"))]
    EmptyTable,

    /// The underlying table is not a time-series table.
    #[snafu(display("Table kind is {kind:?}, expected TableKind::TimeSeries"))]
    NotTimeSeries {
        /// The actual kind of the underlying table that was discovered.
        kind: TableKind,
    },

    /// Attempt to create a table where commits already exist.
    #[snafu(display("Table already exists; current transaction log version is {current_version}"))]
    AlreadyExists {
        /// Current transaction log version that indicates the table already exists.
        current_version: u64,
    },

    /// Segment-level metadata / Parquet error during append.
    #[snafu(display("Segment metadata error while appending: {source}"))]
    SegmentMeta {
        /// Underlying segment metadata error.
        #[snafu(source, backtrace)]
        source: SegmentMetaError,
    },

    /// Schema compatibility error when appending a segment with incompatible schema.
    #[snafu(display("Schema compatibility error: {source}"))]
    SchemaCompatibility {
        /// Underlying schema compatibility error.
        #[snafu(source)]
        source: SchemaCompatibilityError,
    },

    /// Table has progressed past the initial metadata commit but still lacks
    /// a canonical logical schema. That’s an invariant violation for v0.1.
    #[snafu(display("Table has no logical_schema at version {version}; cannot append in v0.1"))]
    MissingCanonicalSchema {
        /// The transaction log version missing a canonical logical schema.
        version: u64,
    },
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::TableLocation;
    use crate::transaction_log::segments::SegmentId;
    use crate::transaction_log::table_metadata::{LogicalDataType, LogicalTimestampUnit};
    use crate::transaction_log::{
        CommitError, LogicalColumn, LogicalSchema, TableKind, TableMeta, TimeBucket, TimeIndexSpec,
        TransactionLogStore,
    };
    use chrono::{TimeZone, Utc};
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
}
