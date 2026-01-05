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
//! - `create` bootstraps a fresh table with an initial metadata commit,
//! - append APIs handle schema enforcement, coverage sidecars, and OCC,
//! - range scans stream filtered record batches.
//!
//! Append entry points:
//! - `append_parquet_segment_with_id`: caller supplies a `SegmentId`, bytes are read from storage, and the core append logic enforces schema and coverage.
//! - `append_parquet_segment`: derives a deterministic `SegmentId` from `(relative_path, bytes)` before delegating to the same core logic.
//!
//! Both append paths:
//! - compute segment coverage, reject overlaps against the current snapshot, and persist sidecars,
//! - commit `AddSegment` with `coverage_path` plus `UpdateTableCoverage` atomically,
//! - fail fast if the table state is missing coverage pointers or has segments without `coverage_path`.
pub mod append;
pub mod coverage_queries;
pub mod coverage_state;
pub mod error;
pub mod scan;

#[cfg(test)]
pub(crate) mod test_util;

use std::pin::Pin;

use arrow::array::RecordBatch;

use futures::Stream;

use snafu::prelude::*;

use crate::time_series_table::error::{
    AlreadyExistsSnafu, EmptyTableSnafu, NotTimeSeriesSnafu, SegmentMetaSnafu, TableError,
    TransactionLogSnafu,
};

use crate::{
    storage::TableLocation,
    transaction_log::{
        LogAction, TableKind, TableMeta, TableState, TimeIndexSpec, TransactionLogStore,
    },
};

/// Stream of Arrow RecordBatch values from a time-series scan.
pub type TimeSeriesScan = Pin<Box<dyn Stream<Item = Result<RecordBatch, TableError>> + Send>>;

/// High-level time-series table handle.
///
/// This is the main entry point for callers. It bundles:
/// - where the table is,
/// - how to talk to the transaction log,
/// - what the current committed state is,
/// - and the extracted time index spec.
#[derive(Debug)]
pub struct TimeSeriesTable {
    log: TransactionLogStore,
    state: TableState,
    index: TimeIndexSpec,
}

impl TimeSeriesTable {
    /// Construct a table handle from an existing snapshot.
    ///
    /// This does not replay the transaction log; callers must provide a state
    /// derived from the same location.
    pub fn from_state(location: TableLocation, state: TableState) -> Result<Self, TableError> {
        let index = match &state.table_meta.kind {
            TableKind::TimeSeries(spec) => spec.clone(),
            other => {
                return NotTimeSeriesSnafu {
                    kind: other.clone(),
                }
                .fail();
            }
        };

        let log = TransactionLogStore::new(location);
        Ok(Self { log, state, index })
    }

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
        self.log.location()
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

        Ok(Self { log, state, index })
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
        Ok(Self { log, state, index })
    }

    /// Load the current log version from disk without mutating in-memory state.
    pub async fn current_version(&self) -> Result<u64, TableError> {
        self.log
            .load_current_version()
            .await
            .context(TransactionLogSnafu)
    }

    /// Rebuild and return the latest table state from the transaction log.
    pub async fn load_latest_state(&self) -> Result<TableState, TableError> {
        self.log
            .rebuild_table_state()
            .await
            .context(TransactionLogSnafu)
    }

    /// Refresh in-memory state if the log has advanced; returns true if updated.
    pub async fn refresh(&mut self) -> Result<bool, TableError> {
        let current = self
            .log
            .load_current_version()
            .await
            .context(TransactionLogSnafu)?;

        if current == self.state.version {
            return Ok(false);
        }

        let state = self
            .log
            .rebuild_table_state()
            .await
            .context(TransactionLogSnafu)?;

        let index = match &state.table_meta.kind {
            TableKind::TimeSeries(spec) => spec.clone(),
            other => {
                return NotTimeSeriesSnafu {
                    kind: other.clone(),
                }
                .fail();
            }
        };

        self.state = state;
        self.index = index;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageLocation;
    use crate::storage::TableLocation;
    use crate::time_series_table::test_util::*;
    use crate::transaction_log::TimeBucket;
    use crate::transaction_log::TransactionLogStore;

    use tempfile::TempDir;

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
        let root = match table.location().storage() {
            StorageLocation::Local(p) => p.clone(),
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
    async fn refresh_returns_false_when_no_new_commits() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta).await?;

        let refreshed = table.refresh().await?;
        assert!(!refreshed);
        assert_eq!(table.state().version, 1);
        Ok(())
    }

    #[tokio::test]
    async fn refresh_updates_state_and_index_on_change() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta.clone()).await?;

        let mut updated_meta = meta.clone();
        if let TableKind::TimeSeries(spec) = &mut updated_meta.kind {
            spec.bucket = TimeBucket::Minutes(5);
        }

        let log = TransactionLogStore::new(location.clone());
        let new_version = log
            .commit_with_expected_version(1, vec![LogAction::UpdateTableMeta(updated_meta.clone())])
            .await?;
        assert_eq!(new_version, 2);

        let refreshed = table.refresh().await?;
        assert!(refreshed);
        assert_eq!(table.state().version, 2);

        match &table.state().table_meta.kind {
            TableKind::TimeSeries(spec) => assert_eq!(spec.bucket, TimeBucket::Minutes(5)),
            other => panic!("expected time series table kind, got {other:?}"),
        }
        assert_eq!(table.index_spec().bucket, TimeBucket::Minutes(5));
        Ok(())
    }
}
