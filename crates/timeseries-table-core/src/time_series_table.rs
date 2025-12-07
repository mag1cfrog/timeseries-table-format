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

use snafu::prelude::*;

use crate::{
    storage::TableLocation,
    transaction_log::{
        CommitError, LogAction, TableKind, TableMeta, TableState, TimeIndexSpec,
        TransactionLogStore,
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
}

/// High-level time-series table handle.
///
/// This is the main entry point for callers. It bundles:
/// - where the table is,
/// - how to talk to the transaction log,
/// - what the current committed state is,
/// - and the extracted time index spec.
#[derive(Debug)]
pub struct TimesSeriesTable {
    location: TableLocation,
    log: TransactionLogStore,
    state: TableState,
    index: TimeIndexSpec,
}

impl TimesSeriesTable {
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

    /// Open an existing time-series table at the given location.
    ///
    /// Steps:
    /// - Build a `TransactionLogStore` for the location.
    /// - Rebuild `TableState` from the transaction log.
    /// - Reject empty tables (version == 0).
    /// - Require `TableKind::TimeSeries` and extract `TimeIndexSpec`.
    pub async fn open(location: TableLocation) -> Result<Self, TableError> {
        let log = TransactionLogStore::new(location.clone());

        // Rebuild the snapshot of state from the log.
        let state = log
            .rebuild_table_state()
            .await
            .context(TransactionLogSnafu)?;

        // For now we treat "no commits" as an error; `create` should have written
        // at least one metadata commit.
        if state.version == 0 {
            return EmptyTableSnafu.fail();
        }

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
        // from sliently appending to a pre-existing table.
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
}
