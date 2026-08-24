//! High-level time-series table abstraction.
//!
//! This module is the canonical home for the user-facing [`TimeSeriesTable`]
//! API.

mod error;
mod operations;

pub use operations::append;
pub use operations::{
    AppendError, CoverageQueryError, CreateTableError, OpenTableError, OptimizeError,
    OptimizeReport, ScanError, TableStateAccessError,
};

#[cfg(test)]
pub(crate) mod test_util;

#[cfg(test)]
mod latest_snapshot_tests;

use std::pin::Pin;

use arrow::array::RecordBatch;
use futures::Stream;

use crate::{
    metadata::protocol::TableProtocolError,
    storage::TableLocation,
    transaction_log::{IndexSpec, TableState, TransactionLogStore},
};

pub use crate::formats::parquet::EntityRewriteError;
pub use error::TableError;

/// Stream of Arrow RecordBatch values from a time-series scan.
///
/// Batch and row order is unspecified.
pub type TimeSeriesScan = Pin<Box<dyn Stream<Item = Result<RecordBatch, TableError>> + Send>>;

/// High-level time-series table handle.
///
/// This is the main entry point for callers. It bundles the table location,
/// transaction log, current committed state, and ordered-index specification.
#[derive(Debug, Clone)]
pub struct TimeSeriesTable {
    log: TransactionLogStore,
    state: TableState,
    index: IndexSpec,
}

impl TimeSeriesTable {
    /// Return the current committed table state.
    pub fn state(&self) -> &TableState {
        &self.state
    }

    /// Return a mutable reference to the current committed table state (crate-internal).
    #[allow(dead_code)]
    pub(crate) fn state_mut(&mut self) -> &mut TableState {
        &mut self.state
    }

    /// Return the ordered-index specification for this table.
    pub fn index_spec(&self) -> &IndexSpec {
        &self.index
    }

    /// Return the table location.
    pub fn location(&self) -> &TableLocation {
        self.log.location()
    }

    pub(crate) fn ensure_write_compatible(&self) -> Result<(), TableProtocolError> {
        self.state.table_meta.ensure_write_compatible()
    }
}
