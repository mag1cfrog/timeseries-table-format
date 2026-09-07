//! Transactional addition of nullable columns.

use snafu::{ResultExt, Snafu};

use crate::{
    metadata::{
        logical_schema::LogicalField, protocol::TableProtocolError,
        schema_evolution::SchemaEvolutionError,
    },
    table::{TableError, TimeSeriesTable},
    transaction_log::{CommitError, LogAction},
};

/// Errors owned by a nullable-column addition.
#[derive(Debug, Snafu)]
#[snafu(module)]
#[non_exhaustive]
pub enum AddColumnsError {
    /// This client cannot safely mutate the selected table snapshot.
    #[snafu(context(false), display("Table protocol error: {source}"))]
    Protocol {
        /// Original compatibility failure.
        source: TableProtocolError,
    },
    /// The proposed schema violates the nullable-addition contract.
    #[snafu(context(false), display("Invalid column addition: {source}"))]
    Schema {
        /// Original schema validation failure.
        #[snafu(source(from(SchemaEvolutionError, Box::new)), backtrace)]
        source: Box<SchemaEvolutionError>,
    },
    /// Publishing the metadata commit failed or had an ambiguous outcome.
    ///
    /// For [`CommitError::AmbiguousOutcome`], reopen and reconcile the log before
    /// retrying. Neither success nor rollback can be assumed.
    #[snafu(context(false), display("Column addition commit error: {source}"))]
    Commit {
        /// Original conflict, storage, or ambiguous-outcome failure.
        #[snafu(backtrace)]
        source: CommitError,
    },
}

impl TimeSeriesTable {
    /// Append new nullable top-level fields to the canonical schema in one commit.
    ///
    /// Returns the committed version and updates this handle on definite success.
    /// Existing rows read as null for the added columns; no data or coverage files
    /// are read or rewritten. New appends may omit nullable payload columns.
    ///
    /// Names are exact and case-sensitive, must not be blank, and must not collide
    /// with existing fields or each other. All new fields must be nullable and
    /// round-trip exactly through the existing Logical/Arrow/Parquet schema model.
    /// The table must already have a canonical schema, even if it has no rows.
    ///
    /// Uses this handle's version without refreshing or retrying. Other handles
    /// and already planned scans retain their snapshots. DataFusion registrations
    /// must be replaced after a schema change.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::AddColumns`] for incompatible protocols, invalid
    /// additions, version conflicts, or publication failures. Errors leave this
    /// handle unchanged. On [`CommitError::AmbiguousOutcome`], reopen and reconcile
    /// the log before retrying; do not assume the commit was rolled back.
    pub async fn add_columns(&mut self, columns: Vec<LogicalField>) -> Result<u64, TableError> {
        let result: Result<u64, AddColumnsError> = async {
            self.ensure_write_compatible()?;
            let next = self.state.table_meta.with_added_columns(columns)?;
            let version = self
                .log
                .commit_with_expected_version(
                    self.state.version,
                    vec![LogAction::UpdateTableMeta(next.clone())],
                )
                .await?;
            self.state.table_meta = next;
            self.state.version = version;
            Ok(version)
        }
        .await;
        result.context(crate::table::error::AddColumnsSnafu)
    }
}

#[cfg(test)]
mod tests;
