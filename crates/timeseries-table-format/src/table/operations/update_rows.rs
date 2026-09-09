//! Snapshot-bound orchestration of preparation, replacements, and publication.

use arrow::record_batch::RecordBatchReader;
use snafu::{Backtrace, ResultExt, Snafu};
use std::collections::HashSet;

use super::{
    update_prepare::{PrepareError, prepare_updates},
    update_rewrite::{RewriteError, StagedUpdates, stage_update_replacements},
};
use crate::{
    metadata::protocol::TableProtocolError,
    table::{TableError, TimeSeriesTable},
    transaction_log::{CommitError, LogAction, checked_next_version},
};

/// Result of assigning selected columns to existing rows by their complete keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRowsReport {
    /// Snapshot against which the caller computed the assignments.
    pub starting_version: u64,
    /// Published version, or `starting_version` for an empty-input no-op.
    pub committed_version: u64,
    /// Assigned rows, including assignments equal to their existing values.
    pub rows_updated: u64,
    /// Source segments replaced; unaffected segments are retained.
    pub segments_rewritten: u64,
    /// Total sizes of affected source Parquet files, not bytes read.
    pub source_file_bytes: u64,
    /// Total sizes of replacement Parquet files, not bytes written.
    pub replacement_file_bytes: u64,
    /// True only for a fully validated stream containing zero rows.
    pub no_op: bool,
}

/// Typed validation, preparation, replacement, and publication failures.
#[derive(Debug, Snafu)]
#[snafu(module)]
#[non_exhaustive]
pub enum UpdateRowsError {
    /// Version zero does not identify a created table snapshot.
    #[snafu(display("Update expected_version must be greater than zero"))]
    ZeroVersion,
    /// This handle is not the snapshot used to compute the assignments.
    #[snafu(display("Update snapshot mismatch: expected version {expected}, handle is {found}"))]
    SnapshotMismatch {
        /// Version supplied by the caller.
        expected: u64,
        /// Version selected by this handle.
        found: u64,
    },
    /// This client cannot safely mutate the selected snapshot.
    #[snafu(context(false), display("Update protocol error: {source}"))]
    Protocol {
        /// Original compatibility failure.
        source: TableProtocolError,
    },
    /// Input validation, exact-key matching, or scratch preparation failed.
    #[snafu(context(false), display("Update preparation failed: {source}"))]
    Preparation {
        /// Complete preparation failure.
        #[snafu(backtrace)]
        source: PrepareError,
    },
    /// Replacement staging or verification failed.
    #[snafu(context(false), display("Update replacement failed: {source}"))]
    Rewrite {
        /// Complete replacement failure.
        #[snafu(source(from(RewriteError, Box::new)), backtrace)]
        source: Box<RewriteError>,
    },
    /// Version checking or publication failed.
    ///
    /// On [`CommitError::AmbiguousOutcome`], files are preserved and the handle
    /// remains unchanged. Reopen and reconcile before retrying.
    #[snafu(context(false), display("Update commit failed: {source}"))]
    Commit {
        /// Original conflict, storage, encoding, or ambiguous-outcome failure.
        #[snafu(backtrace)]
        source: CommitError,
    },
    /// A report counter cannot be represented as `u64`.
    #[snafu(display("Update report counter overflow: {counter}"))]
    CountOverflow {
        /// Counter that overflowed.
        counter: &'static str,
    },
    /// Staged ownership or source bindings do not describe a complete replacement.
    #[snafu(display("Invalid update publication plan: {reason}"))]
    InvalidPlan {
        /// Violated publication invariant.
        reason: &'static str,
    },
    /// A definite failure was followed by a replacement cleanup failure.
    #[snafu(display("{source}; replacement cleanup also failed: {cleanup}"))]
    CleanupAfterFailure {
        /// Original failure.
        #[snafu(backtrace)]
        source: Box<UpdateRowsError>,
        /// Complete cleanup diagnostic, including affected paths.
        cleanup: Box<RewriteError>,
    },
}

impl TimeSeriesTable {
    /// Atomically assign selected existing payload columns using complete row keys.
    ///
    /// The source schema contains exactly the configured entity columns, the raw
    /// ordered-index column, and `columns`, in any order. Each complete source key
    /// must identify exactly one existing row. Null keys, duplicate source keys,
    /// missing targets, ambiguous targets, key-column assignments, and invalid
    /// values are errors. Nested destinations are replaced as whole values.
    ///
    /// `expected_version` must identify the snapshot used to compute these values
    /// and must match both this handle and CURRENT. Add nullable columns before
    /// capturing that version. Do not attach a newer version to stale results.
    /// Any intervening commit conflicts; this method never refreshes or retries.
    /// Preflight occurs before calling the reader's `schema` or `next` methods.
    /// Readers need not implement `Send`; batches can use Arrow's
    /// [`RecordBatchIterator`](arrow::record_batch::RecordBatchIterator) adapter.
    ///
    /// Preparation spills to bounded scratch and affected immutable Parquet files
    /// are replaced in one log commit. An empty, fully validated stream rechecks
    /// CURRENT and returns a no-op. Nonempty equal-value assignments still commit.
    /// Other handles and already planned scans retain their snapshots. Newly
    /// planned DataFusion queries see the update without re-registering the table.
    /// File-byte report fields exclude scratch, sidecars, and discovery IO.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::UpdateRows`] with structured causes. Errors leave
    /// this handle unchanged. Definite failures clean owned replacements; an
    /// ambiguous commit preserves them. Reopen and reconcile an ambiguous result
    /// before computing a new attempt. Original files remain for older snapshots
    /// under the existing vacuum retention rules.
    pub async fn update_rows(
        &mut self,
        source: impl RecordBatchReader,
        columns: Vec<String>,
        expected_version: u64,
    ) -> Result<UpdateRowsReport, TableError> {
        let result: Result<UpdateRowsReport, UpdateRowsError> = async {
            if expected_version == 0 {
                return Err(UpdateRowsError::ZeroVersion);
            }
            self.ensure_write_compatible()?;
            if self.state.version != expected_version {
                return Err(UpdateRowsError::SnapshotMismatch {
                    expected: expected_version,
                    found: self.state.version,
                });
            }
            self.check_update_version(expected_version).await?;
            let prepared = prepare_updates(self.location(), &self.state, source, &columns).await?;
            let mut staged =
                stage_update_replacements(self.location(), &self.state, prepared).await?;
            let result = self.publish_updates(&mut staged).await;
            match result {
                // The commit callback already preserved files on ambiguity.
                Err(source) => match staged.close().await {
                    Ok(()) => Err(source),
                    Err(cleanup) => Err(UpdateRowsError::CleanupAfterFailure {
                        source: Box::new(source),
                        cleanup: Box::new(cleanup),
                    }),
                },
                Ok(report) => Ok(report),
            }
        }
        .await;
        result.context(crate::table::error::UpdateRowsSnafu)
    }

    async fn check_update_version(&self, expected: u64) -> Result<(), UpdateRowsError> {
        let found = self.log.load_current_version().await?;
        if expected != found {
            return Err(CommitError::Conflict {
                expected,
                found,
                backtrace: Backtrace::capture(),
            }
            .into());
        }
        Ok(())
    }

    async fn publish_updates(
        &mut self,
        staged: &mut StagedUpdates,
    ) -> Result<UpdateRowsReport, UpdateRowsError> {
        validate_publication_plan(&self.state, staged)?;
        let mut report = UpdateRowsReport {
            starting_version: staged.version,
            committed_version: staged.version,
            rows_updated: staged.metrics.rows_updated,
            segments_rewritten: u64::try_from(staged.replacements.len()).map_err(|_| {
                UpdateRowsError::CountOverflow {
                    counter: "segments_rewritten",
                }
            })?,
            source_file_bytes: staged.metrics.source_file_bytes,
            replacement_file_bytes: staged.metrics.replacement_file_bytes,
            no_op: staged.metrics.rows_updated == 0,
        };
        if report.no_op {
            self.check_update_version(staged.version).await?;
            return Ok(report);
        }
        report.committed_version = checked_next_version(staged.version)?;
        // Construct the complete next snapshot before publication. Nothing after
        // success can fail halfway through updating the handle.
        let mut next = self.state.clone();
        next.version = report.committed_version;
        let mut actions = Vec::new();
        for replacement in &staged.replacements {
            next.segments.remove(&replacement.source.path);
            next.segments.insert(
                replacement.replacement.path.clone(),
                replacement.replacement.clone(),
            );
            actions.push(LogAction::RemoveSegment {
                path: replacement.source.path.clone(),
            });
            actions.push(LogAction::AddSegment(replacement.replacement.clone()));
        }
        self.log
            .commit_with_path_preservation(staged.version, actions, || staged.preserve())
            .await?;
        self.state = next;
        Ok(report)
    }
}

// Validate only transaction bindings and ownership here. The staging layer has
// already verified physical keys, values, schema, counts, and coverage.
fn validate_publication_plan(
    state: &crate::transaction_log::TableState,
    staged: &StagedUpdates,
) -> Result<(), UpdateRowsError> {
    if staged.version != state.version
        || staged.replacements.is_empty() != (staged.metrics.rows_updated == 0)
    {
        return Err(UpdateRowsError::InvalidPlan {
            reason: "snapshot or empty-plan binding differs",
        });
    }
    let live: HashSet<_> = state
        .segments
        .values()
        .flat_map(|s| std::iter::once(s.path.as_str()).chain(s.coverage_path.as_deref()))
        .chain(
            state
                .table_coverage
                .as_ref()
                .map(|c| c.coverage_path.as_str()),
        )
        .collect();
    let mut owned: HashSet<_> = staged.owned_paths().collect();
    if owned.len() != staged.owned_paths().count() || !owned.is_disjoint(&live) {
        return Err(UpdateRowsError::InvalidPlan {
            reason: "owned outputs overlap each other or live objects",
        });
    }
    let mut sources = HashSet::new();
    for pair in &staged.replacements {
        if !sources.insert(&pair.source.path)
            || state.segments.get(&pair.source.path) != Some(&pair.source)
        {
            return Err(UpdateRowsError::InvalidPlan {
                reason: "replacement sources are duplicated or differ from snapshot",
            });
        }
        if !owned.remove(pair.replacement.path.as_str())
            || !pair
                .replacement
                .coverage_path
                .as_deref()
                .is_some_and(|path| owned.remove(path))
        {
            return Err(UpdateRowsError::InvalidPlan {
                reason: "replacement data or sidecar is not uniquely owned",
            });
        }
    }
    if !owned.is_empty() {
        return Err(UpdateRowsError::InvalidPlan {
            reason: "owned output is absent from replacement plan",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
