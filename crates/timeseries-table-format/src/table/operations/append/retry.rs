use super::{
    AppendError, AppendReport, AppendTelemetry, LogAction, PendingAppendSegment, TimeSeriesTable,
    checked_next_version, ensure_existing_segments_have_coverage,
};
use crate::transaction_log::CommitError;
use std::time::Duration;
use uuid::Uuid;

impl PendingAppendSegment {
    pub(super) async fn commit_with_retry(
        self,
        table: &mut TimeSeriesTable,
        mut next_version: u64,
        max_retries: u32,
        telemetry: &mut AppendTelemetry,
    ) -> Result<AppendReport, AppendError> {
        let original_base = table.state.version;
        let canonical_schema = table.state.table_meta.logical_schema.is_some();
        let mut finished = self.finish(table, telemetry).await?;
        // Refreshed snapshots stay private until this writer publishes successfully.
        let mut candidate: Option<TimeSeriesTable> = None;
        let mut retry_count = 0;
        loop {
            let target = candidate.as_mut().unwrap_or(&mut *table);
            let attempt_base = target.state.version;
            let result = finished.publish(target, next_version, telemetry).await;
            let error = match result {
                Ok(report) => {
                    if let Some(candidate) = candidate {
                        *table = candidate;
                        tracing::debug!(
                            name: "table.append.retry_complete",
                            target: "timeseries_table_format::table::append",
                            original_base,
                            retry_count,
                            final_base = report.starting_version,
                            committed_version = report.committed_version,
                            "Published append with commit retry enabled"
                        );
                    }
                    return Ok(report);
                }
                Err(error @ AppendError::CommitAmbiguous { .. }) => return Err(error),
                Err(error) => error,
            };
            let found = match &error {
                AppendError::Commit {
                    source: CommitError::Conflict { found, .. },
                } if *found > attempt_base && canonical_schema && retry_count < max_retries => {
                    *found
                }
                _ => return Err(finished.rollback(table, error, telemetry).await),
            };
            telemetry.start_phase("commit_retry");
            let refreshed = match table.append_retry_candidate(attempt_base, found).await {
                Ok(Some(candidate)) => candidate,
                Ok(None) => return Err(finished.rollback(table, error, telemetry).await),
                Err(error) => return Err(finished.rollback(table, error, telemetry).await),
            };
            next_version = match checked_next_version(refreshed.state.version) {
                Ok(version) => version,
                Err(error) => return Err(finished.rollback(table, error.into(), telemetry).await),
            };
            let backoff_ms =
                (10_u64 << retry_count.min(5)).min(240) + (Uuid::new_v4().as_u128() % 11) as u64;
            retry_count += 1;
            tracing::debug!(
                name: "table.append.retry",
                target: "timeseries_table_format::table::append",
                original_base,
                retry_count,
                final_base = refreshed.state.version,
                backoff_ms,
                "Retrying append against a newer committed snapshot"
            );
            candidate = Some(refreshed);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            telemetry.complete_phase();
        }
    }
}

impl TimeSeriesTable {
    async fn append_retry_candidate(
        &self,
        attempt_base: u64,
        observed_version: u64,
    ) -> Result<Option<Self>, AppendError> {
        let state = self
            .log
            .rebuild_table_state()
            .await
            .map_err(AppendError::from)?;
        if state.version < observed_version || state.table_meta != self.state.table_meta {
            tracing::debug!(
                name: "table.append.retry_rejected",
                target: "timeseries_table_format::table::append",
                reason = "metadata_changed_or_history_moved_backwards",
                "Cannot rebase append onto the latest snapshot"
            );
            return Ok(None);
        }
        for version in
            checked_next_version(attempt_base).map_err(AppendError::from)?..=state.version
        {
            let (commit, unknown_actions) = self
                .log
                .load_commit_with_unknown_action_flag(version)
                .await
                .map_err(AppendError::from)?;
            let mut additions = 0;
            let mut snapshots = 0;
            let append_only = !unknown_actions
                && commit.version == version
                && commit.base_version == version - 1
                && commit.actions.iter().all(|action| match action {
                    LogAction::AddSegment(_) => {
                        additions += 1;
                        true
                    }
                    LogAction::UpdateTableCoverage { index_kind, .. }
                        if index_kind == &self.index.kind =>
                    {
                        snapshots += 1;
                        true
                    }
                    _ => false,
                })
                && additions > 0
                && snapshots == 1;
            if !append_only {
                tracing::debug!(
                    name: "table.append.retry_rejected",
                    target: "timeseries_table_format::table::append",
                    version,
                    reason = "intervening_commit_is_not_append_only",
                    "Cannot rebase append across this commit"
                );
                return Ok(None);
            }
        }
        state
            .table_meta
            .ensure_write_compatible()
            .map_err(AppendError::from)?;
        ensure_existing_segments_have_coverage(&state)?;
        Ok(Some(Self {
            log: self.log.clone(),
            state,
            index: self.index.clone(),
        }))
    }
}
