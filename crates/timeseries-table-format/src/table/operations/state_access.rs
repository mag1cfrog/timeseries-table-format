//! Reading and refreshing a table handle's committed state.

use snafu::Snafu;

use crate::{
    table::{TableError, TimeSeriesTable},
    transaction_log::{CommitError, IndexSpec, TableKind, TableState},
};

/// Errors owned by table state reads and refreshes.
#[derive(Debug, Snafu)]
#[snafu(module, visibility(pub(crate)))]
pub enum TableStateAccessError {
    /// Latest metadata no longer describes a time-series table.
    #[snafu(display("Latest table kind is {kind:?}, expected a time-series table"))]
    NotTimeSeries {
        /// Rejected table kind.
        kind: TableKind,
    },

    /// Reading or replaying the transaction log failed.
    #[snafu(context(false), display("Table state transaction-log error: {source}"))]
    Commit {
        /// Complete transaction-log failure.
        #[snafu(source, backtrace)]
        source: CommitError,
    },
}

fn time_series_index_from_state(state: &TableState) -> Result<IndexSpec, TableStateAccessError> {
    match &state.table_meta.kind {
        TableKind::TimeSeries(index) => Ok(index.clone()),
        kind => Err(TableStateAccessError::NotTimeSeries { kind: kind.clone() }),
    }
}

impl TimeSeriesTable {
    /// Load the current log version without mutating the in-memory state.
    pub async fn current_version(&self) -> Result<u64, TableError> {
        self.log
            .load_current_version()
            .await
            .map_err(TableStateAccessError::from)
            .map_err(TableError::from)
    }

    /// Rebuild and return the latest time-series table state.
    pub async fn load_latest_state(&self) -> Result<TableState, TableError> {
        let result: Result<TableState, TableStateAccessError> = async {
            let state = self
                .log
                .rebuild_table_state()
                .await
                .map_err(TableStateAccessError::from)?;
            time_series_index_from_state(&state)?;
            Ok(state)
        }
        .await;
        result.map_err(TableError::from)
    }

    /// Refresh in-memory state if the transaction log has advanced.
    #[tracing::instrument(
        name = "table.refresh",
        level = "debug",
        skip_all,
        fields(
            previous_version = tracing::field::Empty,
            observed_version = tracing::field::Empty,
            refreshed = tracing::field::Empty,
            new_version = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    pub async fn refresh(&mut self) -> Result<bool, TableError> {
        tracing::Span::current().record("previous_version", self.state.version);
        let result: Result<bool, TableStateAccessError> = async {
            let current = self
                .log
                .load_current_version()
                .await
                .map_err(TableStateAccessError::from)?;
            tracing::Span::current().record("observed_version", current);
            if current == self.state.version {
                return Ok(false);
            }

            let state = self
                .log
                .rebuild_table_state()
                .await
                .map_err(TableStateAccessError::from)?;
            let index = time_series_index_from_state(&state)?;
            self.state = state;
            self.index = index;
            Ok(true)
        }
        .await;

        let span = tracing::Span::current();
        match &result {
            Ok(refreshed) => {
                span.record("refreshed", *refreshed);
                if *refreshed {
                    span.record("new_version", self.state.version);
                    span.record("outcome", "succeeded");
                } else {
                    span.record("outcome", "no_change");
                }
            }
            Err(_) => {
                span.record("outcome", "failed");
            }
        }
        result.map_err(TableError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        storage::{TableLocation, layout},
        table::test_util::{
            TestResult, TraceCapture, assert_capture_excludes, assert_debug_span, assert_no_event,
            make_basic_table_meta,
        },
        transaction_log::{IndexKind, LogAction, TimeIndexGranularity, TransactionLogStore},
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn refresh_reports_no_change_and_applies_a_new_index() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        let mut table = TimeSeriesTable::create(location.clone(), meta.clone()).await?;
        let no_change_capture = TraceCapture::default();

        assert!(!no_change_capture.run(table.refresh()).await?);
        assert_debug_span(
            &no_change_capture,
            "table.refresh",
            &[
                ("previous_version", Some("1")),
                ("observed_version", Some("1")),
                ("refreshed", Some("false")),
                ("new_version", None),
                ("outcome", Some("no_change")),
            ],
        );

        let mut updated_meta = meta;
        let TableKind::TimeSeries(index) = &mut updated_meta.kind else {
            unreachable!("test metadata is time-series");
        };
        index.kind = IndexKind::Timestamp {
            index_granularity: TimeIndexGranularity::Minutes(5),
            timezone: None,
        };
        TransactionLogStore::new(location)
            .commit_with_expected_version(1, vec![LogAction::UpdateTableMeta(updated_meta)])
            .await?;
        let update_capture = TraceCapture::default();

        assert!(update_capture.run(table.refresh()).await?);
        assert_eq!(table.state().version, 2);
        assert!(matches!(
            table.index_spec().kind,
            IndexKind::Timestamp {
                index_granularity: TimeIndexGranularity::Minutes(5),
                ..
            }
        ));
        assert_debug_span(
            &update_capture,
            "table.refresh",
            &[
                ("previous_version", Some("1")),
                ("observed_version", Some("2")),
                ("refreshed", Some("true")),
                ("new_version", Some("2")),
                ("outcome", Some("succeeded")),
            ],
        );
        assert_no_event(&update_capture, "table.refresh");
        assert_capture_excludes(&update_capture, &[&tmp.path().display().to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn state_access_preserves_commit_failures_without_mutating_state() -> TestResult {
        let current_tmp = TempDir::new()?;
        let current_table = TimeSeriesTable::create(
            TableLocation::local(current_tmp.path()),
            make_basic_table_meta(),
        )
        .await?;
        let current_path = current_tmp.path().join(layout::current_rel_path());
        std::fs::remove_file(&current_path)?;
        std::fs::create_dir(&current_path)?;
        assert!(matches!(
            current_table
                .current_version()
                .await
                .expect_err("unreadable CURRENT must fail"),
            TableError::StateAccess {
                source: TableStateAccessError::Commit {
                    source: CommitError::Storage { .. }
                }
            }
        ));

        let refresh_tmp = TempDir::new()?;
        let mut table = TimeSeriesTable::create(
            TableLocation::local(refresh_tmp.path()),
            make_basic_table_meta(),
        )
        .await?;
        let state_before = table.state().clone();
        std::fs::write(
            refresh_tmp.path().join(layout::commit_rel_path(2)),
            b"not json",
        )?;
        std::fs::write(refresh_tmp.path().join(layout::current_rel_path()), b"2\n")?;
        assert!(matches!(
            table
                .load_latest_state()
                .await
                .expect_err("corrupt commit must fail"),
            TableError::StateAccess {
                source: TableStateAccessError::Commit {
                    source: CommitError::CorruptState { .. }
                }
            }
        ));
        assert!(table.refresh().await.is_err());
        assert_eq!(table.state(), &state_before);
        Ok(())
    }

    #[tokio::test]
    async fn state_access_rejects_a_generic_update_without_mutating_state() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let state_before = table.state().clone();
        let mut generic_meta = make_basic_table_meta();
        generic_meta.kind = TableKind::Generic;
        TransactionLogStore::new(location)
            .commit_with_expected_version(1, vec![LogAction::UpdateTableMeta(generic_meta)])
            .await?;

        assert!(matches!(
            table
                .load_latest_state()
                .await
                .expect_err("generic update must fail"),
            TableError::StateAccess {
                source: TableStateAccessError::NotTimeSeries {
                    kind: TableKind::Generic
                }
            }
        ));
        assert!(matches!(
            table.refresh().await.expect_err("generic update must fail"),
            TableError::StateAccess {
                source: TableStateAccessError::NotTimeSeries {
                    kind: TableKind::Generic
                }
            }
        ));
        assert_eq!(table.state(), &state_before);
        Ok(())
    }
}
