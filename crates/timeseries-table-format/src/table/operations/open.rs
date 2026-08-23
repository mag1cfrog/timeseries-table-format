//! Opening an existing time-series table.

use snafu::Snafu;

use crate::{
    storage::{StorageError, TableLocation},
    table::{TableError, TimeSeriesTable},
    transaction_log::{CommitError, TableKind, TransactionLogStore},
};

/// Errors owned by a table open operation.
#[derive(Debug, Snafu)]
#[snafu(module, visibility(pub(crate)))]
pub enum OpenTableError {
    /// The table location contains no commits.
    #[snafu(display("Cannot open table with no commits"))]
    EmptyTable,

    /// The loaded metadata does not describe a time-series table.
    #[snafu(display("Table kind is {kind:?}, expected a time-series table"))]
    NotTimeSeries {
        /// Loaded table kind.
        kind: TableKind,
    },

    /// Resolving or accessing the requested table location failed.
    #[snafu(context(false), display("Table storage error: {source}"))]
    Storage {
        /// Complete storage failure.
        #[snafu(source, backtrace)]
        source: StorageError,
    },

    /// Reading or replaying the transaction log failed.
    #[snafu(context(false), display("Table open transaction-log error: {source}"))]
    Commit {
        /// Complete transaction-log failure.
        #[snafu(source, backtrace)]
        source: CommitError,
    },
}

impl TimeSeriesTable {
    /// Open an existing time-series table at the given location.
    #[tracing::instrument(
        name = "table.open",
        level = "debug",
        skip_all,
        fields(
            table_version = tracing::field::Empty,
            index_kind = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    pub async fn open(location: TableLocation) -> Result<Self, TableError> {
        let result: Result<Self, OpenTableError> = async {
            let log = TransactionLogStore::new(location);
            let current_version = log
                .load_current_version()
                .await
                .map_err(OpenTableError::from)?;
            tracing::Span::current().record("table_version", current_version);
            if current_version == 0 {
                return Err(OpenTableError::EmptyTable);
            }

            let state = log
                .rebuild_table_state()
                .await
                .map_err(OpenTableError::from)?;
            let index = match &state.table_meta.kind {
                TableKind::TimeSeries(index) => index.clone(),
                kind => return Err(OpenTableError::NotTimeSeries { kind: kind.clone() }),
            };
            tracing::Span::current().record("index_kind", index.kind.name());

            Ok(Self { log, state, index })
        }
        .await;
        tracing::Span::current().record(
            "outcome",
            if result.is_ok() {
                "succeeded"
            } else {
                "failed"
            },
        );
        result.map_err(TableError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        metadata::table_metadata::TABLE_FORMAT_VERSION,
        table::test_util::{
            TestResult, TraceCapture, assert_capture_excludes, assert_debug_span, assert_no_event,
            make_basic_table_meta,
        },
        transaction_log::{LogAction, TransactionLogStore},
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn open_round_trips_a_created_table() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let created = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let capture = TraceCapture::default();

        let reopened = capture.run(TimeSeriesTable::open(location)).await?;

        assert_eq!(created.state().version, reopened.state().version);
        assert_eq!(created.index_spec(), reopened.index_spec());
        assert_debug_span(
            &capture,
            "table.open",
            &[
                ("table_version", Some("1")),
                ("index_kind", Some("timestamp")),
                ("outcome", Some("succeeded")),
            ],
        );
        assert_no_event(&capture, "table.open");
        assert_capture_excludes(&capture, &[&tmp.path().display().to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn open_rejects_an_empty_location() -> TestResult {
        let tmp = TempDir::new()?;
        let capture = TraceCapture::default();

        let error = capture
            .run(TimeSeriesTable::open(TableLocation::local(tmp.path())))
            .await
            .expect_err("empty table must fail");

        assert!(matches!(
            error,
            TableError::Open {
                source: OpenTableError::EmptyTable
            }
        ));
        assert_debug_span(
            &capture,
            "table.open",
            &[
                ("table_version", Some("0")),
                ("index_kind", None),
                ("outcome", Some("failed")),
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn open_preserves_format_and_table_kind_failures() -> TestResult {
        for found in [TABLE_FORMAT_VERSION - 1, TABLE_FORMAT_VERSION + 1] {
            let tmp = TempDir::new()?;
            let location = TableLocation::local(tmp.path());
            let mut meta = make_basic_table_meta();
            meta.format_version = found;
            TransactionLogStore::new(location.clone())
                .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
                .await?;

            assert!(matches!(
                TimeSeriesTable::open(location)
                    .await
                    .expect_err("unsupported format must fail"),
                TableError::Open {
                    source: OpenTableError::Commit {
                        source: CommitError::UnsupportedFormatVersion {
                            expected: TABLE_FORMAT_VERSION,
                            found: actual,
                        }
                    }
                } if actual == u64::from(found)
            ));
        }

        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut meta = make_basic_table_meta();
        meta.kind = TableKind::Generic;
        TransactionLogStore::new(location.clone())
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
            .await?;

        assert!(matches!(
            TimeSeriesTable::open(location)
                .await
                .expect_err("generic table must fail"),
            TableError::Open {
                source: OpenTableError::NotTimeSeries {
                    kind: TableKind::Generic
                }
            }
        ));
        Ok(())
    }
}
