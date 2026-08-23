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
#[non_exhaustive]
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
        target = "timeseries_table_format::table",
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
        metadata::table_metadata::TABLE_PROTOCOL_VERSION,
        table::test_util::{
            TestResult, TraceCapture, assert_capture_excludes, assert_debug_span, assert_no_event,
            captured_span, make_basic_table_meta,
        },
        transaction_log::{LogAction, TableProtocolError, TransactionLogStore},
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
        assert_eq!(
            captured_span(&capture, "table.open").target,
            "timeseries_table_format::table"
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
    async fn open_preserves_protocol_and_table_kind_failures() -> TestResult {
        for found in [TABLE_PROTOCOL_VERSION - 1, TABLE_PROTOCOL_VERSION + 1] {
            let tmp = TempDir::new()?;
            let location = TableLocation::local(tmp.path());
            let mut meta = make_basic_table_meta();
            meta.protocol_version = found;
            TransactionLogStore::new(location.clone())
                .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
                .await?;

            assert!(matches!(
                TimeSeriesTable::open(location)
                    .await
                    .expect_err("unsupported protocol must fail"),
                TableError::Open {
                    source: OpenTableError::Commit {
                        source: CommitError::Protocol {
                            source: TableProtocolError::UnsupportedVersion {
                                expected: TABLE_PROTOCOL_VERSION,
                                found: actual,
                            },
                            ..
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

    #[tokio::test]
    async fn open_applies_reader_requirements_without_requiring_writer_support() -> TestResult {
        let writer_tmp = TempDir::new()?;
        let writer_location = TableLocation::local(writer_tmp.path());
        let mut writer_meta = make_basic_table_meta();
        writer_meta
            .required_writer_features
            .insert("future_writer".to_string());
        TransactionLogStore::new(writer_location.clone())
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(writer_meta)])
            .await?;

        let table = TimeSeriesTable::open(writer_location).await?;
        assert_eq!(
            table.state().table_meta.required_writer_features(),
            &["future_writer".to_string()].into_iter().collect()
        );

        let reader_tmp = TempDir::new()?;
        let reader_location = TableLocation::local(reader_tmp.path());
        let mut reader_meta = make_basic_table_meta();
        reader_meta
            .required_reader_features
            .insert("future_reader".to_string());
        TransactionLogStore::new(reader_location.clone())
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(reader_meta)])
            .await?;

        assert!(matches!(
            TimeSeriesTable::open(reader_location)
                .await
                .expect_err("unknown reader feature must reject open"),
            TableError::Open {
                source: OpenTableError::Commit {
                    source: CommitError::Protocol {
                        source: TableProtocolError::UnsupportedReaderFeatures { features },
                        ..
                    }
                }
            } if features == ["future_reader"]
        ));
        Ok(())
    }
}
