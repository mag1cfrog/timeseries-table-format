//! High-level time-series table abstraction.
//!
//! This module is the canonical home for the user-facing [`TimeSeriesTable`]
//! API.
//!
//! In v0.1 this is intentionally read-heavy and write-light:
//! - `open` reconstructs state from the transaction log,
//! - `create` bootstraps a fresh table with an initial metadata commit,
//! - append APIs handle schema enforcement, coverage sidecars, and OCC,
//! - range scans stream filtered record batches.

pub mod append;
mod append_schema;
pub mod coverage;
pub mod error;
mod optimize;
pub mod scan;

#[cfg(test)]
pub(crate) mod test_util;

#[cfg(test)]
mod latest_snapshot_tests;

use std::pin::Pin;

use arrow::array::RecordBatch;
use futures::Stream;
use snafu::prelude::*;

use crate::table::error::{
    AlreadyExistsSnafu, EmptyTableSnafu, IndexSpecSnafu, NotTimeSeriesSnafu,
    SchemaCompatibilitySnafu, TransactionLogSnafu, UnsupportedFormatVersionSnafu,
};

use crate::{
    metadata::{
        schema_compat::ensure_index_spec_matches_schema, table_metadata::TABLE_FORMAT_VERSION,
    },
    storage::TableLocation,
    transaction_log::{
        IndexSpec, LogAction, TableKind, TableMeta, TableState, TransactionLogStore,
    },
};

pub use coverage::CoverageQueryError;
pub use error::{AppendError, OptimizeError, TableError};
pub use optimize::OptimizeReport;
pub use scan::ScanError;

/// Stream of Arrow RecordBatch values from a time-series scan.
///
/// Batch and row order is unspecified.
pub type TimeSeriesScan = Pin<Box<dyn Stream<Item = Result<RecordBatch, TableError>> + Send>>;

/// High-level time-series table handle.
///
/// This is the main entry point for callers. It bundles:
/// - where the table is,
/// - how to talk to the transaction log,
/// - what the current committed state is,
/// - and the extracted time index spec.
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
    ///
    /// This exists to support internal helpers (for example, tests) without
    /// exposing mutation to library callers.
    #[allow(dead_code)]
    pub(crate) fn state_mut(&mut self) -> &mut TableState {
        &mut self.state
    }

    /// Return the time index specification for this table.
    pub fn index_spec(&self) -> &IndexSpec {
        &self.index
    }

    /// Return the table location.
    pub fn location(&self) -> &TableLocation {
        self.log.location()
    }

    /// Open an existing time-series table at the given location.
    ///
    /// Steps:
    /// - Build a `TransactionLogStore` for the location.
    /// - Rebuild `TableState` from the transaction log.
    /// - Reject empty tables (version == 0).
    /// - Require `TableKind::TimeSeries` and extract `IndexSpec`.
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
        let result: Result<Self, TableError> = async {
            let log = TransactionLogStore::new(location.clone());

            // Early return for tables with no commits so we surface TableError::EmptyTable
            // instead of a lower-level corrupt state error.
            let current_version = log
                .load_current_version()
                .await
                .context(TransactionLogSnafu)?;
            tracing::Span::current().record("table_version", current_version);

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
        result
    }

    /// Create a new time-series table at the given location.
    ///
    /// This:
    /// - Requires `table_meta.format_version` to match [`TABLE_FORMAT_VERSION`],
    /// - Requires `table_meta.kind` to be `TableKind::TimeSeries`,
    /// - Verifies that there are no existing commits (version must be 0),
    /// - Writes an initial commit with `UpdateTableMeta(table_meta.clone())`,
    /// - Returns a `TimeSeriesTable` with a fresh `TableState`.
    #[tracing::instrument(
        name = "table.create",
        level = "debug",
        skip_all,
        fields(
            starting_version = tracing::field::Empty,
            committed_version = tracing::field::Empty,
            index_kind = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    pub async fn create(
        location: TableLocation,
        table_meta: TableMeta,
    ) -> Result<Self, TableError> {
        let result: Result<Self, TableError> = async {
            if table_meta.format_version() != TABLE_FORMAT_VERSION {
                return UnsupportedFormatVersionSnafu {
                    expected: TABLE_FORMAT_VERSION,
                    found: table_meta.format_version(),
                }
                .fail();
            }

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
            index.validate().context(IndexSpecSnafu)?;
            if let Some(schema) = &table_meta.logical_schema {
                ensure_index_spec_matches_schema(schema, &index)
                    .context(SchemaCompatibilitySnafu)?;
            }
            tracing::Span::current().record("index_kind", index.kind.name());

            let log = TransactionLogStore::new(location.clone());

            // 2) Check that there are no existing commits. This keeps `create`
            // from silently appending to a pre-existing table.
            let current_version = log
                .load_current_version()
                .await
                .context(TransactionLogSnafu)?;
            tracing::Span::current().record("starting_version", current_version);

            if current_version != 0 {
                return AlreadyExistsSnafu { current_version }.fail();
            }

            // 3) Write the initial metadata commit at version 1.
            let actions = vec![LogAction::UpdateTableMeta(table_meta.clone())];

            let new_version = log
                .commit_with_expected_version(0, actions)
                .await
                .context(TransactionLogSnafu)?;
            tracing::Span::current().record("committed_version", new_version);

            debug_assert_eq!(new_version, 1);

            // 4) Rebuild state from the log so that `state` is guaranteed to be
            // consistent with what is on disk.
            let state = log
                .rebuild_table_state()
                .await
                .context(TransactionLogSnafu)?;
            let table = Self { log, state, index };
            tracing::info!(
                name: "table.create",
                starting_version = current_version,
                committed_version = new_version,
                index_kind = table.index.kind.name(),
                outcome = "succeeded",
                "Created time-series table"
            );
            Ok(table)
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
        result
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
        let result: Result<bool, TableError> = async {
            let current = self
                .log
                .load_current_version()
                .await
                .context(TransactionLogSnafu)?;
            tracing::Span::current().record("observed_version", current);

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
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::storage::{StorageLocation, layout};
    use crate::table::test_util::*;
    use crate::transaction_log::{
        CommitError, IndexKind, TimeIndexGranularity, TransactionLogStore,
    };

    use tempfile::TempDir;

    fn captured_span(capture: &TraceCapture, name: &str) -> CapturedSpan {
        let mut spans: Vec<_> = capture
            .spans()
            .into_iter()
            .filter(|span| span.name == name)
            .collect();
        assert_eq!(spans.len(), 1, "expected one {name} span");
        spans.pop().expect("captured span")
    }

    fn assert_debug_span(
        capture: &TraceCapture,
        name: &str,
        expected_fields: &[(&str, Option<&str>)],
    ) {
        let span = captured_span(capture, name);
        assert_eq!(span.level, tracing::Level::DEBUG);
        for (field, expected) in expected_fields {
            assert_eq!(
                span.fields.get(*field).map(String::as_str),
                *expected,
                "unexpected {name}.{field}"
            );
        }
    }

    fn assert_no_event(capture: &TraceCapture, name: &str) {
        assert!(!capture.events().iter().any(|event| event.name == name));
    }

    fn assert_capture_excludes(capture: &TraceCapture, forbidden: &[&str]) {
        let values = capture
            .spans()
            .into_iter()
            .flat_map(|span| span.fields.into_values())
            .chain(
                capture
                    .events()
                    .into_iter()
                    .flat_map(|event| event.fields.into_values()),
            );
        for value in values {
            for forbidden in forbidden
                .iter()
                .copied()
                .chain(["LogicalSchema", "RecordBatch"])
            {
                assert!(
                    !value.contains(forbidden),
                    "diagnostic value contains sensitive data '{forbidden}': {value}"
                );
            }
        }
    }

    #[tokio::test]
    async fn create_initializes_log_and_state() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let meta = make_basic_table_meta();
        let capture = TraceCapture::default();
        let table = capture
            .run(TimeSeriesTable::create(location.clone(), meta))
            .await?;

        assert_debug_span(
            &capture,
            "table.create",
            &[
                ("starting_version", Some("0")),
                ("committed_version", Some("1")),
                ("index_kind", Some("timestamp")),
                ("outcome", Some("succeeded")),
            ],
        );
        let events: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| event.name == "table.create")
            .collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, tracing::Level::INFO);
        for (field, expected) in [
            ("starting_version", "0"),
            ("committed_version", "1"),
            ("index_kind", "timestamp"),
            ("outcome", "succeeded"),
        ] {
            assert_eq!(
                events[0].fields.get(field).map(String::as_str),
                Some(expected),
                "unexpected table.create event field {field}"
            );
        }
        assert!(
            events[0]
                .fields
                .get("message")
                .is_some_and(|message| message.contains("Created time-series table"))
        );
        assert_capture_excludes(&capture, &[&tmp.path().display().to_string()]);

        // State should be at version 1 with no segments.
        assert_eq!(table.state().version, 1);
        assert_eq!(TABLE_FORMAT_VERSION, 7);
        assert_eq!(
            table.state().table_meta.format_version(),
            TABLE_FORMAT_VERSION
        );
        assert!(table.state().segments.is_empty());

        // Verify that the log layout exists on disk.
        let root = match table.location().storage() {
            StorageLocation::Local(p) => p.clone(),
        };

        let log_dir = root.join(layout::log_rel_dir());
        assert!(log_dir.is_dir());

        let current_path = root.join(layout::current_rel_path());
        let current_contents = tokio::fs::read_to_string(&current_path).await?;
        assert_eq!(current_contents.trim(), "1");

        Ok(())
    }

    #[tokio::test]
    async fn create_rejects_unsupported_format_without_writing_log() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        for found in [TABLE_FORMAT_VERSION - 1, TABLE_FORMAT_VERSION + 1] {
            let mut meta = make_basic_table_meta();
            meta.format_version = found;

            let err = TimeSeriesTable::create(location.clone(), meta)
                .await
                .expect_err("unsupported format version should be rejected");
            assert!(matches!(
                err,
                TableError::UnsupportedFormatVersion {
                    expected: TABLE_FORMAT_VERSION,
                    found: actual,
                } if actual == found
            ));
            assert!(!tmp.path().join(layout::log_rel_dir()).exists());
        }

        Ok(())
    }

    #[tokio::test]
    async fn open_round_trip_after_create() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let meta = make_basic_table_meta();
        let created = TimeSeriesTable::create(location.clone(), meta).await?;

        let capture = TraceCapture::default();
        let reopened = capture.run(TimeSeriesTable::open(location.clone())).await?;

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
    async fn open_rejects_every_non_current_format_with_typed_error() -> TestResult {
        for found in [TABLE_FORMAT_VERSION - 1, TABLE_FORMAT_VERSION + 1] {
            let tmp = TempDir::new()?;
            let location = TableLocation::local(tmp.path());
            let log = TransactionLogStore::new(location.clone());
            let mut meta = make_basic_table_meta();
            meta.format_version = found;
            log.commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
                .await?;

            let error = TimeSeriesTable::open(location)
                .await
                .expect_err("non-current table format must fail");

            assert!(matches!(
                error,
                TableError::TransactionLog {
                    source: CommitError::UnsupportedFormatVersion {
                        expected: TABLE_FORMAT_VERSION,
                        found: actual,
                    },
                } if actual == u64::from(found)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn open_empty_root_errors() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        // There is no CURRENT and no commits, so opening should fail.
        let capture = TraceCapture::default();
        let result = capture.run(TimeSeriesTable::open(location)).await;
        assert!(matches!(result, Err(TableError::EmptyTable)));
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

        let capture = TraceCapture::default();
        let refreshed = capture.run(table.refresh()).await?;
        assert!(!refreshed);
        assert_eq!(table.state().version, 1);
        assert_debug_span(
            &capture,
            "table.refresh",
            &[
                ("previous_version", Some("1")),
                ("observed_version", Some("1")),
                ("refreshed", Some("false")),
                ("new_version", None),
                ("outcome", Some("no_change")),
            ],
        );
        assert_no_event(&capture, "table.refresh");
        assert_capture_excludes(&capture, &[&tmp.path().display().to_string()]);
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
            spec.kind = IndexKind::Timestamp {
                index_granularity: TimeIndexGranularity::Minutes(5),
                timezone: None,
            };
        }

        let log = TransactionLogStore::new(location.clone());
        let new_version = log
            .commit_with_expected_version(1, vec![LogAction::UpdateTableMeta(updated_meta.clone())])
            .await?;
        assert_eq!(new_version, 2);

        let capture = TraceCapture::default();
        let refreshed = capture.run(table.refresh()).await?;
        assert!(refreshed);
        assert_eq!(table.state().version, 2);

        assert_debug_span(
            &capture,
            "table.refresh",
            &[
                ("previous_version", Some("1")),
                ("observed_version", Some("2")),
                ("refreshed", Some("true")),
                ("new_version", Some("2")),
                ("outcome", Some("succeeded")),
            ],
        );
        assert_no_event(&capture, "table.refresh");
        assert_capture_excludes(&capture, &[&tmp.path().display().to_string()]);

        match &table.state().table_meta.kind {
            TableKind::TimeSeries(spec) => assert_eq!(
                spec.kind,
                IndexKind::Timestamp {
                    index_granularity: TimeIndexGranularity::Minutes(5),
                    timezone: None
                }
            ),
            other => panic!("expected time series table kind, got {other:?}"),
        }
        assert_eq!(
            table.index_spec().kind,
            IndexKind::Timestamp {
                index_granularity: TimeIndexGranularity::Minutes(5),
                timezone: None
            }
        );
        Ok(())
    }
}
