//! Creating a time-series table.

use snafu::{Backtrace, Snafu};

use crate::{
    metadata::{
        schema_compat::{SchemaCompatibilityError, ensure_index_spec_matches_schema},
        table_metadata::{IndexSpecError, TABLE_FORMAT_VERSION},
    },
    storage::{StorageError, TableLocation},
    table::{TableError, TimeSeriesTable},
    transaction_log::{CommitError, LogAction, TableKind, TableMeta, TransactionLogStore},
};

/// Errors owned by a table creation operation.
#[derive(Debug, Snafu)]
#[snafu(module, visibility(pub(crate)))]
pub enum CreateTableError {
    /// The caller requested an unsupported table format version.
    #[snafu(display("Unsupported table format version: expected {expected}, found {found}"))]
    UnsupportedFormatVersion {
        /// Format version supported by this writer.
        expected: u32,
        /// Format version supplied by the caller.
        found: u32,
    },

    /// The supplied metadata does not describe a time-series table.
    #[snafu(display("Cannot create a time-series table from table kind {kind:?}"))]
    NotTimeSeries {
        /// Rejected table kind.
        kind: TableKind,
    },

    /// The supplied ordered-index specification is invalid.
    #[snafu(
        context(false),
        display("Invalid ordered-index specification: {source}")
    )]
    IndexSpecValidation {
        /// Complete ordered-index validation failure.
        #[snafu(source)]
        source: IndexSpecError,
        /// Backtrace captured because index specification errors do not own one.
        backtrace: Backtrace,
    },

    /// The supplied schema is incompatible with its ordered-index specification.
    #[snafu(context(false), display("Table schema validation failed: {source}"))]
    SchemaValidation {
        /// Complete schema compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)))]
        source: Box<SchemaCompatibilityError>,
        /// Backtrace captured because schema compatibility errors do not own one.
        backtrace: Backtrace,
    },

    /// The target already contains a committed table.
    #[snafu(display("Table already exists at transaction log version {current_version}"))]
    AlreadyExists {
        /// Existing transaction log version.
        current_version: u64,
    },

    /// Resolving or accessing the requested table location failed.
    #[snafu(context(false), display("Table storage error: {source}"))]
    Storage {
        /// Complete storage failure.
        #[snafu(source, backtrace)]
        source: StorageError,
    },

    /// Accessing or publishing the transaction log failed.
    #[snafu(context(false), display("Table creation commit error: {source}"))]
    Commit {
        /// Complete transaction-log failure.
        #[snafu(source, backtrace)]
        source: CommitError,
    },
}

impl TimeSeriesTable {
    /// Create a new time-series table at the given location.
    ///
    /// This validates the requested metadata, verifies that the target has no
    /// commits, publishes the initial metadata commit, and rebuilds the state
    /// returned to the caller.
    #[tracing::instrument(
        name = "table.create",
        target = "timeseries_table_format::table",
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
        let result: Result<Self, CreateTableError> = async {
            if table_meta.format_version() != TABLE_FORMAT_VERSION {
                return Err(CreateTableError::UnsupportedFormatVersion {
                    expected: TABLE_FORMAT_VERSION,
                    found: table_meta.format_version(),
                });
            }

            let index = match &table_meta.kind {
                TableKind::TimeSeries(index) => index.clone(),
                kind => {
                    return Err(CreateTableError::NotTimeSeries { kind: kind.clone() });
                }
            };
            index
                .validate()
                .map_err(|source| CreateTableError::IndexSpecValidation {
                    source,
                    backtrace: Backtrace::capture(),
                })?;
            if let Some(schema) = &table_meta.logical_schema {
                ensure_index_spec_matches_schema(schema, &index).map_err(CreateTableError::from)?;
            }
            tracing::Span::current().record("index_kind", index.kind.name());

            let log = TransactionLogStore::new(location);
            let current_version = log
                .load_current_version()
                .await
                .map_err(CreateTableError::from)?;
            tracing::Span::current().record("starting_version", current_version);
            if current_version != 0 {
                return Err(CreateTableError::AlreadyExists { current_version });
            }

            let new_version = log
                .commit_with_expected_version(
                    0,
                    vec![LogAction::UpdateTableMeta(table_meta.clone())],
                )
                .await
                .map_err(CreateTableError::from)?;
            tracing::Span::current().record("committed_version", new_version);
            debug_assert_eq!(new_version, 1);

            let state = log
                .rebuild_table_state()
                .await
                .map_err(CreateTableError::from)?;
            let table = Self { log, state, index };
            tracing::info!(
                name: "table.create",
                target: "timeseries_table_format::table",
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
        result.map_err(TableError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        storage::{StorageLocation, layout},
        table::test_util::{
            TestResult, TraceCapture, assert_capture_excludes, assert_debug_span, captured_span,
            make_basic_table_meta,
        },
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn create_initializes_log_and_state() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let capture = TraceCapture::default();
        let table = capture
            .run(TimeSeriesTable::create(location, make_basic_table_meta()))
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
        assert_eq!(
            captured_span(&capture, "table.create").target,
            "timeseries_table_format::table"
        );
        let events: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| event.name == "table.create")
            .collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].target, "timeseries_table_format::table");
        assert_eq!(events[0].level, tracing::Level::INFO);
        for (field, expected) in [
            ("starting_version", "0"),
            ("committed_version", "1"),
            ("index_kind", "timestamp"),
            ("outcome", "succeeded"),
        ] {
            assert_eq!(
                events[0].fields.get(field).map(String::as_str),
                Some(expected)
            );
        }
        assert!(
            events[0]
                .fields
                .get("message")
                .is_some_and(|message| message.contains("Created time-series table"))
        );
        assert_capture_excludes(&capture, &[&tmp.path().display().to_string()]);

        assert_eq!(table.state().version, 1);
        assert_eq!(
            table.state().table_meta.format_version(),
            TABLE_FORMAT_VERSION
        );
        assert!(table.state().segments.is_empty());
        let StorageLocation::Local(root) = table.location().storage();
        assert!(root.join(layout::log_rel_dir()).is_dir());
        assert_eq!(
            tokio::fs::read_to_string(root.join(layout::current_rel_path()))
                .await?
                .trim(),
            "1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_rejects_invalid_metadata_without_writing_log() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        for found in [TABLE_FORMAT_VERSION - 1, TABLE_FORMAT_VERSION + 1] {
            let mut meta = make_basic_table_meta();
            meta.format_version = found;
            let error = TimeSeriesTable::create(location.clone(), meta)
                .await
                .expect_err("unsupported format version must fail");
            assert!(matches!(
                error,
                TableError::Create {
                    source: CreateTableError::UnsupportedFormatVersion {
                        expected: TABLE_FORMAT_VERSION,
                        found: actual,
                    }
                } if actual == found
            ));
        }

        let mut invalid_index = make_basic_table_meta();
        let TableKind::TimeSeries(index) = &mut invalid_index.kind else {
            unreachable!("test metadata is time-series");
        };
        index.entity_columns = vec![index.column.clone()];
        assert!(matches!(
            TimeSeriesTable::create(location.clone(), invalid_index)
                .await
                .expect_err("invalid ordered index must fail"),
            TableError::Create {
                source: CreateTableError::IndexSpecValidation {
                    source: IndexSpecError::EntityColumnMatchesIndex { .. },
                    ..
                }
            }
        ));

        let mut invalid_schema = make_basic_table_meta();
        let TableKind::TimeSeries(index) = &mut invalid_schema.kind else {
            unreachable!("test metadata is time-series");
        };
        index.entity_columns = vec!["price".to_string()];
        assert!(matches!(
            TimeSeriesTable::create(location.clone(), invalid_schema)
                .await
                .expect_err("unsupported entity type must fail"),
            TableError::Create {
                source: CreateTableError::SchemaValidation { source, .. }
            } if matches!(
                *source,
                SchemaCompatibilityError::UnsupportedEntityColumnType { .. }
            )
        ));

        let mut generic = make_basic_table_meta();
        generic.kind = TableKind::Generic;
        assert!(matches!(
            TimeSeriesTable::create(location, generic)
                .await
                .expect_err("generic metadata must fail"),
            TableError::Create {
                source: CreateTableError::NotTimeSeries {
                    kind: TableKind::Generic
                }
            }
        ));
        assert!(!tmp.path().join(layout::log_rel_dir()).exists());
        Ok(())
    }

    #[tokio::test]
    async fn create_rejects_an_existing_table() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let meta = make_basic_table_meta();
        TimeSeriesTable::create(location.clone(), meta.clone()).await?;

        assert!(matches!(
            TimeSeriesTable::create(location, meta)
                .await
                .expect_err("existing table must fail"),
            TableError::Create {
                source: CreateTableError::AlreadyExists { current_version: 1 }
            }
        ));
        Ok(())
    }
}
