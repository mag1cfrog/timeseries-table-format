//! Async helpers for persisting and reading the metadata log.
//!
//! This module owns all on-disk interactions with `_timeseries_log/`:
//! - Tracking the `CURRENT` pointer and interpreting the "no file" case as
//!   version `0` (fresh table).
//! - Writing zero-padded commit files with optimistic concurrency control so
//!   each version is created exactly once.
//! - Mapping failures into [`CommitError`] variants so callers retain typed
//!   storage, protocol, and state-validation causes.
//!
//! All operations delegate to the async storage backend and remain focused on
//! durability, leaving higher-level planning (which actions to commit) to the
//! caller.
use crate::metadata::table_metadata::TABLE_PROTOCOL_VERSION;
use crate::storage::{self, StorageError, TableLocation};
use crate::transaction_log::actions::{Commit, LogAction};
use crate::transaction_log::*;
use chrono::Utc;
use snafu::Backtrace;
use std::path::{Path, PathBuf};

fn is_unknown_action(action: &serde_json::Value) -> bool {
    let name = match action {
        serde_json::Value::Object(fields) if fields.len() == 1 => fields.keys().next(),
        serde_json::Value::String(name) => Some(name),
        _ => None,
    };

    name.is_some_and(|name| {
        !matches!(
            name.as_str(),
            "AddSegment" | "RemoveSegment" | "UpdateTableMeta" | "UpdateTableCoverage"
        )
    })
}

/// Helper for reading and writing the commit log under a table root.
///
/// Layout:
///   `<root>/_timeseries_log/0000000001.json`
///   `<root>/_timeseries_log/0000000002.json`
///   `<root>/_timeseries_log/CURRENT`
#[derive(Debug, Clone)]
pub struct TransactionLogStore {
    location: TableLocation,
}

impl TransactionLogStore {
    /// Name of the subdirectory containing the commit log.
    pub const LOG_DIR_NAME: &str = storage::layout::LOG_DIR_NAME;
    /// Name of the file that stores the current version pointer.
    pub const CURRENT_FILE_NAME: &str = storage::layout::CURRENT_FILE_NAME;
    /// Number of digits used in zero-padded commit file names.
    pub const COMMIT_FILENAME_DIGITS: usize = storage::layout::COMMIT_FILENAME_DIGITS;

    /// Create a new TransactionLogStore rooted at a table directory.
    pub fn new(location: TableLocation) -> Self {
        Self { location }
    }

    /// Get the TableLocation of the LogStore.
    pub fn location(&self) -> &TableLocation {
        &self.location
    }

    fn commit_rel_path(version: u64) -> PathBuf {
        storage::layout::commit_rel_path(version)
    }

    /// Helper: read a log-relative file and map storage errors into CommitError.
    async fn read_to_string_rel(&self, rel: &Path) -> Result<String, CommitError> {
        match storage::read_to_string(self.location.as_ref(), rel).await {
            Ok(s) => Ok(s),
            Err(source) => Err(CommitError::Storage { source }),
        }
    }

    async fn rollback_unpublished_commit(
        &self,
        commit_rel: &Path,
        publish_error: StorageError,
    ) -> CommitError {
        match storage::remove_file(self.location.as_ref(), commit_rel).await {
            Ok(()) => CommitError::Storage {
                source: publish_error,
            },
            Err(cleanup_error) => CommitError::AmbiguousOutcome {
                commit_path: commit_rel.display().to_string(),
                operation_error: Box::new(publish_error),
                cleanup_error: Box::new(cleanup_error),
            },
        }
    }

    /// Load a single commit by version.
    ///
    /// - On storage-layer failures, returns `CommitError::Storage`.
    /// - On JSON parse failures, returns [`CommitError::CommitDeserialization`].
    pub async fn load_commit(&self, version: u64) -> Result<Commit, CommitError> {
        let rel = Self::commit_rel_path(version);
        let json = self.read_to_string_rel(&rel).await?;

        let mut value: serde_json::Value =
            serde_json::from_str(&json).map_err(|source| CommitError::CommitDeserialization {
                version,
                source,
                backtrace: Backtrace::capture(),
            })?;

        for metadata in value
            .get("actions")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|action| {
                let fields = action.as_object()?;
                if fields.len() == 1 {
                    fields.get("UpdateTableMeta")
                } else {
                    None
                }
            })
        {
            if let Some(found) = metadata
                .get("protocol_version")
                .and_then(serde_json::Value::as_u64)
                .filter(|&found| found != u64::from(TABLE_PROTOCOL_VERSION))
            {
                return Err(CommitError::from(TableProtocolError::UnsupportedVersion {
                    expected: TABLE_PROTOCOL_VERSION,
                    found,
                }));
            }

            serde_json::from_value::<TableMeta>(metadata.clone())
                .map_err(|source| CommitError::CommitDeserialization {
                    version,
                    source,
                    backtrace: Backtrace::capture(),
                })?
                .ensure_read_compatible()
                .map_err(CommitError::from)?;
        }

        if let Some(actions) = value
            .get_mut("actions")
            .and_then(serde_json::Value::as_array_mut)
        {
            actions.retain(|action| !is_unknown_action(action));
        }

        let commit =
            serde_json::from_value(value).map_err(|source| CommitError::CommitDeserialization {
                version,
                source,
                backtrace: Backtrace::capture(),
            })?;

        Ok(commit)
    }

    /// Load the CURRENT version pointer.
    ///
    /// Behavior:
    /// - If CURRENT does not exist, treat as a fresh table and return 0.
    /// - If CURRENT contains invalid or empty content, return a typed pointer error.
    pub async fn load_current_version(&self) -> Result<u64, CommitError> {
        let rel = storage::layout::current_rel_path();

        let contents = match storage::read_to_string(self.location.as_ref(), &rel).await {
            Ok(s) => s,
            Err(StorageError::NotFound { .. }) => return Ok(0),
            Err(source) => return Err(CommitError::Storage { source }),
        };

        let trimmed = contents.trim();
        if trimmed.is_empty() {
            return Err(CommitError::EmptyCurrentPointer {
                path: rel.display().to_string(),
                backtrace: Backtrace::capture(),
            });
        }
        let version =
            trimmed
                .parse::<u64>()
                .map_err(|source| CommitError::CurrentVersionParse {
                    contents: trimmed.to_string(),
                    source,
                    backtrace: Backtrace::capture(),
                })?;

        Ok(version)
    }

    /// Commit a new version with an optimistic concurrency guard.
    ///
    /// ## Concurrency semantics
    ///
    /// - The check on CURRENT is advisory/best-effort and subject to races.
    ///   Two writers may both read the same CURRENT value and attempt to commit
    ///   the same next version. The actual concurrency guard is the atomic
    ///   creation of the commit file using "create only if not exists" semantics.
    /// - If another writer wins the race and creates the commit file first,
    ///   this operation will fail with `StorageError::AlreadyExists`.
    /// - Callers must be prepared to handle `StorageError::AlreadyExists` and
    ///   implement retry logic (e.g., reload CURRENT and retry the commit).
    ///
    /// If updating CURRENT fails, this method removes the commit file created
    /// by this invocation. A cleanup failure returns
    /// [`CommitError::AmbiguousOutcome`] so callers do not assume rollback.
    ///
    /// ## Steps
    ///
    /// 1. Load CURRENT (advisory check).
    /// 2. If CURRENT != expected, return `CommitError::Conflict`.
    /// 3. Compute version = expected + 1 (with overflow check).
    /// 4. Build a `Commit` struct.
    /// 5. Serialize to JSON.
    /// 6. Create commit file `_timeseries_log/<zero-padded>.json` using
    ///    "create only if not exists" semantics (atomic guard).
    /// 7. Update `_timeseries_log/CURRENT` with the new version (e.g. `"1\n"`).
    pub(crate) async fn commit_with_expected_version(
        &self,
        expected: u64,
        actions: Vec<LogAction>,
    ) -> Result<u64, CommitError> {
        self.commit_inner(expected, actions, || {}).await
    }

    /// Commit while preserving newly created paths that may be referenced.
    ///
    /// `preserve_referenced_paths` runs before post-outcome tracing when the
    /// commit succeeds or becomes ambiguous.
    pub(crate) async fn commit_with_path_preservation<F>(
        &self,
        expected: u64,
        actions: Vec<LogAction>,
        preserve_referenced_paths: F,
    ) -> Result<u64, CommitError>
    where
        F: FnOnce(),
    {
        self.commit_inner(expected, actions, preserve_referenced_paths)
            .await
    }

    #[tracing::instrument(
        name = "transaction.commit",
        level = "debug",
        skip_all,
        fields(
            expected_version = expected,
            observed_version = tracing::field::Empty,
            proposed_version = tracing::field::Empty,
            committed_version = tracing::field::Empty,
            action_count = actions.len(),
            failure_stage = tracing::field::Empty,
            rollback_outcome = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    async fn commit_inner<F>(
        &self,
        expected: u64,
        actions: Vec<LogAction>,
        on_commit_may_exist: F,
    ) -> Result<u64, CommitError>
    where
        F: FnOnce(),
    {
        let span = tracing::Span::current();

        // 1) Guard on CURRENT
        let current = match self.load_current_version().await {
            Ok(current) => current,
            Err(error) => {
                span.record("failure_stage", "current_read");
                span.record("outcome", "failed");
                return Err(error);
            }
        };
        span.record("observed_version", current);
        if current != expected {
            span.record("failure_stage", "advisory_check");
            span.record("outcome", "conflict");
            return ConflictSnafu {
                expected,
                found: current,
            }
            .fail();
        }

        // 2) Compute next version with overflow guard
        let version = match checked_next_version(expected) {
            Ok(version) => version,
            Err(error) => {
                span.record("failure_stage", "version_calculation");
                span.record("outcome", "failed");
                return Err(error);
            }
        };
        span.record("proposed_version", version);

        // 3) Build commit payload
        let commit = Commit {
            version,
            base_version: expected,
            timestamp: Utc::now(),
            actions,
        };

        let json = match serde_json::to_vec(&commit) {
            Ok(json) => json,
            Err(error) => {
                span.record("failure_stage", "serialization");
                span.record("outcome", "failed");
                return Err(CommitError::CommitSerialization {
                    version,
                    source: error,
                    backtrace: Backtrace::capture(),
                });
            }
        };

        // 4) Attempt to create the commit file *only if it does not already exist*.
        //    If the file already exists (AlreadyExists error), we propagate it as-is
        //    rather than converting to Conflict. This allows higher-level code to
        //    implement automatic conflict resolution (e.g., retrying with rebased
        //    changes if the operations don't actually conflict, like Delta Lake).
        let commit_rel = Self::commit_rel_path(version);
        let mut commit_guard =
            storage::FileCleanupGuard::new_disarmed(self.location.as_ref(), &commit_rel)
                .map_err(|source| CommitError::Storage { source })?;
        match storage::write_new(self.location.as_ref(), &commit_rel, &json).await {
            Ok(()) => commit_guard.arm(),
            Err(StorageError::CleanupFailed {
                operation_error,
                cleanup_error,
                ..
            }) => {
                on_commit_may_exist();
                span.record("failure_stage", "atomic_create");
                span.record("outcome", "ambiguous");
                return Err(CommitError::AmbiguousOutcome {
                    commit_path: commit_rel.display().to_string(),
                    operation_error,
                    cleanup_error,
                });
            }
            Err(source @ StorageError::AlreadyExists { .. }) => {
                span.record("failure_stage", "atomic_create");
                span.record("outcome", "conflict");
                return Err(CommitError::Storage { source });
            }
            Err(source) => {
                span.record("failure_stage", "atomic_create");
                span.record("outcome", "failed");
                return Err(CommitError::Storage { source });
            }
        }

        // 5) Update CURRENT via atomic write (temp + rename).
        let current_rel = storage::layout::current_rel_path();
        let current_contents = format!("{version}\n");
        if let Err(publish_error) = storage::write_atomic(
            self.location.as_ref(),
            &current_rel,
            current_contents.as_bytes(),
        )
        .await
        {
            let error = self
                .rollback_unpublished_commit(&commit_rel, publish_error)
                .await;
            commit_guard.disarm();
            let ambiguous = matches!(&error, CommitError::AmbiguousOutcome { .. });
            if ambiguous {
                on_commit_may_exist();
            }
            span.record("failure_stage", "current_publication");
            if ambiguous {
                span.record("rollback_outcome", "failed");
                span.record("outcome", "ambiguous");
            } else {
                span.record("rollback_outcome", "succeeded");
                span.record("outcome", "failed");
            }
            return Err(error);
        }

        commit_guard.disarm();
        on_commit_may_exist();

        span.record("committed_version", version);
        span.record("outcome", "succeeded");
        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;
    use crate::storage::layout;
    use crate::table::test_util::TraceCapture;
    use serde_json;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    // ==================== LogStore tests ====================

    fn create_test_log_store() -> (TempDir, TransactionLogStore) {
        let tmp = TempDir::new().expect("create temp dir");
        let location = TableLocation::local(tmp.path());
        let store = TransactionLogStore::new(location);
        (tmp, store)
    }

    fn assert_commit_span(capture: &TraceCapture, expected_fields: &[(&str, Option<&str>)]) {
        let spans: Vec<_> = capture
            .spans()
            .into_iter()
            .filter(|span| span.name == "transaction.commit")
            .collect();
        assert_eq!(spans.len(), 1, "expected one transaction.commit span");
        assert_eq!(spans[0].level, tracing::Level::DEBUG);
        for (field, expected) in expected_fields {
            assert_eq!(
                spans[0].fields.get(*field).map(String::as_str),
                *expected,
                "unexpected transaction.commit.{field}"
            );
        }
        assert!(
            !capture
                .events()
                .iter()
                .any(|event| event.name == "transaction.commit"),
            "transaction commits must not emit duplicate events"
        );
    }

    #[tokio::test]
    async fn load_current_version_returns_zero_when_no_current_file() -> TestResult {
        let (_tmp, store) = create_test_log_store();

        let version = store.load_current_version().await?;

        assert_eq!(version, 0);
        Ok(())
    }

    #[tokio::test]
    async fn load_current_version_returns_version_from_file() -> TestResult {
        let (tmp, store) = create_test_log_store();

        // Manually create CURRENT file with version 5.
        let log_dir = tmp.path().join(layout::log_rel_dir());
        tokio::fs::create_dir_all(&log_dir).await?;
        let current_path = tmp.path().join(layout::current_rel_path());
        tokio::fs::write(&current_path, "5\n").await?;

        let version = store.load_current_version().await?;

        assert_eq!(version, 5);
        Ok(())
    }

    #[tokio::test]
    async fn load_current_version_handles_whitespace() -> TestResult {
        let (tmp, store) = create_test_log_store();

        let log_dir = tmp.path().join(layout::log_rel_dir());
        tokio::fs::create_dir_all(&log_dir).await?;
        let current_path = tmp.path().join(layout::current_rel_path());
        tokio::fs::write(&current_path, "  42  \n").await?;

        let version = store.load_current_version().await?;

        assert_eq!(version, 42);
        Ok(())
    }

    #[tokio::test]
    async fn load_current_version_returns_empty_pointer_error() -> TestResult {
        let (tmp, store) = create_test_log_store();

        let log_dir = tmp.path().join(layout::log_rel_dir());
        tokio::fs::create_dir_all(&log_dir).await?;
        let current_path = tmp.path().join(layout::current_rel_path());
        tokio::fs::write(&current_path, "").await?;

        let result = store.load_current_version().await;

        assert!(result.is_err());
        let err = result.expect_err("expected EmptyCurrentPointer");
        assert!(matches!(err, CommitError::EmptyCurrentPointer { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn load_current_version_preserves_invalid_integer_source() -> TestResult {
        let (tmp, store) = create_test_log_store();

        let log_dir = tmp.path().join(layout::log_rel_dir());
        tokio::fs::create_dir_all(&log_dir).await?;
        let current_path = tmp.path().join(layout::current_rel_path());
        tokio::fs::write(&current_path, "not-a-number").await?;

        let result = store.load_current_version().await;

        assert!(result.is_err());
        let err = result.expect_err("expected CurrentVersionParse");
        assert!(matches!(err, CommitError::CurrentVersionParse { .. }));
        assert!(
            err.source()
                .is_some_and(|source| source.is::<std::num::ParseIntError>())
        );
        assert!(snafu::ErrorCompat::backtrace(&err).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn load_commit_preserves_json_source() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let commit_path = tmp.path().join(layout::commit_rel_path(1));
        tokio::fs::create_dir_all(commit_path.parent().expect("commit parent")).await?;
        tokio::fs::write(&commit_path, "{ invalid json").await?;

        let err = store
            .load_commit(1)
            .await
            .expect_err("invalid JSON must fail");

        assert!(matches!(err, CommitError::CommitDeserialization { .. }));
        assert!(
            err.source()
                .is_some_and(|source| source.is::<serde_json::Error>())
        );
        assert!(snafu::ErrorCompat::backtrace(&err).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn commit_current_read_failure_records_failure_stage() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let current_path = tmp.path().join(layout::current_rel_path());
        tokio::fs::create_dir_all(current_path.parent().expect("CURRENT parent")).await?;
        tokio::fs::write(current_path, "invalid").await?;
        let capture = TraceCapture::default();

        let err = capture
            .run(store.commit_with_expected_version(0, vec![]))
            .await
            .expect_err("invalid CURRENT should fail");

        assert!(matches!(err, CommitError::CurrentVersionParse { .. }));
        assert_commit_span(
            &capture,
            &[
                ("expected_version", Some("0")),
                ("observed_version", None),
                ("proposed_version", None),
                ("committed_version", None),
                ("action_count", Some("0")),
                ("failure_stage", Some("current_read")),
                ("rollback_outcome", None),
                ("outcome", Some("failed")),
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn commit_version_overflow_records_failure_stage() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let current_path = tmp.path().join(layout::current_rel_path());
        tokio::fs::create_dir_all(current_path.parent().expect("CURRENT parent")).await?;
        tokio::fs::write(current_path, format!("{}\n", u64::MAX)).await?;
        let capture = TraceCapture::default();

        let err = capture
            .run(store.commit_with_expected_version(u64::MAX, vec![]))
            .await
            .expect_err("version overflow should fail");

        assert!(matches!(err, CommitError::VersionOverflow { .. }));
        assert_commit_span(
            &capture,
            &[
                ("expected_version", Some("18446744073709551615")),
                ("observed_version", Some("18446744073709551615")),
                ("proposed_version", None),
                ("committed_version", None),
                ("action_count", Some("0")),
                ("failure_stage", Some("version_calculation")),
                ("rollback_outcome", None),
                ("outcome", Some("failed")),
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn commit_first_version_succeeds() -> TestResult {
        let (tmp, store) = create_test_log_store();

        let version = store.commit_with_expected_version(0, vec![]).await?;

        assert_eq!(version, 1);

        // Verify CURRENT was updated.
        let current_version = store.load_current_version().await?;
        assert_eq!(current_version, 1);

        // Verify commit file was created.
        let commit_path = tmp.path().join(layout::commit_rel_path(1));
        assert!(commit_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn commit_subsequent_versions_succeeds() -> TestResult {
        let (_tmp, store) = create_test_log_store();

        // Commit versions 1, 2, 3.
        let v1 = store.commit_with_expected_version(0, vec![]).await?;
        let v2 = store.commit_with_expected_version(1, vec![]).await?;
        let v3 = store.commit_with_expected_version(2, vec![]).await?;

        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
        assert_eq!(v3, 3);

        let current = store.load_current_version().await?;
        assert_eq!(current, 3);

        Ok(())
    }

    #[tokio::test]
    async fn commit_with_wrong_expected_version_returns_conflict() -> TestResult {
        let (_tmp, store) = create_test_log_store();

        // Commit version 1.
        store.commit_with_expected_version(0, vec![]).await?;

        // Try to commit with expected=0 again (stale).
        let capture = TraceCapture::default();
        let result = capture
            .run(store.commit_with_expected_version(0, vec![]))
            .await;

        assert!(result.is_err());
        let err = result.expect_err("expected Conflict");
        match err {
            CommitError::Conflict {
                expected, found, ..
            } => {
                assert_eq!(expected, 0);
                assert_eq!(found, 1);
            }
            _ => panic!("expected Conflict error, got {err:?}"),
        }
        assert_commit_span(
            &capture,
            &[
                ("expected_version", Some("0")),
                ("observed_version", Some("1")),
                ("proposed_version", None),
                ("committed_version", None),
                ("action_count", Some("0")),
                ("failure_stage", Some("advisory_check")),
                ("rollback_outcome", None),
                ("outcome", Some("conflict")),
            ],
        );

        Ok(())
    }

    #[tokio::test]
    async fn commit_creates_valid_json_file() -> TestResult {
        let (tmp, store) = create_test_log_store();

        let action = LogAction::RemoveSegment {
            path: "data/test-seg.parquet".to_string(),
        };

        let capture = TraceCapture::default();
        capture
            .run(store.commit_with_expected_version(0, vec![action]))
            .await?;

        assert_commit_span(
            &capture,
            &[
                ("expected_version", Some("0")),
                ("observed_version", Some("0")),
                ("proposed_version", Some("1")),
                ("committed_version", Some("1")),
                ("action_count", Some("1")),
                ("failure_stage", None),
                ("rollback_outcome", None),
                ("outcome", Some("succeeded")),
            ],
        );
        for value in capture
            .spans()
            .into_iter()
            .flat_map(|span| span.fields.into_values())
        {
            assert!(!value.contains("data/test-seg.parquet"));
            assert!(!value.contains(&tmp.path().display().to_string()));
        }

        // Read and parse the commit file.
        let commit_path = tmp.path().join(layout::commit_rel_path(1));
        let contents = tokio::fs::read_to_string(&commit_path).await?;
        let commit: Commit = serde_json::from_str(&contents)?;

        assert_eq!(commit.version, 1);
        assert_eq!(commit.base_version, 0);
        assert_eq!(commit.actions.len(), 1);
        assert!(matches!(
            &commit.actions[0],
            LogAction::RemoveSegment { path } if path == "data/test-seg.parquet"
        ));

        Ok(())
    }

    #[tokio::test]
    async fn commit_current_file_contains_version_with_newline() -> TestResult {
        let (tmp, store) = create_test_log_store();

        store.commit_with_expected_version(0, vec![]).await?;

        let current_path = tmp.path().join(layout::current_rel_path());
        let contents = tokio::fs::read_to_string(&current_path).await?;

        assert_eq!(contents, "1\n");

        Ok(())
    }

    #[tokio::test]
    async fn commit_returns_already_exists_when_commit_file_already_exists() -> TestResult {
        // Simulates a race condition where another writer created the commit file first.
        // We expect AlreadyExists (not Conflict) so higher-level code can implement
        // automatic conflict resolution (retry with rebased changes if non-conflicting).
        let (tmp, store) = create_test_log_store();

        // Manually create the commit file that version 1 would use
        let log_dir = tmp.path().join(layout::log_rel_dir());
        tokio::fs::create_dir_all(&log_dir).await?;
        let commit_file = tmp.path().join(layout::commit_rel_path(1));
        tokio::fs::write(&commit_file, b"{}").await?;

        // Now try to commit at version 1 - should fail with Storage(AlreadyExists)
        let capture = TraceCapture::default();
        let result = capture
            .run(store.commit_with_expected_version(0, vec![]))
            .await;

        assert!(
            matches!(
                result,
                Err(CommitError::Storage {
                    source: StorageError::AlreadyExists { .. }
                })
            ),
            "expected Storage(AlreadyExists) error, got: {result:?}",
        );
        assert_commit_span(
            &capture,
            &[
                ("observed_version", Some("0")),
                ("proposed_version", Some("1")),
                ("committed_version", None),
                ("failure_stage", Some("atomic_create")),
                ("rollback_outcome", None),
                ("outcome", Some("conflict")),
            ],
        );

        Ok(())
    }

    #[tokio::test]
    async fn commit_write_failure_records_atomic_create_failure() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let commit_path = tmp.path().join(layout::commit_rel_path(1));
        storage::inject_write_new_failure(commit_path.clone(), false);
        let capture = TraceCapture::default();

        let err = capture
            .run(store.commit_with_expected_version(0, vec![]))
            .await
            .expect_err("commit write should fail");

        assert!(matches!(err, CommitError::Storage { .. }));
        assert!(!commit_path.exists());
        assert_commit_span(
            &capture,
            &[
                ("observed_version", Some("0")),
                ("proposed_version", Some("1")),
                ("committed_version", None),
                ("failure_stage", Some("atomic_create")),
                ("rollback_outcome", None),
                ("outcome", Some("failed")),
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn current_update_failure_removes_owned_commit_file() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let current_tmp = tmp
            .path()
            .join(layout::current_rel_path().with_extension("tmp"));
        tokio::fs::create_dir_all(&current_tmp).await?;

        let capture = TraceCapture::default();
        let err = capture
            .run(store.commit_with_expected_version(0, vec![]))
            .await
            .expect_err("CURRENT update should fail");

        assert!(matches!(err, CommitError::Storage { .. }));
        assert!(!tmp.path().join(layout::commit_rel_path(1)).exists());
        assert_eq!(store.load_current_version().await?, 0);
        assert_commit_span(
            &capture,
            &[
                ("observed_version", Some("0")),
                ("proposed_version", Some("1")),
                ("committed_version", None),
                ("failure_stage", Some("current_publication")),
                ("rollback_outcome", Some("succeeded")),
                ("outcome", Some("failed")),
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn current_update_cleanup_failure_records_ambiguous_outcome() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let current_tmp = tmp
            .path()
            .join(layout::current_rel_path().with_extension("tmp"));
        tokio::fs::create_dir_all(&current_tmp).await?;
        let commit_path = tmp.path().join(layout::commit_rel_path(1));
        storage::inject_cleanup_failure(commit_path.clone());
        let capture = TraceCapture::default();

        let err = capture
            .run(store.commit_with_expected_version(0, vec![]))
            .await
            .expect_err("CURRENT update and rollback should fail");

        assert!(matches!(err, CommitError::AmbiguousOutcome { .. }));
        assert!(commit_path.exists());
        assert_eq!(store.load_current_version().await?, 0);
        assert_commit_span(
            &capture,
            &[
                ("observed_version", Some("0")),
                ("proposed_version", Some("1")),
                ("committed_version", None),
                ("failure_stage", Some("current_publication")),
                ("rollback_outcome", Some("failed")),
                ("outcome", Some("ambiguous")),
            ],
        );
        tokio::fs::remove_file(commit_path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_failure_returns_ambiguous_outcome() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let commit_rel = layout::commit_rel_path(1);
        tokio::fs::create_dir_all(tmp.path().join(&commit_rel)).await?;
        let publish_error =
            storage::read_to_string(store.location.as_ref(), Path::new("missing-current.tmp"))
                .await
                .expect_err("missing path should fail");

        let err = store
            .rollback_unpublished_commit(&commit_rel, publish_error)
            .await;
        let message = err.to_string();
        let (operation_error, cleanup_error) = match &err {
            CommitError::AmbiguousOutcome {
                operation_error,
                cleanup_error,
                ..
            } => (operation_error, cleanup_error),
            other => panic!("unexpected commit error: {other:?}"),
        };
        let primary = err
            .source()
            .and_then(|source| source.downcast_ref::<Box<StorageError>>())
            .map(Box::as_ref)
            .expect("primary storage source");

        assert!(matches!(err, CommitError::AmbiguousOutcome { .. }));
        assert!(std::ptr::eq(primary, operation_error.as_ref()));
        assert!(matches!(primary, StorageError::NotFound { .. }));
        assert!(matches!(
            cleanup_error.as_ref(),
            StorageError::OtherIo { .. }
        ));
        assert!(std::ptr::eq(
            snafu::ErrorCompat::backtrace(&err).expect("commit backtrace"),
            snafu::ErrorCompat::backtrace(primary).expect("storage backtrace")
        ));
        assert!(message.contains("missing-current.tmp"));
        assert!(message.contains(&commit_rel.display().to_string()));
        Ok(())
    }

    #[tokio::test]
    async fn commit_write_cleanup_failure_returns_ambiguous_outcome() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let commit_rel = layout::commit_rel_path(1);
        let commit_path = tmp.path().join(&commit_rel);
        storage::inject_write_new_failure(commit_path.clone(), true);

        let capture = TraceCapture::default();
        let err = capture
            .run(store.commit_with_expected_version(0, vec![]))
            .await
            .expect_err("commit write and cleanup should fail");

        assert!(matches!(err, CommitError::AmbiguousOutcome { .. }));
        assert!(commit_path.exists());
        assert_eq!(store.load_current_version().await?, 0);
        assert_commit_span(
            &capture,
            &[
                ("observed_version", Some("0")),
                ("proposed_version", Some("1")),
                ("committed_version", None),
                ("failure_stage", Some("atomic_create")),
                ("rollback_outcome", None),
                ("outcome", Some("ambiguous")),
            ],
        );
        tokio::fs::remove_file(commit_path).await?;
        Ok(())
    }
}
