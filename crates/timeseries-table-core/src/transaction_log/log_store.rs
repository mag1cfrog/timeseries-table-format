//! Async helpers for persisting and reading the metadata log.
//!
//! This module owns all on-disk interactions with `_timeseries_log/`:
//! - Tracking the `CURRENT` pointer and interpreting the "no file" case as
//!   version `0` (fresh table).
//! - Writing zero-padded commit files with optimistic concurrency control so
//!   each version is created exactly once.
//! - Mapping storage-layer failures into [`CommitError`] variants so callers
//!   can differentiate between conflicts, storage errors, and corrupt state.
//!
//! All operations delegate to the async storage backend and remain focused on
//! durability, leaving higher-level planning (which actions to commit) to the
//! caller.
use crate::storage::{self, StorageError, TableLocation};
use crate::transaction_log::actions::{Commit, LogAction};
use crate::transaction_log::*;
use chrono::Utc;
use snafu::{Backtrace, prelude::*};
use std::path::{Path, PathBuf};

/// Helper for reading and writing the commit log under a table root.
///
/// Layout:
///   <root>/_timeseries_log/0000000001.json
///   <root>/_timeseries_log/0000000002.json
///   <root>/_timeseries_log/CURRENT
#[derive(Debug, Clone)]
pub struct TransactionLogStore {
    location: TableLocation,
}

impl TransactionLogStore {
    /// Name of the subdirectory containing the commit log.
    pub const LOG_DIR_NAME: &str = "_timeseries_log";
    /// Name of the file that stores the current version pointer.
    pub const CURRENT_FILE_NAME: &str = "CURRENT";
    /// Number of digits used in zero-padded commit file names.
    pub const COMMIT_FILENAME_DIGITS: usize = 10;

    /// Create a new TransactionLogStore rooted at a table directory.
    pub fn new(location: TableLocation) -> Self {
        Self { location }
    }

    fn log_rel_dir() -> PathBuf {
        PathBuf::from(Self::LOG_DIR_NAME)
    }

    fn current_rel_path() -> PathBuf {
        Self::log_rel_dir().join(Self::CURRENT_FILE_NAME)
    }

    /// Get the TableLocation of the LogStore.
    pub fn location(&self) -> &TableLocation {
        &self.location
    }

    fn commit_rel_path(version: u64) -> PathBuf {
        let file_name = format!(
            "{:0width$}.json",
            version,
            width = Self::COMMIT_FILENAME_DIGITS
        );
        Self::log_rel_dir().join(file_name)
    }

    async fn write_atomic_rel(&self, rel: &Path, contents: &[u8]) -> Result<(), CommitError> {
        storage::write_atomic(self.location.as_ref(), rel, contents)
            .await
            .context(StorageSnafu)?;
        Ok(())
    }

    /// Helper: read a log-relative file and map storage errors into CommitError.
    async fn read_to_string_rel(&self, rel: &Path) -> Result<String, CommitError> {
        match storage::read_to_string(self.location.as_ref(), rel).await {
            Ok(s) => Ok(s),
            Err(source) => Err(CommitError::Storage { source }),
        }
    }

    /// Load a single commit by version.
    ///
    /// - On storage-layer failures, returns `CommitError::Storage`.
    /// - On JSON parse failures, returns `CommitError::CorruptState`.
    pub async fn load_commit(&self, version: u64) -> Result<Commit, CommitError> {
        let rel = Self::commit_rel_path(version);
        let json = self.read_to_string_rel(&rel).await?;

        let commit = serde_json::from_str(&json).map_err(|e| CommitError::CorruptState {
            msg: format!("failed to parse commit {version}: {e}"),
            backtrace: Backtrace::capture(),
        })?;

        Ok(commit)
    }

    /// Load the CURRENT version pointer.
    ///
    /// Behavior:
    /// - If CURRENT does not exist, treat as a fresh table and return 0.
    /// - If CURRENT contains invalid or empty content, return CorruptState.
    pub async fn load_current_version(&self) -> Result<u64, CommitError> {
        let rel = Self::current_rel_path();

        let contents = match storage::read_to_string(self.location.as_ref(), &rel).await {
            Ok(s) => s,
            Err(StorageError::NotFound { .. }) => return Ok(0),
            Err(source) => return Err(CommitError::Storage { source }),
        };

        let trimmed = contents.trim();
        if trimmed.is_empty() {
            return CorruptStateSnafu {
                msg: format!("CURRENT has empty content at {rel:?}",),
            }
            .fail();
        }
        let version = trimmed
            .parse::<u64>()
            .map_err(|e| CommitError::CorruptState {
                msg: format!("CURRENT has invalid content {trimmed:?}: {e}"),
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
    /// ## Crash recovery
    ///
    /// If this method succeeds in creating the commit file but fails while
    /// updating CURRENT (e.g., due to a crash or I/O error), the system will
    /// be left in a state where:
    /// - The commit file `_timeseries_log/<version>.json` exists.
    /// - CURRENT still points to the previous version.
    ///
    /// This is a known edge case. The orphaned commit file is harmless because
    /// readers only consider commits up to the version in CURRENT. Recovery
    /// can detect this situation by scanning for commit files with versions
    /// higher than CURRENT and either:
    /// - Completing the commit by updating CURRENT, or
    /// - Treating the orphaned file as an incomplete transaction to ignore.
    ///
    /// A future version may implement automatic recovery during table
    /// initialization.
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
    pub async fn commit_with_expected_version(
        &self,
        expected: u64,
        actions: Vec<LogAction>,
    ) -> Result<u64, CommitError> {
        // 1) Guard on CURRENT
        let current = self.load_current_version().await?;
        if current != expected {
            return ConflictSnafu {
                expected,
                found: current,
            }
            .fail();
        }

        // 2) Compute next version with overflow guard
        let version = expected.checked_add(1).context(CorruptStateSnafu {
            msg: "version counter overflow".to_string(),
        })?;

        // 3) Build commit payload
        let commit = Commit {
            version,
            base_version: expected,
            timestamp: Utc::now(),
            actions,
        };

        let json = serde_json::to_vec(&commit).map_err(|e| CommitError::CorruptState {
            msg: format!("failed to serialize commit {version}: {e}"),
            backtrace: Backtrace::capture(),
        })?;

        // 4) Attempt to create the commit file *only if it does not already exist*.
        //    If the file already exists (AlreadyExists error), we propagate it as-is
        //    rather than converting to Conflict. This allows higher-level code to
        //    implement automatic conflict resolution (e.g., retrying with rebased
        //    changes if the operations don't actually conflict, like Delta Lake).
        let commit_rel = Self::commit_rel_path(version);
        storage::write_new(self.location.as_ref(), &commit_rel, &json)
            .await
            .map_err(|source| CommitError::Storage { source })?;

        // 5) Update CURRENT via atomic write (temp + rename).
        let current_rel = Self::current_rel_path();
        let current_contents = format!("{version}\n");
        self.write_atomic_rel(&current_rel, current_contents.as_bytes())
            .await?;

        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use crate::transaction_log::segments::SegmentId;

    use super::*;
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
        let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
        tokio::fs::create_dir_all(&log_dir).await?;
        let current_path = log_dir.join(TransactionLogStore::CURRENT_FILE_NAME);
        tokio::fs::write(&current_path, "5\n").await?;

        let version = store.load_current_version().await?;

        assert_eq!(version, 5);
        Ok(())
    }

    #[tokio::test]
    async fn load_current_version_handles_whitespace() -> TestResult {
        let (tmp, store) = create_test_log_store();

        let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
        tokio::fs::create_dir_all(&log_dir).await?;
        let current_path = log_dir.join(TransactionLogStore::CURRENT_FILE_NAME);
        tokio::fs::write(&current_path, "  42  \n").await?;

        let version = store.load_current_version().await?;

        assert_eq!(version, 42);
        Ok(())
    }

    #[tokio::test]
    async fn load_current_version_returns_corrupt_state_for_empty_file() -> TestResult {
        let (tmp, store) = create_test_log_store();

        let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
        tokio::fs::create_dir_all(&log_dir).await?;
        let current_path = log_dir.join(TransactionLogStore::CURRENT_FILE_NAME);
        tokio::fs::write(&current_path, "").await?;

        let result = store.load_current_version().await;

        assert!(result.is_err());
        let err = result.expect_err("expected CorruptState");
        assert!(matches!(err, CommitError::CorruptState { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn load_current_version_returns_corrupt_state_for_invalid_content() -> TestResult {
        let (tmp, store) = create_test_log_store();

        let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
        tokio::fs::create_dir_all(&log_dir).await?;
        let current_path = log_dir.join(TransactionLogStore::CURRENT_FILE_NAME);
        tokio::fs::write(&current_path, "not-a-number").await?;

        let result = store.load_current_version().await;

        assert!(result.is_err());
        let err = result.expect_err("expected CorruptState");
        assert!(matches!(err, CommitError::CorruptState { .. }));
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
        let commit_path = tmp
            .path()
            .join(TransactionLogStore::LOG_DIR_NAME)
            .join("0000000001.json");
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
        let result = store.commit_with_expected_version(0, vec![]).await;

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

        Ok(())
    }

    #[tokio::test]
    async fn commit_creates_valid_json_file() -> TestResult {
        let (tmp, store) = create_test_log_store();

        let action = LogAction::RemoveSegment {
            segment_id: SegmentId("test-seg".to_string()),
        };

        store.commit_with_expected_version(0, vec![action]).await?;

        // Read and parse the commit file.
        let commit_path = tmp
            .path()
            .join(TransactionLogStore::LOG_DIR_NAME)
            .join("0000000001.json");
        let contents = tokio::fs::read_to_string(&commit_path).await?;
        let commit: Commit = serde_json::from_str(&contents)?;

        assert_eq!(commit.version, 1);
        assert_eq!(commit.base_version, 0);
        assert_eq!(commit.actions.len(), 1);
        assert!(matches!(
            &commit.actions[0],
            LogAction::RemoveSegment { segment_id } if segment_id.0 == "test-seg"
        ));

        Ok(())
    }

    #[tokio::test]
    async fn commit_current_file_contains_version_with_newline() -> TestResult {
        let (tmp, store) = create_test_log_store();

        store.commit_with_expected_version(0, vec![]).await?;

        let current_path = tmp
            .path()
            .join(TransactionLogStore::LOG_DIR_NAME)
            .join(TransactionLogStore::CURRENT_FILE_NAME);
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
        let log_dir = tmp.path().join(TransactionLogStore::LOG_DIR_NAME);
        tokio::fs::create_dir_all(&log_dir).await?;
        let commit_file = log_dir.join("0000000001.json");
        tokio::fs::write(&commit_file, b"{}").await?;

        // Now try to commit at version 1 - should fail with Storage(AlreadyExists)
        let result = store.commit_with_expected_version(0, vec![]).await;

        assert!(
            matches!(
                result,
                Err(CommitError::Storage {
                    source: StorageError::AlreadyExists { .. }
                })
            ),
            "expected Storage(AlreadyExists) error, got: {result:?}",
        );

        Ok(())
    }
}
