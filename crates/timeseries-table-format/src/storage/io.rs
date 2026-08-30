#[cfg(test)]
use futures::channel::oneshot;
use parquet::arrow::async_reader::AsyncFileReader;
use snafu::{Backtrace, prelude::*};
#[cfg(test)]
use std::{
    collections::{HashMap, HashSet},
    sync::{LazyLock, Mutex},
};
use std::{
    io,
    path::{Path, PathBuf},
    time::SystemTime,
};
use tokio::{fs, io::AsyncWriteExt};

use crate::storage::{
    NotFoundSnafu, OtherIoSnafu, StorageBackendError, StorageError, StorageLocation, StorageResult,
    normalize_relative_storage_path,
};

/// Metadata for one file stored below a table-relative directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageFileMetadata {
    pub(crate) path: String,
    pub(crate) size_bytes: u64,
    pub(crate) modified_at: SystemTime,
}

/// Guard that removes a file on drop unless disarmed.
pub(crate) struct FileCleanupGuard {
    path: PathBuf,
    armed: bool,
}

impl FileCleanupGuard {
    pub(super) fn new_armed(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    /// Resolve a path for a file that has not been created yet.
    /// Arm the guard immediately after exclusive creation succeeds.
    pub(crate) fn new_disarmed(location: &StorageLocation, rel_path: &Path) -> StorageResult<Self> {
        Ok(Self {
            path: join_local(location, rel_path)?,
            armed: false,
        })
    }

    pub(crate) fn arm(&mut self) {
        self.armed = true;
    }

    /// Disarm the guard so the file is NOT removed on drop.
    /// Call this after a successful rename.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }

    async fn cleanup(&mut self) -> io::Result<()> {
        #[cfg(test)]
        if take_cleanup_failure(&self.path) {
            self.disarm();
            return Err(io::Error::other("injected cleanup failure"));
        }

        let result = fs::remove_file(&self.path).await;
        self.disarm();
        result
    }
}

impl Drop for FileCleanupGuard {
    fn drop(&mut self) {
        if self.armed {
            #[cfg(test)]
            if has_cleanup_failure(&self.path) {
                return;
            }
            // Best-effort cleanup; ignore errors since we're likely already handling another error.
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn cleanup_failure(path: &Path, operation: StorageError, cleanup: io::Error) -> StorageError {
    let path = path.display().to_string();
    let cleanup_error = StorageError::OtherIo {
        path: path.clone(),
        source: cleanup.into(),
        backtrace: Backtrace::capture(),
    };
    StorageError::CleanupFailed {
        path,
        operation_error: Box::new(operation),
        cleanup_error: Box::new(cleanup_error),
    }
}

#[cfg(test)]
static WRITE_NEW_FAILURES: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[cfg(test)]
static CLEANUP_FAILURES: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[cfg(test)]
struct AtomicWritePausePoint {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[cfg(test)]
static ATOMIC_WRITE_OPEN_PAUSES: LazyLock<Mutex<HashMap<PathBuf, AtomicWritePausePoint>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
static ATOMIC_WRITE_RENAME_PAUSES: LazyLock<Mutex<HashMap<PathBuf, AtomicWritePausePoint>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
pub(crate) struct AtomicWritePause {
    entered: Option<oneshot::Receiver<()>>,
    release: Option<oneshot::Sender<()>>,
}

#[cfg(test)]
impl AtomicWritePause {
    pub(crate) async fn wait_until_paused(&mut self) {
        self.entered
            .take()
            .expect("pause wait called once")
            .await
            .expect("atomic write reached rename pause");
    }

    pub(crate) fn release(mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

#[cfg(test)]
fn pause_atomic_write_at(
    pauses: &Mutex<HashMap<PathBuf, AtomicWritePausePoint>>,
    path: PathBuf,
) -> AtomicWritePause {
    let (entered_sender, entered_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = oneshot::channel();
    let previous = pauses
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            path,
            AtomicWritePausePoint {
                entered: entered_sender,
                release: release_receiver,
            },
        );
    assert!(previous.is_none(), "atomic write pause already installed");
    AtomicWritePause {
        entered: Some(entered_receiver),
        release: Some(release_sender),
    }
}

#[cfg(test)]
pub(crate) fn pause_atomic_write_before_open(path: PathBuf) -> AtomicWritePause {
    pause_atomic_write_at(&ATOMIC_WRITE_OPEN_PAUSES, path)
}

#[cfg(test)]
pub(crate) fn pause_atomic_write_before_rename(path: PathBuf) -> AtomicWritePause {
    pause_atomic_write_at(&ATOMIC_WRITE_RENAME_PAUSES, path)
}

#[cfg(test)]
async fn wait_at_atomic_write_pause(
    pauses: &Mutex<HashMap<PathBuf, AtomicWritePausePoint>>,
    path: &Path,
) {
    let pause = pauses
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(path);
    if let Some(pause) = pause {
        let _ = pause.entered.send(());
        let _ = pause.release.await;
    }
}

#[cfg(test)]
pub(crate) fn inject_write_new_failure(path: PathBuf, cleanup_fails: bool) {
    if cleanup_fails {
        inject_cleanup_failure(path.clone());
    }
    WRITE_NEW_FAILURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(path);
}

#[cfg(test)]
pub(crate) fn inject_cleanup_failure(path: PathBuf) {
    CLEANUP_FAILURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(path);
}

#[cfg(test)]
fn take_write_new_failure(path: &Path) -> bool {
    let mut failures = WRITE_NEW_FAILURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(target) = failures
        .iter()
        .find(|target| path.starts_with(target))
        .cloned()
    else {
        return false;
    };
    failures.remove(&target)
}

#[cfg(test)]
fn take_cleanup_failure(path: &Path) -> bool {
    let mut failures = CLEANUP_FAILURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(target) = failures
        .iter()
        .find(|target| path.starts_with(target))
        .cloned()
    else {
        return false;
    };
    failures.remove(&target)
}

#[cfg(test)]
fn has_cleanup_failure(path: &Path) -> bool {
    CLEANUP_FAILURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .any(|target| path.starts_with(target))
}

#[cfg(test)]
fn write_failure(path: &Path) -> StorageError {
    StorageError::OtherIo {
        path: path.display().to_string(),
        source: io::Error::other("injected write failure").into(),
        backtrace: Backtrace::capture(),
    }
}

async fn cleanup_created_file(
    guard: &mut FileCleanupGuard,
    path: &Path,
    operation: StorageError,
) -> StorageError {
    match guard.cleanup().await {
        Ok(()) => operation,
        Err(cleanup) => cleanup_failure(path, operation, cleanup),
    }
}

async fn write_created_file(mut file: fs::File, path: &Path, contents: &[u8]) -> StorageResult<()> {
    let mut guard = FileCleanupGuard::new_armed(path.to_owned());
    #[cfg(test)]
    let injected_write_failure = take_write_new_failure(path);
    let result = async {
        #[cfg(test)]
        if injected_write_failure {
            return Err(write_failure(path));
        }

        file.write_all(contents)
            .await
            .map_err(StorageBackendError::from)
            .context(OtherIoSnafu {
                path: path.display().to_string(),
            })?;

        file.sync_all()
            .await
            .map_err(StorageBackendError::from)
            .context(OtherIoSnafu {
                path: path.display().to_string(),
            })?;

        Ok(())
    }
    .await;

    match result {
        Ok(()) => {
            guard.disarm();
            Ok(())
        }
        Err(operation) => {
            drop(file);
            Err(cleanup_created_file(&mut guard, path, operation).await)
        }
    }
}

pub(super) async fn create_new_file(path: &Path) -> StorageResult<std::fs::File> {
    create_parent_dir(path).await?;

    // Keep exclusive creation synchronous. Tokio filesystem opens run in a
    // non-cancellable blocking task that may create the path after its future
    // has been dropped.
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => Ok(file),
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            Err(StorageError::AlreadyExists {
                path: path.display().to_string(),
                source: source.into(),
                backtrace: Backtrace::capture(),
            })
        }
        Err(source) => Err(StorageError::OtherIo {
            path: path.display().to_string(),
            source: source.into(),
            backtrace: Backtrace::capture(),
        }),
    }
}

/// Validate a backend-relative storage key and resolve it under a local root.
///
/// v0.1: only Local is supported.
pub(super) fn join_local(location: &StorageLocation, rel: &Path) -> StorageResult<PathBuf> {
    let (_, native_path) = normalize_relative_storage_path(rel)?;
    match location {
        StorageLocation::Local(root) => Ok(root.join(native_path)),
    }
}

/// Open a stored Parquet file for asynchronous range reads.
pub(crate) async fn open_parquet_reader(
    location: &StorageLocation,
    rel_path: &Path,
) -> StorageResult<Box<dyn AsyncFileReader>> {
    let path = rel_path.display().to_string();
    let absolute_path = join_local(location, rel_path)?;

    match location {
        StorageLocation::Local(_) => match fs::File::open(absolute_path).await {
            Ok(file) => Ok(Box::new(file)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Err(StorageBackendError::from(error)).context(NotFoundSnafu { path })
            }
            Err(error) => Err(StorageBackendError::from(error)).context(OtherIoSnafu { path }),
        },
    }
}

pub(super) async fn create_parent_dir(abs: &Path) -> StorageResult<()> {
    if let Some(parent) = abs.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(StorageBackendError::from)
            .context(OtherIoSnafu {
                path: parent.display().to_string(),
            })?;
    }
    Ok(())
}

/// Write `contents` to `rel_path` inside `location` using an atomic write.
///
/// This performs a write-then-rename sequence on the local filesystem:
/// it writes the payload to a temporary file next to the target path,
/// syncs the file, and then renames it into place to provide an atomic
/// replacement. Currently only `StorageLocation::Local` is supported.
///
/// # Parameters
///
/// - `location`: the table root location to resolve the relative path.
/// - `rel_path`: the relative path (under `location`) to write the file to.
/// - `contents`: the bytes to write.
///
/// # Errors
///
/// Returns `StorageError::OtherIo` when filesystem I/O fails; other internal
/// helpers may add context to the returned error.
pub(crate) async fn write_atomic(
    location: &StorageLocation,
    rel_path: &Path,
    contents: &[u8],
) -> StorageResult<()> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;

            create_parent_dir(&abs).await?;

            let tmp_path = abs.with_extension("tmp");
            let mut guard = FileCleanupGuard::new_armed(tmp_path.clone());

            #[cfg(test)]
            wait_at_atomic_write_pause(&ATOMIC_WRITE_OPEN_PAUSES, &abs).await;

            {
                // Opening through Tokio would leave a non-cancellable blocking
                // task able to recreate this path after the future is dropped.
                let file = std::fs::File::create(&tmp_path)
                    .map_err(StorageBackendError::from)
                    .context(OtherIoSnafu {
                        path: tmp_path.display().to_string(),
                    })?;
                let mut file = fs::File::from_std(file);

                file.write_all(contents)
                    .await
                    .map_err(StorageBackendError::from)
                    .context(OtherIoSnafu {
                        path: tmp_path.display().to_string(),
                    })?;

                file.sync_all()
                    .await
                    .map_err(StorageBackendError::from)
                    .context(OtherIoSnafu {
                        path: tmp_path.display().to_string(),
                    })?;
            }

            #[cfg(test)]
            wait_at_atomic_write_pause(&ATOMIC_WRITE_RENAME_PAUSES, &abs).await;

            // Keep publication and guard disarming in one synchronous poll so
            // dropping the future cannot publish CURRENT and then roll back
            // the commit file before this function observes success.
            std::fs::rename(&tmp_path, &abs)
                .map_err(StorageBackendError::from)
                .context(OtherIoSnafu {
                    path: abs.display().to_string(),
                })?;

            // Success - don't remove the temp file (it's been renamed)
            guard.disarm();

            Ok(())
        }
    }
}

/// Read the file at `rel_path` within the given `location` and return its
/// contents as a `String`.
///
/// Currently only `StorageLocation::Local` is supported. On success this returns
/// the file contents; if the file cannot be found a `StorageError::NotFound` is
/// returned, while other filesystem problems produce `StorageError::OtherIo`.
pub(crate) async fn read_to_string(
    location: &StorageLocation,
    rel_path: &Path,
) -> StorageResult<String> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;

            match fs::read_to_string(&abs).await {
                Ok(s) => Ok(s),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Err(StorageBackendError::from(e))
                    .context(NotFoundSnafu {
                        path: abs.display().to_string(),
                    }),
                Err(e) => Err(StorageBackendError::from(e)).context(OtherIoSnafu {
                    path: abs.display().to_string(),
                }),
            }
        }
    }
}

pub(crate) async fn remove_file(location: &StorageLocation, rel_path: &Path) -> StorageResult<()> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;
            #[cfg(test)]
            if take_cleanup_failure(&abs) {
                return Err(StorageError::OtherIo {
                    path: abs.display().to_string(),
                    source: io::Error::other("injected cleanup failure").into(),
                    backtrace: Backtrace::capture(),
                });
            }
            match fs::remove_file(&abs).await {
                Ok(()) => Ok(()),
                Err(source) if source.kind() == io::ErrorKind::NotFound => {
                    Err(StorageError::NotFound {
                        path: abs.display().to_string(),
                        source: source.into(),
                        backtrace: Backtrace::capture(),
                    })
                }
                Err(source) => Err(StorageError::OtherIo {
                    path: abs.display().to_string(),
                    source: source.into(),
                    backtrace: Backtrace::capture(),
                }),
            }
        }
    }
}

pub(crate) async fn remove_file_if_exists(
    location: &StorageLocation,
    rel_path: &Path,
) -> StorageResult<()> {
    match remove_file(location, rel_path).await {
        Ok(()) | Err(StorageError::NotFound { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Create a *new* file at `rel_path` and write `contents`, failing if the file
/// already exists.
///
/// This is used for objects that must not overwrite existing data, including
/// commit files and writer-owned sidecars.
pub(crate) async fn write_new(
    location: &StorageLocation,
    rel_path: &Path,
    contents: &[u8],
) -> StorageResult<()> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;
            let file = fs::File::from_std(create_new_file(&abs).await?);
            write_created_file(file, &abs, contents).await
        }
    }
}

/// Read the full contents of a file at `rel_path` within `location` and return
/// them as a `Vec<u8>`.
///
/// Only `StorageLocation::Local` is supported in this crate version.
///
/// Errors:
/// - If the file does not exist this returns `StorageError::NotFound`.
/// - On any other I/O error this returns `StorageError::OtherIo`.
pub(crate) async fn read_all_bytes(
    location: &StorageLocation,
    rel_path: &Path,
) -> StorageResult<Vec<u8>> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;
            let path_str = rel_path.display().to_string();

            match fs::read(&abs).await {
                Ok(bytes) => Ok(bytes),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    Err(StorageBackendError::from(e)).context(NotFoundSnafu { path: path_str })
                }
                Err(e) => {
                    Err(StorageBackendError::from(e)).context(OtherIoSnafu { path: path_str })
                }
            }
        }
    }
}

/// Get the length (in bytes) of a file at `rel_path` within `location`.
///
/// v0.1: only StorageLocation::Local is supported.
pub(crate) async fn file_size(location: &StorageLocation, rel_path: &Path) -> StorageResult<u64> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;
            let path_str = rel_path.display().to_string();

            let meta = fs::metadata(&abs).await;
            match meta {
                Ok(m) => Ok(m.len()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    Err(StorageBackendError::from(e)).context(NotFoundSnafu { path: path_str })
                }
                Err(e) => {
                    Err(StorageBackendError::from(e)).context(OtherIoSnafu { path: path_str })
                }
            }
        }
    }
}

/// List regular files recursively below a table-relative directory.
pub(crate) async fn list_files(
    location: &StorageLocation,
    rel_dir: &Path,
) -> StorageResult<Vec<StorageFileMetadata>> {
    let (canonical_dir, native_dir) = normalize_relative_storage_path(rel_dir)?;

    match location {
        StorageLocation::Local(root) => {
            let mut pending = vec![(root.join(native_dir), PathBuf::from(canonical_dir))];
            let mut files = Vec::new();

            while let Some((absolute_dir, relative_dir)) = pending.pop() {
                let mut entries = match fs::read_dir(&absolute_dir).await {
                    Ok(entries) => entries,
                    Err(source) if source.kind() == io::ErrorKind::NotFound => continue,
                    Err(source) => {
                        return Err(StorageBackendError::from(source)).context(OtherIoSnafu {
                            path: relative_dir.display().to_string(),
                        });
                    }
                };

                while let Some(entry) = entries
                    .next_entry()
                    .await
                    .map_err(StorageBackendError::from)
                    .context(OtherIoSnafu {
                        path: relative_dir.display().to_string(),
                    })?
                {
                    let relative_path = relative_dir.join(entry.file_name());
                    let (path, _) = normalize_relative_storage_path(&relative_path)?;
                    let file_type = entry
                        .file_type()
                        .await
                        .map_err(StorageBackendError::from)
                        .context(OtherIoSnafu { path: path.clone() })?;

                    if file_type.is_dir() {
                        pending.push((entry.path(), relative_path));
                    } else if file_type.is_file() {
                        let metadata = match entry.metadata().await {
                            Ok(metadata) => metadata,
                            Err(source) if source.kind() == io::ErrorKind::NotFound => continue,
                            Err(source) => {
                                return Err(StorageBackendError::from(source))
                                    .context(OtherIoSnafu { path });
                            }
                        };
                        let modified_at = metadata
                            .modified()
                            .map_err(StorageBackendError::from)
                            .context(OtherIoSnafu { path: path.clone() })?;
                        files.push(StorageFileMetadata {
                            path,
                            size_bytes: metadata.len(),
                            modified_at,
                        });
                    }
                }
            }

            files.sort_by(|left, right| left.path.cmp(&right.path));
            Ok(files)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::mpsc;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn write_atomic_creates_file_with_contents() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());

        let rel_path = Path::new("test.txt");
        let contents = b"hello world";

        write_atomic(&location, rel_path, contents).await?;

        // Verify file exists and has correct contents.
        let abs = tmp.path().join(rel_path);
        let read_back = tokio::fs::read_to_string(&abs).await?;
        assert_eq!(read_back, "hello world");
        Ok(())
    }

    #[tokio::test]
    async fn write_atomic_creates_parent_directories() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());

        let rel_path = Path::new("nested/deep/dir/file.txt");
        let contents = b"nested content";

        write_atomic(&location, rel_path, contents).await?;

        let abs = tmp.path().join(rel_path);
        assert!(abs.exists());
        let read_back = tokio::fs::read_to_string(&abs).await?;
        assert_eq!(read_back, "nested content");
        Ok(())
    }

    #[tokio::test]
    async fn write_atomic_overwrites_existing_file() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("overwrite.txt");

        // Write initial content.
        write_atomic(&location, rel_path, b"original").await?;

        // Overwrite with new content.
        write_atomic(&location, rel_path, b"updated").await?;

        let abs = tmp.path().join(rel_path);
        let read_back = tokio::fs::read_to_string(&abs).await?;
        assert_eq!(read_back, "updated");
        Ok(())
    }

    #[tokio::test]
    async fn write_atomic_no_leftover_tmp_file() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("clean.txt");

        write_atomic(&location, rel_path, b"data").await?;

        // The .tmp file should not remain after successful write.
        let tmp_path = tmp.path().join("clean.tmp");
        assert!(!tmp_path.exists());
        Ok(())
    }

    #[test]
    fn cancelling_atomic_write_during_open_does_not_recreate_tmp() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("cancelled.txt");
        let abs = tmp.path().join(rel_path);
        let tmp_path = abs.with_extension("tmp");
        let mut pause = pause_atomic_write_before_open(abs);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?;

        runtime.block_on(async {
            let mut write = Box::pin(write_atomic(&location, rel_path, b"cancelled"));
            tokio::select! {
                () = pause.wait_until_paused() => {}
                result = &mut write => panic!("atomic write completed before pause: {result:?}"),
            }

            let (started_tx, started_rx) = oneshot::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
            });
            started_rx.await?;

            pause.release();
            assert!(futures::poll!(write.as_mut()).is_pending());
            drop(write);
            release_tx.send(())?;
            blocker.await?;
            TestResult::Ok(())
        })?;
        drop(runtime);

        assert!(!tmp_path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn read_to_string_returns_file_contents() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("readable.txt");

        // Create a file directly.
        let abs = tmp.path().join(rel_path);
        tokio::fs::write(&abs, "file contents").await?;

        let result = read_to_string(&location, rel_path).await?;
        assert_eq!(result, "file contents");
        Ok(())
    }

    #[tokio::test]
    async fn read_to_string_returns_not_found_for_missing_file() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("does_not_exist.txt");

        let result = read_to_string(&location, rel_path).await;

        assert!(result.is_err());
        let err = result.expect_err("expected NotFound error");
        assert!(matches!(err, StorageError::NotFound { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("roundtrip.txt");

        let original = "roundtrip content 🎉";
        write_atomic(&location, rel_path, original.as_bytes()).await?;

        let read_back = read_to_string(&location, rel_path).await?;
        assert_eq!(read_back, original);
        Ok(())
    }

    #[tokio::test]
    async fn write_new_creates_file_with_contents() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("new_file.txt");

        write_new(&location, rel_path, b"new content").await?;

        let abs = tmp.path().join(rel_path);
        let read_back = tokio::fs::read_to_string(&abs).await?;
        assert_eq!(read_back, "new content");
        Ok(())
    }

    #[tokio::test]
    async fn write_new_fails_if_file_exists() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("existing.txt");

        // Create the file first.
        write_new(&location, rel_path, b"first").await?;

        // Second write should fail with AlreadyExists.
        let result = write_new(&location, rel_path, b"second").await;

        assert!(result.is_err());
        let err = result.expect_err("expected AlreadyExists error");
        assert!(matches!(err, StorageError::AlreadyExists { .. }));

        // Original content should be unchanged.
        let read_back = read_to_string(&location, rel_path).await?;
        assert_eq!(read_back, "first");
        Ok(())
    }

    #[tokio::test]
    async fn write_new_removes_target_after_write_failure() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("failed.txt");
        let path = tmp.path().join(rel_path);
        inject_write_new_failure(path.clone(), false);

        let err = write_new(&location, rel_path, b"contents")
            .await
            .expect_err("write should fail");

        assert!(matches!(err, StorageError::OtherIo { .. }));
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn write_new_reports_cleanup_failure() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("orphaned.txt");
        let path = tmp.path().join(rel_path);
        inject_write_new_failure(path.clone(), true);

        let err = write_new(&location, rel_path, b"contents")
            .await
            .expect_err("write and cleanup should fail");
        let message = err.to_string();

        assert!(matches!(err, StorageError::CleanupFailed { .. }));
        assert!(message.contains("orphaned.txt"));
        assert!(message.contains("injected write failure"));
        assert!(message.contains("injected cleanup failure"));
        assert!(path.exists());
        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn write_new_creates_parent_directories() -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        let rel_path = Path::new("nested/path/new_file.txt");

        write_new(&location, rel_path, b"nested new").await?;

        let abs = tmp.path().join(rel_path);
        assert!(abs.exists());
        let read_back = tokio::fs::read_to_string(&abs).await?;
        assert_eq!(read_back, "nested new");
        Ok(())
    }

    #[tokio::test]
    async fn storage_operations_reject_paths_outside_root() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        tokio::fs::create_dir(&table_root).await?;
        let location = StorageLocation::local(&table_root);
        let outside = tmp.path().join("outside.txt");

        for path in [PathBuf::from("../outside.txt"), outside.clone()] {
            let write_error = write_new(&location, &path, b"escaped")
                .await
                .expect_err("outside write path must be rejected");
            assert!(matches!(
                write_error,
                StorageError::InvalidRelativePath { .. }
            ));

            let read_error = read_all_bytes(&location, &path)
                .await
                .expect_err("outside read path must be rejected");
            assert!(matches!(
                read_error,
                StorageError::InvalidRelativePath { .. }
            ));

            let list_error = list_files(&location, &path)
                .await
                .expect_err("outside list path must be rejected");
            assert!(matches!(
                list_error,
                StorageError::InvalidRelativePath { .. }
            ));
        }

        assert!(!outside.exists());
        Ok(())
    }

    #[tokio::test]
    async fn list_files_returns_recursive_metadata_without_creating_missing_directories()
    -> TestResult {
        let tmp = TempDir::new()?;
        let location = StorageLocation::local(tmp.path());
        write_new(&location, Path::new("data/b.parquet"), b"second").await?;
        write_new(&location, Path::new("data/nested/a.parquet"), b"first").await?;

        let files = list_files(&location, Path::new("data")).await?;

        assert_eq!(
            files
                .iter()
                .map(|file| (file.path.as_str(), file.size_bytes))
                .collect::<Vec<_>>(),
            [("data/b.parquet", 6), ("data/nested/a.parquet", 5)]
        );
        for file in &files {
            assert_eq!(
                file.modified_at,
                std::fs::metadata(tmp.path().join(&file.path))?.modified()?
            );
        }

        assert!(
            list_files(&location, Path::new("missing"))
                .await?
                .is_empty()
        );
        assert!(!tmp.path().join("missing").exists());
        Ok(())
    }
}
