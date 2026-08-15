use parquet::arrow::async_reader::AsyncFileReader;
use snafu::{Backtrace, prelude::*};
#[cfg(test)]
use std::{
    collections::HashSet,
    sync::{LazyLock, Mutex},
};
use std::{
    io,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

use crate::storage::{
    BackendError, NotFoundSnafu, OtherIoSnafu, StorageError, StorageLocation, StorageResult,
    normalize_relative_storage_path,
};

/// Guard that removes a file on drop unless disarmed.
pub(super) struct TempFileGuard {
    path: PathBuf,
    armed: bool,
}

impl TempFileGuard {
    pub(super) fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    /// Disarm the guard so the file is NOT removed on drop.
    /// Call this after a successful rename.
    pub(super) fn disarm(&mut self) {
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

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.armed {
            // Best-effort cleanup; ignore errors since we're likely already handling another error.
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn cleanup_failure(path: &Path, operation: StorageError, cleanup: io::Error) -> StorageError {
    let path = path.display().to_string();
    let cleanup_error = StorageError::OtherIo {
        path: path.clone(),
        source: BackendError::Local(cleanup),
        backtrace: Backtrace::capture(),
    };
    StorageError::CleanupFailed {
        path,
        operation_error: Box::new(operation),
        cleanup_error: Box::new(cleanup_error),
        backtrace: Backtrace::capture(),
    }
}

#[cfg(test)]
static WRITE_NEW_FAILURES: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[cfg(test)]
static CLEANUP_FAILURES: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

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
    WRITE_NEW_FAILURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(path)
}

#[cfg(test)]
fn take_cleanup_failure(path: &Path) -> bool {
    CLEANUP_FAILURES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(path)
}

#[cfg(test)]
fn write_failure(path: &Path) -> StorageError {
    StorageError::OtherIo {
        path: path.display().to_string(),
        source: BackendError::Local(io::Error::other("injected write failure")),
        backtrace: Backtrace::capture(),
    }
}

async fn cleanup_created_file(
    guard: &mut TempFileGuard,
    path: &Path,
    operation: StorageError,
) -> StorageError {
    match guard.cleanup().await {
        Ok(()) => operation,
        Err(cleanup) => cleanup_failure(path, operation, cleanup),
    }
}

async fn write_created_file(mut file: fs::File, path: &Path, contents: &[u8]) -> StorageResult<()> {
    let mut guard = TempFileGuard::new(path.to_owned());
    #[cfg(test)]
    let injected_write_failure = take_write_new_failure(path);
    let result = async {
        #[cfg(test)]
        if injected_write_failure {
            return Err(write_failure(path));
        }

        file.write_all(contents)
            .await
            .map_err(BackendError::Local)
            .context(OtherIoSnafu {
                path: path.display().to_string(),
            })?;

        file.sync_all()
            .await
            .map_err(BackendError::Local)
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

async fn create_new_file(path: &Path) -> StorageResult<fs::File> {
    create_parent_dir(path).await?;

    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
    {
        Ok(file) => Ok(file),
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            Err(StorageError::AlreadyExists {
                path: path.display().to_string(),
                source: BackendError::Local(source),
                backtrace: Backtrace::capture(),
            })
        }
        Err(source) => Err(StorageError::OtherIo {
            path: path.display().to_string(),
            source: BackendError::Local(source),
            backtrace: Backtrace::capture(),
        }),
    }
}

pub(crate) async fn copy_new_from_local(
    location: &StorageLocation,
    source: &Path,
    rel_path: &Path,
) -> StorageResult<()> {
    match location {
        StorageLocation::Local(_) => {
            let mut source_file = fs::File::open(source).await.map_err(|error| {
                if error.kind() == io::ErrorKind::NotFound {
                    StorageError::NotFound {
                        path: source.display().to_string(),
                        source: BackendError::Local(error),
                        backtrace: Backtrace::capture(),
                    }
                } else {
                    StorageError::OtherIo {
                        path: source.display().to_string(),
                        source: BackendError::Local(error),
                        backtrace: Backtrace::capture(),
                    }
                }
            })?;
            let destination = join_local(location, rel_path)?;
            let mut destination_file = create_new_file(&destination).await?;
            let mut guard = TempFileGuard::new(destination.clone());
            #[cfg(test)]
            let injected_copy_failure = take_write_new_failure(&destination);

            let result = async {
                #[cfg(test)]
                if injected_copy_failure {
                    return Err(write_failure(&destination));
                }

                tokio::io::copy(&mut source_file, &mut destination_file)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: destination.display().to_string(),
                    })?;
                destination_file
                    .sync_all()
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: destination.display().to_string(),
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
                    drop(destination_file);
                    Err(cleanup_created_file(&mut guard, &destination, operation).await)
                }
            }
        }
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
                Err(BackendError::Local(error)).context(NotFoundSnafu { path })
            }
            Err(error) => Err(BackendError::Local(error)).context(OtherIoSnafu { path }),
        },
    }
}

pub(super) async fn create_parent_dir(abs: &Path) -> StorageResult<()> {
    if let Some(parent) = abs.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(BackendError::Local)
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
/// Returns `StorageError::LocalIo` when filesystem I/O fails; other internal
/// helpers may add context to the returned error.
pub async fn write_atomic(
    location: &StorageLocation,
    rel_path: &Path,
    contents: &[u8],
) -> StorageResult<()> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;

            create_parent_dir(&abs).await?;

            let tmp_path = abs.with_extension("tmp");
            let mut guard = TempFileGuard::new(tmp_path.clone());

            {
                let mut file = fs::File::create(&tmp_path)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: tmp_path.display().to_string(),
                    })?;

                file.write_all(contents)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: tmp_path.display().to_string(),
                    })?;

                file.sync_all()
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: tmp_path.display().to_string(),
                    })?;
            }

            fs::rename(&tmp_path, &abs)
                .await
                .map_err(BackendError::Local)
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
/// returned, while other filesystem problems produce `StorageError::LocalIo`.
pub async fn read_to_string(location: &StorageLocation, rel_path: &Path) -> StorageResult<String> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;

            match fs::read_to_string(&abs).await {
                Ok(s) => Ok(s),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Err(BackendError::Local(e))
                    .context(NotFoundSnafu {
                        path: abs.display().to_string(),
                    }),
                Err(e) => Err(BackendError::Local(e)).context(OtherIoSnafu {
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
                    source: BackendError::Local(io::Error::other("injected cleanup failure")),
                    backtrace: Backtrace::capture(),
                });
            }
            fs::remove_file(&abs)
                .await
                .map_err(BackendError::Local)
                .context(OtherIoSnafu {
                    path: abs.display().to_string(),
                })
        }
    }
}

/// Create a *new* file at `rel_path` and write `contents`, failing if the file
/// already exists.
///
/// This is used for objects that must not overwrite existing data, including
/// commit files and writer-owned sidecars.
pub async fn write_new(
    location: &StorageLocation,
    rel_path: &Path,
    contents: &[u8],
) -> StorageResult<()> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;
            let file = create_new_file(&abs).await?;
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
pub async fn read_all_bytes(location: &StorageLocation, rel_path: &Path) -> StorageResult<Vec<u8>> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;
            let path_str = abs.display().to_string();

            match fs::read(&abs).await {
                Ok(bytes) => Ok(bytes),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    Err(BackendError::Local(e)).context(NotFoundSnafu { path: path_str })
                }
                Err(e) => Err(BackendError::Local(e)).context(OtherIoSnafu { path: path_str }),
            }
        }
    }
}

/// Get the length (in bytes) of a file at `rel_path` within `location`.
///
/// v0.1: only StorageLocation::Local is supported.
pub async fn file_size(location: &StorageLocation, rel_path: &Path) -> StorageResult<u64> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path)?;
            let path_str = rel_path.display().to_string();

            let meta = fs::metadata(&abs).await;
            match meta {
                Ok(m) => Ok(m.len()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    Err(BackendError::Local(e)).context(NotFoundSnafu { path: path_str })
                }
                Err(e) => Err(BackendError::Local(e)).context(OtherIoSnafu { path: path_str }),
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
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
    async fn copy_new_from_local_removes_target_after_copy_failure() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        tokio::fs::create_dir(&table_root).await?;
        let location = StorageLocation::local(&table_root);
        let source = tmp.path().join("source.parquet");
        tokio::fs::write(&source, b"parquet").await?;
        let rel_path = Path::new("data/copied.parquet");
        let destination = table_root.join(rel_path);
        inject_write_new_failure(destination.clone(), false);

        let err = copy_new_from_local(&location, &source, rel_path)
            .await
            .expect_err("copy should fail");

        assert!(matches!(err, StorageError::OtherIo { .. }));
        assert!(!destination.exists());
        assert_eq!(tokio::fs::read(source).await?, b"parquet");
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
            assert!(matches!(write_error, StorageError::OtherIo { .. }));

            let read_error = read_all_bytes(&location, &path)
                .await
                .expect_err("outside read path must be rejected");
            assert!(matches!(read_error, StorageError::OtherIo { .. }));
        }

        assert!(!outside.exists());
        Ok(())
    }
}
