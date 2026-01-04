use snafu::{Backtrace, prelude::*};
use std::{
    io::{self, SeekFrom},
    path::{Path, PathBuf},
};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::storage::{
    BackendError, NotFoundSnafu, OtherIoSnafu, StorageError, StorageLocation, StorageResult,
};

/// Guard that removes a temporary file on drop unless disarmed.
/// Used to ensure cleanup on error paths during atomic writes.
struct TempFileGuard {
    path: PathBuf,
    armed: bool,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    /// Disarm the guard so the file is NOT removed on drop.
    /// Call this after a successful rename.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if self.armed {
            // Best-effort cleanup; ingore errors since we're likely already handling another error.
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Join a table location with a relative path into an absolute local path.
///
/// v0.1: only Local is supported.
fn join_local(location: &StorageLocation, rel: &Path) -> PathBuf {
    match location {
        StorageLocation::Local(root) => root.join(rel),
    }
}

async fn create_parent_dir(abs: &Path) -> StorageResult<()> {
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
            let abs = join_local(location, rel_path);

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
            let abs = join_local(location, rel_path);

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

/// Create a *new* file at `rel_path` and write `contents`, failing if the file
/// already exists.
///
/// This is used for commit files where we want per-version uniqueness.
pub async fn write_new(
    location: &StorageLocation,
    rel_path: &Path,
    contents: &[u8],
) -> StorageResult<()> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path);
            create_parent_dir(&abs).await?;

            let path_str = abs.display().to_string();

            // Atomic "create only if not exists" on the target path.
            let open_result = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&abs)
                .await;

            let mut file = match open_result {
                Ok(f) => f,
                Err(e) => {
                    let backend = BackendError::Local(e);
                    // Classify AlreadyExists vs "other I/O"
                    let storage_err = match &backend {
                        BackendError::Local(inner)
                            if inner.kind() == io::ErrorKind::AlreadyExists =>
                        {
                            StorageError::AlreadyExists {
                                path: path_str,
                                source: backend,
                                backtrace: Backtrace::capture(),
                            }
                        }
                        _ => StorageError::OtherIo {
                            path: path_str,
                            source: backend,
                            backtrace: Backtrace::capture(),
                        },
                    };
                    return Err(storage_err);
                }
            };

            file.write_all(contents)
                .await
                .map_err(BackendError::Local)
                .context(OtherIoSnafu {
                    path: abs.display().to_string(),
                })?;

            file.sync_all()
                .await
                .map_err(BackendError::Local)
                .context(OtherIoSnafu {
                    path: abs.display().to_string(),
                })?;

            Ok(())
        }
    }
}

/// Small probe structure used by higher-level code (e.g. segment validators)
/// to inspect a file's length and its first/last 4 bytes.
pub struct FileHeadTail4 {
    /// Length of the file in bytes.
    pub len: u64,
    /// First 4 bytes of the file (zero-filled if the file is shorter).
    pub head: [u8; 4],
    /// Last 4 bytes of the file (zero-filled if the file is shorter).
    pub tail: [u8; 4],
}

/// Read the length, first 4 bytes, and last 4 bytes of a file at `rel_path`
/// within the given `location`.
///
/// Semantics:
/// - On missing file: `StorageError::NotFound`.
/// - On other I/O problems: `StorageError::LocalIo`.
/// - Only `StorageLocation::Local` is supported in v0.1.
///
/// For files shorter than 4 bytes, both `head` and `tail` remain zero-filled.
/// For files between 4 and 7 bytes, `head` contains the first 4 bytes but
/// `tail` remains zero-filled since reading both without overlap is not
/// possible. Callers that need distinct head/tail (e.g., Parquet magic
/// validation) should check `len >= 8` before inspecting `tail`.
pub async fn read_head_tail_4(
    location: &StorageLocation,
    rel_path: &Path,
) -> StorageResult<FileHeadTail4> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path);
            let path_str = abs.display().to_string();

            // Metadata: we special-case NotFound like read_to_string does.
            let meta = match fs::metadata(&abs).await {
                Ok(m) => m,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Err(BackendError::Local(e)).context(NotFoundSnafu { path: path_str });
                }
                Err(e) => {
                    return Err(BackendError::Local(e)).context(OtherIoSnafu { path: path_str });
                }
            };

            // 2) Non-regular file: treat as semantic "NotFound" (no real OS error).
            if !meta.is_file() {
                let synthetic = io::Error::other("not a regular file");
                let backend = BackendError::Local(synthetic);
                return Err(StorageError::NotFound {
                    path: path_str,
                    source: backend,
                    backtrace: Backtrace::capture(),
                });
            }

            let len = meta.len();

            let mut file = fs::File::open(&abs)
                .await
                .map_err(BackendError::Local)
                .context(OtherIoSnafu {
                    path: path_str.clone(),
                })?;

            let mut head = [0u8; 4];
            let mut tail = [0u8; 4];

            // Only attempt to read the header if file is at least 4 bytes.
            if len >= 4 {
                file.read_exact(&mut head)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: path_str.clone(),
                    })?;
            }

            // Only attempt to read the footer if file is at least 8 bytes.
            if len >= 8 {
                file.seek(SeekFrom::End(-4))
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: path_str.clone(),
                    })?;
                file.read_exact(&mut tail)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: path_str.clone(),
                    })?;
            }
            Ok(FileHeadTail4 { len, head, tail })
        }
    }
}

/// Read the full contents of a file at `rel_path` within `location` and return
/// them as a Vec<u8>.
///
/// Only `StorageLocation::Local` is supported in this crate version.
///
/// Errors:
/// - If the file does not exist this returns `StorageError::NotFound`.
/// - On any other I/O error this returns `StorageError::OtherIo`.
pub async fn read_all_bytes(location: &StorageLocation, rel_path: &Path) -> StorageResult<Vec<u8>> {
    match location {
        StorageLocation::Local(_) => {
            let abs = join_local(location, rel_path);
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
}
