//! Filesystem layout and path utilities.
//!
//! This module centralizes all filesystem- and path-related logic for
//! `timeseries-table-core`. It is responsible for mapping a table root
//! directory to the locations of:
//!
//! - The metadata log directory (for example, `<root>/_timeseries_log/`).
//! - Individual commit files (for example, `<root>/_timeseries_log/0000000001.json`).
//! - The `CURRENT` pointer that records the latest committed version.
//! - Data segments (for example, Parquet files) and any directory
//!   structure used to organize them.
//!
//! Goals of this module include:
//!
//! - Keeping path conventions in one place so they can be evolved
//!   without touching higher-level logic.
//! - Providing small helpers for atomic file operations used by the
//!   commit protocol (for example, write-then-rename semantics).
//! - Ensuring that higher-level modules (`log`, `table`) work with
//!   strongly-typed paths and simple helpers instead of hard-coded
//!   string concatenation.
//!
//! This module does not impose any particular storage backend beyond
//! the local filesystem yet, but the API should be designed so that
//! future adapters (for example, object storage) can be introduced
//! without rewriting the log and table logic.

use snafu::{Backtrace, prelude::*};
use std::{
    io,
    path::{Path, PathBuf},
};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

/// General result type used by storage operations.
///
/// This aliases `Result<T, StorageError>` so functions in this module can
/// return a concise result type while still communicating storage-specific
/// error information via `StorageError`.
pub type StorageResult<T> = Result<T, StorageError>;

/// Represents the location of a timeseries table.
///
/// This enum abstracts over different storage backends, currently supporting
/// local filesystem paths with potential future support for object storage.
#[derive(Clone, Debug)]
pub enum TableLocation {
    /// A table stored on the local filesystem at the given path.
    Local(PathBuf),
    // Future:
    // S3 { bucket: string, prefix: string },
}

impl TableLocation {
    /// Creates a new `TableLocation` for a local filesystem path.
    pub fn local(root: impl Into<PathBuf>) -> Self {
        TableLocation::Local(root.into())
    }
}

/// Errors that can occur during storage operations.
#[derive(Debug, Snafu)]
pub enum StorageError {
    /// The specified path was not found.
    #[snafu(display("Path not found: {path}"))]
    /// The path that was not found.
    NotFound {
        /// The path that was not found.
        path: String,
        /// The backtrace at the time the error occurred.
        backtrace: Backtrace,
    },

    /// The specified path already exists when creation was requested with
    /// create-new semantics.
    #[snafu(display("Path already exists: {path}"))]
    AlreadyExists { path: String, backtrace: Backtrace },

    /// An I/O error occurred on the local filesystem.
    #[snafu(display("Local I/O error at {path}: {source}"))]
    LocalIo {
        /// The path where the I/O error occurred.
        path: String,
        /// The underlying I/O error.
        source: io::Error,
        /// The backtrace at the time the error occurred.
        backtrace: Backtrace,
    },
}

/// Join a table location with a relative path into an absolute local path.
///
/// v0.1: only Local is supported.
fn join_local(location: &TableLocation, rel: &Path) -> PathBuf {
    match location {
        TableLocation::Local(root) => root.join(rel),
    }
}

async fn create_parent_dir(abs: &Path) -> StorageResult<()> {
    if let Some(parent) = abs.parent() {
        fs::create_dir_all(parent).await.context(LocalIoSnafu {
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
/// replacement. Currently only `TableLocation::Local` is supported.
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
    location: &TableLocation,
    rel_path: &Path,
    contents: &[u8],
) -> StorageResult<()> {
    match location {
        TableLocation::Local(_) => {
            let abs = join_local(location, rel_path);

            create_parent_dir(&abs).await?;

            let tmp_path = abs.with_extension("tmp");

            {
                let mut file = fs::File::create(&tmp_path).await.context(LocalIoSnafu {
                    path: tmp_path.display().to_string(),
                })?;

                file.write_all(contents).await.context(LocalIoSnafu {
                    path: tmp_path.display().to_string(),
                })?;

                file.sync_all().await.context(LocalIoSnafu {
                    path: tmp_path.display().to_string(),
                })?;
            }

            fs::rename(&tmp_path, &abs).await.context(LocalIoSnafu {
                path: abs.display().to_string(),
            })?;

            Ok(())
        }
    }
}

/// Read the file at `rel_path` within the given `location` and return its
/// contents as a `String`.
///
/// Currently only `TableLocation::Local` is supported. On success this returns
/// the file contents; if the file cannot be found a `StorageError::NotFound` is
/// returned, while other filesystem problems produce `StorageError::LocalIo`.
pub async fn read_to_string(location: &TableLocation, rel_path: &Path) -> StorageResult<String> {
    match location {
        TableLocation::Local(_) => {
            let abs = join_local(location, rel_path);

            match fs::read_to_string(&abs).await {
                Ok(s) => Ok(s),
                Err(e) if e.kind() == io::ErrorKind::NotFound => NotFoundSnafu {
                    path: abs.display().to_string(),
                }
                .fail(),
                Err(e) => Err(e).context(LocalIoSnafu {
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
    location: &TableLocation,
    rel_path: &Path,
    contents: &[u8],
) -> StorageResult<()> {
    match location {
        TableLocation::Local(_) => {
            let abs = join_local(location, rel_path);
            create_parent_dir(&abs).await?;

            // Atomic "create only if not exists" on the target path.
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&abs)
                .await
                .map_err(|e| {
                    if e.kind() == io::ErrorKind::AlreadyExists {
                        StorageError::AlreadyExists {
                            path: abs.display().to_string(),
                            backtrace: Backtrace::capture(),
                        }
                    } else {
                        StorageError::LocalIo {
                            path: abs.display().to_string(),
                            source: e,
                            backtrace: Backtrace::capture(),
                        }
                    }
                })?;

            file.write_all(contents).await.context(LocalIoSnafu {
                path: abs.display().to_string(),
            })?;

            file.sync_all().await.context(LocalIoSnafu {
                path: abs.display().to_string(),
            })?;

            Ok(())
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
        let location = TableLocation::local(tmp.path());

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
        let location = TableLocation::local(tmp.path());

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
        let location = TableLocation::local(tmp.path());
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
        let location = TableLocation::local(tmp.path());
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
        let location = TableLocation::local(tmp.path());
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
        let location = TableLocation::local(tmp.path());
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
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("roundtrip.txt");

        let original = "roundtrip content 🎉";
        write_atomic(&location, rel_path, original.as_bytes()).await?;

        let read_back = read_to_string(&location, rel_path).await?;
        assert_eq!(read_back, original);
        Ok(())
    }
}
