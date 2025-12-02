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
use tokio::{fs, io::AsyncWriteExt};

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
