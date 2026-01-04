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
mod error;
pub use error::*;

use snafu::{Backtrace, IntoError, prelude::*};
use std::{
    io::{self, SeekFrom},
    path::{Path, PathBuf},
};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
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

    /// Ensure `parquet_path` is under this table root.
    /// If not, copy it into `data/<filename>` and return the relative path.
    pub async fn ensure_parquet_under_root(&self, parquet_path: &Path) -> StorageResult<PathBuf> {
        match self {
            TableLocation::Local(table_root) => {
                let root = fs::canonicalize(table_root)
                    .await
                    .map_err(BackendError::Local)
                    .context(NotFoundSnafu {
                        path: table_root.display().to_string(),
                    })?;

                let src = fs::canonicalize(parquet_path)
                    .await
                    .map_err(BackendError::Local)
                    .context(NotFoundSnafu {
                        path: parquet_path.display().to_string(),
                    })?;

                if let Ok(rel) = src.strip_prefix(&root) {
                    return Ok(rel.to_path_buf());
                }

                let file_name = src
                    .file_name()
                    .ok_or_else(|| {
                        OtherIoSnafu {
                            path: src.display().to_string(),
                        }
                        .into_error(BackendError::Local(
                            std::io::Error::other("parquet path has no filename"),
                        ))
                    })?
                    .to_owned();

                let data_dir = root.join("data");
                fs::create_dir_all(&data_dir)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: data_dir.display().to_string(),
                    })?;

                let dst = data_dir.join(file_name);

                match fs::metadata(&dst).await {
                    Ok(_) => {
                        return AlreadyExistsNoSourceSnafu {
                            path: dst.display().to_string(),
                        }
                        .fail();
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // ok to proceed
                    }

                    Err(e) => {
                        return Err(BackendError::Local(e)).context(OtherIoSnafu {
                            path: dst.display().to_string(),
                        });
                    }
                }

                fs::copy(&src, &dst)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: dst.display().to_string(),
                    })?;

                let dst = fs::canonicalize(&dst)
                    .await
                    .map_err(BackendError::Local)
                    .context(OtherIoSnafu {
                        path: dst.display().to_string(),
                    })?;

                let rel = dst.strip_prefix(&root).map_err(|_| {
                    OtherIoSnafu {
                        path: dst.display().to_string(),
                    }
                    .into_error(BackendError::Local(std::io::Error::other(
                        "copied parquet is not under table root",
                    )))
                })?;

                Ok(rel.to_path_buf())
            }
        }
    }

    /// Parse a user-facing table location string into a TableLocation.
    /// v0.1: only local filesystem paths are supported.
    pub fn parse(spec: &str) -> StorageResult<Self> {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            return Err(OtherIoSnafu {
                path: "<empty table location>".to_string(),
            }
            .into_error(BackendError::Local(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "table location is empty",
            ))));
        }

        // Windows drive letter path (e.g. C:\ or C:/)
        if trimmed.len() >= 2 {
            let mut chars = trimmed.chars();
            let first = chars.next();
            let second = chars.next();
            if let (Some(first), Some(second)) = (first, second)
                && first.is_ascii_alphabetic()
                && second == ':'
            {
                return Ok(TableLocation::Local(PathBuf::from(trimmed)));
            }
        }

        // URI-like scheme (e.g. s3://, gs://, https://)
        let scheme = trimmed.split_once("://").and_then(|(scheme, _)| {
            if scheme.is_empty() {
                None
            } else {
                Some(scheme)
            }
        });

        if let Some(scheme) = scheme {
            let scheme_ok = scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.');

            if scheme_ok {
                return Err(OtherIoSnafu {
                    path: trimmed.to_string(),
                }
                .into_error(BackendError::Local(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    format!("unsupported table location scheme: {scheme}"),
                ))));
            }
        }

        Ok(TableLocation::Local(PathBuf::from(trimmed)))
    }
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
        fs::create_dir_all(parent)
            .await
            .map_err(BackendError::Local)
            .context(OtherIoSnafu {
                path: parent.display().to_string(),
            })?;
    }
    Ok(())
}

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
/// Currently only `TableLocation::Local` is supported. On success this returns
/// the file contents; if the file cannot be found a `StorageError::NotFound` is
/// returned, while other filesystem problems produce `StorageError::LocalIo`.
pub async fn read_to_string(location: &TableLocation, rel_path: &Path) -> StorageResult<String> {
    match location {
        TableLocation::Local(_) => {
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
    location: &TableLocation,
    rel_path: &Path,
    contents: &[u8],
) -> StorageResult<()> {
    match location {
        TableLocation::Local(_) => {
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
/// - Only `TableLocation::Local` is supported in v0.1.
///
/// For files shorter than 4 bytes, both `head` and `tail` remain zero-filled.
/// For files between 4 and 7 bytes, `head` contains the first 4 bytes but
/// `tail` remains zero-filled since reading both without overlap is not
/// possible. Callers that need distinct head/tail (e.g., Parquet magic
/// validation) should check `len >= 8` before inspecting `tail`.
pub async fn read_head_tail_4(
    location: &TableLocation,
    rel_path: &Path,
) -> StorageResult<FileHeadTail4> {
    match location {
        TableLocation::Local(_) => {
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
/// Only `TableLocation::Local` is supported in this crate version.
///
/// Errors:
/// - If the file does not exist this returns `StorageError::NotFound`.
/// - On any other I/O error this returns `StorageError::OtherIo`.
pub async fn read_all_bytes(location: &TableLocation, rel_path: &Path) -> StorageResult<Vec<u8>> {
    match location {
        TableLocation::Local(_) => {
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

    #[tokio::test]
    async fn write_new_creates_file_with_contents() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
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
        let location = TableLocation::local(tmp.path());
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
        let location = TableLocation::local(tmp.path());
        let rel_path = Path::new("nested/path/new_file.txt");

        write_new(&location, rel_path, b"nested new").await?;

        let abs = tmp.path().join(rel_path);
        assert!(abs.exists());
        let read_back = tokio::fs::read_to_string(&abs).await?;
        assert_eq!(read_back, "nested new");
        Ok(())
    }

    #[test]
    fn parse_rejects_empty_location() {
        let err = TableLocation::parse("   ").expect_err("expected error");
        match err {
            StorageError::OtherIo { source, .. } => match source {
                BackendError::Local(inner) => {
                    assert_eq!(inner.kind(), io::ErrorKind::InvalidInput);
                }
            },
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_unsupported_scheme() {
        let err =
            TableLocation::parse("s3://bucket/path").expect_err("expected unsupported scheme");
        match err {
            StorageError::OtherIo { source, .. } => match source {
                BackendError::Local(inner) => {
                    assert_eq!(inner.kind(), io::ErrorKind::Unsupported);
                }
            },
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_accepts_local_path() -> TestResult {
        let loc = TableLocation::parse("/tmp/table")?;
        match loc {
            TableLocation::Local(p) => {
                assert_eq!(p, PathBuf::from("/tmp/table"));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn ensure_parquet_under_root_returns_relative_path() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let rel_path = Path::new("data/seg.parquet");
        let abs_path = tmp.path().join(rel_path);
        tokio::fs::create_dir_all(abs_path.parent().unwrap()).await?;
        tokio::fs::write(&abs_path, b"parquet").await?;

        let rel = location.ensure_parquet_under_root(&abs_path).await?;
        assert_eq!(rel, rel_path);
        Ok(())
    }

    #[tokio::test]
    async fn ensure_parquet_under_root_copies_outside_file() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        tokio::fs::create_dir_all(&table_root).await?;
        let location = TableLocation::local(&table_root);

        let src_path = tmp.path().join("outside.parquet");
        tokio::fs::write(&src_path, b"parquet").await?;

        let rel = location.ensure_parquet_under_root(&src_path).await?;
        let expected_rel = PathBuf::from("data/outside.parquet");
        assert_eq!(rel, expected_rel);

        let dst = table_root.join(&expected_rel);
        assert!(dst.exists());
        let contents = tokio::fs::read(&dst).await?;
        assert_eq!(contents, b"parquet");
        Ok(())
    }

    #[tokio::test]
    async fn ensure_parquet_under_root_refuses_overwrite() -> TestResult {
        let tmp = TempDir::new()?;
        let table_root = tmp.path().join("table");
        tokio::fs::create_dir_all(&table_root).await?;
        let location = TableLocation::local(&table_root);

        let data_dir = table_root.join("data");
        tokio::fs::create_dir_all(&data_dir).await?;
        let existing_dst = data_dir.join("seg.parquet");
        tokio::fs::write(&existing_dst, b"existing").await?;

        let src_path = tmp.path().join("seg.parquet");
        tokio::fs::write(&src_path, b"new").await?;

        let err = location
            .ensure_parquet_under_root(&src_path)
            .await
            .expect_err("expected AlreadyExistsNoSource");

        assert!(matches!(err, StorageError::AlreadyExistsNoSource { .. }));
        Ok(())
    }
}
