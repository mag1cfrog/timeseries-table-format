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

mod operations;
pub use operations::*;

use snafu::{IntoError, prelude::*};
use std::path::{Path, PathBuf};
use tokio::fs;

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

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

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
