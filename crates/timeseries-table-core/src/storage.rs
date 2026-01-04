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

mod table_location;
pub use table_location::*;

use snafu::IntoError;
use std::path::PathBuf;

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
pub enum StorageLocation {
    /// A table stored on the local filesystem at the given path.
    Local(PathBuf),
    // Future:
    // S3 { bucket: string, prefix: string },
}

impl StorageLocation {
    /// Creates a new `StorageLocation` for a local filesystem path.
    pub fn local(root: impl Into<PathBuf>) -> Self {
        StorageLocation::Local(root.into())
    }

    /// Parse a user-facing table location string into a StorageLocation.
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
                return Ok(StorageLocation::Local(PathBuf::from(trimmed)));
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

        Ok(StorageLocation::Local(PathBuf::from(trimmed)))
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn parse_rejects_empty_location() {
        let err = StorageLocation::parse("   ").expect_err("expected error");
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
            StorageLocation::parse("s3://bucket/path").expect_err("expected unsupported scheme");
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
        let loc = StorageLocation::parse("/tmp/table")?;
        match loc {
            StorageLocation::Local(p) => {
                assert_eq!(p, PathBuf::from("/tmp/table"));
            }
        }
        Ok(())
    }
}
