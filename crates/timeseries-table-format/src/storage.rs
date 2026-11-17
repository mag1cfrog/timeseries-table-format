//! Storage backend boundary for table artifacts.
//!
//! Route production access to stored table data, coverage sidecars and transaction-log
//! artifacts through this module. Table and transaction code decide what
//! to write; storage owns how each backend reads, writes and publishes it.
//!
//! # Conventions
//!
//! - Call storage operations with a [`StorageLocation`] or [`TableLocation`] and
//!   canonical relative keys. Use `layout` helpers for shared table paths.
//! - Keep backend dispatch, native path and URI conversion, filesystem calls,
//!   backend client calls, locking and sync operations inside `storage`.
//! - Return storage errors, reader/writer interfaces and opaque guards. Native
//!   handles such as `std::fs::File` stay private to backend implementations.
//!   Table and transaction code must not match storage backend variants.
//! - Keep schema and coverage validation, version planning and commit actions
//!   in higher layers.
//!
//! For example, transaction code holds an opaque `CommitLock` through
//! publication and cleanup. Only the local storage implementation opens and
//! locks the underlying file.
//!
//! # Responsibilities within storage
//!
//! `io` implements reads, writes, listing, metadata, cleanup, durability and
//! commit exclusion. `output` implements streaming output sinks. `table_location`
//! handles table locations, key validation and backend path conversion. `layout`
//! defines shared relative path conventions. Backend details may span these
//! internal modules while callers use the `storage` interface.
//!
//! # Adding a backend
//!
//! Only local storage is supported today. A new backend must preserve the
//! contracts callers rely on: exclusive creation, commit/recovery coordination,
//! publication outcome reporting, durability and cancellation-safe cleanup.
//! Unsupported capabilities must fail explicitly, never return a no-op guard or
//! silently weaken a guarantee. Local locks, renames and directory syncs do not
//! supply those guarantees for an object store; adding S3 requires its own safe
//! coordination and publication strategy behind this boundary.
mod error;
pub use error::*;

pub(crate) mod layout;

mod io;
pub(crate) use io::*;

mod table_location;
pub use table_location::*;
pub(crate) use table_location::{
    ensure_canonical_relative_storage_path, normalize_relative_storage_path,
};

mod output;
pub use output::*;

use snafu::IntoError;
use std::path::PathBuf;

/// General result type used by storage operations.
///
/// This aliases `Result<T, StorageError>` so functions in this module can
/// return a concise result type while still communicating storage-specific
/// error information via `StorageError`.
pub type StorageResult<T> = Result<T, StorageError>;

/// Backend + root location for storage operations.
///
/// This type represents the *root* of a storage backend (e.g. a local directory
/// or an object-store prefix). It is intentionally generic and does **not**
/// encode table-specific semantics; use `TableLocation` when you need a table
/// root and table-scoped helpers.
///
/// Many storage helpers take a `StorageLocation` plus a relative path/key,
/// which keeps backend roots and object paths separate and explicit.
#[derive(Clone, Debug)]
pub enum StorageLocation {
    /// A local filesystem root at the given path.
    ///
    /// An existing root is an established, caller-managed namespace: its own
    /// directory entry and external ancestors must already be durable. On Unix,
    /// storage syncs artifact and directory links within that root, and syncs
    /// missing root directories before artifact creation.
    /// Complete interrupted or concurrent initial root setup durably before
    /// adopting an existing root; storage cannot infer its history from existence.
    Local(PathBuf),
    // Future:
    // S3 { bucket: string, prefix: string },
}

impl std::fmt::Display for StorageLocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(root) => root.display().fmt(formatter),
        }
    }
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
            .into_error(StorageBackendError::from(std::io::Error::new(
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
                .into_error(StorageBackendError::from(std::io::Error::new(
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
            StorageError::OtherIo {
                source: StorageBackendError::Filesystem { source },
                ..
            } => {
                assert_eq!(source.kind(), io::ErrorKind::InvalidInput);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_unsupported_scheme() {
        let err =
            StorageLocation::parse("s3://bucket/path").expect_err("expected unsupported scheme");
        match err {
            StorageError::OtherIo {
                source: StorageBackendError::Filesystem { source },
                ..
            } => {
                assert_eq!(source.kind(), io::ErrorKind::Unsupported);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parse_accepts_local_path() -> TestResult {
        let loc = StorageLocation::parse("/tmp/table")?;
        assert_eq!(loc.to_string(), "/tmp/table");
        match loc {
            StorageLocation::Local(p) => {
                assert_eq!(p, PathBuf::from("/tmp/table"));
            }
        }
        Ok(())
    }
}
