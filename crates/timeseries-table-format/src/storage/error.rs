use std::io;

use snafu::{Backtrace, prelude::*};

/// Errors returned by a concrete storage backend.
#[derive(Debug, Snafu)]
pub enum StorageBackendError {
    /// Local filesystem operation failed.
    #[snafu(display("Filesystem error: {source}"))]
    Filesystem {
        /// Underlying filesystem error.
        source: io::Error,
    },
}

impl From<io::Error> for StorageBackendError {
    fn from(source: io::Error) -> Self {
        Self::Filesystem { source }
    }
}

/// Errors that can occur during storage operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum StorageError {
    /// The specified path was not found.
    #[snafu(display("Path not found: {path}"))]
    NotFound {
        /// The path that was not found.
        path: String,
        /// Underlying storage backend error that caused the failure.
        source: StorageBackendError,
        /// The backtrace at the time the error occurred.
        backtrace: Backtrace,
    },

    /// The specified path already exists when creation was requested with
    /// create-new semantics.
    #[snafu(display("Path already exists: {path}"))]
    AlreadyExists {
        /// The path that was found to already exist.
        path: String,
        /// Underlying storage backend error that indicates the existing resource.
        source: StorageBackendError,
        /// The backtrace captured when the error occurred.
        backtrace: Backtrace,
    },

    /// An I/O error occurred in the storage backend.
    #[snafu(display("Storage I/O error at {path}: {source}"))]
    OtherIo {
        /// The path where the I/O error occurred.
        path: String,
        /// Underlying storage backend error with platform-specific details.
        source: StorageBackendError,
        /// The backtrace at the time the error occurred.
        backtrace: Backtrace,
    },

    /// A newly-created object could not be removed after its write failed.
    #[snafu(display(
        "Storage operation failed at {path}: {operation_error}; cleanup also failed: {cleanup_error}"
    ))]
    CleanupFailed {
        /// Path of the object that may remain after the failed cleanup.
        path: String,
        /// Original write or sync failure.
        #[snafu(source, backtrace)]
        operation_error: Box<StorageError>,
        /// Failure encountered while removing the newly-created object.
        cleanup_error: Box<StorageError>,
    },
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use snafu::ErrorCompat;

    use super::*;

    fn filesystem_source(error: &StorageError) -> &io::Error {
        error
            .source()
            .and_then(|source| source.downcast_ref::<StorageBackendError>())
            .and_then(|source| source.source())
            .and_then(|source| source.downcast_ref::<io::Error>())
            .expect("filesystem source")
    }

    #[test]
    fn storage_errors_preserve_filesystem_sources() {
        let cases = [
            (
                StorageError::NotFound {
                    path: "missing.parquet".to_string(),
                    source: io::Error::from(io::ErrorKind::NotFound).into(),
                    backtrace: Backtrace::capture(),
                },
                io::ErrorKind::NotFound,
            ),
            (
                StorageError::AlreadyExists {
                    path: "existing.parquet".to_string(),
                    source: io::Error::from(io::ErrorKind::AlreadyExists).into(),
                    backtrace: Backtrace::capture(),
                },
                io::ErrorKind::AlreadyExists,
            ),
            (
                StorageError::OtherIo {
                    path: "denied.parquet".to_string(),
                    source: io::Error::from(io::ErrorKind::PermissionDenied).into(),
                    backtrace: Backtrace::capture(),
                },
                io::ErrorKind::PermissionDenied,
            ),
        ];

        for (error, expected_kind) in cases {
            assert_eq!(filesystem_source(&error).kind(), expected_kind);
            assert!(ErrorCompat::backtrace(&error).is_some());
        }
    }

    #[test]
    fn cleanup_failure_delegates_to_the_operation_backtrace() {
        let operation = StorageError::OtherIo {
            path: "data/segment.parquet".to_string(),
            source: io::Error::other("write failed").into(),
            backtrace: Backtrace::capture(),
        };
        let cleanup = StorageError::OtherIo {
            path: "data/segment.parquet".to_string(),
            source: io::Error::other("cleanup failed").into(),
            backtrace: Backtrace::capture(),
        };
        let error = StorageError::CleanupFailed {
            path: "data/segment.parquet".to_string(),
            operation_error: Box::new(operation),
            cleanup_error: Box::new(cleanup),
        };

        let wrapper_backtrace = ErrorCompat::backtrace(&error).expect("wrapper backtrace");
        let (operation, cleanup) = match &error {
            StorageError::CleanupFailed {
                operation_error,
                cleanup_error,
                ..
            } => (operation_error.as_ref(), cleanup_error.as_ref()),
            _ => unreachable!(),
        };
        let operation_backtrace = ErrorCompat::backtrace(operation).expect("operation backtrace");
        assert!(std::ptr::eq(wrapper_backtrace, operation_backtrace));
        assert_eq!(filesystem_source(operation).to_string(), "write failed");
        assert_eq!(filesystem_source(cleanup).to_string(), "cleanup failed");
    }
}
