use std::{error::Error, fmt, io};

use snafu::{Backtrace, prelude::*};

/// Errors produced by the storage backend implementation.
///
/// Currently this crate only supports a local filesystem backend; backend-specific
/// I/O errors are wrapped in this enum so higher layers can map them into
/// StorageError variants with additional context.
#[derive(Debug)]
pub enum BackendError {
    /// A local filesystem I/O error.
    Local(io::Error),
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackendError::Local(e) => write!(f, "local I/O error: {e}"),
        }
    }
}

impl Error for BackendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            BackendError::Local(e) => Some(e),
        }
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
        /// Underlying backend error that caused the failure.
        source: BackendError,
        /// The backtrace at the time the error occurred.
        backtrace: Backtrace,
    },

    /// The specified path already exists when creation was requested with
    /// create-new semantics.
    #[snafu(display("Path already exists: {path}"))]
    AlreadyExists {
        /// The path that was found to already exist.
        path: String,
        /// Underlying backend error that indicates the existing resource.
        source: BackendError,
        /// The backtrace captured when the error occurred.
        backtrace: Backtrace,
    },

    /// An I/O error occurred on the local filesystem.
    #[snafu(display("Local I/O error at {path}: {source}"))]
    OtherIo {
        /// The path where the I/O error occurred.
        path: String,
        /// Underlying backend I/O error with platform-specific details.
        source: BackendError,
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
        #[snafu(source)]
        operation_error: Box<StorageError>,
        /// Failure encountered while removing the newly-created object.
        cleanup_error: Box<StorageError>,
        /// The backtrace at the time the cleanup failure was reported.
        backtrace: Backtrace,
    },
}
