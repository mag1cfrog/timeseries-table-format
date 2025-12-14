//! Coverage sidecar file management.
//!
//! This module provides helpers for reading and writing coverage data to
//! sidecar files in the table storage directory. It bridges the coverage
//! module (serialization/deserialization) with the storage layer (disk I/O).
//!
//! # Overview
//!
//! Coverage sidecars are stored alongside table data and segments to track which
//! time buckets have been observed. This module abstracts the I/O details:
//!
//! - Serializes [`Coverage`] instances to bytes using the RoaringBitmap format.
//! - Writes bytes to the table storage with atomic or new-only semantics.
//! - Handles errors from layout validation, serialization, and storage layers.
//!
//! # Atomic vs. New-Only Writes
//!
//! - **Atomic**: Writes using [`write_coverage_sidecar_atomic`] are safe for
//!   overwriting existing sidecars (e.g., updating a table snapshot).
//! - **New-Only**: Writes using [`write_coverage_sidecar_new`] fail if the file
//!   already exists (e.g., creating per-segment coverage for the first time).

use std::path::Path;

use snafu::{ResultExt, Snafu};

use crate::{
    coverage::{
        Coverage,
        serde::{CoverageSerdeError, coverage_to_bytes},
    },
    layout::coverage::CoverageLayoutError,
    storage::{self, StorageError, TableLocation},
};

/// Errors that can occur during coverage sidecar operations.
///
/// These errors propagate from lower layers: layout validation, serialization,
/// storage, and file I/O. Callers should inspect the variant to determine
/// the nature of the failure and how to recover.
#[derive(Debug, Snafu)]
pub enum CoverageError {
    /// Layout validation error (e.g., invalid coverage ID or path).
    #[snafu(display("{source}"))]
    Layout {
        /// The underlying layout error.
        source: CoverageLayoutError,
    },

    /// Serialization or deserialization error.
    #[snafu(display("{source}"))]
    Serde {
        /// The underlying serde error.
        source: CoverageSerdeError,
    },

    /// Coverage sidecar file was not found at the expected path.
    #[snafu(display("Coverage sidecar not found: {path}"))]
    NotFound {
        /// The path where the sidecar was expected.
        path: String,
    },

    /// Storage I/O error (read, write, or metadata operations).
    #[snafu(display("Storage error while reading/writing coverage sidecar: {source}"))]
    Storage {
        /// The underlying storage error.
        source: StorageError,
    },
}

/// Write a coverage bitmap to a sidecar file using atomic semantics.
///
/// Atomically writes the given [`Coverage`] to a file at `rel_path` within the
/// table storage. If the file already exists, it will be overwritten. This is
/// suitable for updating table-level coverage snapshots or refreshing segment
/// coverage metadata.
///
/// # Arguments
///
/// * `location` - The table storage location.
/// * `rel_path` - The relative path within the table root where the sidecar should be written.
/// * `cov` - The coverage bitmap to serialize and write.
///
/// # Returns
///
/// Returns `Ok(())` if the sidecar was written successfully, or an error if
/// serialization or storage fails.
///
/// # Errors
///
/// Returns [`CoverageError`] if:
/// - Serialization of the coverage fails ([`CoverageError::Serde`]).
/// - Storage I/O fails ([`CoverageError::Storage`]).
pub async fn write_coverage_sidecar_atomic(
    location: &TableLocation,
    rel_path: &Path,
    cov: &Coverage,
) -> Result<(), CoverageError> {
    let bytes = coverage_to_bytes(cov).context(SerdeSnafu)?;
    storage::write_atomic(location, rel_path, &bytes)
        .await
        .context(StorageSnafu)?;
    Ok(())
}

/// Write a coverage bitmap to a sidecar file with exclusive creation.
///
/// Writes the given [`Coverage`] to a file at `rel_path` within the table storage,
/// but only if the file does not already exist. This is suitable for creating
/// per-segment coverage files for the first time, ensuring that accidental
/// overwrites do not occur.
///
/// # Arguments
///
/// * `location` - The table storage location.
/// * `rel_path` - The relative path within the table root where the sidecar should be written.
/// * `cov` - The coverage bitmap to serialize and write.
///
/// # Returns
///
/// Returns `Ok(())` if the sidecar was created successfully, or an error if
/// the file already exists or if serialization/storage fails.
///
/// # Errors
///
/// Returns [`CoverageError`] if:
/// - Serialization of the coverage fails ([`CoverageError::Serde`]).
/// - The file already exists (storage layer dependent).
/// - Storage I/O fails for other reasons ([`CoverageError::Storage`]).
pub async fn write_coverage_sidecar_new(
    location: &TableLocation,
    rel_path: &Path,
    cov: &Coverage,
) -> Result<(), CoverageError> {
    let bytes = coverage_to_bytes(cov).context(SerdeSnafu)?;
    storage::write_new(location, rel_path, &bytes)
        .await
        .context(StorageSnafu)?;
    Ok(())
}
