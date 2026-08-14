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
    coverage::layout::CoverageLayoutError,
    coverage::{
        Coverage,
        serde::{CoverageSerdeError, coverage_from_bytes, coverage_to_bytes},
    },
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
        #[snafu(source, backtrace)]
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
    storage::write_atomic(location.as_ref(), rel_path, &bytes)
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
    storage::write_new(location.as_ref(), rel_path, &bytes)
        .await
        .context(StorageSnafu)?;
    Ok(())
}

/// Write a coverage bitmap as bytes to a sidecar file with exclusive creation.
///
/// # Errors
///
/// Returns [`CoverageError::Storage`] when the storage layer rejects the write,
/// including when the file already exists. Callers that perform idempotent
/// writes may choose to treat an `AlreadyExists` storage error as non-fatal.
pub async fn write_coverage_sidecar_new_bytes(
    location: &TableLocation,
    rel_path: &Path,
    bytes: &[u8],
) -> Result<(), CoverageError> {
    storage::write_new(location.as_ref(), rel_path, bytes)
        .await
        .context(StorageSnafu)?;
    Ok(())
}

/// Read a coverage bitmap from a sidecar file.
///
/// Reads and deserializes a [`Coverage`] instance from a sidecar file at `rel_path`
/// within the table storage. If the file is not found, returns a [`CoverageError::NotFound`]
/// error. If deserialization fails, returns a [`CoverageError::Serde`] error.
///
/// # Arguments
///
/// * `location` - The table storage location.
/// * `rel_path` - The relative path within the table root where the sidecar is located.
///
/// # Returns
///
/// Returns `Ok(coverage)` if the sidecar was read and deserialized successfully,
/// or an error if the file is not found or deserialization fails.
///
/// # Errors
///
/// Returns [`CoverageError`] if:
/// - The file does not exist ([`CoverageError::NotFound`]).
/// - Deserialization of the coverage fails ([`CoverageError::Serde`]).
/// - Storage I/O fails for other reasons ([`CoverageError::Storage`]).
pub async fn read_coverage_sidecar(
    location: &TableLocation,
    rel_path: &Path,
) -> Result<Coverage, CoverageError> {
    match storage::read_all_bytes(location.as_ref(), rel_path).await {
        Ok(bytes) => coverage_from_bytes(&bytes).context(SerdeSnafu),
        Err(StorageError::NotFound { path, .. }) => Err(CoverageError::NotFound { path }),
        Err(e) => Err(CoverageError::Storage { source: e }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{coverage::serde::coverage_from_bytes, storage::StorageLocation};
    use tempfile::TempDir;

    fn temp_location() -> (TempDir, TableLocation) {
        let tmp = TempDir::new().expect("tempdir");
        let loc = TableLocation::local(tmp.path());
        (tmp, loc)
    }

    #[tokio::test]
    async fn write_atomic_overwrites_existing() {
        let (_tmp, loc) = temp_location();
        let rel = Path::new("_coverage/table/1.roar");

        let cov1 = Coverage::from_iter(vec![1u32, 2, 3]);
        write_coverage_sidecar_atomic(&loc, rel, &cov1)
            .await
            .expect("first write");

        // Overwrite with different coverage
        let cov2 = Coverage::from_iter(vec![10u32, 11]);
        write_coverage_sidecar_atomic(&loc, rel, &cov2)
            .await
            .expect("overwrite");

        // Read back and verify it matches the second write
        let abs = match &loc.as_ref() {
            StorageLocation::Local(root) => root.join(rel),
        };
        let bytes = std::fs::read(abs).expect("read file");
        let restored = coverage_from_bytes(&bytes).expect("deserialize");
        assert_eq!(cov2.present(), restored.present());
    }

    #[tokio::test]
    async fn write_new_fails_if_exists() {
        let (_tmp, loc) = temp_location();
        let rel = Path::new("_coverage/segments/seg-1.roar");

        let cov = Coverage::from_iter(vec![5u32]);
        write_coverage_sidecar_new(&loc, rel, &cov)
            .await
            .expect("first write");

        let err = write_coverage_sidecar_new(&loc, rel, &cov)
            .await
            .expect_err("second write should fail");

        match err {
            CoverageError::Storage {
                source: StorageError::AlreadyExists { .. },
                ..
            } => {}
            _ => panic!("expected AlreadyExists storage error"),
        }
    }

    #[tokio::test]
    async fn read_sidecar_round_trip() {
        let (_tmp, loc) = temp_location();
        let rel = Path::new("_coverage/table/2.roar");

        let cov = Coverage::from_iter(vec![1u32, 3, 5, 7]);
        write_coverage_sidecar_atomic(&loc, rel, &cov)
            .await
            .expect("write sidecar");

        let restored = read_coverage_sidecar(&loc, rel)
            .await
            .expect("read sidecar");
        assert_eq!(cov.present(), restored.present());
    }

    #[tokio::test]
    async fn read_sidecar_missing_returns_not_found() {
        let (_tmp, loc) = temp_location();
        let rel = Path::new("_coverage/table/missing.roar");

        let err = read_coverage_sidecar(&loc, rel)
            .await
            .expect_err("should be missing");

        match err {
            CoverageError::NotFound { path } => {
                assert!(path.contains("missing.roar"));
            }
            _ => panic!("expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn read_sidecar_corrupt_bytes_returns_serde_error() {
        let (tmp, loc) = temp_location();
        let rel = Path::new("_coverage/table/corrupt.roar");

        // Write garbage bytes to the expected path
        let abs = match &loc.as_ref() {
            StorageLocation::Local(root) => root.join(rel),
        };
        std::fs::create_dir_all(abs.parent().unwrap()).expect("create dirs");
        std::fs::write(&abs, b"not a bitmap").expect("write corrupt");

        let err = read_coverage_sidecar(&loc, rel)
            .await
            .expect_err("should fail to deserialize");

        match err {
            CoverageError::Serde { .. } => {}
            _ => panic!("expected Serde error"),
        }

        drop(tmp); // ensure tempdir not optimized away
    }
}
