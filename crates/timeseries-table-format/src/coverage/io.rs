//! Coverage sidecar file management.
//!
//! This module provides helpers for reading and writing coverage data to
//! sidecar files in the table storage directory. It bridges the coverage
//! module (serialization/deserialization) with the storage layer (disk I/O).
//!
//! # Overview
//!
//! Coverage sidecars are stored alongside table data and segments to track which
//! index intervals have been observed. This module abstracts the I/O details:
//!
//! - Serializes [`Coverage`] instances to bytes using the RoaringTreemap format.
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

use snafu::{Backtrace, Snafu};

use crate::{
    coverage::layout::CoverageLayoutError,
    coverage::{
        Coverage, EntityCoverage,
        serde::{
            CoverageCodecError, coverage_from_bytes, coverage_to_bytes, entity_coverage_from_bytes,
        },
    },
    metadata::schema_compat::SchemaCompatibilityError,
    storage::{self, StorageError, TableLocation},
};

/// Errors that can occur during coverage sidecar operations.
///
/// These errors propagate from lower layers: layout validation, serialization,
/// storage, and file I/O. Callers should inspect the variant to determine
/// the nature of the failure and how to recover.
#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum CoverageSidecarError {
    /// Layout validation error (e.g., invalid coverage ID or path).
    #[snafu(context(false), display("Coverage sidecar layout error: {source}"))]
    Layout {
        /// The underlying layout error.
        source: CoverageLayoutError,
        /// Backtrace captured at the sidecar boundary.
        backtrace: Backtrace,
    },

    /// Coverage serialization or deserialization failed.
    #[snafu(context(false), display("Coverage sidecar codec error: {source}"))]
    Codec {
        /// The underlying coverage codec error.
        #[snafu(source, backtrace)]
        source: CoverageCodecError,
    },

    /// An entity identity does not match the table's configured schema.
    #[snafu(
        context(false),
        display("Entity coverage identity does not match the table schema: {source}")
    )]
    EntityIdentitySchema {
        /// Identity arity or scalar-type mismatch.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)))]
        source: Box<SchemaCompatibilityError>,
        /// Backtrace captured at the sidecar boundary.
        backtrace: Backtrace,
    },

    /// Storage I/O error (read, write, or metadata operations).
    #[snafu(
        context(false),
        display("Storage error while accessing coverage sidecar: {source}")
    )]
    Storage {
        /// The underlying storage error.
        #[snafu(source, backtrace)]
        source: StorageError,
    },
}

impl CoverageSidecarError {
    pub(crate) fn storage_cleanup_failed(&self) -> bool {
        matches!(
            self,
            Self::Storage {
                source: StorageError::CleanupFailed { .. }
            }
        )
    }
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
/// Returns [`CoverageSidecarError`] if:
/// - Serialization of the coverage fails ([`CoverageSidecarError::Codec`]).
/// - Storage I/O fails ([`CoverageSidecarError::Storage`]).
pub async fn write_coverage_sidecar_atomic(
    location: &TableLocation,
    rel_path: &Path,
    cov: &Coverage,
) -> Result<(), CoverageSidecarError> {
    let bytes = coverage_to_bytes(cov)?;
    storage::write_atomic(location.as_ref(), rel_path, &bytes).await?;
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
/// Returns [`CoverageSidecarError`] if:
/// - Serialization of the coverage fails ([`CoverageSidecarError::Codec`]).
/// - The file already exists (storage layer dependent).
/// - Storage I/O fails for other reasons ([`CoverageSidecarError::Storage`]).
pub async fn write_coverage_sidecar_new(
    location: &TableLocation,
    rel_path: &Path,
    cov: &Coverage,
) -> Result<(), CoverageSidecarError> {
    let bytes = coverage_to_bytes(cov)?;
    storage::write_new(location.as_ref(), rel_path, &bytes).await?;
    Ok(())
}

/// Write a coverage bitmap as bytes to a sidecar file with exclusive creation.
///
/// # Errors
///
/// Returns [`CoverageSidecarError::Storage`] when the storage layer rejects the write,
/// including when the file already exists. Callers must not assume that an
/// existing file belongs to the current write attempt.
pub async fn write_coverage_sidecar_new_bytes(
    location: &TableLocation,
    rel_path: &Path,
    bytes: &[u8],
) -> Result<(), CoverageSidecarError> {
    storage::write_new(location.as_ref(), rel_path, bytes).await?;
    Ok(())
}

/// Read a coverage bitmap from a sidecar file.
///
/// Reads and deserializes a [`Coverage`] instance from a sidecar file at `rel_path`
/// within the table storage. Missing files remain [`StorageError::NotFound`]
/// sources inside [`CoverageSidecarError::Storage`].
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
/// Returns [`CoverageSidecarError`] if:
/// - The file does not exist ([`CoverageSidecarError::Storage`]).
/// - Deserialization of the coverage fails ([`CoverageSidecarError::Codec`]).
/// - Storage I/O fails ([`CoverageSidecarError::Storage`]).
pub async fn read_coverage_sidecar(
    location: &TableLocation,
    rel_path: &Path,
) -> Result<Coverage, CoverageSidecarError> {
    let bytes = storage::read_all_bytes(location.as_ref(), rel_path).await?;
    Ok(coverage_from_bytes(&bytes)?)
}

/// Read entity-scoped coverage from a sidecar file.
///
/// # Errors
///
/// Returns [`CoverageSidecarError`] when storage or entity coverage decoding fails.
pub async fn read_entity_coverage_sidecar(
    location: &TableLocation,
    rel_path: &Path,
) -> Result<EntityCoverage, CoverageSidecarError> {
    let bytes = storage::read_all_bytes(location.as_ref(), rel_path).await?;
    Ok(entity_coverage_from_bytes(&bytes)?)
}

#[cfg(test)]
mod tests {
    use std::{error::Error as _, io};

    use snafu::ErrorCompat;

    use super::*;
    use crate::{
        coverage::{
            EntityIdentity,
            serde::{coverage_from_bytes, entity_coverage_to_bytes},
        },
        storage::{StorageBackendError, StorageLocation},
    };
    use tempfile::TempDir;

    fn temp_location() -> (TempDir, TableLocation) {
        let tmp = TempDir::new().expect("tempdir");
        let loc = TableLocation::local(tmp.path());
        (tmp, loc)
    }

    fn storage_source(error: &CoverageSidecarError) -> &StorageError {
        error
            .source()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("storage source")
    }

    fn filesystem_source(error: &StorageError) -> &io::Error {
        error
            .source()
            .and_then(|source| source.downcast_ref::<StorageBackendError>())
            .and_then(|source| source.source())
            .and_then(|source| source.downcast_ref::<io::Error>())
            .expect("filesystem source")
    }

    #[tokio::test]
    async fn write_atomic_overwrites_existing() {
        let (_tmp, loc) = temp_location();
        let rel = Path::new("_coverage/table/1.roar");

        let cov1 = Coverage::from_iter(vec![1u64, 2, 3]);
        write_coverage_sidecar_atomic(&loc, rel, &cov1)
            .await
            .expect("first write");

        // Overwrite with different coverage
        let cov2 = Coverage::from_iter(vec![10u64, 11]);
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

        let cov = Coverage::from_iter(vec![5u64]);
        write_coverage_sidecar_new(&loc, rel, &cov)
            .await
            .expect("first write");

        let err = write_coverage_sidecar_new(&loc, rel, &cov)
            .await
            .expect_err("second write should fail");

        match err {
            CoverageSidecarError::Storage {
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

        let cov = Coverage::from_iter(vec![1u64, 3, 5, 7]);
        write_coverage_sidecar_atomic(&loc, rel, &cov)
            .await
            .expect("write sidecar");

        let restored = read_coverage_sidecar(&loc, rel)
            .await
            .expect("read sidecar");
        assert_eq!(cov.present(), restored.present());
    }

    #[tokio::test]
    async fn read_entity_sidecar_round_trip() {
        let (_tmp, loc) = temp_location();
        let rel = Path::new("_coverage/table/entity.roar");
        let mut coverage = EntityCoverage::empty();
        coverage.union_coverage(
            EntityIdentity::try_new(vec!["A".into()]).unwrap(),
            Coverage::from_iter([1, 2]),
        );
        let bytes = entity_coverage_to_bytes(&coverage).unwrap();
        write_coverage_sidecar_new_bytes(&loc, rel, &bytes)
            .await
            .expect("write entity sidecar");

        assert_eq!(
            read_entity_coverage_sidecar(&loc, rel)
                .await
                .expect("read entity sidecar"),
            coverage
        );
    }

    #[tokio::test]
    async fn read_sidecar_missing_preserves_storage_not_found() {
        let (_tmp, loc) = temp_location();
        let rel = Path::new("_coverage/table/missing.roar");

        let err = read_coverage_sidecar(&loc, rel)
            .await
            .expect_err("should be missing");

        let sidecar_backtrace = ErrorCompat::backtrace(&err).expect("sidecar backtrace");
        let storage = storage_source(&err);
        let storage_backtrace = ErrorCompat::backtrace(storage).expect("storage backtrace");

        assert!(matches!(
            storage,
            StorageError::NotFound { path, .. } if path.contains("missing.roar")
        ));
        assert_eq!(filesystem_source(storage).kind(), io::ErrorKind::NotFound);
        assert!(std::ptr::eq(sidecar_backtrace, storage_backtrace));
    }

    #[tokio::test]
    async fn read_sidecar_corrupt_bytes_returns_codec_error() {
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

        let sidecar_backtrace = ErrorCompat::backtrace(&err).expect("sidecar backtrace");
        let codec = err
            .source()
            .and_then(|source| source.downcast_ref::<CoverageCodecError>())
            .expect("codec source");
        let codec_backtrace = ErrorCompat::backtrace(codec).expect("codec backtrace");

        assert!(matches!(
            codec,
            CoverageCodecError::BitmapDeserialization { .. }
        ));
        assert!(codec.source().is_some());
        assert!(std::ptr::eq(sidecar_backtrace, codec_backtrace));

        drop(tmp); // ensure tempdir not optimized away
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_sidecar_permission_denied_remains_a_storage_error() {
        use std::os::unix::fs::PermissionsExt;

        let (_tmp, loc) = temp_location();
        let rel = Path::new("_coverage/table/denied.roar");
        let absolute = match loc.as_ref() {
            StorageLocation::Local(root) => root.join(rel),
        };
        std::fs::create_dir_all(absolute.parent().unwrap()).expect("create dirs");
        std::fs::write(&absolute, coverage_to_bytes(&Coverage::empty()).unwrap())
            .expect("write sidecar");
        let original_permissions = std::fs::metadata(&absolute).unwrap().permissions();
        let mut denied_permissions = original_permissions.clone();
        denied_permissions.set_mode(0o0);
        std::fs::set_permissions(&absolute, denied_permissions).expect("deny reads");

        let error = read_coverage_sidecar(&loc, rel)
            .await
            .expect_err("read must be denied");
        std::fs::set_permissions(&absolute, original_permissions).expect("restore permissions");

        let storage = storage_source(&error);
        assert!(matches!(storage, StorageError::OtherIo { .. }));
        assert_eq!(
            filesystem_source(storage).kind(),
            io::ErrorKind::PermissionDenied
        );
    }
}
