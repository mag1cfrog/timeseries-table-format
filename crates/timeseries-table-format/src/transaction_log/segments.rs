//! Segment access error types.
//!
//! The canonical segment metadata model lives in [`crate::metadata::segments`]
//! and contains **no storage IO**.
//!
//! This module maps storage failures into the segment errors returned by
//! format-specific readers.

use snafu::prelude::*;

use crate::storage::StorageError;

// Expose the pure segment types alongside their access errors.
pub use crate::metadata::segments::{
    FileFormat, SegmentEntityLayout, SegmentMeta, SegmentMetaError,
};

/// Errors while reading or validating one segment.
#[derive(Debug, Snafu)]
pub enum SegmentError {
    /// The segment file is missing.
    #[snafu(display("Segment file not found: {path}"))]
    MissingFile {
        /// Path to the missing file.
        path: String,
        /// Storage failure that identified the missing file.
        #[snafu(source, backtrace)]
        source: StorageError,
    },

    /// Another storage failure occurred while accessing the segment.
    #[snafu(display("Storage error while accessing segment at {path}: {source}"))]
    Storage {
        /// Path to the segment that could not be accessed.
        path: String,
        /// Underlying storage failure.
        #[snafu(source, backtrace)]
        source: StorageError,
    },

    /// Pure metadata/decoding/validation error.
    #[snafu(context(false), display("{source}"))]
    Metadata {
        /// The underlying pure metadata error.
        #[snafu(source, backtrace)]
        source: SegmentMetaError,
    },
}

/// Convenience alias for results returned by segment access operations.
#[allow(clippy::result_large_err)]
pub type SegmentResult<T> = Result<T, SegmentError>;

impl From<StorageError> for SegmentError {
    fn from(source: StorageError) -> Self {
        let is_missing = matches!(&source, StorageError::NotFound { .. });
        let path = match &source {
            StorageError::NotFound { path, .. }
            | StorageError::AlreadyExists { path, .. }
            | StorageError::OtherIo { path, .. }
            | StorageError::CleanupFailed { path, .. } => path.clone(),
        };

        if is_missing {
            Self::MissingFile { path, source }
        } else {
            Self::Storage { path, source }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error as _, io};

    use super::*;
    use chrono::Utc;
    use chrono::{DateTime, TimeZone};
    use snafu::{Backtrace, ErrorCompat};

    use crate::storage::StorageBackendError;

    fn utc_datetime(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
            .single()
            .expect("valid UTC timestamp")
    }

    fn sample_segment_meta() -> SegmentMeta {
        SegmentMeta {
            path: "data/seg-001.parquet".to_string(),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: (utc_datetime(2025, 1, 1, 0, 0, 0)).into(),
            index_max: (utc_datetime(2025, 1, 1, 1, 0, 0)).into(),
            row_count: 123,
            file_size: None,
            coverage_path: None,
        }
    }

    #[test]
    fn segment_meta_json_roundtrip_with_and_without_coverage_path() {
        // Without coverage_path
        let seg = sample_segment_meta();
        let json = serde_json::to_string(&seg).unwrap();
        let back: SegmentMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(back.coverage_path, None);
        assert_eq!(back.file_size, None);

        // With coverage_path
        let mut seg2 = sample_segment_meta().with_coverage_path("_coverage/segments/a.roar");
        seg2.file_size = Some(42);
        let json2 = serde_json::to_string(&seg2).unwrap();
        let back2: SegmentMeta = serde_json::from_str(&json2).unwrap();
        assert_eq!(
            back2.coverage_path.as_deref(),
            Some("_coverage/segments/a.roar")
        );
        assert_eq!(back2.file_size, Some(42));
    }

    #[test]
    fn segment_meta_json_requires_entity_layout() {
        let mut value = serde_json::to_value(sample_segment_meta()).unwrap();
        value
            .as_object_mut()
            .expect("segment metadata is an object")
            .remove("entity_layout");

        let error = serde_json::from_value::<SegmentMeta>(value)
            .expect_err("segment metadata must include entity_layout");
        assert!(error.to_string().contains("entity_layout"));
    }

    #[test]
    fn missing_segment_preserves_storage_source_and_backtrace() {
        let storage = StorageError::NotFound {
            path: "data/missing.parquet".to_string(),
            source: io::Error::new(io::ErrorKind::NotFound, "missing").into(),
            backtrace: Backtrace::capture(),
        };
        let error = SegmentError::from(storage);

        let segment_backtrace = ErrorCompat::backtrace(&error).expect("segment backtrace");
        let storage = error
            .source()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("storage source");
        let storage_backtrace = ErrorCompat::backtrace(storage).expect("storage backtrace");
        let backend = storage
            .source()
            .and_then(|source| source.downcast_ref::<StorageBackendError>())
            .expect("storage backend source");
        let io_source = backend
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .expect("io source");

        assert!(matches!(&error, SegmentError::MissingFile { .. }));
        assert_eq!(io_source.kind(), io::ErrorKind::NotFound);
        assert!(std::ptr::eq(segment_backtrace, storage_backtrace));
    }

    #[test]
    fn non_missing_storage_failure_remains_a_storage_error() {
        let storage = StorageError::OtherIo {
            path: "data/unreadable.parquet".to_string(),
            source: io::Error::from(io::ErrorKind::PermissionDenied).into(),
            backtrace: Backtrace::capture(),
        };

        assert!(matches!(
            SegmentError::from(storage),
            SegmentError::Storage {
                source: StorageError::OtherIo { .. },
                ..
            }
        ));
    }
}
