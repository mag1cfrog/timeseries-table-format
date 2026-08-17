//! Segment IO error types.
//!
//! The canonical segment metadata model lives in [`crate::metadata::segments`]
//! and contains **no storage IO**.
//!
//! This module maps storage failures into the segment errors returned by
//! format-specific readers.

use snafu::{Backtrace, prelude::*};

use crate::storage::StorageError;

// Expose the pure segment types alongside their IO-layer errors.
pub use crate::metadata::segments::{
    FileFormat, SegmentEntityLayout, SegmentMeta, SegmentMetaError,
};

/// IO-layer errors when constructing/validating segments.
#[derive(Debug, Snafu)]
pub enum SegmentIoError {
    /// The file is missing or not a regular file.
    #[snafu(display("Segment file missing or not a regular file: {path}"))]
    MissingFile {
        /// The path to the missing or invalid file.
        path: String,
        /// Backtrace for debugging.
        backtrace: Backtrace,
    },

    /// Generic I/O error while validating the segment.
    #[snafu(display("I/O error while validating segment at {path}: {source}"))]
    Storage {
        /// The path to the file that caused the I/O error.
        path: String,
        /// Underlying storage error that caused this I/O failure.
        #[snafu(source, backtrace)]
        source: StorageError,
    },
}

/// Segment error at the IO boundary: either a storage failure or a pure metadata failure.
#[derive(Debug, Snafu)]
pub enum SegmentError {
    /// Storage / backend error while accessing a segment.
    #[snafu(transparent)]
    Io {
        /// The underlying IO-layer error.
        source: SegmentIoError,
    },

    /// Pure metadata/decoding/validation error.
    #[snafu(transparent)]
    Meta {
        /// The underlying pure metadata error.
        source: SegmentMetaError,
    },
}

/// Convenience alias for results returned by IO-layer segment operations.
#[allow(clippy::result_large_err)]
pub type SegmentResult<T> = Result<T, SegmentError>;

/// Convert a lower-level `StorageError` into the corresponding `SegmentError`.
///
/// - `StorageError::NotFound` is mapped to `SegmentIoError::MissingFile`.
/// - All other storage errors are wrapped in `SegmentIoError::Storage`,
///   preserving the original `StorageError` as the source for diagnostics.
pub fn map_storage_error(err: StorageError) -> SegmentError {
    let (is_missing, path) = match &err {
        StorageError::NotFound { path, .. } => (true, path.clone()),
        StorageError::AlreadyExists { path, .. }
        | StorageError::OtherIo { path, .. }
        | StorageError::CleanupFailed { path, .. } => (false, path.clone()),
    };

    if is_missing {
        SegmentIoError::MissingFile {
            path,
            backtrace: Backtrace::capture(),
        }
        .into()
    } else {
        SegmentIoError::Storage { path, source: err }.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use chrono::{DateTime, TimeZone};

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
            .expect_err("version 6 segment metadata must include entity_layout");
        assert!(error.to_string().contains("entity_layout"));
    }
}
