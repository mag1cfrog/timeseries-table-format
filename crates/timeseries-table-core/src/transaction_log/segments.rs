//! Segment IO helpers.
//!
//! The canonical segment metadata model lives in [`crate::metadata::segments`]
//! and contains **no storage IO**.
//!
//! This module is the IO boundary: it re-exports the pure types and provides
//! constructors/validators that touch storage (for example, verifying Parquet
//! magic bytes via a storage backend).

use chrono::{DateTime, Utc};
use snafu::{Backtrace, prelude::*};

use crate::storage::{self, StorageError, TableLocation};

// Re-export pure types for compatibility (`transaction_log::segments::*`).
pub use crate::metadata::segments::{
    FileFormat, SegmentId, SegmentMeta, SegmentMetaError, SegmentMetaResult, segment_id_v1,
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
        | StorageError::AlreadyExistsNoSource { path, .. } => (false, path.clone()),
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

impl SegmentMeta {
    /// Construct a validated Parquet SegmentMeta for a file.
    ///
    /// - `location` describes where the table lives (e.g. local root).
    /// - `path` is the logical path stored in the log (e.g. "data/seg1.parquet"
    ///   or an absolute path).
    ///
    /// This is a v0.1 local-filesystem helper: it relies on `storage::read_head_tail_4`
    /// which currently only supports `TableLocation::Local`.
    pub async fn for_parquet(
        location: &TableLocation,
        segment_id: SegmentId,
        path: &str,
        ts_min: DateTime<Utc>,
        ts_max: DateTime<Utc>,
        row_count: u64,
    ) -> SegmentResult<Self> {
        // Use storage layer to get len + first/last 4 bytes.
        let probe = storage::read_head_tail_4(location.as_ref(), std::path::Path::new(path))
            .await
            .map_err(map_storage_error)?;

        if probe.len < 8 {
            return Err(SegmentMetaError::TooShort {
                path: path.to_string(),
            }
            .into());
        }

        const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

        if &probe.head != PARQUET_MAGIC || &probe.tail != PARQUET_MAGIC {
            return Err(SegmentMetaError::InvalidMagic {
                path: path.to_string(),
            }
            .into());
        }

        Ok(SegmentMeta {
            segment_id,
            path: path.to_string(),
            format: FileFormat::Parquet,
            ts_min,
            ts_max,
            row_count,
            file_size: Some(probe.len),
            coverage_path: None,
        })
    }

    /// Format-dispatching constructor that can grow in future versions.
    ///
    /// v0.1: only `FileFormat::Parquet` is supported and validated via
    /// `for_parquet`.
    pub async fn new_validated(
        location: &TableLocation,
        segment_id: SegmentId,
        path: &str,
        format: FileFormat,
        ts_min: DateTime<Utc>,
        ts_max: DateTime<Utc>,
        row_count: u64,
    ) -> SegmentResult<Self> {
        match format {
            FileFormat::Parquet => {
                SegmentMeta::for_parquet(location, segment_id, path, ts_min, ts_max, row_count)
                    .await
            } // other => UnsupportedFormatSnafu { format: other }.fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone};
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

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
            segment_id: SegmentId("seg-001".to_string()),
            path: "data/seg-001.parquet".to_string(),
            format: FileFormat::Parquet,
            ts_min: utc_datetime(2025, 1, 1, 0, 0, 0),
            ts_max: utc_datetime(2025, 1, 1, 1, 0, 0),
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

    async fn write_bytes(path: &std::path::Path, bytes: &[u8]) -> Result<(), std::io::Error> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(path, bytes).await
    }

    fn into_boxed(err: SegmentError) -> Box<dyn std::error::Error> {
        Box::new(err)
    }

    #[tokio::test]
    async fn parquet_segment_validation_succeeds() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = "data/valid.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_bytes(&abs_path, b"PAR1PAR1").await?;

        let ts_min = utc_datetime(2025, 1, 1, 0, 0, 0);
        let ts_max = utc_datetime(2025, 1, 1, 1, 0, 0);

        let meta = SegmentMeta::for_parquet(
            &location,
            SegmentId("seg-001".to_string()),
            rel_path,
            ts_min,
            ts_max,
            1_234,
        )
        .await
        .map_err(into_boxed)?;

        assert_eq!(meta.path, rel_path);
        assert_eq!(meta.segment_id, SegmentId("seg-001".to_string()));
        assert_eq!(meta.format, FileFormat::Parquet);
        assert_eq!(meta.ts_min, ts_min);
        assert_eq!(meta.ts_max, ts_max);
        assert_eq!(meta.row_count, 1_234);
        assert_eq!(meta.file_size, Some(8));

        Ok(())
    }

    #[tokio::test]
    async fn parquet_segment_missing_file_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());

        let result = SegmentMeta::for_parquet(
            &location,
            SegmentId("missing".to_string()),
            "data/missing.parquet",
            utc_datetime(2025, 1, 1, 0, 0, 0),
            utc_datetime(2025, 1, 1, 1, 0, 0),
            42,
        )
        .await;

        assert!(matches!(
            result,
            Err(SegmentError::Io {
                source: SegmentIoError::MissingFile { .. }
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn parquet_segment_too_short_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = "data/short.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_bytes(&abs_path, b"PAR1").await?;

        let result = SegmentMeta::for_parquet(
            &location,
            SegmentId("short".to_string()),
            rel_path,
            utc_datetime(2025, 1, 1, 0, 0, 0),
            utc_datetime(2025, 1, 1, 1, 0, 0),
            10,
        )
        .await;

        assert!(matches!(
            result,
            Err(SegmentError::Meta {
                source: SegmentMetaError::TooShort { .. }
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn parquet_segment_invalid_magic_returns_error() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = "data/invalid_magic.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_bytes(&abs_path, b"PAR1NOPE").await?;

        let result = SegmentMeta::for_parquet(
            &location,
            SegmentId("bad".to_string()),
            rel_path,
            utc_datetime(2025, 1, 1, 0, 0, 0),
            utc_datetime(2025, 1, 1, 1, 0, 0),
            10,
        )
        .await;

        assert!(matches!(
            result,
            Err(SegmentError::Meta {
                source: SegmentMetaError::InvalidMagic { .. }
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn new_validated_delegates_to_parquet_constructor() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let rel_path = "data/delegate.parquet";
        let abs_path = tmp.path().join(rel_path);
        write_bytes(&abs_path, b"PAR1PAR1").await?;

        let ts_min = utc_datetime(2025, 1, 1, 0, 0, 0);
        let ts_max = utc_datetime(2025, 1, 1, 1, 0, 0);

        let meta = SegmentMeta::new_validated(
            &location,
            SegmentId("delegate".to_string()),
            rel_path,
            FileFormat::Parquet,
            ts_min,
            ts_max,
            5,
        )
        .await
        .map_err(into_boxed)?;

        assert_eq!(meta.path, rel_path);
        assert_eq!(meta.format, FileFormat::Parquet);
        assert_eq!(meta.row_count, 5);
        Ok(())
    }
}
