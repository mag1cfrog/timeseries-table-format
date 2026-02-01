//! On-disk layout helpers for a table root.
//!
//! This module centralizes all *relative* path conventions under a table root:
//! - transaction log directory / commit file naming (`_timeseries_log/`)
//! - coverage sidecar directories (`_coverage/`)
//! - conventional data directory (`data/`)
//!
//! The functions here return relative [`std::path::PathBuf`] values. Callers are
//! expected to join these with a table root (for example, a
//! [`crate::storage::TableLocation`] / backend root) before doing IO.

use std::path::PathBuf;

use snafu::Snafu;

use crate::metadata::table_metadata::TimeBucket;

// ====================
// Data layout
// ====================

/// Conventional directory where segment files are stored (v0.1 default).
pub const DATA_DIR_NAME: &str = "data";

/// Relative path: `data/`
pub fn data_rel_dir() -> PathBuf {
    PathBuf::from(DATA_DIR_NAME)
}

// ====================
// Transaction log layout
// ====================

/// Name of the subdirectory containing the commit log.
pub const LOG_DIR_NAME: &str = "_timeseries_log";

/// Name of the file that stores the current version pointer.
pub const CURRENT_FILE_NAME: &str = "CURRENT";

/// Number of digits used in zero-padded commit file names.
pub const COMMIT_FILENAME_DIGITS: usize = 10;

/// Relative path: `_timeseries_log/`
pub fn log_rel_dir() -> PathBuf {
    PathBuf::from(LOG_DIR_NAME)
}

/// Relative path: `_timeseries_log/CURRENT`
pub fn current_rel_path() -> PathBuf {
    log_rel_dir().join(CURRENT_FILE_NAME)
}

/// Relative path: `_timeseries_log/<zero-padded>.json`
pub fn commit_rel_path(version: u64) -> PathBuf {
    let file_name = format!("{:0width$}.json", version, width = COMMIT_FILENAME_DIGITS);
    log_rel_dir().join(file_name)
}

// ====================
// Coverage layout
// ====================

/// Root directory for coverage data.
pub const COVERAGE_ROOT_DIR: &str = "_coverage";
/// Directory for segment coverage data.
pub const SEGMENT_COVERAGE_DIR: &str = "_coverage/segments";
/// Directory for table snapshot coverage data.
pub const TABLE_SNAPSHOT_DIR: &str = "_coverage/table";
/// File extension for coverage files.
pub const COVERAGE_EXT: &str = "roar";

/// Errors that can occur during coverage layout operations.
#[derive(Debug, Snafu)]
pub enum CoverageLayoutError {
    /// Returned when an invalid coverage ID is provided.
    #[snafu(display("Invalid coverage id: {coverage_id}"))]
    InvalidCoverageId {
        /// The invalid coverage ID.
        coverage_id: String,
    },
}

/// Validates that a coverage ID meets security and format requirements.
///
/// A valid coverage ID must:
/// - Not be empty and not exceed 128 characters
/// - Not contain path separators (`/`, `\\`) or `..` sequences
/// - Only contain ASCII alphanumeric characters, dots, underscores, and hyphens
pub fn validate_coverage_id(coverage_id: &str) -> Result<(), CoverageLayoutError> {
    if coverage_id.is_empty() || coverage_id.len() > 128 {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    // Require at least one alphanumeric
    if !coverage_id.chars().any(|c| c.is_ascii_alphanumeric()) {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    // Reject leading dot
    if coverage_id.starts_with('.') {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    // Reject any path separator and any ".." component-ish content.
    if coverage_id.contains('/') || coverage_id.contains('\\') || coverage_id.contains("..") {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    // Restrict to a conservative ASCII allowlist.
    let ok = coverage_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));

    if !ok {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    Ok(())
}

/// Relative path: `_coverage/segments/<coverage_id>.roar`
pub fn segment_coverage_path(coverage_id: &str) -> Result<PathBuf, CoverageLayoutError> {
    validate_coverage_id(coverage_id)?;
    let mut p = PathBuf::from(COVERAGE_ROOT_DIR);
    p.push("segments");
    p.push(format!("{coverage_id}.{COVERAGE_EXT}"));
    Ok(p)
}

/// Relative path: `_coverage/table/<version>-<snapshot_id>.roar`
pub fn table_snapshot_path(
    version: u64,
    snapshot_id: &str,
) -> Result<PathBuf, CoverageLayoutError> {
    validate_coverage_id(snapshot_id)?;
    let mut p = PathBuf::from(COVERAGE_ROOT_DIR);
    p.push("table");
    p.push(format!("{version}-{snapshot_id}.{COVERAGE_EXT}"));
    Ok(p)
}

fn coverage_id_v1(
    domain_prefix: &[u8],
    output_prefix: &str,
    bucket_spec: &TimeBucket,
    time_column: &str,
    coverage_bytes: &[u8],
) -> String {
    let mut h = blake3::Hasher::new();

    // domain separation
    h.update(domain_prefix);
    h.update(b"\0");

    // stable encoding for TimeBucket (avoid Display/to_string)
    match bucket_spec {
        TimeBucket::Seconds(n) => {
            h.update(b"S");
            h.update(&n.to_le_bytes());
        }
        TimeBucket::Minutes(n) => {
            h.update(b"M");
            h.update(&n.to_le_bytes());
        }
        TimeBucket::Hours(n) => {
            h.update(b"H");
            h.update(&n.to_le_bytes());
        }
        TimeBucket::Days(n) => {
            h.update(b"D");
            h.update(&n.to_le_bytes());
        }
    }

    h.update(b"\0");
    h.update(time_column.as_bytes());
    h.update(b"\0");
    h.update(coverage_bytes);

    let hex = h.finalize().to_hex();
    format!("{output_prefix}-{}", &hex[..32])
}

/// Deterministically derive a safe coverage id for a segment coverage sidecar.
///
/// This produces an id that:
/// - is stable across retries given the same inputs
/// - contains only ASCII [a-z0-9-] characters
/// - is bounded in length and passes validate_coverage_id
pub fn segment_coverage_id_v1(
    bucket_spec: &TimeBucket,
    time_column: &str,
    coverage_bytes: &[u8],
) -> String {
    coverage_id_v1(
        b"segcov-v1",
        "segcov",
        bucket_spec,
        time_column,
        coverage_bytes,
    )
}

/// Deterministically derive a safe coverage id for a table snapshot sidecar.
///
/// This produces an id that:
/// - is stable across retries given the same inputs
/// - contains only ASCII [a-z0-9-] characters
/// - is bounded in length and passes validate_coverage_id
pub fn table_coverage_id_v1(
    bucket_spec: &TimeBucket,
    time_column: &str,
    coverage_bytes: &[u8],
) -> String {
    coverage_id_v1(
        b"tblcov-v1",
        "tblcov",
        bucket_spec,
        time_column,
        coverage_bytes,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_coverage_id_accepts_valid_ids() {
        let long = "a".repeat(128);
        let valid_ids = ["abc", "A_B-1.2", long.as_str()];

        for id in valid_ids {
            validate_coverage_id(id).expect("valid id should pass");
        }
    }

    #[test]
    fn validate_coverage_id_rejects_empty_or_too_long() {
        let too_long = "x".repeat(129);
        assert!(validate_coverage_id("").is_err());
        assert!(validate_coverage_id(&too_long).is_err());
    }

    #[test]
    fn validate_coverage_id_rejects_path_components() {
        for id in ["a/b", "a\\b", "a..b", "..", "../etc"] {
            assert!(validate_coverage_id(id).is_err(), "id `{id}` should fail");
        }
    }

    #[test]
    fn validate_coverage_id_rejects_disallowed_chars() {
        for id in ["space id", "id*", "id@", "id$", "id:"] {
            assert!(validate_coverage_id(id).is_err(), "id `{id}` should fail");
        }
    }

    #[test]
    fn segment_coverage_path_formats_and_validates() {
        let id = "seg-001";
        let path = segment_coverage_path(id).expect("valid id");
        assert_eq!(path, PathBuf::from("_coverage/segments/seg-001.roar"));

        // Ensure validation runs
        assert!(segment_coverage_path("bad/id").is_err());
    }

    #[test]
    fn table_snapshot_path_formats() {
        let path = table_snapshot_path(42, "snap-001").expect("valid snapshot id");
        assert_eq!(path, PathBuf::from("_coverage/table/42-snap-001.roar"));
    }

    #[test]
    fn segment_coverage_id_is_deterministic_and_valid() {
        let bucket = TimeBucket::Minutes(1);
        let time_col = "ts";
        let bytes = b"bitmap-bytes";

        let id1 = segment_coverage_id_v1(&bucket, time_col, bytes);
        let id2 = segment_coverage_id_v1(&bucket, time_col, bytes);

        assert_eq!(id1, id2, "same inputs must produce stable id");
        assert!(id1.starts_with("segcov-"));
        assert_eq!(id1.len(), "segcov-".len() + 32, "prefix + 32 hex chars");
        validate_coverage_id(&id1).expect("derived id should be valid");
    }

    #[test]
    fn segment_coverage_id_changes_with_inputs() {
        let bytes = b"bytes";

        let base = segment_coverage_id_v1(&TimeBucket::Seconds(5), "ts", bytes);
        let different_bucket = segment_coverage_id_v1(&TimeBucket::Hours(5), "ts", bytes);
        let different_column = segment_coverage_id_v1(&TimeBucket::Seconds(5), "event_time", bytes);
        let different_bytes = segment_coverage_id_v1(&TimeBucket::Seconds(5), "ts", b"other");

        assert_ne!(base, different_bucket, "bucket spec should affect id");
        assert_ne!(base, different_column, "time column should affect id");
        assert_ne!(base, different_bytes, "coverage bytes should affect id");
    }

    #[test]
    fn table_coverage_id_is_deterministic_and_valid() {
        let bucket = TimeBucket::Hours(1);
        let time_col = "ts";
        let bytes = b"table-bitmap";

        let id1 = table_coverage_id_v1(&bucket, time_col, bytes);
        let id2 = table_coverage_id_v1(&bucket, time_col, bytes);

        assert_eq!(id1, id2, "same inputs must produce stable id");
        assert!(id1.starts_with("tblcov-"));
        assert_eq!(id1.len(), "tblcov-".len() + 32, "prefix + 32 hex chars");
        validate_coverage_id(&id1).expect("derived id should be valid");
    }

    #[test]
    fn table_coverage_id_changes_with_inputs() {
        let bytes = b"bytes";

        let base = table_coverage_id_v1(&TimeBucket::Minutes(15), "ts", bytes);
        let different_bucket = table_coverage_id_v1(&TimeBucket::Days(1), "ts", bytes);
        let different_column = table_coverage_id_v1(&TimeBucket::Minutes(15), "event_time", bytes);
        let different_bytes = table_coverage_id_v1(&TimeBucket::Minutes(15), "ts", b"other");

        assert_ne!(base, different_bucket, "bucket spec should affect id");
        assert_ne!(base, different_column, "time column should affect id");
        assert_ne!(base, different_bytes, "coverage bytes should affect id");
    }
}
