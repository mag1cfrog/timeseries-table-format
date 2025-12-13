//! Directory layout and error types for coverage data storage.
//!
//! Coverage data represents which time-bucket ranges are present in the table,
//! enabling efficient gap analysis and query planning. This module defines:
//!
//! - **Directory structure**: Where segment-level and table-level coverage files
//!   are stored within the table directory.
//! - **File extensions**: The standard extension for coverage files (RoaringBitmap).
//! - **Error types**: Errors that can occur when accessing or validating coverage data.
//!
//! # Directory Layout
//!
//! Coverage data is organized as:
//! ```text
//! <table_root>/
//!   _coverage/              (root coverage directory)
//!     segments/             (per-segment coverage snapshots)
//!     table/                (table-level coverage snapshot)
//! ```
//!
//! Each coverage file uses the `.roar` extension (RoaringBitmap binary format).

use std::path::PathBuf;

use snafu::Snafu;

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
/// - Not contain path separators (`/`, `\`) or `..` sequences
/// - Only contain ASCII alphanumeric characters, dots, underscores, and hyphens
pub fn validate_coverage_id(coverage_id: &str) -> Result<(), CoverageLayoutError> {
    if coverage_id.is_empty() || coverage_id.len() > 128 {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    // Reject any path seperator and any ".." component-ish content.
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
    Ok(PathBuf::from(format!(
        "{SEGMENT_COVERAGE_DIR}/{coverage_id}.{COVERAGE_EXT}"
    )))
}

/// Relative path: `_coverage/table/<version>.roar`
pub fn table_snapshot_path(version: u64) -> PathBuf {
    PathBuf::from(format!("{TABLE_SNAPSHOT_DIR}/{version}.{COVERAGE_EXT}"))
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
        let path = table_snapshot_path(42);
        assert_eq!(path, PathBuf::from("_coverage/table/42.roar"));
    }
}
