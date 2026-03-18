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

// Coverage layout lives under `coverage/` but is re-exported here for convenience
// and to keep older imports compiling.
pub use crate::coverage::layout::{
    COVERAGE_EXT, COVERAGE_ROOT_DIR, CoverageLayoutError, SEGMENT_COVERAGE_DIR, TABLE_SNAPSHOT_DIR,
    segment_coverage_id_v1, segment_coverage_path, table_coverage_id_v1, table_snapshot_path,
    validate_coverage_id,
};

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
// Checkpoint layout
// ====================

/// Root directory for immutable checkpoint files.
pub const CHECKPOINTS_DIR_NAME: &str = "_checkpoints";

/// Subdirectory for table-state cehckpoints.
pub const TABLE_STATE_CHECKPOINT_DIR_NAME: &str = "table_state";

/// File that points to the latest table-state checkpoint version.
pub const LATEST_CHECKPOINT_FILE_NAME: &str = "LATEST";

/// Reuse the same zero-padding width as commit files.
pub const CHECKPOINT_FILENAME_DIGITS: usize = COMMIT_FILENAME_DIGITS;

/// Relative path: `_checkpoints/`
pub fn checkpoints_rel_dir() -> PathBuf {
    PathBuf::from(CHECKPOINTS_DIR_NAME)
}

/// Relative path: `_checkpoints/table_state/`
pub fn table_state_checkpoint_rel_dir() -> PathBuf {
    checkpoints_rel_dir().join(TABLE_STATE_CHECKPOINT_DIR_NAME)
}

/// Relative path: `_checkpoints/table_state/LATEST`
pub fn table_state_checkpoint_latest_rel_path() -> PathBuf {
    table_state_checkpoint_rel_dir().join(LATEST_CHECKPOINT_FILE_NAME)
}

/// Relative path: `_checkpoints/table_state/<zero-padded>.json`
pub fn table_state_checkpoint_rel_path(version: u64) -> PathBuf {
    let file_name = format!(
        "{:0width$}.json",
        version,
        width = CHECKPOINT_FILENAME_DIGITS
    );
    table_state_checkpoint_rel_dir().join(file_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- constants -----

    #[test]
    fn data_dir_name_value() {
        assert_eq!(DATA_DIR_NAME, "data");
    }

    #[test]
    fn log_dir_name_value() {
        assert_eq!(LOG_DIR_NAME, "_timeseries_log");
    }

    #[test]
    fn current_file_name_value() {
        assert_eq!(CURRENT_FILE_NAME, "CURRENT");
    }

    #[test]
    fn commit_filename_digits_value() {
        assert_eq!(COMMIT_FILENAME_DIGITS, 10);
    }

    #[test]
    fn checkpoints_dir_name_value() {
        assert_eq!(CHECKPOINTS_DIR_NAME, "_checkpoints");
    }

    #[test]
    fn table_state_checkpoint_dir_name_value() {
        assert_eq!(TABLE_STATE_CHECKPOINT_DIR_NAME, "table_state");
    }

    #[test]
    fn latest_checkpoint_file_name_value() {
        assert_eq!(LATEST_CHECKPOINT_FILE_NAME, "LATEST");
    }

    #[test]
    fn checkpoint_filename_digits_equals_commit_filename_digits() {
        assert_eq!(CHECKPOINT_FILENAME_DIGITS, COMMIT_FILENAME_DIGITS);
    }

    // ----- data layout -----

    #[test]
    fn data_rel_dir_path() {
        assert_eq!(data_rel_dir(), PathBuf::from("data"));
    }

    // ----- transaction log layout -----

    #[test]
    fn log_rel_dir_path() {
        assert_eq!(log_rel_dir(), PathBuf::from("_timeseries_log"));
    }

    #[test]
    fn current_rel_path_literal() {
        assert_eq!(current_rel_path(), PathBuf::from("_timeseries_log/CURRENT"));
    }

    #[test]
    fn commit_rel_path_zero() {
        assert_eq!(
            commit_rel_path(0),
            PathBuf::from("_timeseries_log/0000000000.json")
        );
    }

    #[test]
    fn commit_rel_path_typical() {
        assert_eq!(
            commit_rel_path(42),
            PathBuf::from("_timeseries_log/0000000042.json")
        );
    }

    #[test]
    fn commit_rel_path_max_unpadded() {
        // Exactly 10 digits — no padding needed, no truncation.
        assert_eq!(
            commit_rel_path(9_999_999_999),
            PathBuf::from("_timeseries_log/9999999999.json")
        );
    }

    #[test]
    fn commit_rel_path_exceeds_padding_width() {
        // Values wider than COMMIT_FILENAME_DIGITS must not be truncated.
        assert_eq!(
            commit_rel_path(10_000_000_000),
            PathBuf::from("_timeseries_log/10000000000.json")
        );
    }

    // ----- checkpoint layout -----

    #[test]
    fn checkpoints_rel_dir_path() {
        assert_eq!(checkpoints_rel_dir(), PathBuf::from("_checkpoints"));
    }

    #[test]
    fn table_state_checkpoint_rel_dir_path() {
        assert_eq!(
            table_state_checkpoint_rel_dir(),
            PathBuf::from("_checkpoints/table_state")
        );
    }

    #[test]
    fn table_state_checkpoint_latest_rel_path_literal() {
        assert_eq!(
            table_state_checkpoint_latest_rel_path(),
            PathBuf::from("_checkpoints/table_state/LATEST")
        );
    }

    #[test]
    fn table_state_checkpoint_rel_path_zero() {
        assert_eq!(
            table_state_checkpoint_rel_path(0),
            PathBuf::from("_checkpoints/table_state/0000000000.json")
        );
    }

    #[test]
    fn table_state_checkpoint_rel_path_typical() {
        assert_eq!(
            table_state_checkpoint_rel_path(7),
            PathBuf::from("_checkpoints/table_state/0000000007.json")
        );
    }

    #[test]
    fn table_state_checkpoint_rel_path_max_unpadded() {
        assert_eq!(
            table_state_checkpoint_rel_path(9_999_999_999),
            PathBuf::from("_checkpoints/table_state/9999999999.json")
        );
    }

    #[test]
    fn table_state_checkpoint_rel_path_exceeds_padding_width() {
        assert_eq!(
            table_state_checkpoint_rel_path(10_000_000_000),
            PathBuf::from("_checkpoints/table_state/10000000000.json")
        );
    }
}
