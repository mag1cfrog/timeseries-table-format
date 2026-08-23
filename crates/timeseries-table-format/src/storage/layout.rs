//! On-disk layout helpers for a table root.
//!
//! This module centralizes all *relative* path conventions under a table root:
//! - transaction log directory / commit file naming (`_timeseries_log/`)
//!
//! The functions here return relative [`std::path::PathBuf`] values. Callers are
//! expected to join these with a table root (for example, a
//! [`crate::storage::TableLocation`] / backend root) before doing IO.

use std::path::PathBuf;

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
