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
