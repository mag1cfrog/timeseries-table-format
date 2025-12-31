//! Helpers for extracting metadata from Parquet files.
//!
//! This module provides [`segment_meta_from_parquet_location`], an async function
//! that reads a Parquet file and constructs a [`SegmentMeta`] with:
//! - Minimum and maximum timestamps from a designated time column
//! - Row count from file metadata
//! - Segment ID and path supplied by the caller
//!
//! The timestamp extraction uses a two-stage approach:
//! 1. **Fast path**: Extract min/max from row-group statistics (if present)
//! 2. **Fallback**: Full row scan if statistics are missing or incomplete
//!
//! Timestamp units (millis, micros, nanos) are automatically detected from the
//! Parquet column's logical type. Both little-endian i64 byte encoding and
//! chrono's timestamp range are validated during conversion.
//!
//! Errors are reported with detailed context (file path, column name, specific
//! mismatch details) to aid debugging of Parquet schema mismatches or corruption.
mod segment_meta;
pub use segment_meta::*;

mod schema;
pub use schema::*;
