//! Filesystem layout and path utilities.
//!
//! This module centralizes all filesystem- and path-related logic for
//! `timeseries-table-core`. It is responsible for mapping a table root
//! directory to the locations of:
//!
//! - The metadata log directory (for example, `<root>/log/`).
//! - Individual commit files (for example, `<root>/log/0000000001.json`).
//! - The `CURRENT` pointer that records the latest committed version.
//! - Data segments (for example, Parquet files) and any directory
//!   structure used to organize them.
//!
//! Goals of this module include:
//!
//! - Keeping path conventions in one place so they can be evolved
//!   without touching higher-level logic.
//! - Providing small helpers for atomic file operations used by the
//!   commit protocol (for example, write-then-rename semantics).
//! - Ensuring that higher-level modules (`log`, `table`) work with
//!   strongly-typed paths and simple helpers instead of hard-coded
//!   string concatenation.
//!
//! This module does not impose any particular storage backend beyond
//! the local filesystem yet, but the API should be designed so that
//! future adapters (for example, object storage) can be introduced
//! without rewriting the log and table logic.
