//! Directory and file layout conventions for table storage.
//!
//! This module defines the standardized structure of the table's directory tree,
//! including where segments, metadata, logs, and coverage data are stored.
//! It provides constants for well-known directory names and file extensions,
//! as well as errors that can occur during layout operations.
//!
//! # Submodules
//!
//! - [`coverage`]: Layout and errors for coverage data storage.

pub mod coverage;
