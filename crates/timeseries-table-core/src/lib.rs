//! Core engine for a log-structured time-series table format.
//!
//! This crate provides the foundational pieces for `timeseries-table-format`:
//!
//! - A Delta-inspired, append-only metadata log with version-guard
//!   optimistic concurrency control (`log` module).
//! - Strongly-typed table metadata that distinguishes time-series tables
//!   (with a `TimeIndexSpec`) from generic tables.
//! - A `TimeSeriesTable` abstraction that exposes a logical view over
//!   the log and segment metadata (`table` module).
//! - RoaringBitmap-based coverage utilities for reasoning about which
//!   time buckets are present or missing (`coverage` module).
//! - Filesystem utilities for managing on-disk layout (log directory,
//!   CURRENT pointer, segment paths, etc.) (`fs` module).
//!
//! Higher-level integration crates (for example, DataFusion, backtesting
//! tools, or a CLI) are expected to depend on this core crate rather than
//! re-implementing the storage and metadata logic.
#![deny(missing_docs)]
pub mod coverage;
pub mod fs;
pub mod log;
pub mod table;
