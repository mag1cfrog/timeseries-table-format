//! Core engine for a log-structured time-series table format.
//!
//! This crate provides the foundational pieces for `timeseries-table-format`.
//!
//! Responsibilities (high level):
//! - A Delta-inspired, append-only metadata log with version-guard optimistic
//!   concurrency control ([`transaction_log`]).
//! - Strongly-typed table metadata that distinguishes time-series tables (with a
//!   [`TimeIndexSpec`]) from generic tables ([`metadata`]).
//! - A [`TimeSeriesTable`] abstraction that exposes a logical view over the log
//!   and segment metadata ([`table`]).
//! - RoaringBitmap-based coverage utilities for reasoning about which time
//!   buckets are present or missing ([`coverage`]).
//! - Filesystem utilities for managing on-disk layout (log directory, CURRENT
//!   pointer, segment paths, etc.) ([`storage`]).
//!
//! Main layers (new paths):
//! - [`metadata`]: metadata model + schema/segment validation (no IO).
//! - [`table`]: the [`table::TimeSeriesTable`] user-facing abstraction.
//! - [`storage`]: storage backend + table-root IO utilities.
//! - [`coverage`]: bitmap-based coverage math and utilities.
//! - [`formats`]: format-specific helpers (currently Parquet).
pub mod coverage;
pub mod formats;
pub mod metadata;
pub mod storage;
pub mod table;
pub mod transaction_log;
