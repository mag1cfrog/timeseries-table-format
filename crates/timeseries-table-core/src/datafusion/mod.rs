//! DataFusion integration for `timeseries-table-core`.
//!
//! This module is enabled by the `datafusion` feature. The main entry point is
//! [`TsTableProvider`].

mod ts_table_provider;
pub use ts_table_provider::TsTableProvider;
/// Test-only helpers for integration tests that need internal predicate logic.
pub mod test_utils;

/// Pretty-print helpers for Arrow record batches (used by examples / CLI output).
pub mod pretty;
