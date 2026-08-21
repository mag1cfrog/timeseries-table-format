//! DataFusion integration for `timeseries-table-format`.
//!
//! This module is enabled by the `datafusion` feature. The main entry point is
//! [`TsTableProvider`].

mod ts_table_provider;
/// The DataFusion crate version compatible with this integration.
pub use ::datafusion as engine;
pub use ts_table_provider::TsTableProvider;

/// Pretty-print helpers for Arrow record batches (used by examples / CLI output).
pub mod pretty;
