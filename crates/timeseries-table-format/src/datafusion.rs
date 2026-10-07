//! DataFusion integration for `timeseries-table-format`.
//!
//! This module is enabled by the `datafusion` feature. The main entry point is
//! [`TsTableProvider`].

mod ts_table_provider;
/// The DataFusion crate version compatible with this integration.
pub use ::datafusion as engine;
pub use ts_table_provider::TsTableProvider;

/// SQL session defaults shared by Rust applications, the CLI, and Python bindings.
///
/// Parquet predicates are evaluated during decoding to avoid materializing
/// nonmatching rows. For queries through [`TsTableProvider`], disable this with
/// `SET datafusion.execution.parquet.pushdown_filters = false` when needed.
/// Caller-created DataFusion configurations retain their own defaults.
pub fn default_session_config() -> engine::prelude::SessionConfig {
    let mut config = engine::prelude::SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config
}

/// Pretty-print helpers for Arrow record batches (used by examples / CLI output).
pub mod pretty;
