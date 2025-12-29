//! DataFusion integration for `timeseries-table-core`.
//!
//! This crate intentionally keeps all DataFusion types out of `timeseries-table-core`.
//! The main entry point is [`TsTableProvider`].

mod ts_table_provider;
pub use ts_table_provider::TsTableProvider;
