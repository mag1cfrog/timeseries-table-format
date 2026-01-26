//! Metadata layer.
//!
//! This module groups the durable metadata model and the append-only metadata
//! log APIs (optimistic concurrency control, table state materialization, ...).
//!
//! For now these are thin re-exports over existing modules to allow an
//! incremental refactor.

pub mod log;
pub mod model;

