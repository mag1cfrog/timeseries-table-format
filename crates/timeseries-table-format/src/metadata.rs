//! Metadata layer.
//!
//! This module groups **pure** metadata data types and validation logic.
//!
//! It must not depend on storage backends or perform IO. IO-heavy modules (log
//! replay, storage access, Parquet scanning, etc.) should live in higher layers
//! such as `transaction_log`, `storage`, and `table`.

pub mod logical_schema;
pub mod schema_compat;
pub mod segments;
pub mod table_metadata;

/// Convenience re-exports for the metadata "model" surface.
pub mod model;
