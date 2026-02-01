//! Parquet format helpers.
//!
//! This module centralizes Parquet-specific logic (schema extraction, segment
//! metadata derivation, coverage compute, and entity identity extraction).

pub mod coverage;
pub mod entity_identity;
pub mod rg_parallel;
pub mod schema;
pub mod segment_meta;

pub use coverage::*;
pub use entity_identity::*;
pub use rg_parallel::*;
pub use schema::*;
pub use segment_meta::*;
