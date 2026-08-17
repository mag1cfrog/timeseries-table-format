//! Parquet format helpers.
//!
//! This module centralizes Parquet-specific logic (schema extraction, segment
//! metadata derivation and coverage computation).

pub mod coverage;
pub mod entity_coverage;
mod entity_rewrite;
pub mod rg_parallel;
pub mod schema;
pub(crate) mod segment_meta;

pub use coverage::*;
pub use entity_coverage::*;
pub use entity_rewrite::*;
pub use rg_parallel::*;
pub use schema::*;
