//! Compatibility shim for older `timeseries_table_core::helpers::segment_entity_identity`.
//!
//! The canonical implementation lives in [`crate::formats::parquet::entity_identity`].

pub use crate::formats::parquet::entity_identity::*;
