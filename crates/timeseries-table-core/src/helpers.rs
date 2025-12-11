//! High-level helpers for constructing and deriving metadata.
//!
//! This module provides convenience functions and builders that wrap lower-level
//! storage and validation APIs, allowing callers to work with higher-level
//! abstractions without managing error handling or storage-layer details directly.
//!
//! Current helpers:
//! - Parquet-based `SegmentMeta` derivation that reads actual file metadata
//!   (timestamp bounds, row counts) from Parquet footer metadata, rather than
//!   requiring explicit per-segment specification.
//!
//! Future extensions may include:
//! - CSV and ORC format support.
//! - Batch segment registration helpers.
//! - Schema inference from segment metadata.
pub mod parquet;
pub mod schema;
pub mod time_bucket;
