//! Compatibility shim for older `timeseries_table_core::layout::coverage::*` paths.
//!
//! The canonical implementation lives in [`crate::storage::layout`].

pub use crate::storage::layout::{
    COVERAGE_EXT, COVERAGE_ROOT_DIR, CoverageLayoutError, SEGMENT_COVERAGE_DIR, TABLE_SNAPSHOT_DIR,
    segment_coverage_id_v1, segment_coverage_path, table_coverage_id_v1, table_snapshot_path,
    validate_coverage_id,
};
