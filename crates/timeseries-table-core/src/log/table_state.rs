use std::collections::HashMap;

use crate::log::{SegmentId, SegmentMeta, TableMeta};

/// In-memory view of table metadata and live segments, reconstructed from the log.
///
/// Invariant:
/// - `version` matches the CURRENT pointer.
/// - `table_meta` and `segments` are the result of applying all commits from
///   version 1 through `version` in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableState {
    /// Latest committed version recorded in CURRENT.
    pub version: u64,
    /// Table-level metadata reconstructed from the log.
    pub table_meta: TableMeta,
    /// Current live segments keyed by SegmentId.
    pub segments: HashMap<SegmentId, SegmentMeta>,
}
