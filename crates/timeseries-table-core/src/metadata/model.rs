//! Metadata model types.
//!
//! Thin re-export shim over metadata types currently defined under
//! `transaction_log`.

pub use crate::transaction_log::{
    Commit, FileFormat, LogAction, SegmentId, SegmentMeta, TableKind, TableMeta, TableMetaDelta,
    TableState, TimeBucket, TimeIndexSpec,
};
