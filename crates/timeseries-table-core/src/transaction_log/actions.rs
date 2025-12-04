//! Log actions and commit payload definitions.
//!
//! Each commit file stores a [`Commit`] containing ordered [`LogAction`] values
//! that mutate table state: adding/removing segments or updating table metadata.
//! The surrounding modules own the data structures referenced here, while this
//! module focuses on the log’s “verbs”.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::transaction_log::{
    segments::{SegmentId, SegmentMeta},
    table_metadata::TableMetaDelta,
};
/// An action recorded in a commit.
///
/// Each commit contains a sequence of actions that are applied in order to
/// evolve table state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogAction {
    /// Add or replace a segment.
    AddSegment(SegmentMeta),

    /// Remove a segment by its logical ID.
    RemoveSegment {
        /// Logical identifier of the segment to remove.
        segment_id: SegmentId,
    },

    /// Update table-level metadata (v0.1 uses full replacement).
    UpdateTableMeta(TableMetaDelta),
}

/// A single, immutable commit in the metadata log.
///
/// Commits are written to files such as `_timeseries_log/0000000001.json`.
/// The version field must match the file name; `base_version` records what
/// the writer believed was the current version when the commit was prepared.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Commit {
    /// The version number of this commit (monotonic, starting from 1).
    pub version: u64,

    /// The version that the writer believed was current when preparing this
    /// commit. Used by the OCC layer as a guard.
    pub base_version: u64,

    /// Commit creation timestamp, stored as RFC3339 UTC.
    pub timestamp: DateTime<Utc>,

    /// Ordered list of actions that describe how table state changes in this commit.
    pub actions: Vec<LogAction>,
}
