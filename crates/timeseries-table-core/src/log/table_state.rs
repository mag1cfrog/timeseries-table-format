//! Reconstructing the current table state by replaying log commits.
//!
//! `TableState` materializes the metadata stored in `_timeseries_log/` and the
//! [`LogStore::rebuild_table_state`] helper walks all commits from version 1 up
//! to the `CURRENT` pointer, applying their actions in order. This keeps read
//! logic isolated from the append-only write path and documents the invariant
//! that table readers must see a state consistent with the latest committed
//! version.
use std::collections::HashMap;

use crate::log::*;

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

impl LogStore {
    /// Rebuild the current TableState by replaying all commits up to CURRENT.
    ///
    /// v0.1 behavior:
    /// - If CURRENT == 0 (no commits), this returns CommitError::CorruptState.
    /// - The first commit must include at least one UpdateTableMeta action
    ///   to bootstrap TableMeta; the last UpdateTableMeta wins.
    pub async fn rebuild_table_state(&self) -> Result<TableState, CommitError> {
        let current_version = self.load_current_version().await?;

        if current_version == 0 {
            // v0.1: treat "no commits" as an uninitialized / corrupt table.
            return CorruptStateSnafu {
                msg: "Cannot rebuild TableState: CURRENT is 0 (no commits)".to_string(),
            }
            .fail();
        }

        let mut table_meta: Option<TableMeta> = None;
        let mut segments: HashMap<SegmentId, SegmentMeta> = HashMap::new();

        // Replay all commits from 1..=current_version in order
        for v in 1..=current_version {
            let commit = self.load_commit(v).await?;

            // Defensive: file name version should match payload
            if commit.version != v {
                return CorruptStateSnafu {
                    msg: format!(
                        "Commit version mismatch: expected {v}, found {} in payload",
                        commit.version
                    ),
                }
                .fail();
            }

            for action in commit.actions {
                match action {
                    LogAction::AddSegment(meta) => {
                        // Insert or replace the segment; lastest info wins.
                        segments.insert(meta.segment_id.clone(), meta);
                    }
                    LogAction::RemoveSegment { segment_id } => {
                        segments.remove(&segment_id);
                    }
                    LogAction::UpdateTableMeta(delta) => {
                        // v0.1: full replacement of TableMeta
                        table_meta = Some(delta);
                    }
                }
            }
        }

        let table_meta = table_meta.context(CorruptStateSnafu {
            msg: format!("No TableMeta found in commits up to version {current_version}",),
        })?;

        Ok(TableState {
            version: current_version,
            table_meta,
            segments,
        })
    }
}
