use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    storage,
    transaction_log::{table_state::TableCoveragePointer, *},
};

/// Version number for the `_checkpoints/table_state/*.json` wrie format.
const TABLE_STATE_CHECKPOINT_FORMAT_VERSION: u32 = 1;

/// Versioned checkpoint encoding for table coverage pointers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TableCoveragePointerCheckpointV1 {
    bucket_spec: TimeBucket,
    coverage_path: String,
    version: u64,
}

/// Versioned checkpoint encoding for materialized table state.
///
/// This intentionally uses a sorted `Vec<SegmentMeta>` rather than a `HashMap`
/// so checkpoint bytes are deterministic and easier to inspect.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TableStateCheckpointV1 {
    checkpoint_format_version: u32,
    table_version: u64,
    table_meta: TableMeta,
    live_segments: Vec<SegmentMeta>,
    table_coverage: Option<TableCoveragePointerCheckpointV1>,
}

impl TransactionLogStore {
    async fn load_latest_table_state_checkpoint_version(&self) -> Result<Option<u64>, CommitError> {
        let rel = storage::layout::table_state_checkpoint_latest_rel_path();

        let contents = match storage::read_to_string(self.location().as_ref(), &rel).await {
            Ok(s) => s,
            Err(storage::StorageError::NotFound { .. }) => return Ok(None),
            Err(source) => return Err(CommitError::Storage { source }),
        };

        let trimmed = contents.trim();
        if trimmed.is_empty() {
            return CorruptStateSnafu {
                msg: format!("checkpoint LATEST has empty content at {rel:?}"),
            }
            .fail();
        }

        let version = trimmed
            .parse::<u64>()
            .map_err(|e| CommitError::CorruptState {
                msg: format!("checkpoint LATEST has invalid content {trimmed:?}: {e}"),
                backtrace: Backtrace::capture(),
            })?;

        if version == 0 {
            return CorruptStateSnafu {
                msg: format!("checkpoint LATEST points to invalid version 0 at {rel:?}"),
            }
            .fail();
        }

        Ok(Some(version))
    }

    async fn load_table_state_checkpoint(&self, version: u64) -> Result<TableState, CommitError> {
        let rel = storage::layout::table_state_checkpoint_rel_path(version);

        let json = match storage::read_to_string(self.location().as_ref(), &rel).await {
            Ok(s) => s,
            Err(storage::StorageError::NotFound { .. }) => {
                return CorruptStateSnafu {
                    msg: format!("checkpoint LATEST points to version {version}, but checkpoint file is missing at {rel:?}")
                }.fail();
            }
            Err(source) => return Err(CommitError::Storage { source }),
        };

        let checkpoint: TableStateCheckpointV1 =
            serde_json::from_str(&json).map_err(|e| CommitError::CorruptState {
                msg: format!("failed to parse table-state checkpoint {version}: {e}"),
                backtrace: Backtrace::capture(),
            })?;

        if checkpoint.checkpoint_format_version != TABLE_STATE_CHECKPOINT_FORMAT_VERSION {
            return CorruptStateSnafu {
                msg: format!(
                    "unsupported table-state checkpoint format version {} in checkpoint {}",
                    checkpoint.checkpoint_format_version, version
                ),
            }
            .fail();
        }

        if checkpoint.table_version != version {
            return CorruptStateSnafu {
                msg: format!(
                    "checkpoint file version mismatch: file points to {version}, payload contains {}",
                    checkpoint.table_version
                ),
            }.fail();
        }

        let mut segments = HashMap::with_capacity(checkpoint.live_segments.len());
        for seg in checkpoint.live_segments {
            let segment_id = seg.segment_id.clone();
            if segments.insert(segment_id.clone(), seg).is_some() {
                return CorruptStateSnafu {
                    msg: format!(
                        "checkpoint {version} contains duplicate segement_id {}",
                        segment_id.0
                    ),
                }
                .fail();
            }
        }

        let table_coverage = checkpoint.table_coverage.map(|ptr| TableCoveragePointer {
            bucket_spec: ptr.bucket_spec,
            coverage_path: ptr.coverage_path,
            version: ptr.version,
        });

        if let Some(ptr) = &table_coverage
            && ptr.version > version
        {
            return CorruptStateSnafu {
                msg: format!("checkpoint {version} contains table coverage version {} newer than the checkpoint itself", ptr.version)
            }.fail();
        }

        Ok(TableState {
            version,
            table_meta: checkpoint.table_meta,
            segments,
            table_coverage,
        })
    }

    pub(super) async fn load_latest_table_state_checkpoint(
        &self,
        current_version: u64,
    ) -> Result<Option<TableState>, CommitError> {
        let Some(version) = self.load_latest_table_state_checkpoint_version().await? else {
            return Ok(None);
        };

        if version > current_version {
            return CorruptStateSnafu {
                msg: format!("latest checkpoint version {version} is newer than CURRENT version {current_version}")
            }.fail();
        }

        self.load_table_state_checkpoint(version).await.map(Some)
    }
}
