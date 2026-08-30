//! Retention-aware removal of unreferenced table-managed artifacts.

use std::{collections::HashMap, path::Path};

use chrono::{DateTime, Utc};
use parquet::arrow::async_reader::AsyncFileReader;
use snafu::{Backtrace, ResultExt, Snafu};
use uuid::Uuid;

use crate::{
    coverage::layout::{COVERAGE_EXT, SEGMENT_COVERAGE_DIR, TABLE_SNAPSHOT_DIR},
    metadata::protocol::TableProtocolError,
    storage::{self, StorageError, StorageFileMetadata},
    table::{TableError, TimeSeriesTable},
    transaction_log::{CommitError, LogAction, TableKind},
};

/// Whether vacuum reports candidates or removes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum VacuumMode {
    /// Inspect the table without deleting files.
    DryRun,
    /// Delete expired, unreferenced table-managed files.
    Apply,
}

impl VacuumMode {
    /// Return the stable snake-case mode name.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Apply => "apply",
        }
    }
}

/// The action vacuum took or would take for one considered file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum VacuumArtifactDisposition {
    /// Vacuum preserved the file.
    Retained,
    /// Dry-run identified the file as removable.
    Removable,
    /// Apply mode removed the file.
    Deleted,
}

impl VacuumArtifactDisposition {
    /// Return the stable snake-case disposition name.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Retained => "retained",
            Self::Removable => "removable",
            Self::Deleted => "deleted",
        }
    }
}

/// Why vacuum retained or selected one file.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum VacuumArtifactReason {
    /// A retained commit references this path.
    ReferencedByCommit {
        /// Earliest retained commit that references the path.
        version: u64,
    },
    /// The file modification time is at or after the required cutoff.
    WithinRetention,
    /// The file changed after vacuum planned it, so apply preserved it.
    ChangedSincePlanning,
    /// The file is below a scanned directory but does not have a reserved managed shape.
    UnrecognizedArtifact,
    /// The file is expired and no retained commit references it.
    Unreferenced,
    /// The expired, unreferenced Parquet file has no readable valid footer.
    InvalidOrUnreadableParquet,
}

impl VacuumArtifactReason {
    /// Return the stable snake-case reason name.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReferencedByCommit { .. } => "referenced_by_commit",
            Self::WithinRetention => "within_retention",
            Self::ChangedSincePlanning => "changed_since_planning",
            Self::UnrecognizedArtifact => "unrecognized_artifact",
            Self::Unreferenced => "unreferenced",
            Self::InvalidOrUnreadableParquet => "invalid_or_unreadable_parquet",
        }
    }
}

/// Vacuum classification for one file below a scanned directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumArtifact {
    /// Canonical table-relative path.
    pub path: String,
    /// Latest file size observed by this invocation.
    pub size_bytes: u64,
    /// Latest modification time observed by this invocation.
    pub modified_at: DateTime<Utc>,
    /// Action taken or proposed by this invocation.
    pub disposition: VacuumArtifactDisposition,
    /// Reason for the disposition.
    pub reason: VacuumArtifactReason,
}

/// Structured result of one vacuum invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumReport {
    /// Latest validated transaction-log version used for deletion safety.
    pub table_version: u64,
    /// Required exclusive upper bound on removable file modification times.
    pub older_than: DateTime<Utc>,
    /// Requested vacuum behavior.
    pub mode: VacuumMode,
    /// Every regular file considered below `data/` and `_coverage/`.
    pub artifacts: Vec<VacuumArtifact>,
    /// Number of files considered by this invocation.
    pub considered_files: usize,
    /// Number of files retained by this invocation.
    pub retained_files: usize,
    /// Number of files reported as removable by dry-run.
    pub removable_files: usize,
    /// Number of files removed by apply mode.
    pub deleted_files: usize,
    /// Bytes across every considered file.
    pub considered_bytes: u128,
    /// Bytes retained by this invocation.
    pub retained_bytes: u128,
    /// Bytes reported as removable by dry-run.
    pub removable_bytes: u128,
    /// Bytes removed by apply mode.
    pub deleted_bytes: u128,
}

impl VacuumReport {
    fn new(
        table_version: u64,
        older_than: DateTime<Utc>,
        mode: VacuumMode,
        artifacts: Vec<VacuumArtifact>,
    ) -> Self {
        let considered_files = artifacts.len();
        let mut retained_files = 0usize;
        let mut removable_files = 0usize;
        let mut deleted_files = 0usize;
        let mut considered_bytes = 0u128;
        let mut retained_bytes = 0u128;
        let mut removable_bytes = 0u128;
        let mut deleted_bytes = 0u128;
        for artifact in &artifacts {
            let bytes = u128::from(artifact.size_bytes);
            considered_bytes += bytes;
            match artifact.disposition {
                VacuumArtifactDisposition::Retained => {
                    retained_files += 1;
                    retained_bytes += bytes;
                }
                VacuumArtifactDisposition::Removable => {
                    removable_files += 1;
                    removable_bytes += bytes;
                }
                VacuumArtifactDisposition::Deleted => {
                    deleted_files += 1;
                    deleted_bytes += bytes;
                }
            }
        }
        Self {
            table_version,
            older_than,
            mode,
            artifacts,
            considered_files,
            retained_files,
            removable_files,
            deleted_files,
            considered_bytes,
            retained_bytes,
            removable_bytes,
            deleted_bytes,
        }
    }
}

/// Errors owned by a vacuum operation.
#[derive(Debug, Snafu)]
#[snafu(module, visibility(pub(crate)))]
#[non_exhaustive]
pub enum VacuumError {
    /// The retention cutoff is later than the current time.
    #[snafu(display("Vacuum cutoff {older_than} is in the future"))]
    FutureCutoff {
        /// Rejected exclusive retention cutoff.
        older_than: DateTime<Utc>,
    },

    /// The latest table protocol does not permit this client to vacuum.
    #[snafu(context(false), display("Table protocol error: {source}"))]
    Protocol {
        /// Complete table protocol failure.
        #[snafu(source)]
        source: TableProtocolError,
        /// Backtrace captured at the vacuum boundary.
        backtrace: Backtrace,
    },

    /// The latest metadata no longer describes a time-series table.
    #[snafu(display("Latest table kind is {kind:?}, expected a time-series table"))]
    NotTimeSeries {
        /// Rejected table kind.
        kind: TableKind,
    },

    /// Reading or validating retained transaction-log history failed.
    #[snafu(context(false), display("Vacuum transaction-log error: {source}"))]
    Commit {
        /// Complete transaction-log failure.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// Listing or inspecting table-managed storage failed.
    #[snafu(context(false), display("Vacuum storage error: {source}"))]
    Storage {
        /// Complete storage failure.
        #[snafu(source, backtrace)]
        source: StorageError,
    },

    /// Removing one selected file failed.
    #[snafu(display("Failed to delete vacuum candidate {path}: {source}"))]
    Delete {
        /// Canonical table-relative path selected for deletion.
        path: String,
        /// Complete storage failure.
        #[snafu(source, backtrace)]
        source: StorageError,
        /// Report state after every deletion completed before this failure.
        partial_report: Box<VacuumReport>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactKind {
    Parquet,
    Coverage,
    Unrecognized,
}

fn is_canonical_uuid(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|uuid| uuid.hyphenated().to_string() == value)
}

fn is_managed_parquet_path(path: &str) -> bool {
    let Some(path) = path.strip_suffix(".parquet") else {
        return false;
    };

    if let Some(id) = path
        .strip_prefix(storage::layout::APPEND_DATA_DIR)
        .and_then(|path| path.strip_prefix('/'))
    {
        return !id.contains('/') && is_canonical_uuid(id);
    }

    let Some(path) = path
        .strip_prefix(storage::layout::ENTITY_REWRITE_DATA_DIR)
        .and_then(|path| path.strip_prefix('/'))
    else {
        return false;
    };
    let Some((attempt_id, ordinal)) = path.split_once('/') else {
        return false;
    };
    !ordinal.contains('/')
        && is_canonical_uuid(attempt_id)
        && ordinal
            .parse::<usize>()
            .is_ok_and(|value| format!("{value:010}") == ordinal)
}

fn artifact_kind(path: &str) -> ArtifactKind {
    if is_managed_parquet_path(path) {
        return ArtifactKind::Parquet;
    }
    let path = Path::new(path);
    let extension = path.extension().and_then(|extension| extension.to_str());
    if (path.starts_with(SEGMENT_COVERAGE_DIR) || path.starts_with(TABLE_SNAPSHOT_DIR))
        && extension == Some(COVERAGE_EXT)
    {
        ArtifactKind::Coverage
    } else {
        ArtifactKind::Unrecognized
    }
}

async fn parquet_footer_is_valid(table: &TimeSeriesTable, path: &str) -> bool {
    let Ok(mut file) =
        storage::open_parquet_reader(table.location().as_ref(), Path::new(path)).await
    else {
        return false;
    };
    file.get_metadata(None).await.is_ok()
}

async fn retained_paths(
    table: &TimeSeriesTable,
) -> Result<(u64, HashMap<String, u64>), VacuumError> {
    let mut paths = HashMap::new();
    let state = table
        .log
        .replay_table_state(|version, action| match action {
            LogAction::AddSegment(segment) => {
                paths.entry(segment.path.clone()).or_insert(version);
                if let Some(path) = &segment.coverage_path {
                    paths.entry(path.clone()).or_insert(version);
                }
            }
            LogAction::RemoveSegment { path } => {
                paths.entry(path.clone()).or_insert(version);
            }
            LogAction::UpdateTableCoverage { coverage_path, .. } => {
                paths.entry(coverage_path.clone()).or_insert(version);
            }
            LogAction::UpdateTableMeta(_) => {}
        })
        .await
        .map_err(VacuumError::from)?;
    state
        .table_meta
        .ensure_write_compatible()
        .map_err(VacuumError::from)?;
    if !matches!(state.table_meta.kind, TableKind::TimeSeries(_)) {
        return Err(VacuumError::NotTimeSeries {
            kind: state.table_meta.kind,
        });
    }

    Ok((state.version, paths))
}

async fn classify_artifact(
    table: &TimeSeriesTable,
    file: StorageFileMetadata,
    older_than: DateTime<Utc>,
    retained: &HashMap<String, u64>,
) -> VacuumArtifact {
    let modified_at = DateTime::<Utc>::from(file.modified_at);
    let kind = artifact_kind(&file.path);
    let (disposition, reason) = if let Some(version) = retained.get(&file.path) {
        (
            VacuumArtifactDisposition::Retained,
            VacuumArtifactReason::ReferencedByCommit { version: *version },
        )
    } else if kind == ArtifactKind::Unrecognized {
        (
            VacuumArtifactDisposition::Retained,
            VacuumArtifactReason::UnrecognizedArtifact,
        )
    } else if modified_at >= older_than {
        (
            VacuumArtifactDisposition::Retained,
            VacuumArtifactReason::WithinRetention,
        )
    } else {
        match kind {
            ArtifactKind::Unrecognized => (
                VacuumArtifactDisposition::Retained,
                VacuumArtifactReason::UnrecognizedArtifact,
            ),
            ArtifactKind::Parquet if !parquet_footer_is_valid(table, &file.path).await => (
                VacuumArtifactDisposition::Removable,
                VacuumArtifactReason::InvalidOrUnreadableParquet,
            ),
            ArtifactKind::Parquet | ArtifactKind::Coverage => (
                VacuumArtifactDisposition::Removable,
                VacuumArtifactReason::Unreferenced,
            ),
        }
    };
    VacuumArtifact {
        path: file.path,
        size_bytes: file.size_bytes,
        modified_at,
        disposition,
        reason,
    }
}

async fn prepare_candidate_for_delete(
    table: &TimeSeriesTable,
    artifact: &mut VacuumArtifact,
) -> Result<bool, StorageError> {
    let fresh =
        match storage::regular_file_metadata(table.location().as_ref(), Path::new(&artifact.path))
            .await
        {
            Ok(fresh) => fresh,
            Err(StorageError::NotFound { .. }) => {
                artifact.disposition = VacuumArtifactDisposition::Deleted;
                return Ok(false);
            }
            Err(source) => return Err(source),
        };
    let modified_at = DateTime::<Utc>::from(fresh.modified_at);
    // ponytail: size and mtime are the portable identity available today; use backend
    // generation tokens when the storage abstraction exposes them.
    if fresh.size_bytes != artifact.size_bytes || modified_at != artifact.modified_at {
        artifact.size_bytes = fresh.size_bytes;
        artifact.modified_at = modified_at;
        artifact.disposition = VacuumArtifactDisposition::Retained;
        artifact.reason = VacuumArtifactReason::ChangedSincePlanning;
        return Ok(false);
    }
    Ok(true)
}

impl TimeSeriesTable {
    /// Inspect or delete expired files unreachable from retained table history.
    ///
    /// Vacuum considers regular files below `data/` and `_coverage/`. It never
    /// deletes transaction-log files, expires snapshots, or rewrites history.
    /// `older_than` is required and exclusive: files modified at or after the
    /// cutoff are retained, and a future cutoff is rejected. Choose a cutoff
    /// older than the longest expected writer duration so active writers remain
    /// inside the retention window.
    ///
    /// Apply mode may delete earlier candidates before a later deletion error.
    /// [`VacuumError::Delete`] includes the partial report from that attempt.
    #[tracing::instrument(
        name = "table.vacuum",
        target = "timeseries_table_format::table::vacuum",
        level = "debug",
        skip_all,
        fields(
            mode = mode.as_str(),
            table_version = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    pub async fn vacuum(
        &self,
        older_than: DateTime<Utc>,
        mode: VacuumMode,
    ) -> Result<VacuumReport, TableError> {
        let result: Result<VacuumReport, VacuumError> = async {
            if older_than > Utc::now() {
                return Err(VacuumError::FutureCutoff { older_than });
            }
            let (mut table_version, mut retained) = retained_paths(self).await?;
            let mut files =
                storage::list_files(self.location().as_ref(), Path::new("data")).await?;
            files.extend(
                storage::list_files(self.location().as_ref(), Path::new("_coverage")).await?,
            );
            files.sort_by(|left, right| left.path.cmp(&right.path));

            let mut artifacts = Vec::with_capacity(files.len());
            for file in files {
                artifacts.push(classify_artifact(self, file, older_than, &retained).await);
            }

            if mode == VacuumMode::Apply {
                (table_version, retained) = retained_paths(self).await?;
                let mut delete_failure = None;
                for artifact in &mut artifacts {
                    if artifact.disposition != VacuumArtifactDisposition::Removable {
                        continue;
                    }
                    if let Some(version) = retained.get(&artifact.path) {
                        artifact.disposition = VacuumArtifactDisposition::Retained;
                        artifact.reason =
                            VacuumArtifactReason::ReferencedByCommit { version: *version };
                        continue;
                    }
                    match prepare_candidate_for_delete(self, artifact).await {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(source) => {
                            delete_failure = Some((artifact.path.clone(), source));
                            break;
                        }
                    }
                    if let Err(source) = storage::remove_file_if_exists(
                        self.location().as_ref(),
                        Path::new(&artifact.path),
                    )
                    .await
                    {
                        delete_failure = Some((artifact.path.clone(), source));
                        break;
                    }
                    artifact.disposition = VacuumArtifactDisposition::Deleted;
                }
                if let Some((path, source)) = delete_failure {
                    return Err(VacuumError::Delete {
                        path,
                        source,
                        partial_report: Box::new(VacuumReport::new(
                            table_version,
                            older_than,
                            mode,
                            artifacts,
                        )),
                    });
                }
            }

            Ok(VacuumReport::new(
                table_version,
                older_than,
                mode,
                artifacts,
            ))
        }
        .await;

        let span = tracing::Span::current();
        match &result {
            Ok(report) => {
                span.record("table_version", report.table_version);
                span.record(
                    "outcome",
                    match mode {
                        VacuumMode::DryRun => "dry_run",
                        VacuumMode::Apply => "applied",
                    },
                );
            }
            Err(_) => {
                span.record("outcome", "failed");
            }
        }
        result.context(crate::table::error::VacuumSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, FileTimes},
        io::Write as _,
        time::{Duration as StdDuration, SystemTime},
    };

    use chrono::Duration;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        coverage::EntityIdentity,
        storage::{TableLocation, inject_cleanup_failure, layout, open_new_output_sink, write_new},
        table::test_util::{
            TestResult, TestRow, make_basic_table_meta, utc_datetime, write_test_parquet,
        },
        transaction_log::{
            FileFormat, IndexValue, SegmentEntityLayout, SegmentMeta, TransactionLogStore,
        },
    };

    fn artifact<'a>(report: &'a VacuumReport, path: &str) -> &'a VacuumArtifact {
        report
            .artifacts
            .iter()
            .find(|artifact| artifact.path == path)
            .unwrap_or_else(|| panic!("missing vacuum artifact {path}"))
    }

    fn mark_expired(path: &Path) -> std::io::Result<()> {
        fs::OpenOptions::new()
            .write(true)
            .open(path)?
            .set_times(FileTimes::new().set_modified(SystemTime::UNIX_EPOCH))
    }

    fn expired_cutoff() -> DateTime<Utc> {
        DateTime::from(SystemTime::UNIX_EPOCH + StdDuration::from_secs(1))
    }

    fn referenced_segment(
        path: &str,
        coverage_path: &str,
    ) -> Result<SegmentMeta, Box<dyn std::error::Error>> {
        Ok(SegmentMeta {
            path: path.to_string(),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::Single(EntityIdentity::try_new(vec!["A".into()])?),
            index_min: IndexValue::Timestamp(utc_datetime(2025, 1, 1, 0, 0, 0)),
            index_max: IndexValue::Timestamp(utc_datetime(2025, 1, 1, 0, 1, 0)),
            row_count: 2,
            file_size: None,
            coverage_path: Some(coverage_path.to_string()),
        })
    }

    #[tokio::test]
    async fn vacuum_dry_run_and_apply_preserve_retained_history_and_logs() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let historical_data = "data/historical.parquet";
        let historical_coverage = "_coverage/segments/historical.roar";
        let historical_snapshot = "_coverage/table/2-historical.roar";
        for (path, bytes) in [
            (historical_data, b"historical data".as_slice()),
            (historical_coverage, b"historical coverage".as_slice()),
            (historical_snapshot, b"historical snapshot".as_slice()),
        ] {
            write_new(location.as_ref(), Path::new(path), bytes).await?;
        }

        let log = TransactionLogStore::new(location.clone());
        log.commit_with_expected_version(
            1,
            vec![
                LogAction::AddSegment(referenced_segment(historical_data, historical_coverage)?),
                LogAction::UpdateTableCoverage {
                    index_kind: table.index_spec().kind.clone(),
                    coverage_path: historical_snapshot.to_string(),
                },
            ],
        )
        .await?;
        log.commit_with_expected_version(
            2,
            vec![LogAction::RemoveSegment {
                path: historical_data.to_string(),
            }],
        )
        .await?;

        let invalid_orphan = "data/_managed/append/00000000-0000-0000-0000-000000000001.parquet";
        let valid_orphan =
            "data/_staged/entity-rewrite/00000000-0000-0000-0000-000000000002/0000000000.parquet";
        let coverage_orphan = "_coverage/segments/orphan.roar";
        let unrecognized = "data/keep.txt";
        let external_source = "data/00000000-0000-0000-0000-000000000009.parquet";
        write_new(location.as_ref(), Path::new(invalid_orphan), b"incomplete").await?;
        write_test_parquet(
            &temp.path().join(valid_orphan),
            true,
            false,
            &[TestRow {
                ts_millis: 0,
                symbol: "A",
                price: 1.0,
            }],
        )?;
        write_new(
            location.as_ref(),
            Path::new(coverage_orphan),
            b"orphan coverage",
        )
        .await?;
        write_new(location.as_ref(), Path::new(unrecognized), b"keep").await?;
        write_test_parquet(
            &temp.path().join(external_source),
            true,
            false,
            &[TestRow {
                ts_millis: 60_000,
                symbol: "A",
                price: 2.0,
            }],
        )?;
        for path in [
            invalid_orphan,
            valid_orphan,
            coverage_orphan,
            unrecognized,
            external_source,
        ] {
            mark_expired(&temp.path().join(path))?;
        }

        let log_paths = [
            layout::current_rel_path(),
            layout::commit_rel_path(1),
            layout::commit_rel_path(2),
            layout::commit_rel_path(3),
        ];
        let log_before = log_paths
            .iter()
            .map(|path| fs::read(temp.path().join(path)))
            .collect::<Result<Vec<_>, _>>()?;
        let older_than = expired_cutoff();

        let dry_run = table.vacuum(older_than, VacuumMode::DryRun).await?;

        assert_eq!(dry_run.table_version, 3);
        assert_eq!(dry_run.mode, VacuumMode::DryRun);
        assert_eq!(dry_run.considered_files, 8);
        assert_eq!(dry_run.retained_files, 5);
        assert_eq!(dry_run.removable_files, 3);
        assert_eq!(dry_run.deleted_files, 0);
        assert_eq!(dry_run.deleted_bytes, 0);
        let historical = artifact(&dry_run, historical_data);
        assert_eq!(historical.size_bytes, b"historical data".len() as u64);
        assert_eq!(historical.disposition, VacuumArtifactDisposition::Retained);
        assert_eq!(
            historical.reason,
            VacuumArtifactReason::ReferencedByCommit { version: 2 }
        );
        assert_eq!(
            artifact(&dry_run, invalid_orphan).disposition,
            VacuumArtifactDisposition::Removable
        );
        assert_eq!(
            artifact(&dry_run, invalid_orphan).reason,
            VacuumArtifactReason::InvalidOrUnreadableParquet
        );
        assert_eq!(
            artifact(&dry_run, valid_orphan).reason,
            VacuumArtifactReason::Unreferenced
        );
        assert_eq!(
            artifact(&dry_run, coverage_orphan).disposition,
            VacuumArtifactDisposition::Removable
        );
        assert_eq!(
            artifact(&dry_run, unrecognized).reason,
            VacuumArtifactReason::UnrecognizedArtifact
        );
        assert_eq!(
            artifact(&dry_run, external_source).reason,
            VacuumArtifactReason::UnrecognizedArtifact
        );
        let removable_bytes = u128::from(b"incomplete".len() as u64)
            + u128::from(fs::metadata(temp.path().join(valid_orphan))?.len())
            + u128::from(b"orphan coverage".len() as u64);
        assert_eq!(dry_run.removable_bytes, removable_bytes);
        for path in [invalid_orphan, valid_orphan, coverage_orphan] {
            assert!(temp.path().join(path).exists());
        }

        let applied = table.vacuum(older_than, VacuumMode::Apply).await?;

        assert_eq!(applied.table_version, 3);
        assert_eq!(applied.considered_files, 8);
        assert_eq!(applied.retained_files, 5);
        assert_eq!(applied.removable_files, 0);
        assert_eq!(applied.deleted_files, 3);
        assert_eq!(applied.removable_bytes, 0);
        assert_eq!(applied.deleted_bytes, removable_bytes);
        for path in [invalid_orphan, valid_orphan, coverage_orphan] {
            assert_eq!(
                artifact(&applied, path).disposition,
                VacuumArtifactDisposition::Deleted
            );
            assert!(!temp.path().join(path).exists());
        }
        for path in [
            historical_data,
            historical_coverage,
            historical_snapshot,
            unrecognized,
            external_source,
        ] {
            assert!(temp.path().join(path).exists(), "vacuum removed {path}");
        }
        let log_after = log_paths
            .iter()
            .map(|path| fs::read(temp.path().join(path)))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(log_after, log_before);
        assert_eq!(table.current_version().await?, 3);
        Ok(())
    }

    #[tokio::test]
    async fn vacuum_preserves_a_recent_reserved_writer_path() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let path = "data/_managed/append/00000000-0000-0000-0000-000000000003.parquet";
        let unrecognized = "data/keep.txt";
        let mut sink = open_new_output_sink(location.as_ref(), Path::new(path)).await?;
        sink.write_all(b"incomplete")?;
        sink.flush()?;
        write_new(location.as_ref(), Path::new(unrecognized), b"keep").await?;

        let report = table
            .vacuum(Utc::now() - Duration::hours(1), VacuumMode::Apply)
            .await?;

        assert_eq!(
            artifact(&report, path).disposition,
            VacuumArtifactDisposition::Retained
        );
        assert_eq!(
            artifact(&report, path).reason,
            VacuumArtifactReason::WithinRetention
        );
        assert_eq!(
            artifact(&report, unrecognized).reason,
            VacuumArtifactReason::UnrecognizedArtifact
        );
        assert!(temp.path().join(path).exists());
        assert!(temp.path().join(unrecognized).exists());
        drop(sink);
        assert!(!temp.path().join(path).exists());
        Ok(())
    }

    #[tokio::test]
    async fn vacuum_fails_closed_when_retained_history_is_corrupt() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let orphan = "data/_managed/append/00000000-0000-0000-0000-000000000004.parquet";
        write_new(location.as_ref(), Path::new(orphan), b"incomplete").await?;
        fs::write(temp.path().join(layout::commit_rel_path(1)), b"{invalid")?;

        let error = table
            .vacuum(expired_cutoff(), VacuumMode::Apply)
            .await
            .expect_err("corrupt retained history must stop vacuum");

        assert!(matches!(
            error,
            TableError::Vacuum {
                source: VacuumError::Commit {
                    source: CommitError::CommitDeserialization { version: 1, .. }
                }
            }
        ));
        assert!(temp.path().join(orphan).exists());
        Ok(())
    }

    #[tokio::test]
    async fn apply_preserves_a_candidate_that_changed_after_planning() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let path = "data/_managed/append/00000000-0000-0000-0000-000000000005.parquet";
        write_new(location.as_ref(), Path::new(path), b"old").await?;
        mark_expired(&temp.path().join(path))?;
        let file = storage::list_files(location.as_ref(), Path::new("data"))
            .await?
            .pop()
            .ok_or("missing planned file")?;
        let mut candidate =
            classify_artifact(&table, file, expired_cutoff(), &HashMap::new()).await;
        fs::write(temp.path().join(path), b"new contents")?;

        assert!(!prepare_candidate_for_delete(&table, &mut candidate).await?);
        assert_eq!(candidate.disposition, VacuumArtifactDisposition::Retained);
        assert_eq!(candidate.reason, VacuumArtifactReason::ChangedSincePlanning);
        assert_eq!(candidate.size_bytes, b"new contents".len() as u64);
        assert!(temp.path().join(path).exists());

        fs::remove_file(temp.path().join(path))?;
        assert!(!prepare_candidate_for_delete(&table, &mut candidate).await?);
        assert_eq!(candidate.disposition, VacuumArtifactDisposition::Deleted);
        Ok(())
    }

    #[tokio::test]
    async fn apply_error_includes_deletions_completed_before_the_failure() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let first = "data/_managed/append/00000000-0000-0000-0000-000000000006.parquet";
        let failed = "data/_managed/append/00000000-0000-0000-0000-000000000007.parquet";
        for path in [first, failed] {
            write_new(location.as_ref(), Path::new(path), b"incomplete").await?;
            mark_expired(&temp.path().join(path))?;
        }
        inject_cleanup_failure(temp.path().join(failed));

        let error = table
            .vacuum(expired_cutoff(), VacuumMode::Apply)
            .await
            .expect_err("injected deletion failure must fail apply");

        let TableError::Vacuum {
            source:
                VacuumError::Delete {
                    path,
                    partial_report,
                    ..
                },
        } = error
        else {
            panic!("unexpected error: {error:?}");
        };
        assert_eq!(path, failed);
        assert_eq!(
            artifact(&partial_report, first).disposition,
            VacuumArtifactDisposition::Deleted
        );
        assert_eq!(
            artifact(&partial_report, failed).disposition,
            VacuumArtifactDisposition::Removable
        );
        assert!(!temp.path().join(first).exists());
        assert!(temp.path().join(failed).exists());
        Ok(())
    }

    #[tokio::test]
    async fn vacuum_rejects_a_future_cutoff_before_inspecting_storage() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let table = TimeSeriesTable::create(location, make_basic_table_meta()).await?;
        let older_than = Utc::now() + Duration::hours(1);

        let error = table
            .vacuum(older_than, VacuumMode::DryRun)
            .await
            .expect_err("a future retention cutoff must fail");

        assert!(matches!(
            error,
            TableError::Vacuum {
                source: VacuumError::FutureCutoff {
                    older_than: rejected
                }
            } if rejected == older_than
        ));
        Ok(())
    }
}
