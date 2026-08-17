//! Entity-layout optimization for time-series tables.

use std::{collections::HashSet, path::Path};

use crate::{
    coverage::{EntityCoverage, EntityIdentity, io::read_entity_coverage_sidecar},
    formats::parquet::{StagedEntityRewrite, rewrite_mixed_parquet_segment},
    metadata::{segments::SegmentEntityLayout, table_metadata::IndexValueError},
    storage::{StorageLocation, normalize_relative_storage_path, remove_file_if_exists},
    table::{TableError, TimeSeriesTable},
    transaction_log::{CommitError, LogAction, SegmentMeta, TableState},
};

/// Result of one entity-layout optimization operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizeReport {
    /// Table version used to select candidates.
    pub starting_version: u64,
    /// Version containing the atomic replacement commit, or the starting
    /// version for a no-op.
    pub committed_version: u64,
    /// Mixed live segments selected from the starting snapshot.
    pub candidate_source_segments: u64,
    /// Selected sources removed by the successful commit.
    pub source_segments_replaced: u64,
    /// Verified single-entity replacements added by the successful commit.
    pub replacement_segments_written: u64,
    /// Unique complete identities represented across all replacements.
    pub distinct_identities_materialized: u64,
    /// Logical rows in the selected source segments.
    pub rows_read: u64,
    /// Logical rows in the committed replacement segments.
    pub rows_written: u64,
    /// Whether no mixed live segments existed at the starting version.
    pub no_op: bool,
}

impl OptimizeReport {
    fn no_op(version: u64) -> Self {
        Self {
            starting_version: version,
            committed_version: version,
            candidate_source_segments: 0,
            source_segments_replaced: 0,
            replacement_segments_written: 0,
            distinct_identities_materialized: 0,
            rows_read: 0,
            rows_written: 0,
            no_op: true,
        }
    }
}

struct StagedCandidate {
    source: SegmentMeta,
    rewrite: StagedEntityRewrite,
}

struct PlanCounts {
    candidates: u64,
    replacements: u64,
    identities: u64,
    rows: u64,
}

fn mixed_segment_candidates(state: &TableState) -> Result<Vec<SegmentMeta>, IndexValueError> {
    Ok(state
        .segments_sorted_by_index()?
        .into_iter()
        .filter(|segment| segment.entity_layout == SegmentEntityLayout::Mixed)
        .cloned()
        .collect())
}

fn count(field: &'static str, value: usize) -> Result<u64, TableError> {
    value
        .try_into()
        .map_err(|_| TableError::OptimizeCountOverflow { field })
}

fn add(field: &'static str, total: &mut u64, value: u64) -> Result<(), TableError> {
    *total = total
        .checked_add(value)
        .ok_or(TableError::OptimizeCountOverflow { field })?;
    Ok(())
}

fn invalid_plan(reason: impl Into<String>) -> TableError {
    TableError::OptimizeInvariant {
        reason: reason.into(),
    }
}

fn ensure_canonical(path: &str, description: &str) -> Result<(), TableError> {
    let (canonical, _) = normalize_relative_storage_path(Path::new(path))
        .map_err(|error| invalid_plan(format!("invalid {description} path {path:?}: {error}")))?;
    if canonical != path {
        return Err(invalid_plan(format!(
            "{description} path {path:?} is not canonical; expected {canonical:?}"
        )));
    }
    Ok(())
}

async fn validate_staged_plan(
    table: &TimeSeriesTable,
    candidates: &[SegmentMeta],
    staged: &[StagedCandidate],
) -> Result<PlanCounts, TableError> {
    if candidates.len() != staged.len() {
        return Err(invalid_plan(format!(
            "staged {} rewrites for {} candidates",
            staged.len(),
            candidates.len()
        )));
    }

    let mut candidate_paths = HashSet::new();
    let mut object_paths = HashSet::new();
    let mut live_object_paths = HashSet::new();
    for segment in table.state.segments.values() {
        live_object_paths.insert(segment.path.as_str());
        if let Some(path) = segment.coverage_path.as_deref() {
            live_object_paths.insert(path);
        }
    }
    if let Some(coverage) = &table.state.table_coverage {
        live_object_paths.insert(coverage.coverage_path.as_str());
    }
    let mut source_coverage = EntityCoverage::empty();
    let mut replacement_coverage = EntityCoverage::empty();
    let mut identities = HashSet::<EntityIdentity>::new();
    let mut source_rows = 0u64;
    let mut replacement_rows = 0u64;
    let mut replacement_count = 0u64;

    for (candidate, staged_candidate) in candidates.iter().zip(staged) {
        if candidate != &staged_candidate.source
            || staged_candidate.rewrite.source_path != candidate.path
        {
            return Err(invalid_plan(format!(
                "candidate {} is not represented exactly once by its staged rewrite",
                candidate.path
            )));
        }
        if !candidate_paths.insert(&candidate.path) {
            return Err(invalid_plan(format!(
                "candidate path {} appears more than once",
                candidate.path
            )));
        }
        if table.state.segments.get(&candidate.path) != Some(candidate) {
            return Err(invalid_plan(format!(
                "candidate {} is not live in the starting snapshot",
                candidate.path
            )));
        }
        add("rows_read", &mut source_rows, candidate.row_count)?;

        let source_coverage_path = candidate.coverage_path.as_deref().ok_or_else(|| {
            invalid_plan(format!(
                "candidate {} has no coverage sidecar",
                candidate.path
            ))
        })?;
        let candidate_coverage =
            read_entity_coverage_sidecar(table.location(), Path::new(source_coverage_path))
                .await
                .map_err(|source| TableError::CoverageSidecar { source })?;
        let mut candidate_replacement_coverage = EntityCoverage::empty();
        let mut candidate_rows = 0u64;

        for replacement in &staged_candidate.rewrite.replacements {
            let coverage_path = replacement.meta.coverage_path.as_deref().ok_or_else(|| {
                invalid_plan(format!(
                    "replacement {} has no coverage sidecar",
                    replacement.meta.path
                ))
            })?;
            for (path, description) in [
                (replacement.meta.path.as_str(), "replacement data"),
                (coverage_path, "replacement coverage"),
            ] {
                ensure_canonical(path, description)?;
                if !object_paths.insert(path.to_string()) {
                    return Err(invalid_plan(format!(
                        "staged object path {path} appears more than once"
                    )));
                }
                if live_object_paths.contains(path) {
                    return Err(invalid_plan(format!(
                        "staged object path {path} conflicts with a live table object"
                    )));
                }
            }
            if replacement.meta.entity_layout
                != SegmentEntityLayout::Single(replacement.identity.clone())
                || replacement.coverage.identity_count() != 1
                || replacement.coverage.get(&replacement.identity).is_none()
            {
                return Err(invalid_plan(format!(
                    "replacement {} is not truthful Single metadata",
                    replacement.meta.path
                )));
            }
            add("replacement_segments_written", &mut replacement_count, 1)?;
            add(
                "rows_written",
                &mut candidate_rows,
                replacement.meta.row_count,
            )?;
            identities.insert(replacement.identity.clone());
            candidate_replacement_coverage.union_inplace(&replacement.coverage);
        }

        if candidate_rows != candidate.row_count {
            return Err(invalid_plan(format!(
                "replacement rows {candidate_rows} do not equal source rows {} for {}",
                candidate.row_count, candidate.path
            )));
        }
        if candidate_replacement_coverage != candidate_coverage {
            return Err(invalid_plan(format!(
                "replacement coverage does not equal source coverage for {}",
                candidate.path
            )));
        }
        add("rows_written", &mut replacement_rows, candidate_rows)?;
        source_coverage.union_inplace(&candidate_coverage);
        replacement_coverage.union_inplace(&candidate_replacement_coverage);
    }

    if source_rows != replacement_rows {
        return Err(invalid_plan(format!(
            "replacement rows {replacement_rows} do not equal source rows {source_rows}"
        )));
    }
    if source_coverage != replacement_coverage {
        return Err(invalid_plan(
            "replacement coverage does not reconstruct selected source coverage",
        ));
    }

    Ok(PlanCounts {
        candidates: count("candidate_source_segments", candidates.len())?,
        replacements: replacement_count,
        identities: count("distinct_identities_materialized", identities.len())?,
        rows: source_rows,
    })
}

impl TimeSeriesTable {
    async fn rollback_optimization(
        &self,
        staged_paths: &[String],
        source: TableError,
    ) -> TableError {
        let mut cleanup_errors = Vec::new();
        for path in staged_paths.iter().rev() {
            if let Err(error) =
                remove_file_if_exists(self.location().as_ref(), Path::new(path)).await
            {
                cleanup_errors.push(format!("{path}: {error}"));
            }
        }
        if cleanup_errors.is_empty() {
            source
        } else {
            TableError::OptimizeRollback {
                source: Box::new(source),
                cleanup_errors,
            }
        }
    }

    /// Replace every live mixed-entity segment with verified single-entity
    /// Parquet segments in one expected-version commit.
    ///
    /// # Errors
    ///
    /// Returns [`TableError`] when optimization is not applicable, staging or
    /// validation fails, the commit cannot be confirmed, or rollback fails.
    pub async fn optimize(&mut self) -> Result<OptimizeReport, TableError> {
        if self.index.entity_columns.is_empty() {
            let table_root = match self.location().as_ref() {
                StorageLocation::Local(root) => root.display().to_string(),
            };
            return Err(TableError::OptimizeNotApplicable { table_root });
        }

        let starting_version = self.state.version;
        let candidates = mixed_segment_candidates(&self.state)
            .map_err(|source| TableError::InvalidSegmentBounds { source })?;
        if candidates.is_empty() {
            return Ok(OptimizeReport::no_op(starting_version));
        }
        let committed_version =
            starting_version
                .checked_add(1)
                .ok_or(TableError::OptimizeCountOverflow {
                    field: "committed_version",
                })?;
        let table_schema = self.state.table_meta.logical_schema.clone().ok_or(
            TableError::MissingCanonicalSchema {
                version: starting_version,
            },
        )?;

        let mut staged = Vec::with_capacity(candidates.len());
        let mut staged_paths = Vec::new();
        for source in &candidates {
            match rewrite_mixed_parquet_segment(self.location(), &table_schema, &self.index, source)
                .await
            {
                Ok(rewrite) => {
                    staged_paths.extend(rewrite.staged_object_paths.iter().cloned());
                    staged.push(StagedCandidate {
                        source: source.clone(),
                        rewrite,
                    });
                }
                Err(source) => {
                    let error = TableError::OptimizeRewrite { source };
                    return Err(self.rollback_optimization(&staged_paths, error).await);
                }
            }
        }

        let counts = match validate_staged_plan(self, &candidates, &staged).await {
            Ok(counts) => counts,
            Err(source) => {
                return Err(self.rollback_optimization(&staged_paths, source).await);
            }
        };

        let mut actions = Vec::new();
        for source in &candidates {
            actions.push(LogAction::RemoveSegment {
                path: source.path.clone(),
            });
        }
        for staged_candidate in &staged {
            actions.extend(
                staged_candidate
                    .rewrite
                    .replacements
                    .iter()
                    .map(|replacement| LogAction::AddSegment(replacement.meta.clone())),
            );
        }

        let new_version = match self
            .log
            .commit_with_expected_version(starting_version, actions)
            .await
        {
            Ok(version) => version,
            Err(source @ CommitError::AmbiguousOutcome { .. }) => {
                return Err(TableError::TransactionLog { source });
            }
            Err(source) => {
                let error = TableError::TransactionLog { source };
                return Err(self.rollback_optimization(&staged_paths, error).await);
            }
        };
        assert_eq!(
            new_version, committed_version,
            "transaction log returned unexpected optimize version"
        );

        for source in &candidates {
            self.state.segments.remove(&source.path);
        }
        for staged_candidate in staged {
            for replacement in staged_candidate.rewrite.replacements {
                self.state
                    .segments
                    .insert(replacement.meta.path.clone(), replacement.meta);
            }
        }
        self.state.version = new_version;

        Ok(OptimizeReport {
            starting_version,
            committed_version: new_version,
            candidate_source_segments: counts.candidates,
            source_segments_replaced: counts.candidates,
            replacement_segments_written: counts.replacements,
            distinct_identities_materialized: counts.identities,
            rows_read: counts.rows,
            rows_written: counts.rows,
            no_op: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use arrow::datatypes::TimeUnit;
    use tempfile::TempDir;

    use crate::{
        coverage::EntityIdentity,
        metadata::{
            logical_schema::LogicalTimestampUnit,
            segments::{FileFormat, SegmentEntityLayout},
            table_metadata::IndexValue,
        },
        storage::TableLocation,
        table::test_util::{
            make_table_meta_with_unit, utc_datetime, write_arrow_parquet_with_unit,
        },
        transaction_log::TableKind,
    };

    fn segment(path: &str, layout: SegmentEntityLayout, minute: u32) -> SegmentMeta {
        SegmentMeta {
            path: path.to_string(),
            format: FileFormat::Parquet,
            entity_layout: layout,
            index_min: IndexValue::Timestamp(utc_datetime(2025, 1, 1, 0, minute, 0)),
            index_max: IndexValue::Timestamp(utc_datetime(2025, 1, 1, 0, minute + 1, 0)),
            row_count: 1,
            file_size: Some(1),
            coverage_path: Some(format!("_coverage/segments/{minute}.roar")),
        }
    }

    fn state(segments: impl IntoIterator<Item = SegmentMeta>) -> TableState {
        TableState {
            version: 7,
            table_meta: make_table_meta_with_unit(LogicalTimestampUnit::Millis),
            segments: segments
                .into_iter()
                .map(|segment| (segment.path.clone(), segment))
                .collect::<HashMap<_, _>>(),
            table_coverage: None,
        }
    }

    fn single_identity() -> SegmentEntityLayout {
        SegmentEntityLayout::Single(
            EntityIdentity::try_new(vec!["A".to_string()]).expect("valid identity"),
        )
    }

    #[test]
    fn discovery_selects_all_and_only_mixed_segments() {
        let state = state([
            segment("data/mixed-late.parquet", SegmentEntityLayout::Mixed, 2),
            segment("data/single.parquet", single_identity(), 0),
            segment(
                "data/not-applicable.parquet",
                SegmentEntityLayout::NotApplicable,
                0,
            ),
            segment("data/mixed.parquet", SegmentEntityLayout::Mixed, 1),
        ]);

        let paths = mixed_segment_candidates(&state)
            .expect("valid segment bounds")
            .into_iter()
            .map(|segment| segment.path)
            .collect::<Vec<_>>();

        assert_eq!(paths, ["data/mixed.parquet", "data/mixed-late.parquet"]);
    }

    #[test]
    fn discovery_order_is_independent_of_hash_map_insertion_order() {
        let segments = [
            segment("data/b.parquet", SegmentEntityLayout::Mixed, 1),
            segment("data/a.parquet", SegmentEntityLayout::Mixed, 1),
            segment("data/later.parquet", SegmentEntityLayout::Mixed, 2),
        ];
        let forward = state(segments.clone());
        let reverse = state(segments.into_iter().rev());

        let paths = |state: &TableState| {
            mixed_segment_candidates(state)
                .expect("valid segment bounds")
                .into_iter()
                .map(|segment| segment.path)
                .collect::<Vec<_>>()
        };

        assert_eq!(paths(&forward), paths(&reverse));
        assert_eq!(
            paths(&forward),
            ["data/a.parquet", "data/b.parquet", "data/later.parquet"]
        );
    }

    #[tokio::test]
    async fn optimize_without_mixed_segments_is_a_zero_write_no_op() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let mut table = TimeSeriesTable::create(
            TableLocation::local(temp.path()),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        let starting_version = table.state().version;

        let report = table.optimize().await?;

        assert_eq!(report, OptimizeReport::no_op(starting_version));
        assert!(!temp.path().join("data/_staged").exists());
        let reopened = TimeSeriesTable::open(TableLocation::local(temp.path())).await?;
        assert_eq!(reopened.state().version, starting_version);
        Ok(())
    }

    #[tokio::test]
    async fn optimize_rejects_a_table_without_entity_columns() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let mut table_meta = make_table_meta_with_unit(LogicalTimestampUnit::Millis);
        let TableKind::TimeSeries(index) = &mut table_meta.kind else {
            unreachable!("test table is time-series");
        };
        index.entity_columns.clear();
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta).await?;

        let error = table
            .optimize()
            .await
            .expect_err("entity-free table must be rejected");

        assert!(matches!(
            error,
            TableError::OptimizeNotApplicable { table_root }
                if table_root == temp.path().display().to_string()
        ));
        assert!(!temp.path().join("data/_staged").exists());
        Ok(())
    }

    #[tokio::test]
    async fn optimize_atomically_replaces_one_mixed_source() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let location = TableLocation::local(temp.path());
        let mut table = TimeSeriesTable::create(
            location.clone(),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        let source_path = "data/mixed.parquet";
        write_arrow_parquet_with_unit(
            &temp.path().join(source_path),
            TimeUnit::Millisecond,
            &[Some(1_000), Some(2_000), Some(3_000), Some(4_000)],
            &["A", "B", "A", "B"],
            &[10.0, 20.0, 11.0, 21.0],
        )
        .expect("write mixed source");
        table.append_parquet_segment(source_path).await?;
        let source = table
            .state()
            .segments
            .get(source_path)
            .expect("committed source")
            .clone();
        assert_eq!(source.entity_layout, SegmentEntityLayout::Mixed);
        let source_bytes = std::fs::read(temp.path().join(source_path)).expect("source bytes");
        let source_coverage_path = source.coverage_path.as_deref().expect("source coverage");
        let source_coverage_bytes =
            std::fs::read(temp.path().join(source_coverage_path)).expect("source coverage bytes");
        let table_coverage = table.state().table_coverage.clone();
        let starting_version = table.state().version;

        let report = table.optimize().await?;

        assert_eq!(report.starting_version, starting_version);
        assert_eq!(report.committed_version, starting_version + 1);
        assert_eq!(report.candidate_source_segments, 1);
        assert_eq!(report.source_segments_replaced, 1);
        assert_eq!(report.replacement_segments_written, 2);
        assert_eq!(report.distinct_identities_materialized, 2);
        assert_eq!(report.rows_read, 4);
        assert_eq!(report.rows_written, 4);
        assert!(!report.no_op);
        assert!(!table.state().segments.contains_key(source_path));
        assert_eq!(table.state().segments.len(), 2);
        assert!(
            table
                .state()
                .segments
                .values()
                .all(|segment| matches!(segment.entity_layout, SegmentEntityLayout::Single(_)))
        );
        assert_eq!(table.state().table_coverage, table_coverage);
        assert_eq!(
            std::fs::read(temp.path().join(source_path)).expect("source remains"),
            source_bytes
        );
        assert_eq!(
            std::fs::read(temp.path().join(source_coverage_path)).expect("source coverage remains"),
            source_coverage_bytes
        );
        Ok(())
    }
}
