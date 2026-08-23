//! Entity-layout optimization for time-series tables.

use std::{collections::HashSet, path::Path};

use snafu::{Backtrace, Snafu};

use crate::{
    coverage::{
        EntityCoverage, EntityIdentity,
        io::{CoverageSidecarError, read_entity_coverage_sidecar},
    },
    formats::parquet::{EntityRewriteError, StagedEntityRewrite, rewrite_mixed_parquet_segment},
    metadata::{
        schema_compat::SchemaCompatibilityError, segments::SegmentEntityLayout,
        table_metadata::IndexValueError,
    },
    storage::{
        StorageError, StorageLocation, normalize_relative_storage_path, remove_file_if_exists,
    },
    table::{TableError, TimeSeriesTable},
    transaction_log::{CommitError, LogAction, SegmentMeta, TableState},
};

/// Errors owned by an entity-layout optimization operation.
#[derive(Debug, Snafu)]
#[snafu(module, visibility(pub(crate)))]
pub enum OptimizeError {
    /// Entity-layout optimization requires at least one entity column.
    #[snafu(display(
        "Entity-layout optimization is not applicable to table {table_root}: no entity columns are configured"
    ))]
    NotApplicable {
        /// User-facing table root.
        table_root: String,
    },

    /// Staging replacements for one mixed segment failed.
    #[snafu(context(false), display("Mixed-segment rewrite failed: {source}"))]
    MixedSegmentRewrite {
        /// Complete mixed-segment rewrite failure.
        #[snafu(source(from(EntityRewriteError, Box::new)), backtrace)]
        source: Box<EntityRewriteError>,
    },

    /// Live segment bounds cannot be ordered in one native index domain.
    #[snafu(
        context(false),
        display("Invalid segment ordered-index bounds: {source}")
    )]
    InvalidSegmentBounds {
        /// Complete bounds validation failure.
        #[snafu(source)]
        source: IndexValueError,
        /// Backtrace captured because index value validation does not own one.
        backtrace: Backtrace,
    },

    /// Optimization cannot use the table's canonical schema.
    #[snafu(
        context(false),
        display("Optimization schema validation failed: {source}")
    )]
    SchemaValidation {
        /// Complete schema compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)))]
        source: Box<SchemaCompatibilityError>,
        /// Backtrace captured because schema compatibility errors do not own one.
        backtrace: Backtrace,
    },

    /// A coverage sidecar required to validate a staged plan could not be read.
    #[snafu(
        context(false),
        display("Optimization coverage sidecar error: {source}")
    )]
    CoverageSidecar {
        /// Complete coverage sidecar failure.
        #[snafu(source(from(CoverageSidecarError, Box::new)), backtrace)]
        source: Box<CoverageSidecarError>,
    },

    /// A staged optimization plan violated an atomic publication invariant.
    #[snafu(display("Invalid staged entity-layout optimization plan: {reason}"))]
    InvalidStagedPlan {
        /// Failed plan invariant.
        reason: String,
        /// Backtrace captured at the failed internal invariant.
        backtrace: Backtrace,
    },

    /// An optimization count could not be represented without wrapping.
    #[snafu(display("Entity-layout optimization count overflow: {field}"))]
    CountOverflow {
        /// Report or version field that overflowed.
        field: &'static str,
        /// Backtrace captured at the failed internal arithmetic boundary.
        backtrace: Backtrace,
    },

    /// Publishing the optimization transaction failed.
    #[snafu(context(false), display("Optimization commit failed: {source}"))]
    Commit {
        /// Complete transaction-log failure.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// Optimization failed and one or more owned staged objects could not be removed.
    #[snafu(display(
        "{source}; staged-object rollback also failed: [{}]",
        cleanup_errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    ))]
    Rollback {
        /// Original optimization failure that triggered rollback.
        #[snafu(source, backtrace)]
        source: Box<OptimizeError>,
        /// Typed cleanup failure for every staged object that could not be removed.
        cleanup_errors: Vec<StorageError>,
    },
}

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

fn count(field: &'static str, value: usize) -> Result<u64, OptimizeError> {
    value.try_into().map_err(|_| OptimizeError::CountOverflow {
        field,
        backtrace: Backtrace::capture(),
    })
}

fn add(field: &'static str, total: &mut u64, value: u64) -> Result<(), OptimizeError> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| OptimizeError::CountOverflow {
            field,
            backtrace: Backtrace::capture(),
        })?;
    Ok(())
}

fn invalid_plan(reason: impl Into<String>) -> OptimizeError {
    OptimizeError::InvalidStagedPlan {
        reason: reason.into(),
        backtrace: Backtrace::capture(),
    }
}

fn ensure_canonical(path: &str, description: &str) -> Result<(), OptimizeError> {
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
    staged: &[StagedEntityRewrite],
) -> Result<PlanCounts, OptimizeError> {
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

    for (candidate, rewrite) in candidates.iter().zip(staged) {
        if rewrite.source_path != candidate.path {
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
                .map_err(OptimizeError::from)?;
        let mut candidate_replacement_coverage = EntityCoverage::empty();
        let mut candidate_rows = 0u64;

        for replacement in &rewrite.replacements {
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
                if !object_paths.insert(path) {
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
        source: OptimizeError,
    ) -> OptimizeError {
        let mut cleanup_errors = Vec::new();
        for path in staged_paths.iter().rev() {
            if let Err(error) =
                remove_file_if_exists(self.location().as_ref(), Path::new(path)).await
            {
                cleanup_errors.push(error);
            }
        }
        if cleanup_errors.is_empty() {
            source
        } else {
            OptimizeError::Rollback {
                source: Box::new(source),
                cleanup_errors,
            }
        }
    }

    /// Replace every live mixed-entity segment with verified single-entity
    /// Parquet segments in one expected-version commit.
    ///
    /// Optimization preserves logical rows, schema, and per-entity coverage,
    /// but may change physical row order.
    ///
    /// # Errors
    ///
    /// Returns [`TableError`] when optimization is not applicable, staging or
    /// validation fails, the commit cannot be confirmed, or rollback fails.
    #[tracing::instrument(
        name = "table.optimize",
        target = "timeseries_table_format::table::optimize",
        level = "debug",
        skip_all,
        fields(
            starting_version = self.state.version,
            candidate_source_segments = tracing::field::Empty,
            replacement_segments_written = tracing::field::Empty,
            distinct_identities_materialized = tracing::field::Empty,
            rows_read = tracing::field::Empty,
            rows_written = tracing::field::Empty,
            committed_version = tracing::field::Empty,
            no_op = tracing::field::Empty,
            outcome = tracing::field::Empty
        )
    )]
    pub async fn optimize(&mut self) -> Result<OptimizeReport, TableError> {
        let result: Result<OptimizeReport, OptimizeError> = async {
            if self.index.entity_columns.is_empty() {
                let table_root = match self.location().as_ref() {
                    StorageLocation::Local(root) => root.display().to_string(),
                };
                return Err(OptimizeError::NotApplicable { table_root });
            }

            let starting_version = self.state.version;
            let candidates = mixed_segment_candidates(&self.state).map_err(|source| {
                OptimizeError::InvalidSegmentBounds {
                    source,
                    backtrace: Backtrace::capture(),
                }
            })?;
            tracing::Span::current().record("candidate_source_segments", candidates.len());
            if candidates.is_empty() {
                return Ok(OptimizeReport::no_op(starting_version));
            }
            let committed_version =
                starting_version
                    .checked_add(1)
                    .ok_or_else(|| OptimizeError::CountOverflow {
                        field: "committed_version",
                        backtrace: Backtrace::capture(),
                    })?;
            let table_schema = self
                .state
                .table_meta
                .logical_schema
                .clone()
                .ok_or_else(|| OptimizeError::from(SchemaCompatibilityError::MissingTableSchema))?;

            let mut staged = Vec::with_capacity(candidates.len());
            let mut staged_paths = Vec::new();
            for source in &candidates {
                match rewrite_mixed_parquet_segment(
                    self.location(),
                    &table_schema,
                    &self.index,
                    source,
                )
                .await
                {
                    Ok(rewrite) => {
                        staged_paths.extend(rewrite.staged_object_paths.iter().cloned());
                        staged.push(rewrite);
                    }
                    Err(source) => {
                        let error = OptimizeError::from(source);
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
            let span = tracing::Span::current();
            span.record("candidate_source_segments", counts.candidates);
            span.record("replacement_segments_written", counts.replacements);
            span.record("distinct_identities_materialized", counts.identities);
            span.record("rows_read", counts.rows);
            span.record("rows_written", counts.rows);

            let mut actions = Vec::new();
            for source in &candidates {
                actions.push(LogAction::RemoveSegment {
                    path: source.path.clone(),
                });
            }
            for rewrite in &staged {
                actions.extend(
                    rewrite
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
                    return Err(OptimizeError::from(source));
                }
                Err(source) => {
                    let error = OptimizeError::from(source);
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
            for rewrite in staged {
                for replacement in rewrite.replacements {
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
        .await;

        let span = tracing::Span::current();
        match &result {
            Ok(report) => {
                span.record(
                    "candidate_source_segments",
                    report.candidate_source_segments,
                );
                span.record(
                    "replacement_segments_written",
                    report.replacement_segments_written,
                );
                span.record(
                    "distinct_identities_materialized",
                    report.distinct_identities_materialized,
                );
                span.record("rows_read", report.rows_read);
                span.record("rows_written", report.rows_written);
                span.record("committed_version", report.committed_version);
                span.record("no_op", report.no_op);
                if report.no_op {
                    span.record("outcome", "no_op");
                } else {
                    span.record("outcome", "succeeded");
                    tracing::info!(
                        name: "table.optimize",
                        target: "timeseries_table_format::table::optimize",
                        starting_version = report.starting_version,
                        committed_version = report.committed_version,
                        candidate_source_segments = report.candidate_source_segments,
                        replacement_segments_written = report.replacement_segments_written,
                        distinct_identities_materialized = report.distinct_identities_materialized,
                        rows_read = report.rows_read,
                        rows_written = report.rows_written,
                        outcome = "succeeded",
                        "Optimized time-series table"
                    );
                }
            }
            Err(OptimizeError::Commit {
                source: CommitError::AmbiguousOutcome { .. },
            }) => {
                span.record("outcome", "ambiguous");
            }
            Err(OptimizeError::Rollback { .. }) => {
                span.record("outcome", "cleanup_failed");
            }
            Err(_) => {
                span.record("outcome", "failed");
            }
        }
        result.map_err(TableError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::AppendError;
    use std::{
        collections::{BTreeSet, HashMap},
        path::{Path, PathBuf},
    };

    use arrow::datatypes::TimeUnit;
    use futures::StreamExt;
    use tempfile::TempDir;

    use crate::{
        coverage::{EntityIdentity, EntityValue, io::CoverageSidecarError},
        formats::parquet::EntityRewriteError,
        metadata::{
            logical_schema::LogicalTimestampUnit,
            segments::{FileFormat, SegmentEntityLayout},
            table_metadata::IndexValue,
        },
        storage::{StorageError, TableLocation, layout},
        table::test_util::{
            CapturedSpan, TraceCapture, append_parquet_fixture, make_int32_entity_table_meta,
            make_table_meta_with_unit, utc_datetime, write_arrow_parquet_with_unit,
            write_int32_entity_parquet,
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
            EntityIdentity::try_new(vec!["A".into()]).expect("valid identity"),
        )
    }

    fn files_below(root: &Path) -> std::io::Result<BTreeSet<PathBuf>> {
        if !root.exists() {
            return Ok(BTreeSet::new());
        }
        let mut files = BTreeSet::new();
        let mut directories = vec![root.to_owned()];
        while let Some(directory) = directories.pop() {
            for entry in std::fs::read_dir(directory)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    directories.push(entry.path());
                } else {
                    files.insert(entry.path());
                }
            }
        }
        Ok(files)
    }

    fn optimization_objects(root: &Path) -> std::io::Result<BTreeSet<PathBuf>> {
        let mut files = BTreeSet::new();
        for relative in ["data/_staged", layout::SEGMENT_COVERAGE_DIR] {
            files.extend(files_below(&root.join(relative))?);
        }
        Ok(files)
    }

    fn captured_optimize_span(capture: &TraceCapture) -> CapturedSpan {
        let mut spans: Vec<_> = capture
            .spans()
            .into_iter()
            .filter(|span| span.name == "table.optimize")
            .collect();
        assert_eq!(spans.len(), 1, "expected one table.optimize span");
        spans.pop().expect("captured optimize span")
    }

    fn assert_no_optimize_event(capture: &TraceCapture) {
        assert!(
            !capture
                .events()
                .iter()
                .any(|event| event.name == "table.optimize")
        );
    }

    async fn append_mixed_source(
        table: &mut TimeSeriesTable,
        root: &Path,
        path: &str,
        start_millis: i64,
    ) -> Result<String, TableError> {
        write_arrow_parquet_with_unit(
            &root.join(path),
            TimeUnit::Millisecond,
            &[
                Some(start_millis + 1_000),
                Some(start_millis + 2_000),
                Some(start_millis + 61_000),
                Some(start_millis + 62_000),
            ],
            &["A", "B", "A", "B"],
            &[10.0, 20.0, 11.0, 21.0],
        )
        .expect("write mixed source");
        let existing_paths = table
            .state()
            .segments
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        append_parquet_fixture(table, path).await?;
        let committed_path = table
            .state()
            .segments
            .keys()
            .find(|path| !existing_paths.contains(*path))
            .expect("append added a segment")
            .clone();
        assert_eq!(
            table.state().segments[&committed_path].entity_layout,
            SegmentEntityLayout::Mixed
        );
        Ok(committed_path)
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

        let capture = TraceCapture::default();
        let report = capture.run(table.optimize()).await?;

        assert_eq!(report, OptimizeReport::no_op(starting_version));
        let span = captured_optimize_span(&capture);
        assert_eq!(span.level, tracing::Level::DEBUG);
        for (field, expected) in [
            ("starting_version", starting_version.to_string()),
            ("candidate_source_segments", "0".to_string()),
            ("replacement_segments_written", "0".to_string()),
            ("distinct_identities_materialized", "0".to_string()),
            ("rows_read", "0".to_string()),
            ("rows_written", "0".to_string()),
            ("committed_version", starting_version.to_string()),
            ("no_op", "true".to_string()),
            ("outcome", "no_op".to_string()),
        ] {
            assert_eq!(span.fields.get(field), Some(&expected));
        }
        assert_no_optimize_event(&capture);
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

        let capture = TraceCapture::default();
        let error = capture
            .run(table.optimize())
            .await
            .expect_err("entity-free table must be rejected");

        assert!(matches!(
            error,
            TableError::Optimize {
                source: OptimizeError::NotApplicable { table_root }
            }
                if table_root == temp.path().display().to_string()
        ));
        let span = captured_optimize_span(&capture);
        assert_eq!(
            span.fields.get("outcome").map(String::as_str),
            Some("failed")
        );
        assert!(
            span.fields
                .values()
                .all(|value| !value.contains(&temp.path().display().to_string()))
        );
        assert_no_optimize_event(&capture);
        assert!(!temp.path().join("data/_staged").exists());
        Ok(())
    }

    #[tokio::test]
    async fn optimize_rejects_missing_canonical_schema_before_staging() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let mut table = TimeSeriesTable::create(
            TableLocation::local(temp.path()),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        append_mixed_source(&mut table, temp.path(), "data/mixed.parquet", 0).await?;
        table.state.table_meta.logical_schema = None;
        let state_before = table.state().clone();
        let objects_before = optimization_objects(temp.path()).expect("optimization objects");

        let error = table
            .optimize()
            .await
            .expect_err("missing canonical schema must fail");

        assert!(matches!(
            error,
            TableError::Optimize {
                source: OptimizeError::SchemaValidation { source, .. }
            } if matches!(*source, SchemaCompatibilityError::MissingTableSchema)
        ));
        assert_eq!(table.state(), &state_before);
        assert_eq!(
            optimization_objects(temp.path()).expect("optimization objects"),
            objects_before
        );
        Ok(())
    }

    #[tokio::test]
    async fn optimize_rejects_invalid_segment_bounds_before_staging() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let mut table = TimeSeriesTable::create(
            TableLocation::local(temp.path()),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        let source_path =
            append_mixed_source(&mut table, temp.path(), "data/mixed.parquet", 0).await?;
        table
            .state
            .segments
            .get_mut(&source_path)
            .expect("mixed source")
            .index_max = IndexValue::Int64(1);
        let state_before = table.state().clone();
        let objects_before = optimization_objects(temp.path()).expect("optimization objects");

        let error = table
            .optimize()
            .await
            .expect_err("mixed ordered-index domains must fail");

        assert!(matches!(
            error,
            TableError::Optimize {
                source: OptimizeError::InvalidSegmentBounds {
                    source: IndexValueError::DomainMismatch { .. },
                    ..
                }
            }
        ));
        assert_eq!(table.state(), &state_before);
        assert_eq!(
            optimization_objects(temp.path()).expect("optimization objects"),
            objects_before
        );
        Ok(())
    }

    #[tokio::test]
    async fn optimize_preserves_a_missing_source_coverage_error() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let mut table = TimeSeriesTable::create(
            TableLocation::local(temp.path()),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        let source_path =
            append_mixed_source(&mut table, temp.path(), "data/mixed.parquet", 0).await?;
        let coverage_path = table.state().segments[&source_path]
            .coverage_path
            .as_deref()
            .expect("source coverage")
            .to_string();
        std::fs::remove_file(temp.path().join(&coverage_path)).expect("remove source coverage");
        let state_before = table.state().clone();
        let objects_before = optimization_objects(temp.path()).expect("optimization objects");

        let error = table
            .optimize()
            .await
            .expect_err("missing source coverage must fail");

        assert!(matches!(
            error,
            TableError::Optimize {
                source: OptimizeError::MixedSegmentRewrite { source }
            } if matches!(
                *source,
                EntityRewriteError::CoverageSidecar {
                    source: CoverageSidecarError::Storage {
                        source: StorageError::NotFound { .. }
                    }
                }
            )
        ));
        assert_eq!(table.state(), &state_before);
        assert_eq!(
            optimization_objects(temp.path()).expect("optimization objects"),
            objects_before
        );
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
        let fixture_path = "data/mixed.parquet";
        write_arrow_parquet_with_unit(
            &temp.path().join(fixture_path),
            TimeUnit::Millisecond,
            &[Some(1_000), Some(2_000), Some(61_000), Some(62_000)],
            &["A", "B", "A", "B"],
            &[10.0, 20.0, 11.0, 21.0],
        )
        .expect("write mixed source");
        append_parquet_fixture(&mut table, fixture_path).await?;
        let source_path = table
            .state()
            .segments
            .keys()
            .next()
            .expect("committed source")
            .clone();
        let source = table
            .state()
            .segments
            .get(&source_path)
            .expect("committed source")
            .clone();
        assert_eq!(source.entity_layout, SegmentEntityLayout::Mixed);
        let source_bytes = std::fs::read(temp.path().join(&source_path)).expect("source bytes");
        let source_coverage_path = source.coverage_path.as_deref().expect("source coverage");
        let source_coverage_bytes =
            std::fs::read(temp.path().join(source_coverage_path)).expect("source coverage bytes");
        let table_coverage = table.state().table_coverage.clone();
        let starting_version = table.state().version;

        let capture = TraceCapture::default();
        let report = capture.run(table.optimize()).await?;

        assert_eq!(report.starting_version, starting_version);
        assert_eq!(report.committed_version, starting_version + 1);
        assert_eq!(report.candidate_source_segments, 1);
        assert_eq!(report.source_segments_replaced, 1);
        assert_eq!(report.replacement_segments_written, 2);
        assert_eq!(report.distinct_identities_materialized, 2);
        assert_eq!(report.rows_read, 4);
        assert_eq!(report.rows_written, 4);
        assert!(!report.no_op);
        let span = captured_optimize_span(&capture);
        assert_eq!(span.target, "timeseries_table_format::table::optimize");
        assert_eq!(span.level, tracing::Level::DEBUG);
        for (field, expected) in [
            ("starting_version", report.starting_version.to_string()),
            (
                "candidate_source_segments",
                report.candidate_source_segments.to_string(),
            ),
            (
                "replacement_segments_written",
                report.replacement_segments_written.to_string(),
            ),
            (
                "distinct_identities_materialized",
                report.distinct_identities_materialized.to_string(),
            ),
            ("rows_read", report.rows_read.to_string()),
            ("rows_written", report.rows_written.to_string()),
            ("committed_version", report.committed_version.to_string()),
            ("no_op", "false".to_string()),
            ("outcome", "succeeded".to_string()),
        ] {
            assert_eq!(span.fields.get(field), Some(&expected));
        }
        let events: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| event.name == "table.optimize")
            .collect();
        assert_eq!(events.len(), 1, "expected one table.optimize event");
        assert_eq!(events[0].target, "timeseries_table_format::table::optimize");
        assert_eq!(events[0].level, tracing::Level::INFO);
        for (field, expected) in [
            ("starting_version", report.starting_version.to_string()),
            ("committed_version", report.committed_version.to_string()),
            (
                "candidate_source_segments",
                report.candidate_source_segments.to_string(),
            ),
            (
                "replacement_segments_written",
                report.replacement_segments_written.to_string(),
            ),
            (
                "distinct_identities_materialized",
                report.distinct_identities_materialized.to_string(),
            ),
            ("rows_read", report.rows_read.to_string()),
            ("rows_written", report.rows_written.to_string()),
            ("outcome", "succeeded".to_string()),
        ] {
            assert_eq!(events[0].fields.get(field), Some(&expected));
        }
        assert!(
            events[0]
                .fields
                .get("message")
                .is_some_and(|message| message.contains("Optimized time-series table"))
        );
        for value in span.fields.into_values().chain(
            events
                .into_iter()
                .flat_map(|event| event.fields.into_values()),
        ) {
            assert!(!value.contains(&temp.path().display().to_string()));
            assert!(!value.contains("EntityIdentity"));
            assert!(!value.contains("LogicalSchema"));
            assert_ne!(value, "A");
            assert_ne!(value, "B");
        }
        assert!(!table.state().segments.contains_key(&source_path));
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
            std::fs::read(temp.path().join(&source_path)).expect("source remains"),
            source_bytes
        );
        assert_eq!(
            std::fs::read(temp.path().join(source_coverage_path)).expect("source coverage remains"),
            source_coverage_bytes
        );
        Ok(())
    }

    #[tokio::test]
    async fn optimize_rewrites_numeric_entities_with_typed_single_layouts() -> Result<(), TableError>
    {
        let temp = TempDir::new().expect("temp directory");
        let location = TableLocation::local(temp.path());
        let mut table =
            TimeSeriesTable::create(location.clone(), make_int32_entity_table_meta()).await?;
        let fixture_path = "data/numeric-mixed.parquet";
        write_int32_entity_parquet(
            &temp.path().join(fixture_path),
            &[1_000, 2_000, 61_000, 62_000],
            &[-1, i32::MAX, -1, i32::MAX],
            &[10.0, 20.0, 11.0, 21.0],
        )
        .expect("write numeric mixed source");
        append_parquet_fixture(&mut table, fixture_path).await?;
        let source_path = table
            .state()
            .segments
            .keys()
            .next()
            .expect("committed source");
        assert_eq!(
            table.state().segments[source_path].entity_layout,
            SegmentEntityLayout::Mixed
        );
        let table_meta = table.state().table_meta.clone();

        let report = table.optimize().await?;

        assert_eq!(report.replacement_segments_written, 2);
        assert_eq!(report.rows_written, 4);
        assert_eq!(table.state().table_meta, table_meta);
        let identities = table
            .state()
            .segments
            .values()
            .map(|segment| match &segment.entity_layout {
                SegmentEntityLayout::Single(identity) => identity.clone(),
                layout => panic!("expected typed single-entity layout, found {layout:?}"),
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            identities,
            BTreeSet::from([
                EntityIdentity::try_new(vec![EntityValue::Int32(-1)]).expect("negative identity"),
                EntityIdentity::try_new(vec![EntityValue::Int32(i32::MAX)])
                    .expect("maximum identity"),
            ])
        );

        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state(), table.state());
        Ok(())
    }

    #[tokio::test]
    async fn later_staging_failure_cleans_every_earlier_rewrite() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let mut table = TimeSeriesTable::create(
            TableLocation::local(temp.path()),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        append_mixed_source(&mut table, temp.path(), "data/first.parquet", 0).await?;
        let broken_path =
            append_mixed_source(&mut table, temp.path(), "data/broken.parquet", 120_000).await?;
        let state_before = table.state().clone();
        let objects_before = optimization_objects(temp.path()).expect("optimization objects");
        std::fs::remove_file(temp.path().join(broken_path)).expect("remove later source");

        let error = table.optimize().await.expect_err("later staging must fail");

        assert!(matches!(
            error,
            TableError::Optimize {
                source: OptimizeError::MixedSegmentRewrite { .. }
            }
        ));
        assert_eq!(table.state(), &state_before);
        assert_eq!(
            optimization_objects(temp.path()).expect("optimization objects"),
            objects_before
        );
        Ok(())
    }

    #[tokio::test]
    async fn occ_conflict_cleans_staged_objects_and_preserves_state() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let location = TableLocation::local(temp.path());
        let mut table = TimeSeriesTable::create(
            location.clone(),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        append_mixed_source(&mut table, temp.path(), "data/candidate.parquet", 0).await?;
        let state_before = table.state().clone();
        let mut concurrent = TimeSeriesTable::open(location).await?;
        append_mixed_source(
            &mut concurrent,
            temp.path(),
            "data/concurrent.parquet",
            120_000,
        )
        .await?;
        let objects_before = optimization_objects(temp.path()).expect("optimization objects");

        let error = table.optimize().await.expect_err("stale commit must fail");

        assert!(matches!(
            error,
            TableError::Optimize {
                source: OptimizeError::Commit {
                    source: CommitError::Conflict { .. }
                }
            }
        ));
        assert_eq!(table.state(), &state_before);
        assert_eq!(
            table
                .log
                .load_current_version()
                .await
                .expect("current version"),
            state_before.version + 1
        );
        assert_eq!(
            optimization_objects(temp.path()).expect("optimization objects"),
            objects_before
        );
        Ok(())
    }

    #[tokio::test]
    async fn ambiguous_commit_retains_staged_objects_and_preserves_state() -> Result<(), TableError>
    {
        let temp = TempDir::new().expect("temp directory");
        let mut table = TimeSeriesTable::create(
            TableLocation::local(temp.path()),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        append_mixed_source(&mut table, temp.path(), "data/candidate.parquet", 0).await?;
        let state_before = table.state().clone();
        let objects_before = optimization_objects(temp.path()).expect("optimization objects");
        let commit_path = temp
            .path()
            .join(layout::commit_rel_path(state_before.version + 1));
        crate::storage::inject_write_new_failure(commit_path.clone(), true);

        let capture = TraceCapture::default();
        let error = capture
            .run(table.optimize())
            .await
            .expect_err("commit outcome must be ambiguous");

        assert!(matches!(
            error,
            TableError::Optimize {
                source: OptimizeError::Commit {
                    source: CommitError::AmbiguousOutcome { .. }
                }
            }
        ));
        let span = captured_optimize_span(&capture);
        assert_eq!(
            span.fields.get("outcome").map(String::as_str),
            Some("ambiguous")
        );
        assert_no_optimize_event(&capture);
        assert_eq!(table.state(), &state_before);
        assert_eq!(
            table
                .log
                .load_current_version()
                .await
                .expect("current version"),
            state_before.version
        );
        assert!(commit_path.exists());
        let objects_after = optimization_objects(temp.path()).expect("optimization objects");
        assert_eq!(objects_after.difference(&objects_before).count(), 4);
        assert_eq!(
            files_below(&temp.path().join("data/_staged"))
                .expect("staged data")
                .len(),
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn rollback_reports_every_cleanup_failure_in_reverse_order() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let table = TimeSeriesTable::create(
            TableLocation::local(temp.path()),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        let paths = [
            "data/_staged/entity-rewrite/first.parquet".to_string(),
            format!("{}/second.roar", layout::SEGMENT_COVERAGE_DIR),
        ];
        for path in &paths {
            let absolute = temp.path().join(path);
            std::fs::create_dir_all(absolute.parent().expect("object parent"))
                .expect("create object parent");
            std::fs::write(&absolute, b"staged").expect("write staged object");
            crate::storage::inject_cleanup_failure(absolute);
        }

        let error = table
            .rollback_optimization(&paths, invalid_plan("primary failure"))
            .await;
        let message = error.to_string();

        assert!(matches!(
            error,
            OptimizeError::Rollback {
                source,
                cleanup_errors,
            } if matches!(*source, OptimizeError::InvalidStagedPlan { .. })
                && cleanup_errors.len() == 2
                && cleanup_errors[0].to_string().contains("second.roar")
                && cleanup_errors[1].to_string().contains("first.parquet")
        ));
        assert!(message.contains("primary failure"));
        assert!(paths.iter().all(|path| temp.path().join(path).exists()));
        Ok(())
    }

    #[tokio::test]
    async fn multiple_sources_reopen_recover_and_repeat_as_a_no_op() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let location = TableLocation::local(temp.path());
        let mut table = TimeSeriesTable::create(
            location.clone(),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        let source_paths = [
            append_mixed_source(&mut table, temp.path(), "data/first.parquet", 0).await?,
            append_mixed_source(&mut table, temp.path(), "data/second.parquet", 120_000).await?,
        ];
        let sources = source_paths
            .iter()
            .map(|path| table.state().segments[path].clone())
            .collect::<Vec<_>>();
        let expected_coverage = table
            .load_entity_coverage_with_recovery::<AppendError>()
            .await?;
        let coverage_pointer = table
            .state()
            .table_coverage
            .clone()
            .expect("table coverage pointer");
        let coverage_bytes = std::fs::read(temp.path().join(&coverage_pointer.coverage_path))
            .expect("table coverage bytes");
        let snapshot_files =
            files_below(&temp.path().join(layout::TABLE_SNAPSHOT_DIR)).expect("snapshot files");
        let table_meta = table.state().table_meta.clone();
        let starting_version = table.state().version;

        let report = table.optimize().await?;

        assert_eq!(
            report,
            OptimizeReport {
                starting_version,
                committed_version: starting_version + 1,
                candidate_source_segments: 2,
                source_segments_replaced: 2,
                replacement_segments_written: 4,
                distinct_identities_materialized: 2,
                rows_read: 8,
                rows_written: 8,
                no_op: false,
            }
        );
        assert_eq!(table.state().segments.len(), 4);
        assert_eq!(table.state().table_meta, table_meta);
        assert!(
            table
                .state()
                .segments
                .values()
                .all(|segment| matches!(segment.entity_layout, SegmentEntityLayout::Single(_)))
        );
        assert_eq!(table.state().table_coverage, Some(coverage_pointer.clone()));
        assert_eq!(
            std::fs::read(temp.path().join(&coverage_pointer.coverage_path))
                .expect("table coverage bytes"),
            coverage_bytes
        );
        assert_eq!(
            files_below(&temp.path().join(layout::TABLE_SNAPSHOT_DIR)).expect("snapshot files"),
            snapshot_files
        );
        for source in &sources {
            assert!(temp.path().join(&source.path).exists());
            assert!(
                temp.path()
                    .join(source.coverage_path.as_deref().expect("source coverage"))
                    .exists()
            );
        }

        let commit = table
            .log
            .load_commit(report.committed_version)
            .await
            .expect("optimization commit");
        assert_eq!(commit.base_version, starting_version);
        assert_eq!(commit.actions.len(), 6);
        assert!(
            commit.actions[..2]
                .iter()
                .zip(&source_paths)
                .all(|(action, expected)| matches!(
                    action,
                    LogAction::RemoveSegment { path } if path == expected
                ))
        );
        assert!(
            commit.actions[2..]
                .iter()
                .all(|action| matches!(action, LogAction::AddSegment(_)))
        );

        let state_after_first = table.state().clone();
        let objects_after_first = optimization_objects(temp.path()).expect("optimization objects");
        let second_report = table.optimize().await?;
        assert_eq!(
            second_report,
            OptimizeReport::no_op(report.committed_version)
        );
        assert_eq!(table.state(), &state_after_first);
        assert_eq!(
            optimization_objects(temp.path()).expect("optimization objects"),
            objects_after_first
        );
        assert_eq!(
            table
                .log
                .load_current_version()
                .await
                .expect("current version"),
            report.committed_version
        );

        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state(), table.state());
        assert_eq!(
            reopened
                .recover_entity_coverage_from_segments::<AppendError>()
                .await?,
            expected_coverage
        );
        let mut scan = reopened
            .scan_range(
                chrono::DateTime::from_timestamp_millis(0).expect("range start"),
                chrono::DateTime::from_timestamp_millis(240_000).expect("range end"),
            )
            .await?;
        let mut rows = 0;
        while let Some(batch) = scan.next().await {
            rows += batch?.num_rows();
        }
        assert_eq!(rows, 8);
        Ok(())
    }

    #[test]
    fn accumulated_report_counts_do_not_wrap() {
        let mut total = u64::MAX;

        let error = add("rows_written", &mut total, 1).expect_err("count overflow must fail");

        assert!(matches!(
            error,
            OptimizeError::CountOverflow {
                field: "rows_written",
                ..
            }
        ));
        assert_eq!(total, u64::MAX);
    }

    #[tokio::test]
    async fn version_overflow_fails_before_staging() -> Result<(), TableError> {
        let temp = TempDir::new().expect("temp directory");
        let mut table = TimeSeriesTable::create(
            TableLocation::local(temp.path()),
            make_table_meta_with_unit(LogicalTimestampUnit::Millis),
        )
        .await?;
        append_mixed_source(&mut table, temp.path(), "data/candidate.parquet", 0).await?;
        table.state.version = u64::MAX;
        let objects_before = optimization_objects(temp.path()).expect("optimization objects");

        let error = table
            .optimize()
            .await
            .expect_err("version overflow must fail");

        assert!(matches!(
            error,
            TableError::Optimize {
                source: OptimizeError::CountOverflow {
                    field: "committed_version",
                    ..
                }
            }
        ));
        assert_eq!(table.state().version, u64::MAX);
        assert_eq!(
            optimization_objects(temp.path()).expect("optimization objects"),
            objects_before
        );
        Ok(())
    }
}
