//! Reconstructing the current table state by replaying log commits.
//!
//! `TableState` materializes the metadata stored in `_timeseries_log/` and the
//! [`TransactionLogStore::rebuild_table_state`] helper walks all commits from version 1 up
//! to the `CURRENT` pointer, applying their actions in order. This keeps read
//! logic isolated from the append-only write path and documents the invariant
//! that table readers must see a state consistent with the latest committed
//! version.
use std::{collections::HashMap, path::Path};

#[cfg(feature = "test-counters")]
use std::cell::Cell;

#[cfg(feature = "test-counters")]
thread_local! {
    static REBUILD_TABLE_STATE_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[cfg(feature = "test-counters")]
/// Return the number of rebuilds invoked on the current thread (test-only).
pub fn rebuild_table_state_count() -> usize {
    REBUILD_TABLE_STATE_COUNT.with(|c| c.get())
}

#[cfg(feature = "test-counters")]
/// Reset the rebuild counter to zero (test-only).
pub fn reset_rebuild_table_state_count() {
    REBUILD_TABLE_STATE_COUNT.with(|c| c.set(0));
}

use crate::{
    metadata::{
        schema_compat::{ensure_entity_identity_matches_schema, ensure_index_spec_matches_schema},
        segments::sort_segment_meta_by_index,
    },
    storage::normalize_relative_storage_path,
    transaction_log::*,
};

fn validate_persisted_storage_path(path: &str, description: &str) -> Result<(), CommitError> {
    let (canonical, _) = match normalize_relative_storage_path(Path::new(path)) {
        Ok(path) => path,
        Err(source) => {
            return CorruptStateSnafu {
                msg: format!("Invalid persisted {description} {path:?}: {source}"),
            }
            .fail();
        }
    };

    if canonical != path {
        return CorruptStateSnafu {
            msg: format!(
                "Non-canonical persisted {description} {path:?}; canonical form is {canonical:?}"
            ),
        }
        .fail();
    }

    Ok(())
}

/// Pointer to table coverage metadata including index descriptor, path, and version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCoveragePointer {
    /// Canonical ordered-index coverage descriptor.
    pub index_kind: IndexKind,
    /// Path to the coverage metadata file.
    pub coverage_path: String,
    /// Version number associated with this coverage pointer.
    pub version: u64,
}

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
    /// Current live segments keyed by canonical table-relative path.
    pub segments: HashMap<String, SegmentMeta>,

    /// Optional pointer to the latest table coverage metadata.
    pub table_coverage: Option<TableCoveragePointer>,
}

impl TableState {
    /// Return live segments sorted deterministically by ordered-index bounds.
    ///
    /// Ordering is by `index_min`, then `index_max`, and finally `path` as a
    /// stable tie-breaker.
    pub fn segments_sorted_by_index(
        &self,
    ) -> Result<Vec<&SegmentMeta>, crate::metadata::table_metadata::IndexValueError> {
        let mut v: Vec<&SegmentMeta> = self.segments.values().collect();
        sort_segment_meta_by_index(&mut v)?;
        Ok(v)
    }
}

impl TransactionLogStore {
    /// Rebuild the current TableState by replaying all commits up to CURRENT.
    ///
    /// v0.1 behavior:
    /// - If CURRENT == 0 (no commits), this returns CommitError::CorruptState.
    /// - The first commit must include at least one UpdateTableMeta action
    ///   to bootstrap TableMeta; the last UpdateTableMeta wins.
    pub async fn rebuild_table_state(&self) -> Result<TableState, CommitError> {
        #[cfg(feature = "test-counters")]
        REBUILD_TABLE_STATE_COUNT.with(|c| c.set(c.get() + 1));

        let current_version = self.load_current_version().await?;

        if current_version == 0 {
            // v0.1: treat "no commits" as an uninitialized / corrupt table.
            return CorruptStateSnafu {
                msg: "Cannot rebuild TableState: CURRENT is 0 (no commits)".to_string(),
            }
            .fail();
        }

        let mut table_meta: Option<TableMeta> = None;
        let mut segments: HashMap<String, SegmentMeta> = HashMap::new();
        let mut persisted_segment_layouts = Vec::new();

        let mut table_coverage: Option<TableCoveragePointer> = None;

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
                        validate_persisted_storage_path(&meta.path, "segment path")?;
                        if let Some(coverage_path) = &meta.coverage_path {
                            validate_persisted_storage_path(
                                coverage_path,
                                "segment coverage path",
                            )?;
                        }
                        if segments.contains_key(&meta.path) {
                            return CorruptStateSnafu {
                                msg: format!("Duplicate live segment path: {}", meta.path),
                            }
                            .fail();
                        }
                        persisted_segment_layouts
                            .push((meta.path.clone(), meta.entity_layout.clone()));
                        segments.insert(meta.path.clone(), meta);
                    }
                    LogAction::RemoveSegment { path } => {
                        validate_persisted_storage_path(&path, "segment path")?;
                        segments.remove(&path);
                    }
                    LogAction::UpdateTableMeta(delta) => {
                        // v0.1: full replacement of TableMeta
                        table_meta = Some(delta);
                    }
                    LogAction::UpdateTableCoverage {
                        index_kind,
                        coverage_path,
                    } => {
                        validate_persisted_storage_path(&coverage_path, "table coverage path")?;
                        table_coverage = Some(TableCoveragePointer {
                            index_kind,
                            coverage_path,
                            version: v,
                        })
                    }
                }
            }
        }

        let table_meta = table_meta.context(CorruptStateSnafu {
            msg: format!("No TableMeta found in commits up to version {current_version}",),
        })?;

        if let TableKind::TimeSeries(index) = &table_meta.kind {
            index
                .validate()
                .map_err(|source| CommitError::CorruptState {
                    msg: format!("Invalid ordered index specification: {source}"),
                    backtrace: snafu::Backtrace::capture(),
                })?;
            if let Some(pointer) = &table_coverage
                && pointer.index_kind != index.kind
            {
                return CorruptStateSnafu {
                    msg: format!(
                        "Table coverage index kind does not match table index: expected {:?}, found {:?} in pointer from version {}",
                        index.kind, pointer.index_kind, pointer.version
                    ),
                }
                .fail();
            }
            let schema = table_meta.logical_schema.as_ref();
            if let Some(schema) = schema {
                ensure_index_spec_matches_schema(schema, index).map_err(|source| {
                    CommitError::CorruptState {
                        msg: format!("Index specification does not match logical schema: {source}"),
                        backtrace: snafu::Backtrace::capture(),
                    }
                })?;
            }
            if schema.is_none() && !persisted_segment_layouts.is_empty() {
                return CorruptStateSnafu {
                    msg: "Persisted segments require a logical schema".to_string(),
                }
                .fail();
            }
            let entity_column_count = index.entity_columns.len();
            for (path, layout) in persisted_segment_layouts {
                match (&layout, entity_column_count) {
                    (SegmentEntityLayout::NotApplicable, 0) | (SegmentEntityLayout::Mixed, 1..) => {
                    }
                    (SegmentEntityLayout::Single(identity), 1..) => {
                        let Some(schema) = schema else {
                            return CorruptStateSnafu {
                                msg: "Persisted segments require a logical schema".to_string(),
                            }
                            .fail();
                        };
                        ensure_entity_identity_matches_schema(schema, index, identity).map_err(
                            |source| CommitError::CorruptState {
                                msg: format!(
                                    "Invalid single-entity identity in segment at {path}: {source}"
                                ),
                                backtrace: snafu::Backtrace::capture(),
                            },
                        )?;
                    }
                    _ => {
                        return CorruptStateSnafu {
                            msg: format!(
                                "Invalid entity layout in segment at {path}: table has {entity_column_count} entity columns, layout is {layout:?}"
                            ),
                        }
                        .fail();
                    }
                }
            }
            for segment in segments.values() {
                segment.validate_bounds(&index.kind).map_err(|source| {
                    CommitError::CorruptState {
                        msg: source.to_string(),
                        backtrace: snafu::Backtrace::capture(),
                    }
                })?;
            }
        }

        Ok(TableState {
            version: current_version,
            table_meta,
            segments,
            table_coverage,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coverage::EntityIdentity;
    use crate::metadata::{
        logical_schema::{LogicalDataType, LogicalField, LogicalSchema, LogicalTimestampUnit},
        table_metadata::TABLE_FORMAT_VERSION,
    };
    use crate::storage::layout;
    use crate::storage::{StorageError, TableLocation};
    use crate::transaction_log::{
        FileFormat, IndexKind, IndexSpec, LogAction, SegmentEntityLayout, SegmentMeta, TableKind,
        TableMeta, TimeIndexGranularity, TransactionLogStore,
    };
    use chrono::TimeZone;
    use tempfile::TempDir;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn create_test_log_store() -> (TempDir, TransactionLogStore) {
        let tmp = TempDir::new().expect("create temp dir");
        let location = TableLocation::local(tmp.path());
        let store = TransactionLogStore::new(location);
        (tmp, store)
    }

    fn sample_table_meta() -> TableMeta {
        let entity_columns = vec!["symbol".to_string()];
        TableMeta {
            kind: TableKind::TimeSeries(IndexSpec {
                column: "ts".to_string(),
                entity_columns: entity_columns.clone(),
                kind: IndexKind::Timestamp {
                    index_granularity: TimeIndexGranularity::Minutes(1),
                    timezone: None,
                },
            }),
            logical_schema: Some(schema_for_entities(&entity_columns)),
            created_at: chrono::Utc
                .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
                .single()
                .expect("valid sample table metadata timestamp"),
            format_version: TABLE_FORMAT_VERSION,
        }
    }

    fn schema_for_entities(entity_columns: &[String]) -> LogicalSchema {
        let mut fields = vec![LogicalField {
            name: "ts".to_string(),
            data_type: LogicalDataType::Timestamp {
                unit: LogicalTimestampUnit::Millis,
                timezone: None,
            },
            nullable: false,
        }];
        fields.extend(entity_columns.iter().map(|column| LogicalField {
            name: column.clone(),
            data_type: LogicalDataType::Utf8,
            nullable: false,
        }));
        LogicalSchema::new(fields).expect("valid test schema")
    }

    fn sample_segment(id: &str) -> SegmentMeta {
        SegmentMeta {
            path: format!("data/{id}.parquet"),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::Single(
                EntityIdentity::try_new(vec!["A".into()]).expect("valid sample identity"),
            ),
            index_min: IndexValue::Timestamp(
                chrono::Utc
                    .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
                    .single()
                    .expect("valid sample segment index_min"),
            ),
            index_max: IndexValue::Timestamp(
                chrono::Utc
                    .with_ymd_and_hms(2025, 1, 1, 1, 0, 0)
                    .single()
                    .expect("valid sample segment index_max"),
            ),
            row_count: 42,
            file_size: None,
            coverage_path: None,
        }
    }

    fn segment_with_ts(id: &str, ts_min: i64, ts_max: i64) -> SegmentMeta {
        SegmentMeta {
            path: format!("data/{id}.parquet"),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::Single(
                EntityIdentity::try_new(vec!["A".into()]).expect("valid sample identity"),
            ),
            index_min: (chrono::Utc.timestamp_opt(ts_min, 0).single().unwrap()).into(),
            index_max: (chrono::Utc.timestamp_opt(ts_max, 0).single().unwrap()).into(),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        }
    }

    #[test]
    fn segments_sorted_by_index_orders_hashmap_deterministically() {
        let mut segments = HashMap::new();
        let seg_c = segment_with_ts("c", 10, 30);
        let seg_a = segment_with_ts("a", 10, 20);
        let seg_d = segment_with_ts("d", 5, 7);
        let seg_b = segment_with_ts("b", 10, 20);

        segments.insert(seg_c.path.clone(), seg_c);
        segments.insert(seg_a.path.clone(), seg_a);
        segments.insert(seg_d.path.clone(), seg_d);
        segments.insert(seg_b.path.clone(), seg_b);

        let state = TableState {
            version: 3,
            table_meta: sample_table_meta(),
            segments,
            table_coverage: None,
        };

        let ordered: Vec<(i64, i64, String)> = state
            .segments_sorted_by_index()
            .unwrap()
            .iter()
            .map(|seg| match (&seg.index_min, &seg.index_max) {
                (IndexValue::Timestamp(min), IndexValue::Timestamp(max)) => {
                    (min.timestamp(), max.timestamp(), seg.path.clone())
                }
                _ => panic!("expected timestamp test bounds"),
            })
            .collect();

        let mut expected = ordered.clone();
        expected.sort();
        assert_eq!(ordered, expected);
    }

    #[tokio::test]
    async fn rebuild_table_state_happy_path() -> TestResult {
        let (_tmp, store) = create_test_log_store();
        let meta = sample_table_meta();
        let seg1 = sample_segment("seg1");
        let seg2 = sample_segment("seg2");

        let v1 = store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta.clone())])
            .await?;
        let v2 = store
            .commit_with_expected_version(
                v1,
                vec![
                    LogAction::AddSegment(seg1.clone()),
                    LogAction::AddSegment(seg2.clone()),
                ],
            )
            .await?;
        let v3 = store
            .commit_with_expected_version(
                v2,
                vec![LogAction::RemoveSegment {
                    path: seg1.path.clone(),
                }],
            )
            .await?;

        let state = store.rebuild_table_state().await?;
        assert_eq!(state.version, v3);
        assert_eq!(state.table_meta, meta);
        assert!(state.segments.contains_key(&seg2.path));
        assert!(!state.segments.contains_key(&seg1.path));
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_leaves_table_kind_validation_to_callers() -> TestResult {
        let (_tmp, store) = create_test_log_store();
        let mut meta = sample_table_meta();
        meta.kind = TableKind::Generic;
        store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta.clone())])
            .await?;

        let state = store.rebuild_table_state().await?;

        assert_eq!(state.table_meta, meta);
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_errors_when_current_zero() {
        let (_tmp, store) = create_test_log_store();

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("expected error");
        assert!(matches!(err, CommitError::CorruptState { .. }));
    }

    #[tokio::test]
    async fn rebuild_table_state_errors_when_no_table_meta() -> TestResult {
        let (_tmp, store) = create_test_log_store();
        let seg = sample_segment("seg");

        store
            .commit_with_expected_version(0, vec![LogAction::AddSegment(seg.clone())])
            .await?;

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("expected error");
        assert!(matches!(err, CommitError::CorruptState { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_rejects_version_6_metadata() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let log_dir = tmp.path().join(layout::LOG_DIR_NAME);
        tokio::fs::create_dir_all(&log_dir).await?;
        tokio::fs::write(
            tmp.path().join(layout::commit_rel_path(1)),
            r#"{
                "version": 1,
                "base_version": 0,
                "timestamp": "2025-01-01T00:00:00Z",
                "actions": [{
                    "UpdateTableMeta": {
                        "kind": {"TimeSeries": {
                            "column": "ts",
                            "entity_columns": ["symbol"],
                            "kind": {
                                "type": "timestamp",
                                "bucket": {"Minutes": 1}
                            }
                        }},
                        "logical_schema": null,
                        "created_at": "2025-01-01T00:00:00Z",
                        "format_version": 6
                    }
                }]
            }"#,
        )
        .await?;
        tokio::fs::write(tmp.path().join(layout::current_rel_path()), "1\n").await?;

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("version 6 should be rejected");
        assert!(matches!(
            err,
            CommitError::UnsupportedFormatVersion {
                expected: TABLE_FORMAT_VERSION,
                found: 6,
            }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_rejects_invalid_persisted_segment_bounds() -> TestResult {
        let (_tmp, store) = create_test_log_store();
        let mut segment = sample_segment("reversed");
        segment.index_min =
            IndexValue::Timestamp(chrono::Utc.timestamp_opt(2, 0).single().unwrap());
        segment.index_max =
            IndexValue::Timestamp(chrono::Utc.timestamp_opt(1, 0).single().unwrap());

        store
            .commit_with_expected_version(
                0,
                vec![
                    LogAction::UpdateTableMeta(sample_table_meta()),
                    LogAction::AddSegment(segment),
                ],
            )
            .await?;

        let error = store.rebuild_table_state().await.unwrap_err();
        assert!(matches!(error, CommitError::CorruptState { .. }));
        assert!(error.to_string().contains("Invalid ordered-index bounds"));
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_rejects_inapplicable_entity_layouts() -> TestResult {
        let single = SegmentEntityLayout::Single(EntityIdentity::try_new(vec!["A".into()])?);
        let cases = [
            (
                vec!["symbol".to_string()],
                SegmentEntityLayout::NotApplicable,
                "Invalid entity layout",
            ),
            (
                Vec::new(),
                SegmentEntityLayout::Mixed,
                "Invalid entity layout",
            ),
            (Vec::new(), single.clone(), "Invalid entity layout"),
            (
                vec!["site".to_string(), "device".to_string()],
                single,
                "has 1 components, but the table configures 2",
            ),
        ];

        for (entity_columns, entity_layout, expected_message) in cases {
            let (_tmp, store) = create_test_log_store();
            let mut table_meta = sample_table_meta();
            let TableKind::TimeSeries(index) = &mut table_meta.kind else {
                unreachable!("sample metadata is time-series");
            };
            index.entity_columns = entity_columns.clone();
            table_meta.logical_schema = Some(schema_for_entities(&entity_columns));

            let mut segment = sample_segment("invalid-layout");
            segment.entity_layout = entity_layout;
            store
                .commit_with_expected_version(
                    0,
                    vec![
                        LogAction::UpdateTableMeta(table_meta),
                        LogAction::AddSegment(segment),
                    ],
                )
                .await?;

            let error = store
                .rebuild_table_state()
                .await
                .expect_err("inapplicable entity layout should be rejected");
            assert!(matches!(error, CommitError::CorruptState { .. }));
            assert!(error.to_string().contains(expected_message), "{error}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_validates_persisted_entity_component_types() -> TestResult {
        let typed_schema = LogicalSchema::new(vec![
            LogicalField {
                name: "ts".to_string(),
                data_type: LogicalDataType::Timestamp {
                    unit: LogicalTimestampUnit::Millis,
                    timezone: None,
                },
                nullable: false,
            },
            LogicalField {
                name: "symbol".to_string(),
                data_type: LogicalDataType::Int32,
                nullable: false,
            },
        ])?;
        let mut typed_meta = sample_table_meta();
        typed_meta.logical_schema = Some(typed_schema);
        let mut typed_segment = sample_segment("typed-layout");
        typed_segment.entity_layout = SegmentEntityLayout::Single(EntityIdentity::try_new(vec![
            crate::coverage::EntityValue::Int32(-1),
        ])?);

        let (_valid_tmp, valid_store) = create_test_log_store();
        valid_store
            .commit_with_expected_version(
                0,
                vec![
                    LogAction::UpdateTableMeta(typed_meta.clone()),
                    LogAction::AddSegment(typed_segment.clone()),
                ],
            )
            .await?;
        valid_store.rebuild_table_state().await?;

        let (_invalid_tmp, invalid_store) = create_test_log_store();
        let mut string_meta = typed_meta;
        string_meta.logical_schema = Some(schema_for_entities(&["symbol".to_string()]));
        invalid_store
            .commit_with_expected_version(
                0,
                vec![
                    LogAction::UpdateTableMeta(string_meta),
                    LogAction::AddSegment(typed_segment),
                ],
            )
            .await?;
        let error = invalid_store
            .rebuild_table_state()
            .await
            .expect_err("persisted component type must match the logical schema");
        assert!(matches!(error, CommitError::CorruptState { .. }));
        assert!(
            error
                .to_string()
                .contains("column symbol has type int32; expected utf8"),
            "{error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_validates_removed_segment_layouts() -> TestResult {
        let (_tmp, store) = create_test_log_store();
        let mut segment = sample_segment("removed-invalid-layout");
        segment.entity_layout = SegmentEntityLayout::NotApplicable;
        let path = segment.path.clone();

        store
            .commit_with_expected_version(
                0,
                vec![
                    LogAction::UpdateTableMeta(sample_table_meta()),
                    LogAction::AddSegment(segment),
                    LogAction::RemoveSegment { path },
                ],
            )
            .await?;

        let error = store
            .rebuild_table_state()
            .await
            .expect_err("removed segment metadata should still be validated");
        assert!(matches!(error, CommitError::CorruptState { .. }));
        assert!(error.to_string().contains("Invalid entity layout"));
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_requires_valid_entity_layout_json() -> TestResult {
        for (replacement, expected_message) in [
            (None, "entity_layout"),
            (
                Some(serde_json::json!({"Single": []})),
                "at least one component",
            ),
        ] {
            let (tmp, store) = create_test_log_store();
            store
                .commit_with_expected_version(
                    0,
                    vec![
                        LogAction::UpdateTableMeta(sample_table_meta()),
                        LogAction::AddSegment(sample_segment("invalid-json")),
                    ],
                )
                .await?;

            let commit_path = tmp.path().join(layout::commit_rel_path(1));
            let mut commit: serde_json::Value =
                serde_json::from_slice(&tokio::fs::read(&commit_path).await?)?;
            let segment = commit["actions"][1]["AddSegment"]
                .as_object_mut()
                .expect("valid committed AddSegment action");
            match replacement {
                Some(layout) => {
                    segment.insert("entity_layout".to_string(), layout);
                }
                None => {
                    segment.remove("entity_layout");
                }
            }
            tokio::fs::write(&commit_path, serde_json::to_vec(&commit)?).await?;

            let error = store
                .rebuild_table_state()
                .await
                .expect_err("missing or malformed entity layout should be rejected");
            assert!(matches!(error, CommitError::CorruptState { .. }));
            assert!(error.to_string().contains(expected_message), "{error}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_rejects_noncanonical_segment_action_paths() -> TestResult {
        for path in [
            "",
            "/data/seg.parquet",
            "../data/seg.parquet",
            "data/../seg.parquet",
            r"data\seg.parquet",
            "data//seg.parquet",
            r"C:\data\seg.parquet",
            "data/C:/seg.parquet",
            "data/C:seg.parquet",
        ] {
            let mut segment = sample_segment("seg");
            segment.path = path.to_owned();

            for action in [
                LogAction::AddSegment(segment.clone()),
                LogAction::RemoveSegment {
                    path: path.to_owned(),
                },
            ] {
                let (_tmp, store) = create_test_log_store();
                store
                    .commit_with_expected_version(
                        0,
                        vec![LogAction::UpdateTableMeta(sample_table_meta()), action],
                    )
                    .await?;

                let err = store
                    .rebuild_table_state()
                    .await
                    .expect_err("noncanonical segment action path should be rejected");
                assert!(matches!(err, CommitError::CorruptState { .. }));
                assert!(err.to_string().contains("segment path"), "{err}");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_rejects_noncanonical_coverage_paths() -> TestResult {
        for path in [
            "",
            "/tmp/coverage.roar",
            "../coverage.roar",
            "_coverage/../coverage.roar",
            r"_coverage\segments\coverage.roar",
            "_coverage//segments/coverage.roar",
            r"C:\coverage.roar",
        ] {
            let mut segment = sample_segment("seg");
            segment.coverage_path = Some(path.to_owned());

            let index_kind = match sample_table_meta().kind {
                TableKind::TimeSeries(index) => index.kind,
                TableKind::Generic => unreachable!("sample metadata is time-series"),
            };
            for (description, action) in [
                ("segment coverage path", LogAction::AddSegment(segment)),
                (
                    "table coverage path",
                    LogAction::UpdateTableCoverage {
                        index_kind,
                        coverage_path: path.to_owned(),
                    },
                ),
            ] {
                let (_tmp, store) = create_test_log_store();
                store
                    .commit_with_expected_version(
                        0,
                        vec![LogAction::UpdateTableMeta(sample_table_meta()), action],
                    )
                    .await?;

                let err = store
                    .rebuild_table_state()
                    .await
                    .expect_err("noncanonical coverage path should be rejected");
                assert!(matches!(err, CommitError::CorruptState { .. }));
                assert!(err.to_string().contains(description), "{err}");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_rejects_mismatched_table_coverage_index() -> TestResult {
        let (_tmp, store) = create_test_log_store();
        store
            .commit_with_expected_version(
                0,
                vec![
                    LogAction::UpdateTableMeta(sample_table_meta()),
                    LogAction::UpdateTableCoverage {
                        index_kind: IndexKind::Int64 {
                            index_granularity: std::num::NonZeroU64::new(1).unwrap(),
                        },
                        coverage_path: "_coverage/table/1-mismatched.roar".to_string(),
                    },
                ],
            )
            .await?;

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("mismatched coverage index should be rejected during replay");
        assert!(matches!(err, CommitError::CorruptState { .. }));
        assert!(err.to_string().contains("Table coverage index kind"));
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_fails_on_corrupt_commit_payload() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let meta = sample_table_meta();

        store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
            .await?;

        let commit_path = tmp.path().join(layout::commit_rel_path(1));
        tokio::fs::write(&commit_path, b"not-json").await?;

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("expected error");
        assert!(matches!(err, CommitError::CorruptState { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn rebuild_table_state_fails_when_commit_missing() -> TestResult {
        let (tmp, store) = create_test_log_store();
        let meta = sample_table_meta();

        store
            .commit_with_expected_version(0, vec![LogAction::UpdateTableMeta(meta)])
            .await?;

        let commit_path = tmp.path().join(layout::commit_rel_path(1));
        tokio::fs::remove_file(&commit_path).await?;

        let err = store
            .rebuild_table_state()
            .await
            .expect_err("expected error");
        match err {
            CommitError::Storage { source } => match source {
                StorageError::NotFound { .. } => {}
                other => panic!("unexpected storage error: {other:?}"),
            },
            other => panic!("expected storage error, got {other:?}"),
        }
        Ok(())
    }
}
