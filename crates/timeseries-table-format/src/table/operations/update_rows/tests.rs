use super::*;
use crate::{
    IndexKind, IndexSpec, LogicalDataType, LogicalField, LogicalSchema, TableLocation, TableMeta,
    UpdateKeyValue, UpdateKeyViolation,
    storage::{self, layout},
};
use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchIterator},
};
use futures::TryStreamExt;
use std::{
    cell::Cell,
    collections::BTreeMap,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};
use tempfile::TempDir;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn batch(rows: &[(i64, &str, Option<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}

fn reader(batch: RecordBatch) -> impl RecordBatchReader {
    RecordBatchIterator::new([Ok(batch.clone())], batch.schema())
}

async fn populated() -> TestResult<(TempDir, TimeSeriesTable)> {
    let dir = TempDir::new()?;
    let schema = LogicalSchema::try_from_arrow_schema(batch(&[]).schema().as_ref())?;
    let meta = TableMeta::new_time_series_with_schema(
        IndexSpec {
            column: "idx".into(),
            entity_columns: vec!["entity".into()],
            kind: IndexKind::Int64 {
                index_granularity: std::num::NonZeroU64::MIN,
            },
        },
        schema,
    );
    let mut table = TimeSeriesTable::create(TableLocation::local(dir.path()), meta).await?;
    for idx in [0, 1, 2] {
        table
            .append(batch(&[(idx, "A", Some(idx)), (idx, "B", Some(idx + 10))]))
            .await?;
    }
    Ok((dir, table))
}

fn files(root: &Path) -> std::io::Result<BTreeMap<PathBuf, Vec<u8>>> {
    let mut result = BTreeMap::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            result.extend(files(&entry.path())?);
        } else {
            result.insert(entry.path(), std::fs::read(entry.path())?);
        }
    }
    Ok(result)
}

async fn values(
    table: &TimeSeriesTable,
    column: &str,
) -> TestResult<BTreeMap<(i64, String), Option<i64>>> {
    let batches: Vec<_> = table
        .scan_range(0_i64, 100_i64)
        .await?
        .try_collect()
        .await?;
    let mut result = BTreeMap::new();
    for b in batches {
        let idx = b
            .column_by_name("idx")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let entity = b
            .column_by_name("entity")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value = b
            .column_by_name(column)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..b.num_rows() {
            assert!(
                result
                    .insert(
                        (idx.value(row), entity.value(row).into()),
                        if value.is_null(row) {
                            None
                        } else {
                            Some(value.value(row))
                        }
                    )
                    .is_none(),
                "duplicate stored key"
            );
        }
    }
    Ok(result)
}

#[tokio::test]
async fn atomic_updates_preserve_snapshots_coverage_and_compose_with_mutations() -> TestResult {
    let (dir, mut table) = populated().await?;
    table
        .add_columns(vec![LogicalField {
            name: "score".into(),
            data_type: LogicalDataType::Int64,
            nullable: true,
        }])
        .await?;
    let old = table.clone();
    let old_values = values(&old, "value").await?;
    let old_scan = old.scan_range(0_i64, 100_i64).await?;
    let before = table.state().clone();
    let original_files = files(dir.path())?;
    let source = batch(&[(2, "B", None), (0, "A", Some(99))]);
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("score", DataType::Int64, true),
        Field::new("entity", DataType::Utf8, false),
        Field::new("idx", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ]));
    let source = RecordBatch::try_new(
        source_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(8), None])),
            source.column(1).clone(),
            source.column(0).clone(),
            source.column(2).clone(),
        ],
    )?;
    let report = table
        .update_rows(
            RecordBatchIterator::new(
                [Ok(source.slice(0, 1)), Ok(source.slice(1, 1))],
                source_schema,
            ),
            vec!["value".into(), "score".into()],
            before.version,
        )
        .await?;
    assert_eq!(
        report,
        UpdateRowsReport {
            starting_version: before.version,
            committed_version: before.version + 1,
            rows_updated: 2,
            segments_rewritten: 2,
            source_file_bytes: before
                .segments
                .values()
                .filter(|s| !table.state.segments.contains_key(&s.path))
                .map(|s| s.file_size.unwrap())
                .sum(),
            replacement_file_bytes: table
                .state
                .segments
                .values()
                .filter(|s| !before.segments.contains_key(&s.path))
                .map(|s| s.file_size.unwrap())
                .sum(),
            no_op: false
        }
    );
    assert_eq!(table.state.table_meta, before.table_meta);
    assert_eq!(table.state.table_coverage, before.table_coverage);
    assert_eq!(table.state.segments.len(), before.segments.len());
    assert_eq!(
        table
            .log
            .load_commit(report.committed_version)
            .await?
            .actions
            .len(),
        4
    );
    for (path, bytes) in original_files {
        if path != dir.path().join(layout::current_rel_path()) {
            assert_eq!(std::fs::read(path)?, bytes);
        }
    }
    assert_eq!(values(&old, "value").await?, old_values);
    let batches: Vec<_> = old_scan.try_collect().await?;
    assert!(
        batches
            .iter()
            .all(|b| b.column_by_name("score").unwrap().null_count() == b.num_rows())
    );
    let mut expected = old_values.clone();
    expected.insert((0, "A".into()), Some(99));
    expected.insert((2, "B".into()), None);
    let reopened = TimeSeriesTable::open(table.location().clone()).await?;
    assert_eq!(reopened.state(), table.state());
    assert_eq!(values(&reopened, "value").await?, expected);
    let scores = values(&reopened, "score").await?;
    assert_eq!(scores[&(2, "B".into())], Some(8));
    assert_eq!(scores.values().filter(|v| v.is_some()).count(), 1);
    table.append(batch(&[(3, "A", Some(3))])).await?;
    table.optimize().await?;
    expected.insert((3, "A".into()), Some(3));
    assert_eq!(values(&table, "value").await?, expected);
    assert_eq!(values(&old, "value").await?, old_values);
    Ok(())
}

struct ObservedReader {
    schema: SchemaRef,
    input: std::vec::IntoIter<Result<RecordBatch, ArrowError>>,
    calls: Rc<Cell<usize>>,
    at_end: Option<Box<dyn FnOnce()>>,
}
impl Iterator for ObservedReader {
    type Item = Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.calls.set(self.calls.get() + 1);
        let next = self.input.next();
        if next.is_none()
            && let Some(f) = self.at_end.take()
        {
            f();
        }
        next
    }
}
impl RecordBatchReader for ObservedReader {
    fn schema(&self) -> SchemaRef {
        self.calls.set(self.calls.get() + 1);
        self.schema.clone()
    }
}

#[tokio::test]
async fn preflight_never_inspects_reader_and_non_send_reader_is_supported() -> TestResult {
    let (dir, mut table) = populated().await?;
    let version = table.state.version;
    let current = dir.path().join(layout::current_rel_path());
    for case in 0..4 {
        let calls = Rc::new(Cell::new(0));
        let input = ObservedReader {
            schema: batch(&[]).schema(),
            input: vec![].into_iter(),
            calls: calls.clone(),
            at_end: None,
        };
        let expected = match case {
            0 => 0,
            1 => version - 1,
            _ => version,
        };
        if case == 2 {
            std::fs::write(&current, (version + 1).to_string())?;
        }
        if case == 3 {
            table
                .state
                .table_meta
                .required_writer_features
                .insert("unsupported".into());
        }
        let error = table
            .update_rows(input, vec!["value".into()], expected)
            .await
            .unwrap_err();
        assert_eq!(calls.get(), 0);
        match (case, error) {
            (
                0,
                TableError::UpdateRows {
                    source: UpdateRowsError::ZeroVersion,
                },
            )
            | (
                1,
                TableError::UpdateRows {
                    source: UpdateRowsError::SnapshotMismatch { .. },
                },
            )
            | (
                2,
                TableError::UpdateRows {
                    source:
                        UpdateRowsError::Commit {
                            source: CommitError::Conflict { .. },
                        },
                },
            )
            | (
                3,
                TableError::UpdateRows {
                    source: UpdateRowsError::Protocol { .. },
                },
            ) => {}
            (_, other) => panic!("wrong preflight diagnostic: {other:?}"),
        }
        std::fs::write(&current, version.to_string())?;
        table.state.table_meta.required_writer_features.clear();
    }
    let input = ObservedReader {
        schema: batch(&[]).schema(),
        input: vec![Ok(batch(&[(0, "A", Some(42))]))].into_iter(),
        calls: Rc::new(Cell::new(0)),
        at_end: None,
    };
    table
        .update_rows(input, vec!["value".into()], version)
        .await?;
    assert_eq!(values(&table, "value").await?[&(0, "A".into())], Some(42));
    Ok(())
}

#[tokio::test]
async fn empty_and_late_failures_are_fully_validated_without_publication() -> TestResult {
    let (dir, mut table) = populated().await?;
    let before = files(dir.path())?;
    let state = table.state.clone();
    let empty = batch(&[]);
    let report = table
        .update_rows(reader(empty.clone()), vec!["value".into()], state.version)
        .await?;
    assert!(report.no_op);
    assert_eq!(report.committed_version, state.version);
    assert_eq!(report.source_file_bytes, 0);
    let invalid = RecordBatch::new_empty(Arc::new(Schema::empty()));
    assert!(
        table
            .update_rows(reader(invalid), vec!["value".into()], state.version)
            .await
            .is_err()
    );
    for first in [empty.clone(), batch(&[(0, "A", Some(90))])] {
        let input = RecordBatchIterator::new(
            vec![
                Ok(first),
                Err(ArrowError::ComputeError("late input failure".into())),
            ],
            empty.schema(),
        );
        assert!(matches!(
            table
                .update_rows(input, vec!["value".into()], state.version)
                .await,
            Err(TableError::UpdateRows {
                source: UpdateRowsError::Preparation {
                    source: PrepareError::Reader { .. }
                }
            })
        ));
    }
    for (bad, kind) in [
        (
            batch(&[(0, "A", Some(2))]),
            UpdateKeyViolation::DuplicateSource,
        ),
        (
            batch(&[(99, "A", None)]),
            UpdateKeyViolation::UnmatchedSource,
        ),
    ] {
        let input =
            RecordBatchIterator::new([Ok(batch(&[(0, "A", Some(90))])), Ok(bad)], empty.schema());
        let error = table
            .update_rows(input, vec!["value".into()], state.version)
            .await
            .unwrap_err();
        match error {
            TableError::UpdateRows {
                source:
                    UpdateRowsError::Preparation {
                        source:
                            PrepareError::Key {
                                kind: found,
                                example_key,
                                ..
                            },
                    },
            } => {
                assert_eq!(found, kind);
                assert_eq!(
                    example_key.components()[0],
                    ("entity".into(), Some(UpdateKeyValue::Utf8("A".into())))
                );
                assert_eq!(example_key.components().len(), 2);
            }
            other => panic!("unexpected key diagnostic {other:?}"),
        }
    }
    assert_eq!(files(dir.path())?, before);
    assert_eq!(table.state, state);
    // The reader may trigger a concurrent publication even when it has no rows.
    let current = dir.path().join(layout::current_rel_path());
    let input = ObservedReader {
        schema: empty.schema(),
        input: vec![Ok(empty)].into_iter(),
        calls: Rc::new(Cell::new(0)),
        at_end: Some(Box::new(move || {
            std::fs::write(current, (state.version + 1).to_string()).unwrap()
        })),
    };
    assert!(matches!(
        table
            .update_rows(input, vec!["value".into()], state.version)
            .await,
        Err(TableError::UpdateRows {
            source: UpdateRowsError::Commit {
                source: CommitError::Conflict { .. }
            }
        })
    ));
    assert_eq!(table.state, state);
    Ok(())
}

#[tokio::test]
async fn equal_assignments_commit_and_reusing_old_version_conflicts() -> TestResult {
    let (_dir, mut table) = populated().await?;
    let version = table.state.version;
    for expected in [version, version + 1] {
        let report = table
            .update_rows(
                reader(batch(&[(0, "A", Some(0))])),
                vec!["value".into()],
                expected,
            )
            .await?;
        assert_eq!(report.rows_updated, 1);
        assert!(!report.no_op);
        assert_eq!(report.committed_version, expected + 1);
        assert!(
            table
                .update_rows(
                    reader(batch(&[(0, "A", Some(0))])),
                    vec!["value".into()],
                    expected
                )
                .await
                .is_err()
        );
    }
    assert_eq!(values(&table, "value").await?.len(), 6);
    Ok(())
}

#[tokio::test]
async fn cancellation_before_publication_removes_only_owned_outputs() -> TestResult {
    let (dir, mut table) = populated().await?;
    let before = files(dir.path())?;
    let state = table.state.clone();
    let mut pause =
        storage::pause_atomic_write_before_rename(dir.path().join(layout::current_rel_path()));
    let mut future = Box::pin(table.update_rows(
        reader(batch(&[(0, "A", Some(88))])),
        vec!["value".into()],
        state.version,
    ));
    tokio::select! { ()=pause.wait_until_paused()=>{}, result=&mut future=>panic!("finished before cancellation: {result:?}") }
    assert!(
        dir.path()
            .join(layout::commit_rel_path(state.version + 1))
            .is_file()
    );
    drop(future);
    pause.release();
    assert_eq!(files(dir.path())?, before);
    assert_eq!(table.state, state);
    assert_eq!(
        TimeSeriesTable::open(table.location().clone())
            .await?
            .state(),
        &state
    );
    Ok(())
}

#[tokio::test]
async fn commit_failures_clean_replacements_but_ambiguity_preserves_them() -> TestResult {
    for (publication, cleanup_fails) in [(false, false), (false, true), (true, false), (true, true)]
    {
        let (dir, mut table) = populated().await?;
        let state = table.state.clone();
        let before = files(dir.path())?;
        let commit = dir.path().join(layout::commit_rel_path(state.version + 1));
        if publication {
            std::fs::create_dir(
                dir.path()
                    .join(layout::current_rel_path())
                    .with_extension("tmp"),
            )?;
            if cleanup_fails {
                storage::inject_cleanup_failure(commit.clone());
            }
        } else {
            storage::inject_write_new_failure(commit.clone(), cleanup_fails);
        }
        let error = table
            .update_rows(
                reader(batch(&[(0, "A", Some(99))])),
                vec!["value".into()],
                state.version,
            )
            .await
            .unwrap_err();
        let TableError::UpdateRows {
            source: UpdateRowsError::Commit { source },
        } = error
        else {
            panic!("wrong failure: {error:?}")
        };
        assert_eq!(
            matches!(source, CommitError::AmbiguousOutcome { .. }),
            cleanup_fails
        );
        assert_eq!(commit.exists(), cleanup_fails);
        assert_eq!(table.state, state);
        let after = files(dir.path())?;
        for (path, bytes) in &before {
            assert_eq!(after.get(path), Some(bytes));
        }
        assert_eq!(
            after.len() - before.len(),
            if cleanup_fails { 3 } else { 0 },
            "commit, replacement, sidecar must share outcome"
        );
        assert_eq!(
            TimeSeriesTable::open(table.location().clone())
                .await?
                .state(),
            &state
        );
    }
    Ok(())
}

#[tokio::test]
async fn create_only_commit_race_remains_storage_and_cleans_loser() -> TestResult {
    let (dir, mut winner) = populated().await?;
    let mut loser = winner.clone();
    let state = loser.state.clone();
    let mut pause =
        storage::pause_atomic_write_before_rename(dir.path().join(layout::current_rel_path()));
    let mut winning = Box::pin(winner.update_rows(
        reader(batch(&[(0, "A", Some(90))])),
        vec!["value".into()],
        state.version,
    ));
    tokio::select! { ()=pause.wait_until_paused()=>{}, result=&mut winning=>panic!("not paused: {result:?}") }
    let before = files(dir.path())?;
    let error = loser
        .update_rows(
            reader(batch(&[(1, "B", Some(91))])),
            vec!["value".into()],
            state.version,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        TableError::UpdateRows {
            source: UpdateRowsError::Commit {
                source: CommitError::Storage {
                    source: storage::StorageError::AlreadyExists { .. }
                }
            }
        }
    ));
    assert_eq!(loser.state, state);
    assert_eq!(files(dir.path())?, before);
    pause.release();
    winning.await?;
    assert_eq!(values(&winner, "value").await?[&(0, "A".into())], Some(90));
    Ok(())
}

#[tokio::test]
async fn every_intervening_mutation_conflicts_and_cleans_the_update_attempt() -> TestResult {
    for mutation in 0..4 {
        let (dir, mut table) = populated().await?;
        let mut concurrent = table.clone();
        let state = table.state.clone();
        let before = files(dir.path())?;
        let mut future = Box::pin(table.update_rows(
            reader(batch(&[(0, "A", Some(90))])),
            vec!["value".into()],
            state.version,
        ));
        // Suspend during replacement writing, before the log's create-only claim.
        loop {
            tokio::select! {
                result=&mut future=>panic!("finished before concurrent mutation: {result:?}"),
                ()=tokio::task::yield_now()=>{
                    let root=dir.path().join(layout::UPDATE_REWRITE_DATA_DIR);
                    if root.exists() && !files(&root)?.is_empty() { break; }
                }
            }
        }
        match mutation {
            0 => {
                concurrent.append(batch(&[(3, "A", Some(3))])).await?;
            }
            1 => {
                concurrent
                    .update_rows(
                        reader(batch(&[(2, "B", Some(92))])),
                        vec!["value".into()],
                        state.version,
                    )
                    .await?;
            }
            2 => {
                concurrent.optimize().await?;
            }
            _ => {
                concurrent
                    .add_columns(vec![LogicalField {
                        name: "new".into(),
                        data_type: LogicalDataType::Int64,
                        nullable: true,
                    }])
                    .await?;
            }
        }
        let error = future.await.unwrap_err();
        assert!(
            matches!(error,TableError::UpdateRows{source:UpdateRowsError::Commit{source:CommitError::Conflict{expected,found,..}}} if expected==state.version && found==concurrent.state.version)
        );
        assert_eq!(table.state, state);
        assert_eq!(
            TimeSeriesTable::open(table.location().clone())
                .await?
                .state(),
            concurrent.state()
        );
        let after = files(dir.path())?;
        for path in after.keys().filter(|path| !before.contains_key(*path)) {
            let relative = path
                .strip_prefix(dir.path())?
                .to_string_lossy()
                .replace('\\', "/");
            assert!(
                relative
                    == layout::commit_rel_path(concurrent.state.version)
                        .to_string_lossy()
                        .replace('\\', "/")
                    || concurrent
                        .state
                        .segments
                        .values()
                        .any(|segment| segment.path == relative
                            || segment.coverage_path.as_ref() == Some(&relative))
                    || concurrent
                        .state
                        .table_coverage
                        .as_ref()
                        .is_some_and(|coverage| coverage.coverage_path == relative),
                "unowned output {relative}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn primary_commit_failure_retains_typed_cleanup_failure() -> TestResult {
    let (dir, mut table) = populated().await?;
    let state = table.state.clone();
    let current = dir.path().join(layout::current_rel_path());
    let mut pause = storage::pause_atomic_write_before_open(current.clone());
    let mut future = Box::pin(table.update_rows(
        reader(batch(&[(0, "A", Some(90))])),
        vec!["value".into()],
        state.version,
    ));
    tokio::select! { ()=pause.wait_until_paused()=>{}, result=&mut future=>panic!("not paused: {result:?}") }
    let output = files(&dir.path().join(layout::UPDATE_REWRITE_DATA_DIR))?
        .into_keys()
        .next()
        .unwrap();
    storage::inject_cleanup_failure(output.clone());
    std::fs::create_dir(current.with_extension("tmp"))?;
    pause.release();
    let error = future.await.unwrap_err();
    match error {
        TableError::UpdateRows {
            source: UpdateRowsError::CleanupAfterFailure { source, cleanup },
        } => {
            assert!(matches!(
                *source,
                UpdateRowsError::Commit {
                    source: CommitError::Storage { .. }
                }
            ));
            let RewriteError::Cleanup { cleanup_errors } = *cleanup else {
                panic!("wrong cleanup: {cleanup:?}")
            };
            assert_eq!(cleanup_errors.len(), 1);
            assert!(
                matches!(&cleanup_errors[0],storage::StorageError::OtherIo{path,..} if Path::new(path)==output)
            );
        }
        other => panic!("lost cleanup diagnostic: {other:?}"),
    }
    assert_eq!(table.state, state);
    Ok(())
}

#[tokio::test]
async fn observer_panic_after_commit_preserves_published_files() -> TestResult {
    use crate::table::test_util::panic_on_commit_close_dispatch;
    use futures::FutureExt;
    use tracing::instrument::WithSubscriber;
    let (_dir, mut table) = populated().await?;
    let state = table.state.clone();
    let callsite_guard = panic_on_commit_close_dispatch();
    let outcome = std::panic::AssertUnwindSafe(
        table
            .update_rows(
                reader(batch(&[(0, "A", Some(90))])),
                vec!["value".into()],
                state.version,
            )
            .with_subscriber(panic_on_commit_close_dispatch()),
    )
    .catch_unwind()
    .await;
    drop(callsite_guard);
    assert!(outcome.is_err());
    assert_eq!(table.state, state);
    let reopened = TimeSeriesTable::open(table.location().clone()).await?;
    assert_eq!(reopened.state.version, state.version + 1);
    assert_eq!(
        values(&reopened, "value").await?[&(0, "A".into())],
        Some(90)
    );
    Ok(())
}

#[tokio::test]
async fn version_overflow_cleans_staging_and_empty_max_version_is_a_no_op() -> TestResult {
    let (dir, mut table) = populated().await?;
    table.state.version = u64::MAX;
    std::fs::write(
        dir.path().join(layout::current_rel_path()),
        u64::MAX.to_string(),
    )?;
    let before = files(dir.path())?;
    let error = table
        .update_rows(
            reader(batch(&[(0, "A", Some(90))])),
            vec!["value".into()],
            u64::MAX,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        TableError::UpdateRows {
            source: UpdateRowsError::Commit {
                source: CommitError::VersionOverflow { .. }
            }
        }
    ));
    assert_eq!(files(dir.path())?, before);
    assert!(
        table
            .update_rows(reader(batch(&[])), vec!["value".into()], u64::MAX)
            .await?
            .no_op
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_during_preparation_and_rewrite_cleans_open_files() -> TestResult {
    for stage in [layout::UPDATE_PREPARE_DIR, layout::UPDATE_REWRITE_DATA_DIR] {
        let (dir, mut table) = populated().await?;
        let before = files(dir.path())?;
        let state = table.state.clone();
        let observer = table.clone();
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(1);
        let mut future = Box::pin(table.update_rows(
            reader(batch(&[(0, "A", Some(90))])),
            vec!["value".into()],
            state.version,
        ));
        loop {
            tokio::select! {
                result=&mut future=>panic!("finished before cancellation in {stage}: {result:?}"),
                ()=tokio::task::yield_now()=>{
                    let root=dir.path().join(stage);
                    if root.exists() && !files(&root)?.is_empty() { break; }
                }
            }
        }
        let staged = files(dir.path())?;
        let report = observer.vacuum(cutoff, crate::VacuumMode::Apply).await?;
        assert_eq!(report.deleted_files, 0);
        assert_eq!(
            files(dir.path())?,
            staged,
            "vacuum removed an active attempt"
        );
        drop(future);
        assert_eq!(files(dir.path())?, before);
        assert_eq!(table.state, state);
    }
    Ok(())
}

#[tokio::test]
async fn vacuum_retains_historical_sources_and_sidecars_after_updates() -> TestResult {
    let (dir, mut table) = populated().await?;
    let old = table.clone();
    let version = table.state.version;
    table
        .update_rows(
            reader(batch(&[(0, "A", Some(90))])),
            vec!["value".into()],
            version,
        )
        .await?;
    let before = files(dir.path())?;
    let report = table
        .vacuum(chrono::Utc::now(), crate::VacuumMode::Apply)
        .await?;
    assert_eq!(report.deleted_files, 0);
    assert_eq!(files(dir.path())?, before);
    assert_eq!(values(&old, "value").await?[&(0, "A".into())], Some(0));
    assert_eq!(values(&table, "value").await?[&(0, "A".into())], Some(90));
    Ok(())
}

#[tokio::test]
async fn publication_rejects_changed_source_and_incomplete_output_ownership() -> TestResult {
    for mutation in 0..5 {
        let (dir, mut table) = populated().await?;
        let before = files(dir.path())?;
        let state = table.state.clone();
        let prepared = prepare_updates(
            table.location(),
            &state,
            reader(batch(&[(0, "A", Some(90))])),
            &["value".into()],
        )
        .await?;
        let mut staged = stage_update_replacements(table.location(), &state, prepared).await?;
        match mutation {
            0 => staged.version += 1,
            1 => staged.replacements[0].source.row_count += 1,
            2 => {
                staged.replacements[0].replacement.path = staged.replacements[0].source.path.clone()
            }
            3 => staged.replacements[0].replacement.coverage_path = None,
            _ => {
                staged.replacements.clear();
                staged.metrics.rows_updated = 0;
            }
        }
        assert!(matches!(
            table.publish_updates(&mut staged).await,
            Err(UpdateRowsError::InvalidPlan { .. })
        ));
        staged.close().await?;
        assert_eq!(table.state, state);
        assert_eq!(files(dir.path())?, before);
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn sql_refreshes_values_without_registration_and_keeps_planned_scans() -> TestResult {
    use datafusion::{physical_plan::collect, prelude::SessionContext};
    let (_dir, mut table) = populated().await?;
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(crate::TsTableProvider::try_new(Arc::new(table.clone()))?),
    )?;
    let plan = ctx
        .sql("SELECT sum(value) AS total FROM t")
        .await?
        .create_physical_plan()
        .await?;
    let version = table.state.version;
    table
        .update_rows(
            reader(batch(&[(0, "A", Some(100)), (1, "B", None)])),
            vec!["value".into()],
            version,
        )
        .await?;
    let old = collect(plan, ctx.task_ctx()).await?;
    assert_eq!(
        old[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        36
    );
    for (sql, expected) in [
        ("SELECT sum(value) FROM t", 125),
        ("SELECT count(*) FROM t WHERE value IS NULL", 1),
        (
            "SELECT count(*) FROM t WHERE value >= 100 AND entity = 'A'",
            1,
        ),
        ("SELECT count(*) FROM t", 6),
    ] {
        let rows = ctx.sql(sql).await?.collect().await?;
        assert_eq!(
            rows[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected,
            "{sql}"
        );
    }
    Ok(())
}
