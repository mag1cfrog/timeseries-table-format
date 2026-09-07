use super::*;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    metadata::{
        index::{IndexKind, IndexSpec},
        logical_schema::{LogicalDataType, LogicalSchema, LogicalSchemaValidationError},
        table::TableMeta,
    },
    storage::{self, TableLocation, layout},
    table::test_util::TestResult,
    transaction_log::TransactionLogStore,
};
use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use futures::TryStreamExt;
use tempfile::TempDir;

fn field(name: &str, data_type: LogicalDataType) -> LogicalField {
    LogicalField {
        name: name.into(),
        data_type,
        nullable: true,
    }
}

fn table_meta() -> TableMeta {
    TableMeta::new_time_series_with_schema(
        IndexSpec {
            column: "idx".into(),
            entity_columns: vec!["entity".into()],
            kind: IndexKind::Int64 {
                index_granularity: std::num::NonZeroU64::MIN,
            },
        },
        LogicalSchema::new(vec![
            LogicalField {
                nullable: false,
                ..field("idx", LogicalDataType::Int64)
            },
            LogicalField {
                nullable: false,
                ..field("entity", LogicalDataType::Utf8)
            },
            field("value", LogicalDataType::Float64),
        ])
        .unwrap(),
    )
}

fn batch(idx: i64, score: Option<Vec<Option<i64>>>) -> RecordBatch {
    let mut fields = vec![
        Field::new("idx", DataType::Int64, false),
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![idx, idx])),
        Arc::new(StringArray::from(vec!["A", "B"])),
        Arc::new(Float64Array::from(vec![1.0, 2.0])),
    ];
    if let Some(values) = score {
        fields.push(Field::new("score", DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(values)));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn files(root: &Path) -> std::io::Result<BTreeMap<PathBuf, Vec<u8>>> {
    fn visit(
        root: &Path,
        dir: &Path,
        result: &mut BTreeMap<PathBuf, Vec<u8>>,
    ) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                visit(root, &entry.path(), result)?;
            } else {
                result.insert(
                    entry.path().strip_prefix(root).unwrap().to_owned(),
                    std::fs::read(entry.path())?,
                );
            }
        }
        Ok(())
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result)?;
    Ok(result)
}

#[tokio::test]
async fn addition_is_one_metadata_commit_and_composes_with_append_scan_and_optimize() -> TestResult
{
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut table = TimeSeriesTable::create(location.clone(), table_meta()).await?;
    table.append(batch(0, None)).await?;
    let old_handle = table.clone();
    let old_scan = table.scan_range(0_i64, 10_i64).await?;
    let before = table.state().clone();
    let objects = files(temp.path())?;
    let score = field("score", LogicalDataType::Int64);
    let quality = field("quality", LogicalDataType::Bool);
    let version = table
        .add_columns(vec![score.clone(), quality.clone()])
        .await?;
    assert_eq!(version, before.version + 1);
    assert_eq!(table.state().version, version);
    assert_eq!(table.state().segments, before.segments);
    assert_eq!(table.state().table_coverage, before.table_coverage);
    let mut expected = before.table_meta.clone();
    expected.logical_schema = Some(LogicalSchema::new(
        before
            .table_meta
            .logical_schema()
            .unwrap()
            .columns()
            .iter()
            .cloned()
            .chain([score, quality])
            .collect(),
    )?);
    expected
        .required_reader_features
        .insert("schema_add_columns".into());
    assert_eq!(table.state().table_meta, expected);
    let after = files(temp.path())?;
    for (path, bytes) in &objects {
        if *path != layout::current_rel_path() {
            assert_eq!(after.get(path), Some(bytes), "changed {path:?}");
        }
    }
    assert_eq!(after.len(), objects.len() + 1);
    let commit = table.log.load_commit(version).await?;
    assert_eq!(commit.actions, vec![LogAction::UpdateTableMeta(expected)]);
    assert_eq!(
        TimeSeriesTable::open(location).await?.state(),
        table.state()
    );
    assert_eq!(old_handle.state(), &before);
    let old_batches: Vec<_> = old_scan.try_collect().await?;
    assert_eq!(
        old_batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2
    );
    assert!(old_batches.iter().all(|b| b.num_columns() == 3));
    table.append(batch(1, Some(vec![Some(7), None]))).await?;
    table.append(batch(2, None)).await?;
    let rows: Vec<_> = table.scan_range(0_i64, 3_i64).await?.try_collect().await?;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
    assert_eq!(
        rows.iter().map(|b| b.column(3).null_count()).sum::<usize>(),
        5
    );
    table.optimize().await?;
    let optimized: Vec<_> = table.scan_range(0_i64, 3_i64).await?.try_collect().await?;
    assert_eq!(
        optimized.iter().map(RecordBatch::num_rows).sum::<usize>(),
        6
    );
    assert_eq!(
        optimized
            .iter()
            .map(|b| b.column(3).null_count())
            .sum::<usize>(),
        5
    );
    // Existing source objects remain byte-for-byte intact after optimization.
    for (path, bytes) in objects
        .iter()
        .filter(|(p, _)| !p.starts_with(layout::log_rel_dir()))
    {
        assert_eq!(std::fs::read(temp.path().join(path))?, *bytes);
    }
    let second = table
        .add_columns(vec![field("later", LogicalDataType::Bool)])
        .await?;
    assert_eq!(table.state().version, second);
    assert_eq!(table.state().table_meta.required_reader_features().len(), 1);
    assert!(
        table
            .state()
            .table_meta
            .required_writer_features()
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn invalid_requests_publish_nothing_and_preserve_the_handle() -> TestResult {
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()).await?;
    let before = table.state().clone();
    let objects = files(temp.path())?;
    let invalid = vec![
        vec![],
        vec![field("", LogicalDataType::Bool)],
        vec![field(" \t", LogicalDataType::Bool)],
        vec![field("idx", LogicalDataType::Int64)],
        vec![field("entity", LogicalDataType::Utf8)],
        vec![field("value", LogicalDataType::Float64)],
        vec![
            field("x", LogicalDataType::Bool),
            field("x", LogicalDataType::Int64),
        ],
        vec![LogicalField {
            nullable: false,
            ..field("required", LogicalDataType::Int64)
        }],
        vec![field("x", LogicalDataType::Other("future".into()))],
        vec![field("x", LogicalDataType::Int96)],
        vec![field("x", LogicalDataType::FixedBinary { byte_width: 0 })],
        vec![field(
            "x",
            LogicalDataType::Decimal {
                precision: 77,
                scale: 0,
            },
        )],
        vec![field(
            "x",
            LogicalDataType::Decimal {
                precision: 4,
                scale: -1,
            },
        )],
        vec![field(
            "x",
            LogicalDataType::Decimal {
                precision: 4,
                scale: 5,
            },
        )],
        vec![field("x", LogicalDataType::Struct { fields: vec![] })],
        vec![field(
            "x",
            LogicalDataType::Struct {
                fields: vec![
                    field("a", LogicalDataType::Bool),
                    field("a", LogicalDataType::Bool),
                ],
            },
        )],
        vec![field(
            "x",
            LogicalDataType::Map {
                key: Box::new(field("key", LogicalDataType::Utf8)),
                value: None,
                keys_sorted: false,
            },
        )],
        vec![field(
            "x",
            LogicalDataType::Map {
                key: Box::new(LogicalField {
                    nullable: false,
                    ..field("renamed", LogicalDataType::Utf8)
                }),
                value: None,
                keys_sorted: false,
            },
        )],
    ];
    for request in invalid {
        assert!(
            matches!(
                table.add_columns(request.clone()).await,
                Err(TableError::AddColumns {
                    source: AddColumnsError::Schema { .. }
                })
            ),
            "accepted {request:?}"
        );
        assert_eq!(table.state(), &before);
        assert_eq!(files(temp.path())?, objects);
    }
    for (reader, writer) in [(true, false), (false, true)] {
        if reader {
            table
                .state
                .table_meta
                .required_reader_features
                .insert("unknown".into());
        }
        if writer {
            table
                .state
                .table_meta
                .required_writer_features
                .insert("unknown".into());
        }
        // Compatibility is checked even before invalid request validation.
        assert!(matches!(
            table.add_columns(vec![]).await,
            Err(TableError::AddColumns {
                source: AddColumnsError::Protocol { .. }
            })
        ));
        assert_eq!(files(temp.path())?, objects);
        table.state = before.clone();
    }
    Ok(())
}

#[tokio::test]
async fn schemaless_tables_require_initial_adoption_and_names_are_exact() -> TestResult {
    let temp = TempDir::new()?;
    let mut meta = table_meta();
    meta.logical_schema = None;
    let mut table = TimeSeriesTable::create(TableLocation::local(temp.path()), meta).await?;
    assert!(
        matches!(table.add_columns(vec![field("x", LogicalDataType::Int64)]).await, Err(TableError::AddColumns { source: AddColumnsError::Schema { source } }) if matches!(*source, SchemaEvolutionError::MissingSchema))
    );
    table.append(batch(0, None)).await?;
    assert!(
        table
            .state()
            .table_meta
            .required_reader_features()
            .is_empty()
    );
    assert!(
        table
            .state()
            .table_meta
            .required_writer_features()
            .is_empty()
    );
    let names = ["Value", " value ", "nested.value"];
    table
        .add_columns(
            names
                .iter()
                .map(|name| field(name, LogicalDataType::Int64))
                .collect(),
        )
        .await?;
    let columns = table.state().table_meta.logical_schema().unwrap().columns();
    assert_eq!(
        columns[3..]
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        names
    );
    Ok(())
}

#[tokio::test]
async fn valid_types_round_trip_through_real_append_scan_and_optimize() -> TestResult {
    use crate::metadata::logical_schema::LogicalTimestampUnit;
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()).await?;
    table.append(batch(0, None)).await?;
    let types = vec![
        LogicalDataType::Bool,
        LogicalDataType::Int32,
        LogicalDataType::Int64,
        LogicalDataType::UInt64,
        LogicalDataType::Float32,
        LogicalDataType::Float64,
        LogicalDataType::Binary,
        LogicalDataType::FixedBinary { byte_width: 4 },
        LogicalDataType::Utf8,
        LogicalDataType::Timestamp {
            unit: LogicalTimestampUnit::Millis,
            timezone: Some("Asia/Shanghai".into()),
        },
        LogicalDataType::Timestamp {
            unit: LogicalTimestampUnit::Micros,
            timezone: None,
        },
        LogicalDataType::Timestamp {
            unit: LogicalTimestampUnit::Nanos,
            timezone: Some("UTC".into()),
        },
        LogicalDataType::Decimal {
            precision: 38,
            scale: 4,
        },
        LogicalDataType::Decimal {
            precision: 76,
            scale: 6,
        },
        LogicalDataType::Struct {
            fields: vec![LogicalField {
                nullable: false,
                ..field("child", LogicalDataType::Int64)
            }],
        },
        LogicalDataType::List {
            elements: Box::new(field("item", LogicalDataType::Int64)),
        },
        LogicalDataType::Map {
            key: Box::new(LogicalField {
                nullable: false,
                ..field("key", LogicalDataType::Utf8)
            }),
            value: Some(Box::new(field("value", LogicalDataType::Int64))),
            keys_sorted: true,
        },
        LogicalDataType::Map {
            key: Box::new(LogicalField {
                nullable: false,
                ..field("key", LogicalDataType::Utf8)
            }),
            value: None,
            keys_sorted: false,
        },
    ];
    table
        .add_columns(
            types
                .into_iter()
                .enumerate()
                .map(|(i, dt)| field(&format!("c{i}"), dt))
                .collect(),
        )
        .await?;
    table.append(batch(1, None)).await?;
    for optimize in [false, true] {
        if optimize {
            table.optimize().await?;
        }
        let batches: Vec<_> = table.scan_range(0_i64, 2_i64).await?.try_collect().await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        for batch in batches {
            assert_eq!(batch.schema(), table.state().table_meta.arrow_schema_ref()?);
            assert!(
                batch.columns()[3..]
                    .iter()
                    .all(|a| a.null_count() == batch.num_rows())
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn stale_add_add_append_and_optimize_conflict_without_lost_updates() -> TestResult {
    for winner in ["add", "append", "optimize"] {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()).await?;
        table.append(batch(0, None)).await?;
        let mut concurrent = table.clone();
        let before = table.state().clone();
        match winner {
            "add" => {
                concurrent
                    .add_columns(vec![field("winner", LogicalDataType::Bool)])
                    .await?;
            }
            "append" => {
                concurrent.append(batch(1, None)).await?;
            }
            _ => {
                concurrent.optimize().await?;
            }
        }
        let objects = files(temp.path())?;
        assert!(matches!(
            table
                .add_columns(vec![field("loser", LogicalDataType::Bool)])
                .await,
            Err(TableError::AddColumns {
                source: AddColumnsError::Commit {
                    source: CommitError::Conflict { .. }
                }
            })
        ));
        assert_eq!(table.state(), &before);
        assert_eq!(files(temp.path())?, objects);
        assert_eq!(table.load_latest_state().await?, *concurrent.state());
    }
    Ok(())
}

#[tokio::test]
async fn create_only_races_preserve_the_winning_schema_commit() -> TestResult {
    for (winner, loser) in [
        ("add", "add"),
        ("add", "append"),
        ("add", "optimize"),
        ("append", "add"),
        ("optimize", "add"),
    ] {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()).await?;
        table.append(batch(0, None)).await?;
        let mut concurrent = table.clone();
        let before = concurrent.state().clone();
        let mut pause =
            storage::pause_atomic_write_before_rename(temp.path().join(layout::current_rel_path()));
        let mut addition = Box::pin(async {
            match winner {
                "add" => {
                    table
                        .add_columns(vec![field("winner", LogicalDataType::Bool)])
                        .await?;
                }
                "append" => {
                    table.append(batch(1, None)).await?;
                }
                _ => {
                    table.optimize().await?;
                }
            }
            Ok::<_, TableError>(())
        });
        tokio::select! { () = pause.wait_until_paused() => {}, result = &mut addition => panic!("not paused: {result:?}") }
        let objects = files(temp.path())?;
        let error = match loser {
            "add" => concurrent
                .add_columns(vec![field("loser", LogicalDataType::Bool)])
                .await
                .unwrap_err(),
            "append" => concurrent.append(batch(1, None)).await.unwrap_err(),
            _ => concurrent.optimize().await.unwrap_err(),
        };
        // Assert the typed commit wrapper rather than relying on messages.
        let commit = match &error {
            TableError::AddColumns {
                source: AddColumnsError::Commit { source },
            } => source,
            TableError::Append {
                source: crate::table::AppendError::Commit { source },
            } => source,
            TableError::Optimize {
                source: crate::table::OptimizeError::Commit { source },
            } => source,
            other => panic!("unexpected failure: {other:?}"),
        };
        assert!(matches!(
            commit,
            CommitError::Storage {
                source: storage::StorageError::AlreadyExists { .. }
            }
        ));
        assert_eq!(concurrent.state(), &before);
        assert_eq!(files(temp.path())?, objects);
        pause.release();
        addition.await?;
        assert_eq!(concurrent.load_latest_state().await?, *table.state());
    }
    Ok(())
}

#[tokio::test]
async fn commit_failures_and_ambiguity_preserve_handle_and_existing_objects() -> TestResult {
    for (publication, cleanup_fails) in [(false, false), (false, true), (true, false), (true, true)]
    {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()).await?;
        table.append(batch(0, None)).await?;
        let before = table.state().clone();
        let objects = files(temp.path())?;
        let commit_path = temp
            .path()
            .join(layout::commit_rel_path(before.version + 1));
        if publication {
            // Atomic CURRENT writing fails before publication; CURRENT remains readable.
            std::fs::create_dir(
                temp.path()
                    .join(layout::current_rel_path())
                    .with_extension("tmp"),
            )?;
            if cleanup_fails {
                storage::inject_cleanup_failure(commit_path.clone());
            }
        } else {
            storage::inject_write_new_failure(commit_path.clone(), cleanup_fails);
        }
        let error = table
            .add_columns(vec![field("score", LogicalDataType::Int64)])
            .await
            .unwrap_err();
        let TableError::AddColumns {
            source: AddColumnsError::Commit { source },
        } = error
        else {
            panic!("unexpected error: {error:?}")
        };
        assert_eq!(
            matches!(source, CommitError::AmbiguousOutcome { .. }),
            cleanup_fails
        );
        assert_eq!(commit_path.exists(), cleanup_fails);
        assert_eq!(table.state(), &before);
        assert_eq!(table.current_version().await?, before.version);
        let after = files(temp.path())?;
        for (path, bytes) in &objects {
            assert_eq!(after.get(path), Some(bytes));
        }
        assert_eq!(after.len(), objects.len() + usize::from(cleanup_fails));
        assert_eq!(
            TimeSeriesTable::open(table.location().clone())
                .await?
                .state(),
            &before
        );
    }
    Ok(())
}

#[tokio::test]
async fn version_overflow_is_typed_and_creates_nothing() -> TestResult {
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()).await?;
    table.state.version = u64::MAX;
    std::fs::write(
        temp.path().join(layout::current_rel_path()),
        u64::MAX.to_string(),
    )?;
    let before = table.state().clone();
    let objects = files(temp.path())?;
    assert!(matches!(
        table
            .add_columns(vec![field("score", LogicalDataType::Int64)])
            .await,
        Err(TableError::AddColumns {
            source: AddColumnsError::Commit {
                source: CommitError::VersionOverflow { .. }
            }
        })
    ));
    assert_eq!(table.state(), &before);
    assert_eq!(files(temp.path())?, objects);
    Ok(())
}

#[tokio::test]
async fn current_rename_failure_reports_rollback_or_ambiguity() -> TestResult {
    for cleanup_fails in [false, true] {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()).await?;
        let before = table.state().clone();
        let current = temp.path().join(layout::current_rel_path());
        let commit = temp.path().join(layout::commit_rel_path(2));
        let mut pause = storage::pause_atomic_write_before_rename(current.clone());
        let mut addition =
            Box::pin(table.add_columns(vec![field("score", LogicalDataType::Int64)]));
        tokio::select! { () = pause.wait_until_paused() => {}, result = &mut addition => panic!("not paused: {result:?}") }
        std::fs::remove_file(&current)?;
        std::fs::create_dir(&current)?;
        if cleanup_fails {
            storage::inject_cleanup_failure(commit.clone());
        }
        pause.release();
        let error = addition.await.unwrap_err();
        let TableError::AddColumns {
            source: AddColumnsError::Commit { source },
        } = error
        else {
            panic!("unexpected error: {error:?}")
        };
        assert_eq!(
            matches!(source, CommitError::AmbiguousOutcome { .. }),
            cleanup_fails
        );
        assert_eq!(commit.exists(), cleanup_fails);
        assert!(!current.with_extension("tmp").exists());
        assert_eq!(table.state(), &before);
        // Restore only the pointer deliberately damaged by this fault injection.
        std::fs::remove_dir(&current)?;
        std::fs::write(current, "1\n")?;
        assert_eq!(
            TimeSeriesTable::open(table.location().clone())
                .await?
                .state(),
            &before
        );
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_before_publication_removes_only_its_unpublished_commit() -> TestResult {
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()).await?;
    let before = table.state().clone();
    let objects = files(temp.path())?;
    let mut pause =
        storage::pause_atomic_write_before_rename(temp.path().join(layout::current_rel_path()));
    let mut addition = Box::pin(table.add_columns(vec![field("score", LogicalDataType::Int64)]));
    tokio::select! { () = pause.wait_until_paused() => {}, result = &mut addition => panic!("not paused: {result:?}") }
    drop(addition);
    pause.release();
    assert_eq!(table.state(), &before);
    assert_eq!(files(temp.path())?, objects);
    table
        .add_columns(vec![field("score", LogicalDataType::Int64)])
        .await?;
    Ok(())
}

#[tokio::test]
async fn replay_rejects_undeclared_and_non_additive_transitions() -> TestResult {
    for change in [
        "undeclared",
        "drop",
        "reorder",
        "rename",
        "retype",
        "nullability",
        "remove_schema",
        "key",
        "required",
        "blank",
        "duplicate",
        "unsupported",
    ] {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let table = TimeSeriesTable::create(location.clone(), table_meta()).await?;
        let before = table.state().clone();
        let mut next = before.table_meta.clone();
        let mut fields = next.logical_schema().unwrap().columns().to_vec();
        next.required_reader_features
            .insert("schema_add_columns".into());
        match change {
            "undeclared" => {
                fields.push(field("new", LogicalDataType::Bool));
                next.required_reader_features.clear();
            }
            "drop" => {
                fields.pop();
            }
            "reorder" => fields.swap(0, 1),
            "rename" => fields[2].name = "renamed".into(),
            "retype" => fields[2].data_type = LogicalDataType::Int64,
            "nullability" => fields[2].nullable = false,
            "key" => {
                let crate::metadata::table::TableKind::TimeSeries(index) = &mut next.kind else {
                    unreachable!()
                };
                index.entity_columns.clear();
            }
            "required" => fields.push(LogicalField {
                nullable: false,
                ..field("new", LogicalDataType::Bool)
            }),
            "blank" => fields.push(field(" ", LogicalDataType::Bool)),
            "duplicate" => fields.push(fields[2].clone()),
            "unsupported" => fields.push(field("new", LogicalDataType::Other("future".into()))),
            _ => {}
        }
        // Deserialization intentionally bypasses LogicalSchema::new, as a corrupt log can.
        next.logical_schema = if change == "remove_schema" {
            None
        } else {
            Some(serde_json::from_value(
                serde_json::json!({"columns": fields}),
            )?)
        };
        TransactionLogStore::new(location.clone())
            .commit_with_expected_version(
                1,
                vec![
                    LogAction::UpdateTableMeta(next),
                    LogAction::UpdateTableMeta(before.table_meta.clone()),
                ],
            )
            .await?;
        assert!(
            matches!(
                TimeSeriesTable::open(location).await,
                Err(TableError::Open {
                    source: crate::table::OpenTableError::Commit {
                        source: CommitError::SchemaEvolution { .. }
                    }
                })
            ),
            "accepted invalid intermediate transition: {change}"
        );
    }
    Ok(())
}

#[test]
fn additions_preserve_existing_requirements_and_typed_error_sources() -> TestResult {
    use std::error::Error;
    let mut meta = table_meta();
    meta.required_reader_features.insert("future_reader".into());
    meta.required_writer_features.insert("future_writer".into());
    let next = meta.with_added_columns(vec![field("new", LogicalDataType::Bool)])?;
    assert!(
        next.required_reader_features
            .is_superset(&meta.required_reader_features)
    );
    assert_eq!(next.required_writer_features, meta.required_writer_features);
    assert_eq!(next.protocol_version(), 7);
    let error = TableError::AddColumns {
        source: AddColumnsError::from(
            meta.with_added_columns(vec![field("value", LogicalDataType::Int64)])
                .unwrap_err(),
        ),
    };
    let operation = error
        .source()
        .unwrap()
        .downcast_ref::<AddColumnsError>()
        .unwrap();
    let schema = operation
        .source()
        .unwrap()
        .downcast_ref::<Box<SchemaEvolutionError>>()
        .unwrap();
    assert!(
        matches!(schema.source().unwrap().downcast_ref::<LogicalSchemaValidationError>(), Some(LogicalSchemaValidationError::DuplicateColumn { column }) if column == "value")
    );
    Ok(())
}

#[tokio::test]
async fn adding_columns_never_reads_historical_data_or_coverage() -> TestResult {
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()).await?;
    table.append(batch(0, None)).await?;
    for (path, _) in files(temp.path())?
        .iter()
        .filter(|(path, _)| !path.starts_with(layout::log_rel_dir()))
    {
        std::fs::write(temp.path().join(path), b"unreadable data or coverage")?;
    }
    table
        .add_columns(vec![field("score", LogicalDataType::Int64)])
        .await?;
    assert_eq!(
        TimeSeriesTable::open(table.location().clone())
            .await?
            .state(),
        table.state()
    );
    Ok(())
}

#[tokio::test]
async fn older_reader_capabilities_reject_evolved_open_refresh_and_payload_before_decode()
-> TestResult {
    use crate::metadata::protocol::{TEST_READER_FEATURES, TableProtocolError};
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut table = TimeSeriesTable::create(location.clone(), table_meta()).await?;
    let mut old = table.clone();
    let before = old.state().clone();
    table
        .add_columns(vec![field("score", LogicalDataType::Int64)])
        .await?;
    TEST_READER_FEATURES.scope(&[], async {
        assert!(matches!(TimeSeriesTable::open(location.clone()).await, Err(TableError::Open { source: crate::table::OpenTableError::Commit { source: CommitError::Protocol { source: TableProtocolError::UnsupportedReaderFeatures { features }, .. } } }) if features == ["schema_add_columns"]));
        assert!(matches!(old.refresh().await, Err(TableError::StateAccess { source: crate::table::TableStateAccessError::Commit { source: CommitError::Protocol { source: TableProtocolError::UnsupportedReaderFeatures { .. }, .. } } })));
        assert_eq!(old.state(), &before);
        let path = temp.path().join(layout::commit_rel_path(table.state().version));
        let mut commit: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        commit["actions"][0]["UpdateTableMeta"]["logical_schema"] = serde_json::json!({"future_payload": true});
        std::fs::write(path, serde_json::to_vec(&commit)?)?;
        assert!(matches!(table.log.load_commit(table.state().version).await, Err(CommitError::Protocol { source: TableProtocolError::UnsupportedReaderFeatures { .. }, .. })));
        Ok::<_, Box<dyn std::error::Error>>(())
    }).await?;
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn sql_schema_changes_require_registration_and_preserve_planned_snapshots() -> TestResult {
    use crate::{datafusion::TsTableProvider, metadata::protocol::TEST_READER_FEATURES};
    use datafusion::{physical_plan::collect, prelude::SessionContext};
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), table_meta()).await?;
    table.append(batch(0, None)).await?;
    let ctx = SessionContext::new();
    let provider = Arc::new(TsTableProvider::try_new(Arc::new(table.clone()))?);
    ctx.register_table("t", provider)?;
    let plan = ctx
        .sql("SELECT * FROM t")
        .await?
        .create_physical_plan()
        .await?;
    table
        .add_columns(vec![field("score", LogicalDataType::Int64)])
        .await?;
    TEST_READER_FEATURES
        .scope(&[], async {
            let error = ctx
                .sql("SELECT count(*) FROM t")
                .await?
                .collect()
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("unsupported table reader features"),
                "{error}"
            );
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await?;
    for query in ["SELECT * FROM t", "SELECT count(*) FROM t"] {
        let error = ctx.sql(query).await?.collect().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Table schema changed; re-register"),
            "{error}"
        );
    }
    let old = collect(plan, ctx.task_ctx()).await?;
    assert_eq!(old.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(old.iter().all(|batch| batch.num_columns() == 3));
    ctx.deregister_table("t")?;
    ctx.register_table(
        "t",
        Arc::new(TsTableProvider::try_new(Arc::new(table.clone()))?),
    )?;
    // Same-schema version changes must refresh normally, including optimization.
    table.append(batch(1, Some(vec![Some(7), None]))).await?;
    table.append(batch(2, None)).await?;
    for optimize in [false, true] {
        if optimize {
            table.optimize().await?;
        }
        for (predicate, expected) in [
            ("true", 6),
            ("score IS NULL", 5),
            ("score IS NOT NULL", 1),
            ("score = 7", 1),
            ("score > 7", 0),
            ("score IS NULL AND entity = 'A'", 2),
            ("score = 7 OR idx = 0", 3),
        ] {
            let result = ctx
                .sql(&format!("SELECT count(*) FROM t WHERE {predicate}"))
                .await?
                .collect()
                .await?;
            assert_eq!(
                result[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                expected,
                "{predicate}"
            );
        }
        let projected = ctx
            .sql("SELECT score FROM t ORDER BY idx, entity")
            .await?
            .collect()
            .await?;
        let scores = arrow::compute::concat_batches(&projected[0].schema(), &projected)?;
        assert_eq!(
            scores
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![None, None, Some(7), None, None, None])
        );
        let aggregate = ctx
            .sql("SELECT count(score), sum(score) FROM t")
            .await?
            .collect()
            .await?;
        assert_eq!(
            aggregate[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            aggregate[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
    }
    Ok(())
}
