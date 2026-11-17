use super::*;
use chrono::{TimeZone, Utc};
use std::{future::Future, pin::Pin, task::Poll};

async fn wait_for_retry<F: Future>(mut operation: Pin<&mut F>, capture: &TraceCapture) {
    std::future::poll_fn(|cx| {
        assert!(
            operation.as_mut().poll(cx).is_pending(),
            "commit ended before retry"
        );
        if capture
            .events()
            .iter()
            .any(|event| event.name == "table.append.retry")
        {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

fn normalize_actions(mut actions: Vec<LogAction>) -> Vec<LogAction> {
    for action in &mut actions {
        match action {
            LogAction::AddSegment(meta) => {
                meta.path = "segment".into();
                meta.coverage_path = Some("segment-coverage".into());
            }
            LogAction::UpdateTableCoverage { coverage_path, .. } => {
                *coverage_path = "table-coverage".into()
            }
            _ => {}
        }
    }
    actions
}

fn stored_timestamps(
    table: &TimeSeriesTable,
    root: &Path,
) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
    let mut values = Vec::new();
    for segment in table.state().segments.values() {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(root.join(&segment.path))?)?
                .build()?;
        for batch in reader {
            let batch = batch?;
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or("timestamps")?;
            values.extend(column.values().iter().copied());
        }
    }
    values.sort_unstable();
    Ok(values)
}

#[tokio::test]
async fn retry_keeps_all_intervening_appends_and_reuses_the_finished_file() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let batch = timestamp_only_batch(2)?;
    let mut writer = stale.writer(batch.schema())?;
    writer.write(&batch).await?;
    let pending_path = data_files(temp.path())?.pop().ok_or("pending file")?;
    let mut winner = TimeSeriesTable::open(location.clone()).await?;
    winner
        .append(timestamp_only_batch_starting_at_minute_offset(10, 2)?)
        .await?;
    let mut pause = storage::pause_write_new_after_create(temp.path().join(TABLE_SNAPSHOT_DIR));
    let capture = TraceCapture::default();
    let _callsite = capture.dispatch();
    let mut publication = Box::pin(
        writer
            .commit_with_retry(2)
            .with_subscriber(capture.dispatch()),
    );
    tokio::select! {
        () = pause.wait_until_paused() => {}
        result = &mut publication => panic!("publication ended before pause: {result:?}"),
    }
    let finished_bytes = std::fs::read(&pending_path)?;
    winner
        .append(timestamp_only_batch_starting_at_minute_offset(20, 2)?)
        .await?;
    pause.release();
    let report = publication.await?.ok_or("report")?;
    writer.abort().await?;
    assert_eq!((report.starting_version, report.committed_version), (3, 4));
    assert_eq!(temp.path().join(&report.segment_path), pending_path);
    assert_eq!(std::fs::read(&pending_path)?, finished_bytes);
    assert_eq!(data_files(temp.path())?.len(), 3);
    let reopened = TimeSeriesTable::open(location).await?;
    assert_eq!(stale.state(), reopened.state());
    assert_eq!(reopened.state().segments.len(), 3);
    for (path, segment) in &winner.state().segments {
        assert_eq!(reopened.state().segments.get(path), Some(segment));
    }
    for minute in [0, 10, 20] {
        let start = Utc
            .timestamp_millis_opt(minute * 60_000)
            .single()
            .ok_or("start")?;
        let end = Utc
            .timestamp_millis_opt((minute + 2) * 60_000)
            .single()
            .ok_or("end")?;
        assert_eq!(reopened.max_gap_len_for_range(start, end).await?, 0);
    }
    let event = capture
        .events()
        .into_iter()
        .find(|event| event.name == "table.append.retry_complete")
        .ok_or("retry trace")?;
    assert_eq!(
        event.fields.get("original_base").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        event.fields.get("retry_count").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        event.fields.get("final_base").map(String::as_str),
        Some("3")
    );
    let serial_root = TempDir::new()?;
    let mut serial = TimeSeriesTable::create(
        TableLocation::local(serial_root.path()),
        timestamp_only_meta(),
    )
    .await?;
    for minute in [10, 20, 0] {
        serial
            .append(timestamp_only_batch_starting_at_minute_offset(minute, 2)?)
            .await?;
    }
    assert_eq!(
        stored_timestamps(&stale, temp.path())?,
        [0, 60_000, 600_000, 660_000, 1_200_000, 1_260_000]
    );
    assert_eq!(
        stored_timestamps(&stale, temp.path())?,
        stored_timestamps(&serial, serial_root.path())?
    );
    assert_eq!(
        stale
            .load_global_coverage_with_recovery::<AppendError>()
            .await?,
        serial
            .load_global_coverage_with_recovery::<AppendError>()
            .await?
    );
    assert_eq!(
        normalize_actions(stale.log.load_commit(4).await?.actions),
        normalize_actions(serial.log.load_commit(4).await?.actions)
    );
    Ok(())
}

#[tokio::test]
async fn retry_distinguishes_every_component_of_a_composite_identity() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let batch = composite_entity_batch(&[(0, "A", "Y", 2.0), (60_000, "A", "Y", 3.0)])?;
    let mut index = timestamp_only_index();
    index.entity_columns = vec!["symbol".into(), "venue".into()];
    let meta = TableMeta::new_time_series_with_schema(
        index,
        LogicalSchema::try_from_arrow_schema(batch.schema().as_ref())?,
    );
    let mut stale = TimeSeriesTable::create(location.clone(), meta).await?;
    let mut writer = stale.writer(batch.schema())?;
    writer.write(&batch).await?;
    let pending_path = data_files(temp.path())?.pop().ok_or("pending file")?;
    let mut winner = TimeSeriesTable::open(location.clone()).await?;
    winner
        .append(composite_entity_batch(&[
            (0, "A", "X", 1.0),
            (60_000, "A", "X", 4.0),
        ])?)
        .await?;
    let capture = TraceCapture::default();
    let _callsite = capture.dispatch();
    let mut publication = Box::pin(
        writer
            .commit_with_retry(1)
            .with_subscriber(capture.dispatch()),
    );
    wait_for_retry(publication.as_mut(), &capture).await;
    storage::inject_parquet_read_failure(pending_path.clone());
    let report = publication.await?.ok_or("report")?;
    assert_eq!((report.starting_version, report.committed_version), (2, 3));
    writer.abort().await?;
    assert!(
        matches!(storage::open_parquet_reader(stale.location().as_ref(), pending_path.strip_prefix(temp.path())?).await,
        Err(StorageError::OtherIo { source, .. }) if source.to_string().contains("injected Parquet reader failure"))
    );
    let coverage = stale
        .load_entity_coverage_with_recovery::<AppendError>()
        .await?;
    assert_eq!(coverage.identity_count(), 2);
    assert_eq!(coverage.cardinality(), 4);
    for venue in ["X", "Y"] {
        let identity = EntityIdentity::try_new(vec!["A".into(), venue.into()])?;
        assert_eq!(coverage.get(&identity).ok_or("identity")?.cardinality(), 2);
    }
    assert_eq!(
        TimeSeriesTable::open(location).await?.state(),
        stale.state()
    );
    Ok(())
}

#[tokio::test]
async fn retry_rejects_overlapping_intervals_even_for_identical_payloads() -> TestResult {
    for winning_timestamp in [0, 30_000] {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
        let before = stale.state().clone();
        let batch = timestamp_only_batch(1)?;
        let mut writer = stale.writer(batch.schema())?;
        writer.write(&batch).await?;
        let mut winner = TimeSeriesTable::open(location.clone()).await?;
        winner
            .append(timestamp_only_batch_with_millis([winning_timestamp])?)
            .await?;
        let sidecars = coverage_files(temp.path())?;
        assert!(matches!(
            writer.commit_with_retry(3).await,
            Err(TableError::Append {
                source: AppendError::PersistedIndexIntervalOverlap { .. }
            })
        ));
        assert_eq!(writer.pending_rows(), 0);
        assert!(writer.commit().await?.is_none());
        writer.abort().await?;
        assert_eq!(stale.state(), &before);
        assert_eq!(data_files(temp.path())?.len(), 1);
        assert_eq!(coverage_files(temp.path())?, sidecars);
        assert_eq!(
            TimeSeriesTable::open(location).await?.state(),
            winner.state()
        );
    }
    Ok(())
}

#[tokio::test]
async fn zero_budget_and_first_schema_adoption_do_not_rebase() -> TestResult {
    for (max_retries, canonical) in [(0, true), (3, false)] {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut meta = timestamp_only_meta();
        if !canonical {
            meta.logical_schema = None;
        }
        let mut stale = TimeSeriesTable::create(location.clone(), meta).await?;
        let before = stale.state().clone();
        let batch = timestamp_only_batch(1)?;
        let mut writer = stale.writer(batch.schema())?;
        writer.write(&batch).await?;
        let mut winner = TimeSeriesTable::open(location.clone()).await?;
        winner
            .append(timestamp_only_batch_starting_at_minute_offset(10, 1)?)
            .await?;
        assert!(matches!(
            writer.commit_with_retry(max_retries).await,
            Err(TableError::Append {
                source: AppendError::Commit {
                    source: CommitError::Conflict { .. }
                }
            })
        ));
        writer.abort().await?;
        assert_eq!(stale.state(), &before);
        assert_eq!(data_files(temp.path())?.len(), 1);
        assert_eq!(
            TimeSeriesTable::open(location).await?.state(),
            winner.state()
        );
    }
    Ok(())
}

#[tokio::test]
async fn empty_retry_does_not_refresh_a_stale_handle_or_create_output() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let before = stale.state().clone();
    let mut winner = TimeSeriesTable::open(location).await?;
    winner.append(timestamp_only_batch(1)?).await?;
    let files = data_files(temp.path())?;
    let sidecars = coverage_files(temp.path())?;
    let mut writer = stale.writer(timestamp_only_batch(0)?.schema())?;
    assert!(writer.commit_with_retry(3).await?.is_none());
    writer.abort().await?;
    assert_eq!(stale.state(), &before);
    assert_eq!(data_files(temp.path())?, files);
    assert_eq!(coverage_files(temp.path())?, sidecars);
    Ok(())
}

#[tokio::test]
async fn retry_rejects_metadata_changes_rewrites_and_unknown_actions() -> TestResult {
    for change in [
        "metadata",
        "noop_metadata",
        "protocol",
        "schema",
        "index",
        "remove",
        "rewrite",
        "unknown",
    ] {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
        stale
            .append(timestamp_only_batch_starting_at_minute_offset(10, 1)?)
            .await?;
        let before = stale.state().clone();
        let existing_data = data_files(temp.path())?;
        let batch = timestamp_only_batch(1)?;
        let mut writer = stale.writer(batch.schema())?;
        writer.write(&batch).await?;
        let mut winner = TimeSeriesTable::open(location.clone()).await?;
        let segment = winner
            .state()
            .segments
            .values()
            .next()
            .ok_or("segment")?
            .clone();
        let mut meta = winner.state().table_meta.clone();
        match change {
            "schema" => {
                winner
                    .add_columns(vec![LogicalField {
                        name: "extra".into(),
                        data_type: LogicalDataType::Float64,
                        nullable: true,
                        metadata: Default::default(),
                    }])
                    .await?;
            }
            "unknown" => {
                winner
                    .append(timestamp_only_batch_starting_at_minute_offset(20, 1)?)
                    .await?;
                let path = temp.path().join(layout::commit_rel_path(3));
                let mut json: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
                json.get_mut("actions")
                    .and_then(serde_json::Value::as_array_mut)
                    .ok_or("actions")?
                    .push(serde_json::json!({"FutureMutation": {}}));
                std::fs::write(path, serde_json::to_vec(&json)?)?;
            }
            _ => {
                let actions = match change {
                    "metadata" => {
                        meta.created_at += chrono::Duration::seconds(1);
                        vec![LogAction::UpdateTableMeta(meta)]
                    }
                    "noop_metadata" => vec![LogAction::UpdateTableMeta(meta)],
                    "protocol" => {
                        meta.required_writer_features.insert("future_writer".into());
                        vec![LogAction::UpdateTableMeta(meta)]
                    }
                    "index" => {
                        let mut index = timestamp_only_index();
                        index.kind = IndexKind::Timestamp {
                            index_granularity: TimeIndexGranularity::Minutes(2),
                            timezone: None,
                        };
                        meta.kind = crate::transaction_log::TableKind::TimeSeries(index.clone());
                        vec![
                            LogAction::UpdateTableMeta(meta),
                            LogAction::UpdateTableCoverage {
                                index_kind: index.kind,
                                coverage_path: winner
                                    .state()
                                    .table_coverage
                                    .as_ref()
                                    .ok_or("coverage")?
                                    .coverage_path
                                    .clone(),
                            },
                        ]
                    }
                    "remove" => vec![LogAction::RemoveSegment { path: segment.path }],
                    "rewrite" => vec![
                        LogAction::RemoveSegment {
                            path: segment.path.clone(),
                        },
                        LogAction::AddSegment(segment),
                        LogAction::UpdateTableCoverage {
                            index_kind: winner.index_spec().kind.clone(),
                            coverage_path: winner
                                .state()
                                .table_coverage
                                .as_ref()
                                .ok_or("coverage")?
                                .coverage_path
                                .clone(),
                        },
                    ],
                    _ => unreachable!(),
                };
                winner.log.commit_with_expected_version(2, actions).await?;
            }
        }
        let sidecars = coverage_files(temp.path())?;
        let published_log = std::fs::read(temp.path().join(layout::commit_rel_path(3)))?;
        assert!(writer.commit_with_retry(3).await.is_err(), "{change}");
        writer.abort().await?;
        assert_eq!(stale.state(), &before, "{change}");
        assert_eq!(stale.log.load_current_version().await?, 3, "{change}");
        assert_eq!(
            std::fs::read(temp.path().join(layout::commit_rel_path(3)))?,
            published_log,
            "{change}"
        );
        assert_eq!(coverage_files(temp.path())?, sidecars, "{change}");
        if change != "unknown" {
            assert_eq!(data_files(temp.path())?, existing_data, "{change}");
        } else {
            assert_eq!(data_files(temp.path())?.len(), 2);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_second_winner_during_retry_respects_the_remaining_budget() -> TestResult {
    for budget in [1, 2] {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
        let before = stale.state().clone();
        let batch = timestamp_only_batch(2)?;
        let mut writer = stale.writer(batch.schema())?;
        writer.write(&batch).await?;
        let path = data_files(temp.path())?.pop().ok_or("pending file")?;
        let mut winner = TimeSeriesTable::open(location.clone()).await?;
        winner
            .append(timestamp_only_batch_starting_at_minute_offset(10, 2)?)
            .await?;
        let capture = TraceCapture::default();
        let _callsite = capture.dispatch();
        let mut publication = Box::pin(
            writer
                .commit_with_retry(budget)
                .with_subscriber(capture.dispatch()),
        );
        wait_for_retry(publication.as_mut(), &capture).await;
        let bytes = std::fs::read(&path)?;
        storage::inject_parquet_read_failure(path.clone());
        winner
            .append(timestamp_only_batch_starting_at_minute_offset(20, 2)?)
            .await?;
        let sidecars = coverage_files(temp.path())?;
        let result = publication.await;
        assert_eq!(writer.pending_rows(), 0);
        writer.abort().await?;
        if budget == 1 {
            assert!(matches!(
                result,
                Err(TableError::Append {
                    source: AppendError::Commit {
                        source: CommitError::Conflict {
                            expected: 2,
                            found: 3,
                            ..
                        }
                    }
                })
            ));
            assert_eq!(stale.state(), &before);
            assert!(!path.exists());
            assert_eq!(coverage_files(temp.path())?, sidecars);
            assert_eq!(
                TimeSeriesTable::open(location).await?.state(),
                winner.state()
            );
        } else {
            let report = result?.ok_or("report")?;
            assert_eq!((report.starting_version, report.committed_version), (3, 4));
            assert_eq!(std::fs::read(&path)?, bytes);
            assert_eq!(stale.state().segments.len(), 3);
            assert_eq!(
                TimeSeriesTable::open(location).await?.state(),
                stale.state()
            );
        }
        let retries = capture
            .events()
            .into_iter()
            .filter(|event| event.name == "table.append.retry")
            .collect::<Vec<_>>();
        assert_eq!(retries.len(), budget as usize);
        for event in retries {
            let delay: u64 = event.fields.get("backoff_ms").ok_or("backoff")?.parse()?;
            assert!((10..=250).contains(&delay));
        }
        assert!(
            matches!(storage::open_parquet_reader(stale.location().as_ref(), path.strip_prefix(temp.path())?).await,
            Err(StorageError::OtherIo { source, .. }) if source.to_string().contains("injected Parquet reader failure"))
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_during_backoff_closes_writer_and_preserves_original_snapshot() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let before = stale.state().clone();
    let batch = timestamp_only_batch(1)?;
    let mut writer = stale.writer(batch.schema())?;
    writer.write(&batch).await?;
    let path = data_files(temp.path())?.pop().ok_or("pending file")?;
    let mut winner = TimeSeriesTable::open(location.clone()).await?;
    winner
        .append(timestamp_only_batch_starting_at_minute_offset(10, 1)?)
        .await?;
    let sidecars = coverage_files(temp.path())?;
    let capture = TraceCapture::default();
    let _callsite = capture.dispatch();
    let mut publication = Box::pin(
        writer
            .commit_with_retry(3)
            .with_subscriber(capture.dispatch()),
    );
    wait_for_retry(publication.as_mut(), &capture).await;
    drop(publication);
    assert!(!path.exists());
    assert!(matches!(
        writer.commit().await,
        Err(TableError::Append {
            source: AppendError::WriterClosed
        })
    ));
    writer.abort().await?;
    assert_eq!(stale.state(), &before);
    assert_eq!(coverage_files(temp.path())?, sidecars);
    assert_eq!(
        TimeSeriesTable::open(location).await?.state(),
        winner.state()
    );
    Ok(())
}

#[tokio::test]
async fn uncertain_publication_is_preserved_and_never_retried() -> TestResult {
    for after_retry in [false, true] {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut table = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
        let before = table.state().clone();
        let batch = timestamp_only_batch(1)?;
        let mut writer = table.writer(batch.schema())?;
        writer.write(&batch).await?;
        if after_retry {
            let mut winner = TimeSeriesTable::open(location.clone()).await?;
            winner
                .append(timestamp_only_batch_starting_at_minute_offset(10, 1)?)
                .await?;
        }
        let capture = TraceCapture::default();
        let _callsite = capture.dispatch();
        let mut publication = Box::pin(
            writer
                .commit_with_retry(3)
                .with_subscriber(capture.dispatch()),
        );
        if after_retry {
            wait_for_retry(publication.as_mut(), &capture).await;
        }
        storage::inject_parent_sync_failure(temp.path().join(layout::current_rel_path()));
        let result = publication.await;
        assert!(matches!(
            result,
            Err(TableError::Append {
                source: AppendError::CommitAmbiguous { .. }
            })
        ));
        assert!(matches!(
            writer.commit().await,
            Err(TableError::Append {
                source: AppendError::WriterClosed
            })
        ));
        writer.abort().await?;
        assert_eq!(table.state(), &before);
        assert_eq!(
            capture
                .events()
                .iter()
                .filter(|event| event.name == "table.append.retry")
                .count(),
            usize::from(after_retry)
        );
        let reopened = TimeSeriesTable::open(location).await?;
        let expected_version = if after_retry { 3 } else { 2 };
        assert_eq!(reopened.state().version, expected_version);
        assert_eq!(
            reopened.state().segments.len(),
            expected_version as usize - 1
        );
        assert_eq!(
            data_files(temp.path())?.len(),
            expected_version as usize - 1
        );
        assert!(
            !temp
                .path()
                .join(layout::commit_rel_path(expected_version + 1))
                .exists()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn retry_does_not_touch_an_active_publishers_unpublished_log() -> TestResult {
    for before_rename in [false, true] {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
        let before = stale.state().clone();
        let batch = timestamp_only_batch(1)?;
        let mut writer = stale.writer(batch.schema())?;
        writer.write(&batch).await?;
        let mut winner = TimeSeriesTable::open(location.clone()).await?;
        let log_path = temp.path().join(layout::commit_rel_path(2));
        let mut pause = if before_rename {
            storage::pause_atomic_write_before_rename(temp.path().join(layout::current_rel_path()))
        } else {
            storage::pause_write_new_after_create(log_path.clone())
        };
        let mut publication =
            Box::pin(winner.append(timestamp_only_batch_starting_at_minute_offset(10, 1)?));
        tokio::select! {
            () = pause.wait_until_paused() => {}
            result = &mut publication => panic!("winner ended before pause: {result:?}"),
        }
        let log_bytes = std::fs::read(&log_path)?;
        let sidecars = coverage_files(temp.path())?;
        let capture = TraceCapture::default();
        assert!(matches!(
            capture.run(writer.commit_with_retry(3)).await,
            Err(TableError::Append {
                source: AppendError::Commit {
                    source: CommitError::CommitInProgress { .. }
                }
            })
        ));
        writer.abort().await?;
        assert_eq!(stale.state(), &before);
        assert_eq!(std::fs::read(&log_path)?, log_bytes);
        assert_eq!(coverage_files(temp.path())?, sidecars);
        assert_eq!(stale.log.load_current_version().await?, 1);
        assert!(
            !capture
                .events()
                .iter()
                .any(|event| event.name == "table.append.retry")
        );
        pause.release();
        assert_eq!(publication.await?.committed_version, 2);
        assert_eq!(
            TimeSeriesTable::open(location).await?.state(),
            winner.state()
        );
    }
    Ok(())
}

#[tokio::test]
async fn cleanup_failure_stops_retries_and_reports_every_owned_path() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let before = stale.state().clone();
    let batch = timestamp_only_batch(1)?;
    let mut writer = stale.writer(batch.schema())?;
    writer.write(&batch).await?;
    let mut winner = TimeSeriesTable::open(location.clone()).await?;
    winner
        .append(timestamp_only_batch_starting_at_minute_offset(10, 1)?)
        .await?;
    let winner_data = winner
        .state()
        .segments
        .keys()
        .map(|path| temp.path().join(path))
        .collect::<Vec<_>>();
    let winner_sidecars = coverage_files(temp.path())?;
    for dir in ["data", SEGMENT_COVERAGE_DIR, TABLE_SNAPSHOT_DIR] {
        storage::inject_cleanup_failure(temp.path().join(dir));
    }
    let capture = TraceCapture::default();
    let result = capture.run(writer.commit_with_retry(3)).await;
    let Err(TableError::Append {
        source: AppendError::Rollback {
            source,
            cleanup_errors,
        },
    }) = result
    else {
        panic!("expected cleanup failure: {result:?}");
    };
    assert!(matches!(
        *source,
        AppendError::Commit {
            source: CommitError::Conflict {
                expected: 1,
                found: 2,
                ..
            }
        }
    ));
    let mut expected = data_files(temp.path())?
        .into_iter()
        .filter(|path| !winner_data.contains(path))
        .collect::<std::collections::HashSet<_>>();
    expected.extend(
        coverage_files(temp.path())?
            .into_keys()
            .filter(|path| !winner_sidecars.contains_key(path))
            .map(|path| temp.path().join(path)),
    );
    let paths = cleanup_errors
        .into_iter()
        .map(|error| match error {
            StorageError::OtherIo { path, .. } => PathBuf::from(path),
            other => panic!("unexpected cleanup error: {other:?}"),
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(paths.len(), 3);
    assert_eq!(paths, expected);
    assert_eq!(writer.pending_rows(), 0);
    assert!(writer.commit().await?.is_none());
    writer.abort().await?;
    assert_eq!(stale.state(), &before);
    assert!(
        !capture
            .events()
            .iter()
            .any(|event| event.name == "table.append.retry")
    );
    for (path, bytes) in winner_sidecars {
        assert_eq!(std::fs::read(temp.path().join(path))?, bytes);
    }
    assert_eq!(
        TimeSeriesTable::open(location).await?.state(),
        winner.state()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_a_retried_publication_keeps_the_original_handle_and_winner() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let before = stale.state().clone();
    let batch = timestamp_only_batch(1)?;
    let mut writer = stale.writer(batch.schema())?;
    writer.write(&batch).await?;
    let mut winner = TimeSeriesTable::open(location.clone()).await?;
    winner
        .append(timestamp_only_batch_starting_at_minute_offset(10, 1)?)
        .await?;
    let sidecars = coverage_files(temp.path())?;
    let winner_path = temp
        .path()
        .join(winner.state().segments.keys().next().ok_or("winner")?);
    let winner_bytes = std::fs::read(&winner_path)?;
    let capture = TraceCapture::default();
    let _callsite = capture.dispatch();
    let mut publication = Box::pin(
        writer
            .commit_with_retry(3)
            .with_subscriber(capture.dispatch()),
    );
    wait_for_retry(publication.as_mut(), &capture).await;
    let mut pause =
        storage::pause_atomic_write_before_rename(temp.path().join(layout::current_rel_path()));
    tokio::select! {
        () = pause.wait_until_paused() => {}
        result = &mut publication => panic!("retry ended before cancellation: {result:?}"),
    }
    drop(publication);
    pause.release();
    assert!(matches!(
        writer.commit().await,
        Err(TableError::Append {
            source: AppendError::WriterClosed
        })
    ));
    writer.abort().await?;
    assert_eq!(stale.state(), &before);
    assert_eq!(std::fs::read(&winner_path)?, winner_bytes);
    assert_eq!(data_files(temp.path())?, [winner_path]);
    assert_eq!(coverage_files(temp.path())?, sidecars);
    assert!(!temp.path().join(layout::commit_rel_path(3)).exists());
    assert_eq!(
        TimeSeriesTable::open(location).await?.state(),
        winner.state()
    );
    Ok(())
}

#[tokio::test]
async fn first_schema_adoption_succeeds_without_a_conflict_and_writer_remains_reusable()
-> TestResult {
    let temp = TempDir::new()?;
    let mut meta = timestamp_only_meta();
    meta.logical_schema = None;
    let mut table = TimeSeriesTable::create(TableLocation::local(temp.path()), meta).await?;
    let batch = timestamp_only_batch(1)?;
    let mut writer = table.writer(batch.schema())?;
    writer.write(&batch).await?;
    assert_eq!(
        writer
            .commit_with_retry(3)
            .await?
            .ok_or("first commit")?
            .committed_version,
        2
    );
    writer
        .write(&timestamp_only_batch_starting_at_minute_offset(1, 1)?)
        .await?;
    assert_eq!(
        writer
            .commit()
            .await?
            .ok_or("second commit")?
            .committed_version,
        3
    );
    writer.abort().await?;
    assert_eq!(stored_timestamps(&table, temp.path())?, [0, 60_000]);
    assert!(table.state().table_meta.logical_schema.is_some());
    Ok(())
}

#[tokio::test]
async fn failure_while_loading_the_retry_snapshot_leaves_the_handle_and_winner_unchanged()
-> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let before = stale.state().clone();
    let batch = timestamp_only_batch(1)?;
    let mut writer = stale.writer(batch.schema())?;
    writer.write(&batch).await?;
    let mut winner = TimeSeriesTable::open(location.clone()).await?;
    winner
        .append(timestamp_only_batch_starting_at_minute_offset(10, 1)?)
        .await?;
    let sidecars = coverage_files(temp.path())?;
    let log_path = temp.path().join(layout::commit_rel_path(2));
    let bytes = std::fs::read(&log_path)?;
    std::fs::write(&log_path, b"{")?;
    let result = writer.commit_with_retry(3).await;
    std::fs::write(&log_path, bytes)?;
    assert!(matches!(
        result,
        Err(TableError::Append {
            source: AppendError::Commit {
                source: CommitError::CommitDeserialization { version: 2, .. }
            }
        })
    ));
    assert_eq!(writer.pending_rows(), 0);
    writer.abort().await?;
    assert_eq!(stale.state(), &before);
    assert_eq!(coverage_files(temp.path())?, sidecars);
    assert_eq!(data_files(temp.path())?.len(), 1);
    assert_eq!(
        TimeSeriesTable::open(location).await?.state(),
        winner.state()
    );
    Ok(())
}
