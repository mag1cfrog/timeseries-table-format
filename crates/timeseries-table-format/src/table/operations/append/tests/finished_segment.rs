use super::*;
use chrono::{TimeZone, Utc};

async fn finish_segment(table: &TimeSeriesTable) -> Result<FinishedAppendSegment, AppendError> {
    let batch = timestamp_only_batch(2).context(ArrowInputSnafu)?;
    let mut telemetry = AppendTelemetry::new(false);
    let mut pending = PendingAppendSegment::new(
        table,
        batch.schema(),
        AppendWriterOptions::default(),
        &mut telemetry,
    )
    .await?;
    pending.write(&batch)?;
    drop(batch);
    pending.finish(table, &mut telemetry).await
}

#[tokio::test]
async fn finished_segment_reuses_its_file_after_a_definite_conflict() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let before = stale.state().clone();
    let mut finished = finish_segment(&stale).await?;
    let path = finished.relative_path.clone();
    let bytes = std::fs::read(temp.path().join(&path))?;
    let mut winner = TimeSeriesTable::open(location.clone()).await?;
    winner
        .append(timestamp_only_batch_starting_at_minute_offset(10, 2)?)
        .await?;
    let winner_sidecars = coverage_files(temp.path())?;
    let mut telemetry = AppendTelemetry::new(false);

    assert!(matches!(
        finished.publish(&mut stale, 2, &mut telemetry).await,
        Err(AppendError::Commit {
            source: CommitError::Conflict {
                expected: 1,
                found: 2,
                ..
            }
        })
    ));
    assert_eq!(stale.state(), &before);
    assert_eq!(coverage_files(temp.path())?, winner_sidecars);
    assert_eq!(std::fs::read(temp.path().join(&path))?, bytes);
    assert_eq!(data_files(temp.path())?.len(), 2);

    let mut candidate = TimeSeriesTable::open(location.clone()).await?;
    let report = finished.publish(&mut candidate, 3, &mut telemetry).await?;
    assert_eq!(report.starting_version, 2);
    assert_eq!(report.committed_version, 3);
    assert_eq!(report.segment_path, path);
    assert_eq!(report.row_count, 2);
    drop(finished);
    assert_eq!(std::fs::read(temp.path().join(&path))?, bytes);
    assert_eq!(data_files(temp.path())?.len(), 2);
    assert_eq!(stale.state(), &before);

    let reopened = TimeSeriesTable::open(location).await?;
    assert_eq!(reopened.state(), candidate.state());
    assert_eq!(reopened.state().segments.len(), 2);
    for (path, meta) in &winner.state().segments {
        assert_eq!(reopened.state().segments.get(path), Some(meta));
    }
    for start in [0, 10] {
        let start_time = Utc
            .timestamp_millis_opt(start * 60_000)
            .single()
            .ok_or("start")?;
        let end_time = Utc
            .timestamp_millis_opt((start + 2) * 60_000)
            .single()
            .ok_or("end")?;
        assert_eq!(
            reopened.max_gap_len_for_range(start_time, end_time).await?,
            0
        );
    }
    Ok(())
}

#[tokio::test]
async fn dropping_or_cancelling_a_retained_segment_only_removes_its_file() -> TestResult {
    for cancel in [false, true] {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
        let before = stale.state().clone();
        let mut finished = finish_segment(&stale).await?;
        let retained_path = temp.path().join(&finished.relative_path);
        let mut winner = TimeSeriesTable::open(location.clone()).await?;
        winner
            .append(timestamp_only_batch_starting_at_minute_offset(10, 2)?)
            .await?;
        let winner_sidecars = coverage_files(temp.path())?;
        let winner_path = temp
            .path()
            .join(winner.state().segments.keys().next().ok_or("winner")?);
        let winner_bytes = std::fs::read(&winner_path)?;
        assert!(matches!(
            finished
                .publish(&mut stale, 2, &mut AppendTelemetry::new(false))
                .await,
            Err(AppendError::Commit {
                source: CommitError::Conflict { .. }
            })
        ));
        assert!(retained_path.exists());
        if cancel {
            let mut waiting = Box::pin(async move {
                let _finished = finished;
                std::future::pending::<()>().await;
            });
            assert!(waiting.as_mut().now_or_never().is_none());
            assert!(retained_path.exists());
            drop(waiting);
        } else {
            drop(finished);
        }
        assert!(!retained_path.exists());
        assert_eq!(std::fs::read(&winner_path)?, winner_bytes);
        assert_eq!(data_files(temp.path())?, [winner_path]);
        assert_eq!(coverage_files(temp.path())?, winner_sidecars);
        assert_eq!(stale.state(), &before);
        assert_eq!(
            TimeSeriesTable::open(location).await?.state(),
            winner.state()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_publication_after_a_conflict_preserves_the_winners_artifacts() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let before = stale.state().clone();
    let mut finished = finish_segment(&stale).await?;
    let mut winner = TimeSeriesTable::open(location.clone()).await?;
    winner
        .append(timestamp_only_batch_starting_at_minute_offset(10, 2)?)
        .await?;
    let winner_sidecars = coverage_files(temp.path())?;
    let winner_data = data_files(temp.path())?
        .into_iter()
        .filter(|path| !path.ends_with(&finished.relative_path))
        .collect::<Vec<_>>();
    assert!(matches!(
        finished
            .publish(&mut stale, 2, &mut AppendTelemetry::new(false))
            .await,
        Err(AppendError::Commit {
            source: CommitError::Conflict { .. }
        })
    ));
    let mut candidate = TimeSeriesTable::open(location.clone()).await?;
    let mut pause =
        storage::pause_atomic_write_before_rename(temp.path().join(layout::current_rel_path()));
    let mut publication = Box::pin(async move {
        finished
            .publish(&mut candidate, 3, &mut AppendTelemetry::new(false))
            .await
    });
    tokio::select! {
        () = pause.wait_until_paused() => {}
        result = &mut publication => panic!("publication completed before cancellation: {result:?}"),
    }
    drop(publication);
    pause.release();
    assert_eq!(data_files(temp.path())?, winner_data);
    assert_eq!(coverage_files(temp.path())?, winner_sidecars);
    assert!(!temp.path().join(layout::commit_rel_path(3)).exists());
    assert_eq!(stale.state(), &before);
    assert_eq!(
        TimeSeriesTable::open(location).await?.state(),
        winner.state()
    );
    Ok(())
}
