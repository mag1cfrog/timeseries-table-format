use super::*;
use chrono::{TimeZone, Utc};

#[tokio::test]
async fn one_writer_commits_multiple_segments_and_adopts_schema() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut meta = timestamp_only_meta();
    meta.logical_schema = None;
    let mut table = TimeSeriesTable::create(location.clone(), meta).await?;
    let first = timestamp_only_batch_starting_at_minute_offset(0, 2)?;
    let second = timestamp_only_batch_starting_at_minute_offset(2, 3)?;
    let schema = Arc::new(
        first
            .schema()
            .as_ref()
            .clone()
            .with_metadata(HashMap::from([("source".into(), "writer".into())])),
    );
    let first = RecordBatch::try_new(schema.clone(), first.columns().to_vec())?;
    let second = RecordBatch::try_new(schema, second.columns().to_vec())?;
    let options = AppendWriterOptions {
        compression: ParquetCompression::Snappy,
        max_rows_per_row_group: 1,
        max_bytes_per_row_group: 128,
    };
    let mut writer = table.writer_with(first.schema(), options)?;
    assert_eq!(writer.pending_rows(), 0);
    assert!(!temp.path().join("data").exists());
    writer.write(&first).await?;
    assert_eq!(writer.pending_rows(), 2);
    let first_report = writer.commit().await?.expect("first segment");
    assert_eq!(
        (
            first_report.starting_version,
            first_report.committed_version
        ),
        (1, 2)
    );
    assert_eq!(writer.pending_rows(), 0);
    assert!(writer.commit().await?.is_none());
    writer.write(&second).await?;
    assert_eq!(writer.pending_rows(), 3);
    let second_report = writer.commit().await?.expect("second segment");
    assert_eq!(
        (
            second_report.starting_version,
            second_report.committed_version
        ),
        (2, 3)
    );
    assert_eq!(writer.pending_rows(), 0);
    for report in [&first_report, &second_report] {
        assert_eq!(report.compression, options.compression);
        assert_eq!(
            report.max_rows_per_row_group,
            options.max_rows_per_row_group
        );
        assert_eq!(
            report.max_bytes_per_row_group,
            options.max_bytes_per_row_group
        );
    }
    writer.abort().await?;

    let reopened = TimeSeriesTable::open(location).await?;
    assert_eq!(table.state(), reopened.state());
    assert_eq!(
        table
            .state()
            .table_meta
            .logical_schema
            .as_ref()
            .expect("adopted schema")
            .metadata()
            .get("source")
            .map(String::as_str),
        Some("writer")
    );
    assert_eq!(reopened.state().segments.len(), 2);
    assert_eq!(
        reopened.state().segments[&first_report.segment_path].row_count,
        2
    );
    assert_eq!(
        reopened.state().segments[&second_report.segment_path].row_count,
        3
    );
    assert_eq!(data_files(temp.path())?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn idle_and_zero_row_writers_create_no_artifacts() -> TestResult {
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta()).await?;
    let before = table.state().clone();
    let zero = timestamp_only_batch(0)?;
    let mut writer = table.writer(zero.schema())?;
    assert!(writer.commit().await?.is_none());
    writer.write(&zero).await?;
    writer.write(&zero).await?;
    assert_eq!(writer.pending_rows(), 0);
    assert!(writer.commit().await?.is_none());
    writer.abort().await?;
    assert_eq!(table.state(), &before);
    assert!(!temp.path().join("data").exists());
    assert!(coverage_files(temp.path())?.is_empty());
    Ok(())
}

#[tokio::test]
async fn another_handle_observes_rows_only_after_commit_and_refresh() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut table = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let mut reader = TimeSeriesTable::open(location.clone()).await?;
    let batch = timestamp_only_batch(2)?;
    let mut writer = table.writer(batch.schema())?;
    writer.write(&batch).await?;
    assert!(!reader.refresh().await?);
    assert!(reader.state().segments.is_empty());
    assert!(
        TimeSeriesTable::open(location)
            .await?
            .state()
            .segments
            .is_empty()
    );
    let report = writer.commit().await?.expect("committed rows");
    assert!(reader.state().segments.is_empty());
    assert!(reader.refresh().await?);
    assert_eq!(reader.state().version, 2);
    assert_eq!(reader.state().segments[&report.segment_path].row_count, 2);
    writer.abort().await?;
    Ok(())
}

#[tokio::test]
async fn overlap_with_existing_and_own_commits_cleans_up_and_allows_reuse() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
    let a = time_series_batch(vec![0], vec!["A"], vec![1.0])?;
    let b = time_series_batch(vec![0], vec!["B"], vec![2.0])?;
    table.append(a.clone()).await?;
    let mut writer = table.writer(a.schema())?;
    for (overlap, next, version) in [
        (a, b.clone(), 3),
        (b, time_series_batch(vec![60_000], vec!["A"], vec![3.0])?, 4),
    ] {
        let files_before = data_files(temp.path())?;
        let coverage_before = coverage_files(temp.path())?;
        writer.write(&overlap).await?;
        let error = writer
            .commit()
            .await
            .expect_err("overlap waits until commit");
        assert!(matches!(
            error,
            TableError::Append {
                source: AppendError::PersistedIndexIntervalOverlap { .. }
            }
        ));
        assert_eq!(writer.pending_rows(), 0);
        assert_eq!(data_files(temp.path())?, files_before);
        assert_eq!(coverage_files(temp.path())?, coverage_before);
        assert_eq!(
            TimeSeriesTable::open(location.clone())
                .await?
                .state()
                .version,
            version - 1
        );
        assert!(writer.commit().await?.is_none());
        writer.write(&next).await?;
        assert_eq!(
            writer.commit().await?.expect("reuse").committed_version,
            version
        );
    }
    writer.abort().await?;
    assert_eq!(table.state().version, 4);
    assert_eq!(table.state().segments.len(), 3);
    Ok(())
}

#[tokio::test]
async fn duplicate_intervals_fail_at_commit_within_and_across_batches() -> TestResult {
    for split_batches in [false, true] {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta())
                .await?;
        let before = table.state().clone();
        let mut writer = table.writer_with(
            timestamp_only_batch(0)?.schema(),
            AppendWriterOptions {
                max_rows_per_row_group: 1,
                ..Default::default()
            },
        )?;
        if split_batches {
            writer
                .write(&timestamp_only_batch_with_millis([0])?)
                .await?;
            writer
                .write(&timestamp_only_batch_with_millis([30_000])?)
                .await?;
        } else {
            writer
                .write(&timestamp_only_batch_with_millis([0, 30_000])?)
                .await?;
        }
        assert_eq!(writer.pending_rows(), 2);
        assert!(matches!(writer.commit().await, Err(TableError::Append {
            source: AppendError::GeneratedSegmentCoverage { source }
        }) if matches!(*source, SegmentCoverageError::DuplicateIndexInterval { .. })));
        assert_eq!(writer.pending_rows(), 0);
        assert!(data_files(temp.path())?.is_empty());
        assert!(coverage_files(temp.path())?.is_empty());
        writer.abort().await?;
        assert_eq!(table.state(), &before);
    }
    Ok(())
}

#[tokio::test]
async fn out_of_order_commits_fill_all_gaps() -> TestResult {
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta()).await?;
    let mut writer = table.writer(timestamp_only_batch(0)?.schema())?;
    for (index, offset) in [20, 0, 10].into_iter().enumerate() {
        writer
            .write(&timestamp_only_batch_starting_at_minute_offset(offset, 10)?)
            .await?;
        assert_eq!(
            writer.commit().await?.expect("segment").committed_version,
            index as u64 + 2
        );
    }
    writer.abort().await?;
    let start = Utc.timestamp_millis_opt(0).single().expect("start");
    let end = Utc.timestamp_millis_opt(30 * 60_000).single().expect("end");
    assert_eq!(table.max_gap_len_for_range(start, end).await?, 0);
    assert_eq!(table.coverage_ratio_for_range(start, end).await?, 1.0);
    Ok(())
}

#[tokio::test]
async fn conflict_preserves_stale_handle_until_caller_refreshes() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut stale = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let before = stale.state().clone();
    let mut other = TimeSeriesTable::open(location).await?;
    let batch = timestamp_only_batch(1)?;
    let mut writer = stale.writer(batch.schema())?;
    other
        .append(timestamp_only_batch_starting_at_minute_offset(1, 1)?)
        .await?;
    let data_before = data_files(temp.path())?;
    let coverage_before = coverage_files(temp.path())?;
    writer.write(&batch).await?;
    assert!(matches!(
        writer.commit().await,
        Err(TableError::Append {
            source: AppendError::Commit {
                source: CommitError::Conflict {
                    expected: 1,
                    found: 2,
                    ..
                }
            }
        })
    ));
    assert_eq!(writer.pending_rows(), 0);
    assert!(writer.commit().await?.is_none());
    assert_eq!(data_files(temp.path())?, data_before);
    assert_eq!(coverage_files(temp.path())?, coverage_before);
    writer.abort().await?;
    assert_eq!(stale.state(), &before);
    assert!(stale.refresh().await?);
    let mut writer = stale.writer(batch.schema())?;
    writer.write(&batch).await?;
    assert_eq!(
        writer
            .commit()
            .await?
            .expect("after refresh")
            .committed_version,
        3
    );
    writer.abort().await?;
    Ok(())
}

#[tokio::test]
async fn mismatched_batches_discard_pending_rows_including_on_empty_input() -> TestResult {
    for zero_rows in [false, true] {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta())
                .await?;
        let batch = timestamp_only_batch(1)?;
        let mut writer = table.writer(batch.schema())?;
        writer.write(&batch).await?;
        let wrong = input_batch(if zero_rows { vec![] } else { vec![1] })?;
        assert!(matches!(
            writer.write(&wrong).await,
            Err(TableError::Append {
                source: AppendError::ArrowInput { .. }
            })
        ));
        assert_eq!(writer.pending_rows(), 0);
        assert!(data_files(temp.path())?.is_empty());
        assert!(coverage_files(temp.path())?.is_empty());
        assert!(writer.commit().await?.is_none());
        writer.write(&batch).await?;
        assert_eq!(writer.commit().await?.expect("reuse").row_count, 1);
        writer.abort().await?;
    }
    Ok(())
}

#[tokio::test]
async fn mid_segment_write_failure_cleans_up_and_allows_reuse() -> TestResult {
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta()).await?;
    let batch = timestamp_only_batch(1)?;
    let mut writer = table.writer_with(
        batch.schema(),
        AppendWriterOptions {
            max_rows_per_row_group: 1024,
            ..Default::default()
        },
    )?;
    writer.write(&batch).await?;
    crate::storage::inject_output_write_failure(temp.path().join("data"), 1);
    assert!(matches!(
        writer
            .write(&timestamp_only_batch_starting_at_minute_offset(
                1,
                DEFAULT_MAX_ROWS_PER_ROW_GROUP
            )?)
            .await,
        Err(TableError::Append {
            source: AppendError::ParquetWrite { .. }
        })
    ));
    assert_eq!(writer.pending_rows(), 0);
    assert!(data_files(temp.path())?.is_empty());
    assert!(coverage_files(temp.path())?.is_empty());
    writer.write(&batch).await?;
    assert_eq!(writer.commit().await?.expect("reuse").committed_version, 2);
    writer.abort().await?;
    Ok(())
}

#[tokio::test]
async fn failed_finalization_and_sidecars_leave_writer_reusable() -> TestResult {
    for stage in ["footer", "sync", "segment_sidecar", "table_sidecar"] {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta())
                .await?;
        let batch = timestamp_only_batch(1)?;
        let mut writer = table.writer(batch.schema())?;
        writer.write(&batch).await?;
        match stage {
            "footer" => crate::storage::inject_output_write_failure(temp.path().join("data"), 1),
            "sync" => crate::storage::inject_output_finish_failure(temp.path().join("data")),
            "segment_sidecar" => crate::storage::inject_write_new_failure(
                temp.path().join(SEGMENT_COVERAGE_DIR),
                false,
            ),
            "table_sidecar" => crate::storage::inject_write_new_failure(
                temp.path().join(TABLE_SNAPSHOT_DIR),
                false,
            ),
            _ => unreachable!(),
        }
        assert!(writer.commit().await.is_err(), "{stage}");
        assert_eq!(writer.pending_rows(), 0, "{stage}");
        assert!(data_files(temp.path())?.is_empty(), "{stage}");
        assert!(coverage_files(temp.path())?.is_empty(), "{stage}");
        writer.write(&batch).await?;
        assert_eq!(writer.commit().await?.expect("reuse").committed_version, 2);
        writer.abort().await?;
    }
    Ok(())
}

#[tokio::test]
async fn abort_removes_pending_output_and_reports_cleanup_failures() -> TestResult {
    for cleanup_fails in [false, true] {
        let temp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta())
                .await?;
        let before = table.state().clone();
        let batch = timestamp_only_batch(1)?;
        let mut writer = table.writer(batch.schema())?;
        writer.write(&batch).await?;
        let path = data_files(temp.path())?.pop().expect("pending output");
        if cleanup_fails {
            crate::storage::inject_cleanup_failure(path.clone());
        }
        let result = writer.abort().await;
        if cleanup_fails {
            assert!(
                matches!(result, Err(TableError::Append { source: AppendError::Cleanup { cleanup_errors } })
                if matches!(cleanup_errors.as_slice(), [StorageError::OtherIo { path: failed, .. }]
                    if failed == &path.display().to_string()))
            );
            assert!(path.is_file());
        } else {
            result?;
            assert!(!path.exists());
        }
        assert_eq!(table.state(), &before);
        assert!(coverage_files(temp.path())?.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn rollback_cleanup_failure_preserves_original_error_and_allows_reuse() -> TestResult {
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta()).await?;
    let batch = timestamp_only_batch(1)?;
    let mut writer = table.writer(batch.schema())?;
    writer.write(&batch).await?;
    let path = data_files(temp.path())?.pop().expect("pending output");
    crate::storage::inject_cleanup_failure(path.clone());
    let error = writer
        .write(&input_batch(vec![1])?)
        .await
        .expect_err("schema and cleanup fail");
    let TableError::Append {
        source: AppendError::Rollback {
            source,
            cleanup_errors,
        },
    } = error
    else {
        panic!("unexpected error: {error}");
    };
    assert!(matches!(*source, AppendError::ArrowInput { .. }));
    assert!(
        matches!(cleanup_errors.as_slice(), [StorageError::OtherIo { path: failed, .. }]
        if failed == &path.display().to_string())
    );
    assert!(path.is_file());
    assert_eq!(writer.pending_rows(), 0);
    writer.write(&batch).await?;
    assert_eq!(writer.commit().await?.expect("reuse").committed_version, 2);
    writer.abort().await?;
    assert_eq!(table.state().segments.len(), 1);
    std::fs::remove_file(path)?;
    Ok(())
}

#[tokio::test]
async fn drop_is_invisible_and_vacuum_reports_any_failed_cleanup() -> TestResult {
    for cleanup_fails in [false, true] {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut table = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
        let batch = timestamp_only_batch(1)?;
        let mut writer = table.writer(batch.schema())?;
        writer.write(&batch).await?;
        let path = data_files(temp.path())?.pop().expect("pending output");
        if cleanup_fails {
            crate::storage::inject_cleanup_failure(path.clone());
        }
        drop(writer);
        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state().version, 1);
        assert!(reopened.state().segments.is_empty());
        if cleanup_fails {
            File::options().write(true).open(&path)?.set_times(
                std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH),
            )?;
            let report = reopened
                .vacuum(Utc::now(), crate::VacuumMode::DryRun)
                .await?;
            assert_eq!(report.artifacts.len(), 1);
            assert_eq!(
                report.artifacts[0].path,
                path.strip_prefix(temp.path())?.to_str().expect("path")
            );
            assert_eq!(
                report.artifacts[0].reason,
                crate::VacuumArtifactReason::InvalidOrUnreadableParquet
            );
            assert_eq!(report.removable_files, 1);
            assert!(
                storage::remove_file_if_exists(
                    table.location().as_ref(),
                    path.strip_prefix(temp.path())?
                )
                .await
                .is_err()
            );
            storage::remove_file_if_exists(
                table.location().as_ref(),
                path.strip_prefix(temp.path())?,
            )
            .await?;
        } else {
            assert!(!path.exists());
        }
    }
    Ok(())
}

#[tokio::test]
async fn ambiguous_commit_retains_artifacts_and_closes_writer() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut table = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let before = table.state().clone();
    let batch = timestamp_only_batch(1)?;
    let mut writer = table.writer(batch.schema())?;
    writer.write(&batch).await?;
    crate::storage::inject_write_new_failure(temp.path().join(layout::commit_rel_path(2)), true);
    let error = writer.commit().await.expect_err("ambiguous publication");
    let TableError::Append {
        source: AppendError::CommitAmbiguous { segment_path, .. },
    } = error
    else {
        panic!("unexpected error: {error}");
    };
    assert!(temp.path().join(&segment_path).is_file());
    let files_before = data_files(temp.path())?;
    let coverage_before = coverage_files(temp.path())?;
    assert_eq!(coverage_before.len(), 2);
    assert_eq!(writer.pending_rows(), 0);
    for _ in 0..2 {
        assert!(matches!(
            writer.write(&batch).await,
            Err(TableError::Append {
                source: AppendError::WriterClosed
            })
        ));
        assert!(matches!(
            writer.commit().await,
            Err(TableError::Append {
                source: AppendError::WriterClosed
            })
        ));
    }
    writer.abort().await?;
    assert_eq!(table.state(), &before);
    assert_eq!(TimeSeriesTable::open(location).await?.state(), &before);
    assert_eq!(data_files(temp.path())?, files_before);
    assert_eq!(coverage_files(temp.path())?, coverage_before);
    Ok(())
}

#[tokio::test]
async fn creation_checks_protocol_options_version_and_schema_before_artifacts() -> TestResult {
    let temp = TempDir::new()?;
    let mut table =
        TimeSeriesTable::create(TableLocation::local(temp.path()), timestamp_only_meta()).await?;
    let invalid_schema = input_batch(vec![1])?.schema();
    let invalid_options = AppendWriterOptions {
        max_rows_per_row_group: 0,
        max_bytes_per_row_group: 0,
        ..Default::default()
    };
    table
        .state
        .table_meta
        .required_writer_features
        .insert("future_writer".into());
    table.state.version = u64::MAX;
    assert!(matches!(
        table.writer_with(invalid_schema.clone(), invalid_options),
        Err(TableError::Append {
            source: AppendError::Protocol { .. }
        })
    ));
    table
        .state
        .table_meta
        .required_writer_features
        .remove("future_writer");
    assert!(matches!(
        table.writer_with(invalid_schema.clone(), invalid_options),
        Err(TableError::Append {
            source: AppendError::InvalidMaxRowsPerRowGroup { .. }
        })
    ));
    assert!(matches!(
        table.writer_with(
            invalid_schema.clone(),
            AppendWriterOptions {
                max_rows_per_row_group: 1,
                ..invalid_options
            }
        ),
        Err(TableError::Append {
            source: AppendError::InvalidMaxBytesPerRowGroup { .. }
        })
    ));
    assert!(matches!(
        table.writer(invalid_schema.clone()),
        Err(TableError::Append {
            source: AppendError::Commit {
                source: CommitError::VersionOverflow { .. }
            }
        })
    ));
    table.state.version = 1;
    assert!(matches!(
        table.writer(invalid_schema),
        Err(TableError::Append {
            source: AppendError::SchemaValidation { .. }
        })
    ));
    assert!(!temp.path().join("data").exists());
    assert!(coverage_files(temp.path())?.is_empty());
    assert_eq!(table.current_version().await?, 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_commit_cleans_artifacts_and_closes_writer() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut table = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let batch = timestamp_only_batch(1)?;
    let mut writer = table.writer(batch.schema())?;
    writer.write(&batch).await?;
    let mut pause = crate::storage::pause_atomic_write_before_rename(
        temp.path().join(layout::current_rel_path()),
    );
    let mut commit = Box::pin(writer.commit());
    tokio::select! {
        () = pause.wait_until_paused() => {}
        result = &mut commit => panic!("commit completed before cancellation: {result:?}"),
    }
    drop(commit);
    pause.release();
    assert!(matches!(
        writer.commit().await,
        Err(TableError::Append {
            source: AppendError::WriterClosed
        })
    ));
    assert!(matches!(
        writer.write(&batch).await,
        Err(TableError::Append {
            source: AppendError::WriterClosed
        })
    ));
    writer.abort().await?;
    assert!(data_files(temp.path())?.is_empty());
    assert!(coverage_files(temp.path())?.is_empty());
    assert_eq!(TimeSeriesTable::open(location).await?.state().version, 1);
    Ok(())
}

#[tokio::test]
async fn writer_and_append_produce_equivalent_rows_coverage_and_log_actions() -> TestResult {
    for adopt_schema in [false, true] {
        let append_root = TempDir::new()?;
        let writer_root = TempDir::new()?;
        let mut meta = widening_table_meta();
        if adopt_schema {
            meta = make_basic_table_meta();
            meta.logical_schema = None;
        }
        let batches = if adopt_schema {
            vec![
                time_series_batch(vec![0], vec!["A"], vec![1.0])?,
                time_series_batch(vec![60_000], vec!["B"], vec![2.0])?,
            ]
        } else {
            vec![
                widening_batch(vec![0], vec![1], vec![Some(1.0)], vec!["a"])?,
                widening_batch(vec![2], vec![2], vec![None], vec!["b"])?,
            ]
        };
        let options = AppendWriterOptions {
            compression: ParquetCompression::Snappy,
            max_rows_per_row_group: 1,
            max_bytes_per_row_group: 128,
        };
        let mut appended =
            TimeSeriesTable::create(TableLocation::local(append_root.path()), meta.clone()).await?;
        let mut written =
            TimeSeriesTable::create(TableLocation::local(writer_root.path()), meta).await?;
        let append_report = appended
            .append(
                AppendRequest::new(batches.clone())
                    .compression(options.compression)
                    .max_rows_per_row_group(options.max_rows_per_row_group)
                    .max_bytes_per_row_group(options.max_bytes_per_row_group),
            )
            .await?;
        let mut writer = written.writer_with(batches[0].schema(), options)?;
        for batch in &batches {
            writer.write(batch).await?;
        }
        let writer_report = writer.commit().await?.expect("segment");
        writer.abort().await?;
        assert_eq!(append_report.row_count, writer_report.row_count);
        assert_eq!(append_report.row_group_count, writer_report.row_group_count);
        assert_eq!(append_report.file_size_bytes, writer_report.file_size_bytes);
        assert_eq!(
            std::fs::read(append_root.path().join(&append_report.segment_path))?,
            std::fs::read(writer_root.path().join(&writer_report.segment_path))?
        );
        let mut append_coverage = coverage_files(append_root.path())?
            .into_values()
            .collect::<Vec<_>>();
        let mut writer_coverage = coverage_files(writer_root.path())?
            .into_values()
            .collect::<Vec<_>>();
        append_coverage.sort();
        writer_coverage.sort();
        assert_eq!(append_coverage, writer_coverage);
        let normalize = |mut actions: Vec<LogAction>| {
            for action in &mut actions {
                match action {
                    LogAction::AddSegment(segment) => {
                        segment.path = "data/segment.parquet".into();
                        segment.coverage_path = Some("_coverage/segments/coverage.roar".into());
                    }
                    LogAction::UpdateTableCoverage { coverage_path, .. } => {
                        *coverage_path = "_coverage/table/coverage.roar".into()
                    }
                    LogAction::UpdateTableMeta(_) => {}
                    LogAction::RemoveSegment { .. } => panic!("append must not remove segments"),
                }
            }
            actions
        };
        assert_eq!(
            normalize(appended.log.load_commit(2).await?.actions),
            normalize(written.log.load_commit(2).await?.actions)
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_sidecar_or_commit_creation_cleans_artifacts_and_closes_writer() -> TestResult {
    for stage in [
        PathBuf::from(SEGMENT_COVERAGE_DIR),
        PathBuf::from(TABLE_SNAPSHOT_DIR),
        layout::commit_rel_path(2),
    ] {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let mut table = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
        let batch = timestamp_only_batch(1)?;
        let mut writer = table.writer(batch.schema())?;
        writer.write(&batch).await?;
        let mut pause = crate::storage::pause_write_new_after_create(temp.path().join(&stage));
        let mut commit = Box::pin(writer.commit());
        tokio::select! {
            () = pause.wait_until_paused() => {}
            result = &mut commit => panic!("commit completed before cancellation at {stage:?}: {result:?}"),
        }
        drop(commit);
        pause.release();
        assert!(data_files(temp.path())?.is_empty(), "{stage:?}");
        assert!(coverage_files(temp.path())?.is_empty(), "{stage:?}");
        assert!(
            !temp.path().join(layout::commit_rel_path(2)).exists(),
            "{stage:?}"
        );
        assert!(matches!(
            writer.write(&batch).await,
            Err(TableError::Append {
                source: AppendError::WriterClosed
            })
        ));
        assert!(matches!(
            writer.commit().await,
            Err(TableError::Append {
                source: AppendError::WriterClosed
            })
        ));
        writer.abort().await?;
        let reopened = TimeSeriesTable::open(location).await?;
        assert_eq!(reopened.state().version, 1, "{stage:?}");
        assert!(reopened.state().segments.is_empty(), "{stage:?}");
    }
    Ok(())
}

#[tokio::test]
async fn current_publication_and_rollback_failure_preserve_finished_segment() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut table = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let before = table.state().clone();
    let batch = timestamp_only_batch(1)?;
    let mut writer = table.writer(batch.schema())?;
    writer.write(&batch).await?;
    let current_tmp = temp
        .path()
        .join(layout::current_rel_path())
        .with_extension("tmp");
    std::fs::create_dir(&current_tmp)?;
    let commit_path = temp.path().join(layout::commit_rel_path(2));
    crate::storage::inject_cleanup_failure(commit_path.clone());
    let error = writer
        .commit()
        .await
        .expect_err("CURRENT publication and commit rollback fail");
    let TableError::Append {
        source:
            AppendError::CommitAmbiguous {
                segment_path,
                source,
            },
    } = error
    else {
        panic!("unexpected error: {error}");
    };
    assert!(matches!(*source, CommitError::AmbiguousOutcome { .. }));
    let batches =
        ParquetRecordBatchReaderBuilder::try_new(File::open(temp.path().join(&segment_path))?)?
            .build()?
            .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        arrow::compute::concat_batches(&batch.schema(), &batches)?,
        batch
    );
    assert!(commit_path.is_file());
    assert_eq!(coverage_files(temp.path())?.len(), 2);
    assert!(matches!(
        writer.commit().await,
        Err(TableError::Append {
            source: AppendError::WriterClosed
        })
    ));
    assert!(matches!(
        writer.write(&batch).await,
        Err(TableError::Append {
            source: AppendError::WriterClosed
        })
    ));
    writer.abort().await?;
    assert_eq!(table.state(), &before);
    assert_eq!(TimeSeriesTable::open(location).await?.state(), &before);
    std::fs::remove_dir(current_tmp)?;
    Ok(())
}

#[tokio::test]
async fn conflict_reports_all_failed_cleanup_paths_and_leaves_writer_reusable() -> TestResult {
    let temp = TempDir::new()?;
    let location = TableLocation::local(temp.path());
    let mut table = TimeSeriesTable::create(location.clone(), timestamp_only_meta()).await?;
    let mut other = TimeSeriesTable::open(location.clone()).await?;
    other
        .append(timestamp_only_batch_starting_at_minute_offset(1, 1)?)
        .await?;
    let winner_data = data_files(temp.path())?;
    let winner_coverage = coverage_files(temp.path())?;
    let batch = timestamp_only_batch(1)?;
    let mut writer = table.writer(batch.schema())?;
    writer.write(&batch).await?;
    for dir in ["data", SEGMENT_COVERAGE_DIR, TABLE_SNAPSHOT_DIR] {
        crate::storage::inject_cleanup_failure(temp.path().join(dir));
    }
    let error = writer
        .commit()
        .await
        .expect_err("conflict and all cleanup attempts fail");
    let TableError::Append {
        source: AppendError::Rollback {
            source,
            cleanup_errors,
        },
    } = error
    else {
        panic!("unexpected error: {error}");
    };
    assert!(matches!(
        *source,
        AppendError::Commit {
            source: CommitError::Conflict { .. }
        }
    ));
    assert_eq!(cleanup_errors.len(), 3);
    let paths = cleanup_errors
        .iter()
        .map(|error| match error {
            StorageError::OtherIo { path, .. } => PathBuf::from(path),
            other => panic!("unexpected cleanup error: {other:?}"),
        })
        .collect::<std::collections::HashSet<_>>();
    let mut expected_paths = data_files(temp.path())?
        .into_iter()
        .filter(|path| !winner_data.contains(path))
        .collect::<std::collections::HashSet<_>>();
    expected_paths.extend(
        coverage_files(temp.path())?
            .into_keys()
            .filter(|path| !winner_coverage.contains_key(path))
            .map(|path| temp.path().join(path)),
    );
    assert_eq!(paths.len(), 3);
    assert_eq!(paths, expected_paths);
    for path in winner_data {
        assert!(path.is_file());
    }
    for (path, contents) in winner_coverage {
        assert_eq!(std::fs::read(temp.path().join(path))?, contents);
    }
    assert_eq!(writer.pending_rows(), 0);
    assert!(writer.commit().await?.is_none());
    writer.write(&batch).await?;
    assert_eq!(writer.pending_rows(), 1);
    writer.abort().await?;
    assert_eq!(table.state().version, 1);
    assert_eq!(TimeSeriesTable::open(location).await?.state().version, 2);
    Ok(())
}
