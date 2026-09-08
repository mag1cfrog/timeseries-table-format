use super::*;
use crate::{
    metadata::{
        index::{IndexKind, IndexValue, TimeIndexGranularity},
        segments::{FileFormat, SegmentEntityLayout},
        table::TableMeta,
    },
    storage::layout::UPDATE_PREPARE_DIR,
};
use arrow::{
    array::{
        Int32Array, Int64Array, RecordBatchIterator, StringArray, StructArray,
        TimestampNanosecondArray, UInt64Array,
    },
    datatypes::{Field, TimeUnit},
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{collections::HashMap, fs, num::NonZeroU64};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

struct Fixture {
    dir: tempfile::TempDir,
    state: TableState,
    index: IndexSpec,
    location: TableLocation,
}

impl Fixture {
    fn new(
        batches: &[RecordBatch],
        entities: &[&str],
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let schema = batches[0].schema();
        let (kind, min, max) = match schema.field_with_name("idx")?.data_type() {
            DataType::UInt64 => (
                IndexKind::UInt64 {
                    index_granularity: NonZeroU64::MIN,
                },
                IndexValue::UInt64(0),
                IndexValue::UInt64(u64::MAX),
            ),
            DataType::Timestamp(_, timezone) => (
                IndexKind::Timestamp {
                    index_granularity: TimeIndexGranularity::Seconds(1),
                    timezone: timezone.as_ref().map(ToString::to_string),
                },
                IndexValue::Timestamp(chrono::DateTime::UNIX_EPOCH),
                IndexValue::Timestamp(chrono::DateTime::UNIX_EPOCH),
            ),
            _ => (
                IndexKind::Int64 {
                    index_granularity: NonZeroU64::new(10).ok_or("nonzero")?,
                },
                IndexValue::Int64(i64::MIN),
                IndexValue::Int64(i64::MAX),
            ),
        };
        let index = IndexSpec {
            column: "idx".into(),
            entity_columns: entities.iter().map(|name| (*name).into()).collect(),
            kind,
        };
        let table_meta = TableMeta::new_time_series_with_schema(
            index.clone(),
            LogicalSchema::try_from_arrow_schema(&schema)?,
        );
        fs::create_dir(dir.path().join("data"))?;
        fs::create_dir(dir.path().join("_timeseries_log"))?;
        fs::create_dir(dir.path().join("_coverage"))?;
        fs::write(dir.path().join("_timeseries_log/CURRENT"), b"17")?;
        fs::write(
            dir.path().join("_timeseries_log/0000000017.json"),
            b"immutable log sentinel",
        )?;
        fs::write(
            dir.path().join("_coverage/sentinel"),
            b"immutable coverage sentinel",
        )?;
        let mut segments = HashMap::new();
        for (i, batch) in batches.iter().enumerate() {
            let path = format!("data/source-{i}.parquet");
            let file = File::create(dir.path().join(&path))?;
            let properties = WriterProperties::builder()
                .set_max_row_group_row_count(Some(1024))
                .build();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))?;
            writer.write(batch)?;
            writer.close()?;
            segments.insert(
                path.clone(),
                SegmentMeta {
                    path,
                    format: FileFormat::Parquet,
                    entity_layout: if entities.is_empty() {
                        SegmentEntityLayout::NotApplicable
                    } else {
                        SegmentEntityLayout::Mixed
                    },
                    index_min: min.clone(),
                    index_max: max.clone(),
                    row_count: batch.num_rows() as u64,
                    file_size: None,
                    coverage_path: None,
                },
            );
        }
        let location = TableLocation::local(dir.path());
        Ok(Self {
            dir,
            state: TableState {
                version: 17,
                table_meta,
                segments,
                table_coverage: None,
            },
            index,
            location,
        })
    }

    fn source(
        &self,
        batches: Vec<std::result::Result<RecordBatch, ArrowError>>,
    ) -> RecordBatchIterator<std::vec::IntoIter<std::result::Result<RecordBatch, ArrowError>>> {
        let schema = self
            .state
            .table_meta
            .arrow_schema_ref()
            .expect("fixture schema");
        RecordBatchIterator::new(batches.into_iter(), schema)
    }

    fn snapshot_bytes(&self) -> std::result::Result<Vec<Vec<u8>>, std::io::Error> {
        let mut paths: Vec<_> = self.state.segments.keys().cloned().collect();
        paths.sort();
        paths.extend([
            "_timeseries_log/CURRENT".into(),
            "_timeseries_log/0000000017.json".into(),
            "_coverage/sentinel".into(),
        ]);
        paths
            .iter()
            .map(|path| fs::read(self.dir.path().join(path)))
            .collect()
    }

    fn no_scratch(&self) -> TestResult {
        let parent = self.dir.path().join(UPDATE_PREPARE_DIR);
        if parent.exists() {
            assert_eq!(fs::read_dir(parent)?.count(), 0);
        }
        Ok(())
    }
}

fn simple(
    keys: Vec<Option<i64>>,
    values: Vec<Option<i64>>,
) -> std::result::Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
}

fn assert_key_error(error: PrepareError, kind: KeyViolation, seen: u64) -> UpdateKey {
    match error {
        PrepareError::Key {
            kind: actual,
            input_rows_seen,
            observed_violations,
            example_key,
        } => {
            assert_eq!(actual, kind);
            assert_eq!(input_rows_seen, seen);
            assert_eq!(observed_violations, 1);
            example_key
        }
        other => panic!("unexpected preparation error: {other:?}"),
    }
}

#[tokio::test]
async fn two_way_runs_preserve_every_record() -> std::result::Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let mut scratch = Scratch::create(dir.path())?;
    let mut sorter = Sorter::new(&scratch, 128);
    for value in (0_u64..129).rev() {
        sorter.push(
            &mut scratch,
            Record {
                order: value.to_be_bytes().to_vec(),
                key: vec![],
                values: vec![],
                segment: 0,
                row: value,
            },
        )?;
    }
    let run = sorter.finish(&mut scratch).await?;
    let mut reader = RunReader::open(&scratch, run)?;
    for value in 0..129 {
        assert_eq!(reader.next()?.map(|record| record.row), Some(value));
    }
    assert!(reader.next()?.is_none());
    assert!(scratch.metrics.initial_runs > 2);
    drop(reader);
    scratch.cleanup()?;
    assert!(!scratch.directory.exists());
    Ok(())
}

#[tokio::test]
async fn shuffled_updates_bind_exact_rows_across_segments_and_leave_table_unchanged() -> TestResult
{
    let fixture = Fixture::new(
        &[
            simple(vec![Some(-2), Some(3)], vec![Some(1); 2])?,
            simple(vec![Some(i64::MIN), Some(i64::MAX)], vec![Some(2); 2])?,
        ],
        &[],
    )?;
    let before = fixture.snapshot_bytes()?;
    let source = fixture.source(vec![
        Ok(simple(
            vec![Some(i64::MAX), Some(-2)],
            vec![Some(42), None],
        )?),
        Ok(simple(
            vec![Some(i64::MIN), Some(3)],
            vec![Some(41), Some(40)],
        )?),
    ]);
    let mut prepared = prepare_with_budget(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        source,
        &["value".into()],
        128,
    )
    .await?;
    assert_eq!((prepared.version, prepared.matched_rows), (17, 4));
    assert_eq!(prepared.destination_indices, [1]);
    let mut actual = Vec::new();
    while let Some(update) = prepared.next()? {
        actual.push((
            update.segment_index,
            update.row,
            update.key,
            update
                .values
                .column(1)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .iter()
                .next()
                .flatten(),
        ));
    }
    assert_eq!(
        actual
            .iter()
            .map(|row| (row.0, row.1, row.3))
            .collect::<Vec<_>>(),
        [
            (0, 0, None),
            (0, 1, Some(40)),
            (1, 0, Some(41)),
            (1, 1, Some(42))
        ]
    );
    assert_eq!(before, fixture.snapshot_bytes()?);
    fixture.no_scratch()?;
    Ok(())
}

#[tokio::test]
async fn global_duplicates_unmatched_ambiguity_and_null_keys_are_typed() -> TestResult {
    let fixture = Fixture::new(&[simple((0..40).map(Some).collect(), vec![None; 40])?], &[])?;
    let before = fixture.snapshot_bytes()?;
    for same_value in [false, true] {
        let mut batches: Vec<_> = (0..40)
            .map(|i| simple(vec![Some(i)], vec![Some(i)]).map_err(|e| e.into()))
            .collect::<std::result::Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        batches.push(simple(
            vec![Some(0)],
            vec![Some(if same_value { 0 } else { 999 })],
        )?);
        let result = prepare_with_budget(
            &fixture.location,
            &fixture.state,
            &fixture.index,
            fixture.source(batches.into_iter().map(Ok).collect()),
            &["value".into()],
            128,
        )
        .await;
        assert_key_error(
            result.err().ok_or("duplicate accepted")?,
            KeyViolation::DuplicateSource,
            41,
        );
        fixture.no_scratch()?;
    }
    let unmatched = prepare_with_budget(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        fixture.source(vec![Ok(simple(vec![Some(1), Some(99)], vec![None; 2])?)]),
        &["value".into()],
        128,
    )
    .await;
    assert_key_error(
        unmatched.err().ok_or("unmatched accepted")?,
        KeyViolation::UnmatchedSource,
        2,
    );
    let null = prepare_with_budget(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        fixture.source(vec![Ok(simple(vec![None], vec![None])?)]),
        &["value".into()],
        128,
    )
    .await;
    let key = assert_key_error(
        null.err().ok_or("null accepted")?,
        KeyViolation::NullIdentity,
        1,
    );
    assert_eq!(key.0, [("idx".into(), None)]);
    assert_eq!(before, fixture.snapshot_bytes()?);
    fixture.no_scratch()?;
    let corrupt = Fixture::new(
        &[simple(vec![Some(1), Some(1), Some(2)], vec![None; 3])?],
        &[],
    )?;
    let ambiguous = prepare_updates(
        &corrupt.location,
        &corrupt.state,
        &corrupt.index,
        corrupt.source(vec![Ok(simple(vec![Some(1)], vec![None])?)]),
        &["value".into()],
    )
    .await;
    assert_key_error(
        ambiguous.err().ok_or("ambiguity accepted")?,
        KeyViolation::AmbiguousTarget,
        1,
    );
    // Corruption at an unrequested key is outside this operation's scope.
    prepare_updates(
        &corrupt.location,
        &corrupt.state,
        &corrupt.index,
        corrupt.source(vec![Ok(simple(vec![Some(2)], vec![None])?)]),
        &["value".into()],
    )
    .await?
    .close()?;
    corrupt.no_scratch()?;
    Ok(())
}

#[tokio::test]
async fn composite_entities_reordered_columns_and_widening() -> TestResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new("idx", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("slot", DataType::UInt64, true),
        Field::new("first", DataType::Int64, true),
        Field::new("second", DataType::Int64, true),
    ]));
    let target = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3, 3, 4])),
            Arc::new(StringArray::from(vec!["a|b", "a", "a"])),
            Arc::new(UInt64Array::from(vec![u64::MAX, u64::MAX, 0])),
            Arc::new(Int64Array::from(vec![0; 3])),
            Arc::new(Int64Array::from(vec![0; 3])),
        ],
    )?;
    let fixture = Fixture::new(&[target], &["slot", "name"])?;
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("second", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("idx", DataType::Int32, true),
        Field::new("slot", DataType::UInt64, true),
        Field::new("first", DataType::Int32, true),
    ]));
    let source = RecordBatch::try_new(
        source_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![20, 21])),
            Arc::new(StringArray::from(vec!["a", "a|b"])),
            Arc::new(Int32Array::from(vec![3, 3])),
            Arc::new(UInt64Array::from(vec![u64::MAX; 2])),
            Arc::new(Int32Array::from(vec![10, 11])),
        ],
    )?;
    let reader = RecordBatchIterator::new(vec![Ok(source)], source_schema);
    let mut prepared = prepare_with_budget(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        reader,
        &["second".into(), "first".into()],
        128,
    )
    .await?;
    assert_eq!(prepared.destination_indices, [4, 3]);
    let a = prepared.next()?.ok_or("missing row")?;
    assert_eq!(a.row, 0);
    assert_eq!(
        a.key.0[0],
        ("slot".into(), Some(KeyValue::UInt64(u64::MAX)))
    );
    assert_eq!(
        a.values
            .column(3)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        21
    );
    assert_eq!(
        a.values
            .column(4)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        11
    );
    assert_eq!(prepared.next()?.ok_or("missing row")?.row, 1);
    assert!(prepared.next()?.is_none());
    fixture.no_scratch()?;
    Ok(())
}

#[tokio::test]
async fn unsigned_and_timestamp_keys_preserve_raw_values() -> TestResult {
    for (data_type, keys) in [
        (
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![0, i64::MAX as u64 + 1, u64::MAX])) as ArrayRef,
        ),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            Arc::new(TimestampNanosecondArray::from(vec![-1, 1, 2]).with_timezone("UTC"))
                as ArrayRef,
        ),
    ] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("idx", data_type, true),
            Field::new("value", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![keys, Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let fixture = Fixture::new(std::slice::from_ref(&batch), &[])?;
        let mut prepared = prepare_with_budget(
            &fixture.location,
            &fixture.state,
            &fixture.index,
            fixture.source(vec![Ok(batch.clone())]),
            &["value".into()],
            128,
        )
        .await?;
        for row in 0..3 {
            assert_eq!(
                prepared.next()?.ok_or("missing key")?.key,
                key_at(&batch, 1, row)?
            );
        }
        assert!(prepared.next()?.is_none());
        fixture.no_scratch()?;
    }
    // 1 and 2 share a coverage bucket but only 1 exists.
    let fixture = Fixture::new(&[simple(vec![Some(1)], vec![None])?], &[])?;
    let result = prepare_updates(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        fixture.source(vec![Ok(simple(vec![Some(2)], vec![None])?)]),
        &["value".into()],
    )
    .await;
    assert_key_error(
        result.err().ok_or("bucket matched")?,
        KeyViolation::UnmatchedSource,
        1,
    );
    Ok(())
}

#[tokio::test]
async fn advertised_schema_empty_batches_and_reader_failures_are_validated() -> TestResult {
    let fixture = Fixture::new(&[simple(vec![Some(1)], vec![None])?], &[])?;
    let before = fixture.snapshot_bytes()?;
    let empty = fixture.source(vec![]);
    let mut prepared = prepare_updates(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        empty,
        &["value".into()],
    )
    .await?;
    assert_eq!(prepared.matched_rows, 0);
    assert!(prepared.next()?.is_none());
    for columns in [
        vec![],
        vec!["idx".into()],
        vec!["unknown".into()],
        vec!["value".into(), "value".into()],
        vec![" value".into()],
    ] {
        assert!(
            prepare_updates(
                &fixture.location,
                &fixture.state,
                &fixture.index,
                fixture.source(vec![]),
                &columns
            )
            .await
            .is_err()
        );
    }
    for fields in [
        vec![Field::new("idx", DataType::Int64, true)],
        vec![
            Field::new("idx", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
            Field::new("extra", DataType::Int64, true),
        ],
        vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ],
        vec![
            Field::new("idx", DataType::UInt64, true),
            Field::new("value", DataType::Int64, true),
        ],
        vec![
            Field::new("idx", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
        ],
    ] {
        let reader = RecordBatchIterator::new(
            Vec::<std::result::Result<RecordBatch, ArrowError>>::new(),
            Arc::new(Schema::new(fields)),
        );
        assert!(matches!(
            prepare_updates(
                &fixture.location,
                &fixture.state,
                &fixture.index,
                reader,
                &["value".into()]
            )
            .await,
            Err(PrepareError::Schema { .. })
        ));
    }
    let changed = RecordBatch::new_empty(Arc::new(Schema::new(vec![
        Field::new("idx", DataType::Int64, false),
        Field::new("value", DataType::Int64, true),
    ])));
    assert!(matches!(
        prepare_updates(
            &fixture.location,
            &fixture.state,
            &fixture.index,
            fixture.source(vec![Ok(changed)]),
            &["value".into()]
        )
        .await,
        Err(PrepareError::Arrow { .. })
    ));
    let error = ArrowError::IoError(
        "late input failure".into(),
        std::io::Error::other("original failure"),
    );
    let result = prepare_with_budget(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        fixture.source(vec![Ok(simple(vec![Some(1)], vec![None])?), Err(error)]),
        &["value".into()],
        128,
    )
    .await;
    assert!(matches!(
        result,
        Err(PrepareError::Reader {
            source: ArrowError::IoError(_, _)
        })
    ));
    assert_eq!(before, fixture.snapshot_bytes()?);
    fixture.no_scratch()?;
    Ok(())
}

#[tokio::test]
async fn drop_cancellation_and_explicit_cleanup_errors_preserve_ownership() -> TestResult {
    let fixture = Fixture::new(&[simple(vec![Some(1)], vec![None])?], &[])?;
    let before = fixture.snapshot_bytes()?;
    let source = fixture.source(vec![Ok(simple(vec![Some(1)], vec![None])?)]);
    let columns = ["value".into()];
    let mut future = Box::pin(prepare_with_budget(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        source,
        &columns,
        128,
    ));
    assert!(futures::poll!(&mut future).is_pending());
    assert!(
        fs::read_dir(fixture.dir.path().join(UPDATE_PREPARE_DIR))?
            .next()
            .is_some()
    );
    drop(future);
    fixture.no_scratch()?;
    let prepared = prepare_updates(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        fixture.source(vec![Ok(simple(vec![Some(1)], vec![None])?)]),
        &columns,
    )
    .await?;
    drop(prepared);
    fixture.no_scratch()?;
    // An unexpected directory at an exclusively owned file path forces both a
    // real create-new failure and a real cleanup failure on Windows and Unix.
    let mut scratch = Scratch::create(fixture.dir.path())?;
    let collision = scratch.path(0);
    fs::create_dir(&collision)?;
    let mut sorter = Sorter::new(&scratch, 1);
    let error = sorter
        .push(
            &mut scratch,
            Record {
                order: vec![1],
                key: vec![],
                values: vec![],
                segment: 0,
                row: 0,
            },
        )
        .err()
        .ok_or("disk collision accepted")?;
    let error = cleanup_failure(&mut scratch, error);
    assert!(matches!(error, PrepareError::CleanupAfterFailure { .. }));
    fs::remove_dir(&collision)?;
    scratch.cleanup()?;
    assert_eq!(before, fixture.snapshot_bytes()?);
    fixture.no_scratch()?;
    Ok(())
}

#[test]
fn visible_nested_nullability_respects_parent_validity() -> TestResult {
    let children = vec![Arc::new(Field::new("child", DataType::Int64, false))].into();
    let array: ArrayRef = Arc::new(StructArray::try_new(
        children,
        vec![Arc::new(Int64Array::from(vec![None, Some(9)]))],
        Some(vec![false, true].into()),
    )?);
    let field = Field::new("parent", array.data_type().clone(), true);
    validate_value(&field, &array, 0, "parent")?;
    validate_value(&field, &array, 1, "parent")?;
    let null: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    assert!(validate_value(&Field::new("x", DataType::Int64, false), &null, 0, "x").is_err());
    Ok(())
}

/// Run the compiled native test executable directly to measure its process RSS.
/// Source and target construction are streaming; no all-input fixture allocation.
#[tokio::test]
#[ignore = "native memory benchmark; see scripts/bench/bench_update_prepare.ps1"]
async fn preparation_memory_benchmark() -> TestResult {
    fn parameter(
        name: &str,
        default: usize,
    ) -> std::result::Result<usize, Box<dyn std::error::Error>> {
        Ok(std::env::var(name)
            .ok()
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(default))
    }
    let target_rows = parameter("TST_UPDATE_TARGET_ROWS", 16_384)?;
    let updates = parameter("TST_UPDATE_ROWS", target_rows / 4)?;
    let budget = parameter("TST_UPDATE_SORT_BYTES", 65_536)?;
    let concentrated = std::env::var("TST_UPDATE_MODE").as_deref() == Ok("concentrated");
    assert!(
        target_rows.is_power_of_two()
            && target_rows >= 4096
            && updates > 0
            && updates <= target_rows / 4
    );
    let empty = simple(vec![], vec![])?;
    let mut fixture = Fixture::new(&vec![empty; 4], &[])?;
    let rows_per_segment = target_rows / 4;
    for segment in 0..4 {
        let path = format!("data/source-{segment}.parquet");
        let mut writer = ArrowWriter::try_new(
            File::create(fixture.dir.path().join(&path))?,
            fixture.state.table_meta.arrow_schema_ref()?,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1024))
                    .build(),
            ),
        )?;
        for start in (0..rows_per_segment).step_by(256) {
            let end = (start + 256).min(rows_per_segment);
            writer.write(&simple(
                (start..end)
                    .map(|row| Some((segment * rows_per_segment + row) as i64))
                    .collect(),
                vec![None; end - start],
            )?)?;
        }
        writer.close()?;
        fixture
            .state
            .segments
            .get_mut(&path)
            .ok_or("segment missing")?
            .row_count = rows_per_segment as u64;
    }
    let schema = fixture.state.table_meta.arrow_schema_ref()?;
    let source = (0..updates).step_by(256).map(|start| {
        let end = (start + 256).min(updates);
        // Odd multiplication permutes a power-of-two domain without an index vector.
        let domain = if concentrated {
            rows_per_segment
        } else {
            target_rows
        };
        simple(
            (start..end)
                .map(|row| Some(((row * 12_289) % domain) as i64))
                .collect(),
            vec![Some(7); end - start],
        )
    });
    println!(
        "UPDATE_BENCH_READY {}",
        serde_json::json!({
            "target_rows": target_rows, "update_rows": updates, "sort_budget_bytes": budget,
            "mode": if concentrated { "concentrated" } else { "shuffled" },
            "segment_descriptors": 4, "source_batch_rows": 256, "logical_source_batch_bytes": 4096,
            "logical_update_bytes": updates * 16, "target_row_group_rows": 1024,
            "baseline_segment_metadata_bytes": serde_json::to_vec(&fixture.state.segments)?.len(),
        })
    );
    use std::io::Write;
    std::io::stdout().flush()?;
    let started = std::time::Instant::now();
    let mut prepared = prepare_with_budget(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        RecordBatchIterator::new(source, schema),
        &["value".into()],
        budget,
    )
    .await?;
    let metrics = prepared.metrics().clone();
    let mut consumed = 0;
    while prepared.next()?.is_some() {
        consumed += 1;
    }
    assert_eq!(consumed, updates);
    assert!(
        metrics.peak_sort_bytes
            <= budget + metrics.largest_record_bytes + std::mem::size_of::<Record>()
    );
    fixture.no_scratch()?;
    println!(
        "UPDATE_BENCH_RESULT {}",
        serde_json::json!({
            "elapsed_seconds": started.elapsed().as_secs_f64(),
            "peak_sort_bytes": metrics.peak_sort_bytes,
            "largest_record_bytes": metrics.largest_record_bytes,
            "peak_scratch_bytes": metrics.peak_scratch_bytes,
            "initial_runs": metrics.initial_runs,
            "target_rows_read": metrics.target_rows_read,
            "projected_column_bytes": metrics.projected_column_bytes,
            "key_discovery_bytes_read": metrics.key_discovery_bytes_read,
        })
    );
    Ok(())
}

#[tokio::test]
async fn nested_replacements_and_metadata_rules_survive_staging() -> TestResult {
    let child = Arc::new(Field::new("child", DataType::Int64, false));
    let nested: ArrayRef = Arc::new(StructArray::try_new(
        vec![child.clone()].into(),
        vec![Arc::new(Int64Array::from(vec![None, Some(9)]))],
        Some(vec![false, true].into()),
    )?);
    let schema = Arc::new(Schema::new(vec![
        Field::new("idx", DataType::Int64, true),
        Field::new("value", nested.data_type().clone(), true),
        Field::new("unselected", DataType::Int64, true),
    ]));
    let target = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            nested.clone(),
            Arc::new(Int64Array::from(vec![0, 0])),
        ],
    )?;
    let fixture = Fixture::new(&[target], &[])?;
    let metadata = HashMap::from([("annotation".into(), "ignored".into())]);
    let source_schema = Arc::new(Schema::new_with_metadata(
        vec![
            schema.field(0).clone().with_metadata(metadata.clone()),
            schema.field(1).clone().with_metadata(metadata.clone()),
        ],
        metadata.clone(),
    ));
    let source = RecordBatch::try_new(
        source_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2])), nested.clone()],
    )?;
    let mut prepared = prepare_with_budget(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        RecordBatchIterator::new(vec![Ok(source)], source_schema),
        &["value".into()],
        128,
    )
    .await?;
    let first = prepared.next()?.ok_or("missing nested null")?;
    assert!(first.values.column(1).is_null(0));
    assert!(first.values.schema().metadata().is_empty());
    assert!(first.values.schema().field(1).metadata().is_empty());
    let second = prepared.next()?.ok_or("missing nested value")?;
    assert_eq!(
        second
            .values
            .column(1)
            .as_struct()
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(0),
        9
    );
    prepared.close()?;
    // Nested metadata is part of the Arrow type and must not be stripped.
    let nested_type =
        DataType::Struct(vec![Arc::new(child.as_ref().clone().with_metadata(metadata))].into());
    let bad = Arc::new(Schema::new(vec![
        schema.field(0).clone(),
        Field::new("value", nested_type, true),
    ]));
    let reader = RecordBatchIterator::new(
        Vec::<std::result::Result<RecordBatch, ArrowError>>::new(),
        bad,
    );
    assert!(matches!(
        prepare_updates(
            &fixture.location,
            &fixture.state,
            &fixture.index,
            reader,
            &["value".into()]
        )
        .await,
        Err(PrepareError::Schema { .. })
    ));
    // Even an existing unselected payload is an invalid extra field.
    assert!(matches!(
        prepare_updates(
            &fixture.location,
            &fixture.state,
            &fixture.index,
            fixture.source(vec![]),
            &["value".into()]
        )
        .await,
        Err(PrepareError::Schema { .. })
    ));
    fixture.no_scratch()?;
    Ok(())
}

#[tokio::test]
async fn discovery_does_not_decode_or_stage_wide_unselected_payloads() -> TestResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new("wide", DataType::Binary, true),
        Field::new("idx", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));
    let bytes = vec![7_u8; 128 * 1024];
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::BinaryArray::from(vec![bytes.as_slice(); 4])),
            Arc::new(Int64Array::from(vec![0, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![0; 4])),
        ],
    )?;
    let fixture = Fixture::new(&[batch], &[])?;
    let input = simple(vec![Some(3)], vec![Some(99)])?;
    let reader = RecordBatchIterator::new(vec![Ok(input.clone())], input.schema());
    let prepared = prepare_with_budget(
        &fixture.location,
        &fixture.state,
        &fixture.index,
        reader,
        &["value".into()],
        128,
    )
    .await?;
    assert!(prepared.metrics().key_discovery_bytes_read < 32 * 1024);
    assert!(prepared.metrics().peak_scratch_bytes < 16 * 1024);
    assert_eq!(prepared.destination_indices, [2]);
    prepared.close()?;
    fixture.no_scratch()?;
    Ok(())
}

#[tokio::test]
async fn merging_can_be_cancelled_and_corrupt_runs_fail_without_large_allocations() -> TestResult {
    let dir = tempfile::tempdir()?;
    let mut scratch = Scratch::create(dir.path())?;
    let mut sorter = Sorter::new(&scratch, 1);
    for row in 0_u64..20 {
        sorter.push(
            &mut scratch,
            Record {
                order: row.to_be_bytes().to_vec(),
                key: vec![],
                values: vec![],
                segment: 0,
                row,
            },
        )?;
    }
    let directory = scratch.directory.clone();
    let mut merging = Box::pin(async move { sorter.finish(&mut scratch).await });
    assert!(futures::poll!(&mut merging).is_pending());
    drop(merging);
    assert!(!directory.exists());
    let mut scratch = Scratch::create(dir.path())?;
    let mut sorter = Sorter::new(&scratch, 1);
    sorter.push(
        &mut scratch,
        Record {
            order: vec![0],
            key: vec![],
            values: vec![],
            segment: 0,
            row: 0,
        },
    )?;
    let id = sorter.finish(&mut scratch).await?;
    fs::write(scratch.path(id), u64::MAX.to_le_bytes())?;
    let mut reader = RunReader::open(&scratch, id)?;
    assert!(matches!(reader.next(), Err(PrepareError::Io { .. })));
    drop(reader);
    scratch.cleanup()?;
    Ok(())
}

#[test]
fn oversized_target_key_layout_is_a_typed_resource_error() -> TestResult {
    check_target_budget("data/example.parquet", 3, 64, 64)?;
    assert!(matches!(
        check_target_budget("data/example.parquet", 3, 65, 64),
        Err(PrepareError::TargetResource {
            row_group: 3,
            uncompressed_bytes: 65,
            limit: 64,
            ..
        })
    ));
    Ok(())
}
