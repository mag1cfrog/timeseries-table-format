use super::*;
use crate::{
    coverage::io::write_coverage_sidecar_new_bytes,
    formats::parquet::{compute_segment_coverage, compute_segment_entity_coverage},
    metadata::{
        index::IndexKind, logical_schema::LogicalSchema, protocol::SCHEMA_ADD_COLUMNS_FEATURE,
        table::TableMeta,
    },
};
use arrow::{
    array::{ArrayRef, BinaryArray, Int64Array, RecordBatchIterator, StringArray, StructArray},
    datatypes::{DataType, Field, Schema},
};
use std::{collections::HashMap, fs, num::NonZeroU64};
type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Fixture {
    dir: tempfile::TempDir,
    location: TableLocation,
    state: TableState,
}
impl Fixture {
    fn empty(schema: SchemaRef, entities: &[&str]) -> TestResult<Self> {
        let dir = tempfile::tempdir()?;
        let location = TableLocation::local(dir.path());
        let index = IndexSpec {
            column: "idx".into(),
            entity_columns: entities.iter().map(|s| (*s).into()).collect(),
            kind: match schema.field_with_name("idx")?.data_type() {
                DataType::UInt64 => IndexKind::UInt64 {
                    index_granularity: NonZeroU64::MIN,
                },
                DataType::Timestamp(_, timezone) => IndexKind::Timestamp {
                    index_granularity: crate::metadata::index::TimeIndexGranularity::Seconds(1),
                    timezone: timezone.as_ref().map(ToString::to_string),
                },
                _ => IndexKind::Int64 {
                    index_granularity: NonZeroU64::MIN,
                },
            },
        };
        let table_meta = TableMeta::new_time_series_with_schema(
            index,
            LogicalSchema::try_from_arrow_schema(&schema)?,
        );
        fs::create_dir(dir.path().join("data"))?;
        fs::create_dir(dir.path().join("_timeseries_log"))?;
        fs::write(dir.path().join("_timeseries_log/CURRENT"), b"17")?;
        fs::write(
            dir.path().join("_timeseries_log/0000000017.json"),
            b"immutable log",
        )?;
        Ok(Self {
            dir,
            location,
            state: TableState {
                version: 17,
                table_meta,
                segments: HashMap::new(),
                table_coverage: None,
            },
        })
    }
    async fn add(
        &mut self,
        schema: SchemaRef,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> TestResult<()> {
        let path = format!("data/source-{}.parquet", self.state.segments.len());
        let mut writer = ArrowWriter::try_new(
            File::create(self.dir.path().join(&path))?,
            schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1024))
                    .set_dictionary_enabled(false)
                    .build(),
            ),
        )?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.close()?;
        let TableKind::TimeSeries(index) = &self.state.table_meta.kind else {
            unreachable!()
        };
        let (mut meta, _) =
            segment_meta_from_parquet(&self.location, Path::new(&path), index).await?;
        let bytes = if index.entity_columns.is_empty() {
            coverage_to_bytes(
                &compute_segment_coverage(&self.location, Path::new(&path), index).await?,
            )?
        } else {
            let coverage =
                compute_segment_entity_coverage(&self.location, Path::new(&path), index).await?;
            meta.entity_layout = if coverage.identity_count() == 1 {
                SegmentEntityLayout::Single(coverage.iter().next().ok_or("identity")?.0.clone())
            } else {
                SegmentEntityLayout::Mixed
            };
            entity_coverage_to_bytes(&coverage)?
        };
        let sidecar = format!(
            "_coverage/segments/source-{}.roar",
            self.state.segments.len()
        );
        write_coverage_sidecar_new_bytes(&self.location, Path::new(&sidecar), &bytes).await?;
        meta.coverage_path = Some(sidecar);
        self.state.segments.insert(path, meta);
        Ok(())
    }
    async fn prepare(&self, batch: RecordBatch, columns: &[&str]) -> TestResult<PreparedUpdates> {
        let schema = batch.schema();
        Ok(super::super::update_prepare::prepare_updates(
            &self.location,
            &self.state,
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            &columns.iter().map(|s| (*s).into()).collect::<Vec<_>>(),
        )
        .await?)
    }
    fn originals(&self) -> TestResult<Vec<(String, Vec<u8>)>> {
        let mut paths = vec![
            "_timeseries_log/CURRENT".into(),
            "_timeseries_log/0000000017.json".into(),
        ];
        for segment in self.state.segments.values() {
            paths.push(segment.path.clone());
            paths.push(segment.coverage_path.clone().ok_or("coverage")?);
        }
        paths.sort();
        paths
            .into_iter()
            .map(|path| Ok((path.clone(), fs::read(self.dir.path().join(path))?)))
            .collect()
    }
    fn no_outputs(&self) -> TestResult<()> {
        assert!(files_under(&self.dir.path().join(UPDATE_REWRITE_DATA_DIR))?.is_empty());
        assert!(
            files_under(&self.dir.path().join(storage::layout::UPDATE_PREPARE_DIR))?.is_empty()
        );
        let sidecars = files_under(&self.dir.path().join("_coverage/segments"))?;
        assert_eq!(sidecars.len(), self.state.segments.len());
        Ok(())
    }
}
fn files_under(path: &Path) -> std::io::Result<Vec<std::path::PathBuf>> {
    let mut result = Vec::new();
    if path.exists() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                result.extend(files_under(&entry.path())?);
            } else {
                result.push(entry.path());
            }
        }
    }
    Ok(result)
}
fn batch(start: i64, rows: usize, payload: usize, entities: usize) -> TestResult<RecordBatch> {
    let keys = (0..rows).map(|row| start + row as i64).collect::<Vec<_>>();
    let values = keys.iter().map(|key| Some(*key * 10)).collect::<Vec<_>>();
    let wide = vec![42_u8; payload];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(Int64Array::from(values)),
        Arc::new(BinaryArray::from(vec![wide.as_slice(); rows])),
    ];
    let mut fields = vec![
        Field::new("idx", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
        Field::new("wide", DataType::Binary, true),
    ];
    if entities > 0 {
        fields.push(Field::new("entity", DataType::Utf8, true));
        arrays.push(Arc::new(StringArray::from(
            (0..rows)
                .map(|row| format!("E{}", row % entities))
                .collect::<Vec<_>>(),
        )));
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}
fn read_all(location: &TableLocation, path: &str) -> TestResult<RecordBatch> {
    let StorageLocation::Local(root) = location.as_ref();
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(root.join(path))?)?;
    let schema = reader.schema().clone();
    let batches = reader
        .with_batch_size(127)
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(concat_batches(&schema, &batches)?)
}

#[tokio::test]
async fn rewrites_only_affected_segments_preserving_values_layout_and_order() -> TestResult {
    for entities in [0, 1, 3] {
        let original = batch(0, 600, 4096, entities)?;
        let mut fixture = Fixture::empty(
            original.schema(),
            if entities == 0 { &[] } else { &["entity"] },
        )?;
        for n in 0..3 {
            let data = batch(n * 1000, 600, 4096, entities)?;
            fixture.add(data.schema(), [data]).await?;
        }
        let mut input =
            original
                .slice(0, 1)
                .project(if entities == 0 { &[0, 1] } else { &[0, 1, 3] })?;
        let mut arrays = input.columns().to_vec();
        arrays[1] = Arc::new(Int64Array::from(vec![None]));
        input = RecordBatch::try_new(input.schema(), arrays)?;
        let equal = batch(2000, 600, 4096, entities)?
            .slice(599, 1)
            .project(if entities == 0 { &[0, 1] } else { &[0, 1, 3] })?;
        let input = concat_batches(&input.schema(), [&equal, &input])?;
        let prepared = fixture.prepare(input, &["value"]).await?;
        let before = fixture.originals()?;
        let staged = stage_update_replacements(&fixture.location, &fixture.state, prepared).await?;
        assert_eq!(staged.version, 17);
        assert_eq!(staged.metrics.rows_updated, 2);
        assert_eq!(staged.replacements.len(), 2);
        assert_eq!(staged.metrics.rows_rewritten, 1200);
        assert_eq!(staged.owned_paths().count(), 4);
        for replacement in &staged.replacements {
            assert_eq!(
                replacement.source.entity_layout,
                replacement.replacement.entity_layout
            );
            let actual = read_all(&fixture.location, &replacement.replacement.path)?;
            let old = read_all(&fixture.location, &replacement.source.path)?;
            let mut expected = old.columns().to_vec();
            if replacement.source.path.ends_with("source-0.parquet") {
                let values = (0..600)
                    .map(|n| if n == 0 { None } else { Some(n * 10) })
                    .collect::<Vec<_>>();
                expected[1] = Arc::new(Int64Array::from(values));
            }
            assert_eq!(actual, RecordBatch::try_new(old.schema(), expected)?);
        }
        assert_eq!(fixture.originals()?, before);
        staged.close().await?;
        fixture.no_outputs()?;
    }
    Ok(())
}

#[tokio::test]
async fn historical_nullable_nested_columns_are_filled_and_replaced_whole() -> TestResult {
    let old = batch(0, 3, 8, 0)?;
    let child = Arc::new(Field::new("child", DataType::Int64, false));
    let nested: ArrayRef = Arc::new(StructArray::new(
        vec![child].into(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        None,
    ));
    let mut fields = old.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "nested",
        nested.data_type().clone(),
        true,
    )));
    let mut columns = old.columns().to_vec();
    columns.push(nested.clone());
    let current = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    let mut fixture = Fixture::empty(current.schema(), &[])?;
    fixture.add(old.schema(), [old.clone()]).await?;
    let input = current.slice(1, 1).project(&[3, 0])?;
    let prepared = fixture.prepare(input.clone(), &["nested"]).await?;
    assert!(
        stage_update_replacements(&fixture.location, &fixture.state, prepared)
            .await
            .is_err()
    );
    fixture.no_outputs()?;
    fixture
        .state
        .table_meta
        .required_reader_features
        .insert(SCHEMA_ADD_COLUMNS_FEATURE.into());
    fixture
        .state
        .table_meta
        .required_writer_features
        .insert(SCHEMA_ADD_COLUMNS_FEATURE.into());
    let prepared = fixture.prepare(input, &["nested"]).await?;
    let staged = stage_update_replacements(&fixture.location, &fixture.state, prepared).await?;
    let actual = read_all(&fixture.location, &staged.replacements[0].replacement.path)?;
    assert_eq!(actual.schema(), current.schema());
    for n in 0..3 {
        assert_eq!(actual.column(n), old.column(n));
    }
    assert!(actual.column(3).is_null(0));
    assert!(actual.column(3).is_null(2));
    assert_eq!(
        actual.column(3).slice(1, 1).to_data(),
        nested.slice(1, 1).to_data()
    );
    drop(staged);
    fixture.no_outputs()?;
    Ok(())
}

#[tokio::test]
async fn empty_updates_create_nothing_and_preservation_transfers_ownership() -> TestResult {
    let data = batch(0, 3, 8, 0)?;
    let mut fixture = Fixture::empty(data.schema(), &[])?;
    fixture.add(data.schema(), [data.clone()]).await?;
    let empty = fixture
        .prepare(data.slice(0, 0).project(&[0, 1])?, &["value"])
        .await?;
    let staged = stage_update_replacements(&fixture.location, &fixture.state, empty).await?;
    assert!(staged.replacements.is_empty());
    assert_eq!(staged.owned_paths().count(), 0);
    drop(staged);
    fixture.no_outputs()?;
    let prepared = fixture
        .prepare(data.slice(1, 1).project(&[0, 1])?, &["value"])
        .await?;
    let mut staged = stage_update_replacements(&fixture.location, &fixture.state, prepared).await?;
    let paths = staged.owned_paths().map(str::to_owned).collect::<Vec<_>>();
    staged.preserve();
    drop(staged);
    for path in &paths {
        assert!(fixture.dir.path().join(path).exists());
    }
    Ok(())
}

#[tokio::test]
async fn wrong_snapshot_mapping_and_changed_source_keys_fail_without_publication() -> TestResult {
    for mutation in 0..3 {
        let data = batch(0, 3, 8, 0)?;
        let mut fixture = Fixture::empty(data.schema(), &[])?;
        fixture.add(data.schema(), [data.clone()]).await?;
        fixture
            .state
            .segments
            .values_mut()
            .next()
            .ok_or("segment")?
            .file_size = None;
        let mut prepared = fixture
            .prepare(data.slice(1, 1).project(&[0, 1])?, &["value"])
            .await?;
        match mutation {
            0 => prepared.version += 1,
            1 => prepared.destination_indices[0] = 0,
            _ => {
                let reordered = concat_batches(
                    &data.schema(),
                    [data.slice(1, 1), data.slice(0, 1), data.slice(2, 1)].iter(),
                )?;
                let mut writer = ArrowWriter::try_new(
                    File::create(fixture.dir.path().join("data/source-0.parquet"))?,
                    data.schema(),
                    None,
                )?;
                writer.write(&reordered)?;
                writer.close()?;
            }
        }
        let before = fixture.originals()?;
        assert!(
            stage_update_replacements(&fixture.location, &fixture.state, prepared)
                .await
                .is_err()
        );
        assert_eq!(before, fixture.originals()?);
        fixture.no_outputs()?;
    }
    Ok(())
}

#[tokio::test]
async fn late_verification_and_output_failures_clean_all_owned_artifacts() -> TestResult {
    for failure in 0..4 {
        let data = batch(0, 3, 8, 0)?;
        let second = batch(10, 3, 8, 0)?;
        let mut fixture = Fixture::empty(data.schema(), &[])?;
        fixture.add(data.schema(), [data.clone()]).await?;
        fixture.add(second.schema(), [second.clone()]).await?;
        let input = concat_batches(&data.schema(), [&data, &second])?.project(&[0, 1])?;
        let prepared = fixture.prepare(input, &["value"]).await?;
        match failure {
            0 => fs::write(
                fixture.dir.path().join("_coverage/segments/source-1.roar"),
                b"damaged",
            )?,
            1 => storage::inject_output_finish_failure(
                fixture.dir.path().join(UPDATE_REWRITE_DATA_DIR),
            ),
            2 => storage::inject_output_write_failure(
                fixture.dir.path().join(UPDATE_REWRITE_DATA_DIR),
                1,
            ),
            _ => {
                storage::inject_output_finish_failure(fixture.dir.path().join("_coverage/segments"))
            }
        }
        let before = fixture.originals()?;
        assert!(
            stage_update_replacements(&fixture.location, &fixture.state, prepared)
                .await
                .is_err()
        );
        assert_eq!(fixture.originals()?, before);
        fixture.no_outputs()?;
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_and_explicit_cleanup_failures_keep_ownership_scoped() -> TestResult {
    let data = batch(0, 2048, 64, 0)?;
    let mut fixture = Fixture::empty(data.schema(), &[])?;
    fixture.add(data.schema(), [data.clone()]).await?;
    let prepared = fixture.prepare(data.project(&[0, 1])?, &["value"]).await?;
    let before = fixture.originals()?;
    let mut future = Box::pin(stage_update_replacements(
        &fixture.location,
        &fixture.state,
        prepared,
    ));
    loop {
        tokio::select! {
            result = &mut future => { result?; return Err("rewrite completed before cancellation".into()); },
            () = tokio::task::yield_now() => {
                if !files_under(&fixture.dir.path().join(UPDATE_REWRITE_DATA_DIR))?.is_empty() { break; }
            }
        }
    }
    drop(future);
    fixture.no_outputs()?;
    assert_eq!(fixture.originals()?, before);
    let prepared = fixture
        .prepare(data.slice(0, 1).project(&[0, 1])?, &["value"])
        .await?;
    let staged = stage_update_replacements(&fixture.location, &fixture.state, prepared).await?;
    let path = staged.owned_paths().next().ok_or("owned file")?.to_owned();
    storage::inject_cleanup_failure(fixture.dir.path().join(&path));
    assert!(
        matches!(staged.close().await,Err(RewriteError::Cleanup{cleanup_errors,..}) if cleanup_errors.len()==1)
    );
    assert_eq!(fixture.originals()?, before);
    Ok(())
}

#[tokio::test]
async fn unsigned_and_timestamp_rewrites_preserve_exact_raw_keys() -> TestResult {
    let keys: Vec<ArrayRef> = vec![
        Arc::new(arrow::array::UInt64Array::from(vec![
            u64::MAX - 2,
            u64::MAX - 1,
            u64::MAX,
        ])),
        Arc::new(
            arrow::array::TimestampNanosecondArray::from(vec![
                1_000_000_001,
                2_000_000_002,
                3_000_000_003,
            ])
            .with_timezone("UTC"),
        ),
    ];
    for key in keys {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("idx", key.data_type().clone(), true),
                Field::new("value", DataType::Int64, true),
            ])),
            vec![key, Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let mut fixture = Fixture::empty(data.schema(), &[])?;
        fixture.add(data.schema(), [data.clone()]).await?;
        let mut columns = data.slice(1, 1).columns().to_vec();
        columns[1] = Arc::new(Int64Array::from(vec![None]));
        let prepared = fixture
            .prepare(RecordBatch::try_new(data.schema(), columns)?, &["value"])
            .await?;
        let staged = stage_update_replacements(&fixture.location, &fixture.state, prepared).await?;
        let actual = read_all(&fixture.location, &staged.replacements[0].replacement.path)?;
        assert_eq!(actual.column(0), data.column(0));
        assert!(actual.column(1).is_null(1));
        staged.close().await?;
        fixture.no_outputs()?;
    }
    Ok(())
}

#[tokio::test]
async fn output_verification_rejects_reordered_keys_with_unchanged_coverage() -> TestResult {
    let data = batch(0, 3, 8, 0)?;
    let mut fixture = Fixture::empty(data.schema(), &[])?;
    fixture.add(data.schema(), [data.clone()]).await?;
    let mut expected = blake3::Hasher::new();
    project_and_hash_keys(&data, &[0], &mut expected)?;
    let reordered = concat_batches(&data.schema(), [data.slice(1, 2), data.slice(0, 1)].iter())?;
    fixture.add(reordered.schema(), [reordered]).await?;
    let source = fixture
        .state
        .segments
        .get("data/source-0.parquet")
        .ok_or("source")?;
    let builder = reader_builder(
        &fixture.location,
        "data/source-1.parquet",
        Arc::new(AtomicU64::new(0)),
    )?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), [0]);
    let reader = builder.with_projection(mask).build()?;
    let TableKind::TimeSeries(index) = &fixture.state.table_meta.kind else {
        unreachable!()
    };
    assert!(
        matches!(verify_replacement_keys_and_coverage(reader,&fixture.location,source,index,expected.finalize()).await,
        Err(RewriteError::Invalid {reason}) if reason.contains("exact keys"))
    );
    Ok(())
}

#[tokio::test]
async fn late_failure_preserves_primary_error_and_cleanup_error() -> TestResult {
    let data = batch(0, 512, 8, 0)?;
    let second = batch(1000, 512, 8, 0)?;
    let mut fixture = Fixture::empty(data.schema(), &[])?;
    fixture.add(data.schema(), [data.clone()]).await?;
    fixture.add(second.schema(), [second.clone()]).await?;
    let prepared = fixture
        .prepare(
            concat_batches(&data.schema(), [&data, &second])?.project(&[0, 1])?,
            &["value"],
        )
        .await?;
    fs::write(
        fixture.dir.path().join("_coverage/segments/source-1.roar"),
        b"broken",
    )?;
    let before = fixture.originals()?;
    let mut future = Box::pin(stage_update_replacements(
        &fixture.location,
        &fixture.state,
        prepared,
    ));
    loop {
        tokio::select! {
            result = &mut future => { result?; return Err("rewrite finished before injection".into()); },
            () = tokio::task::yield_now() => {
                let paths = files_under(&fixture.dir.path().join(UPDATE_REWRITE_DATA_DIR))?;
                if let Some(path) = paths.first() { storage::inject_cleanup_failure(path.clone()); break; }
            }
        }
    }
    match future.await {
        Err(RewriteError::CleanupAfterFailure { source, cleanup }) => {
            assert!(matches!(*source, RewriteError::Sidecar { .. }));
            assert!(
                matches!(*cleanup,RewriteError::Cleanup{cleanup_errors} if cleanup_errors.len()==1)
            );
        }
        _ => return Err("expected both primary and cleanup errors".into()),
    }
    assert_eq!(before, fixture.originals()?);
    Ok(())
}

#[tokio::test]
async fn malformed_payload_metadata_fails_before_rewrite_decoding() -> TestResult {
    use parquet::file::metadata::ParquetMetaDataWriter;
    use std::io::{Seek, SeekFrom, Write};
    for mutation in 0..3 {
        let data = batch(0, 3, 8, 0)?;
        let mut fixture = Fixture::empty(data.schema(), &[])?;
        fixture.add(data.schema(), [data.clone()]).await?;
        let prepared = fixture
            .prepare(data.slice(0, 1).project(&[0, 1])?, &["value"])
            .await?;
        let path = fixture.dir.path().join("data/source-0.parquet");
        if mutation == 2 {
            let mut file = fs::OpenOptions::new().write(true).open(&path)?;
            file.seek(SeekFrom::End(-8))?;
            file.write_all(&((MAX_FOOTER_BYTES + 1) as u32).to_le_bytes())?;
        } else {
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
            let metadata = builder.metadata().as_ref().clone();
            drop(builder);
            let mut columns = metadata.row_group(0).columns().to_vec();
            let column = columns[2].clone().into_builder();
            columns[2] = if mutation == 0 {
                column.set_total_compressed_size(-1).build()?
            } else {
                column
                    .set_total_uncompressed_size(MAX_ROW_GROUP_BYTES as i64 + 1)
                    .build()?
            };
            let group = metadata
                .row_group(0)
                .clone()
                .into_builder()
                .set_column_metadata(columns)
                .build()?;
            let metadata = metadata.into_builder().set_row_groups(vec![group]).build();
            ParquetMetaDataWriter::new(fs::OpenOptions::new().append(true).open(&path)?, &metadata)
                .finish()?;
        }
        let before = fixture.originals()?;
        let error = stage_update_replacements(&fixture.location, &fixture.state, prepared)
            .await
            .err()
            .ok_or("accepted invalid payload layout")?;
        if mutation == 0 {
            assert!(matches!(error, RewriteError::Invalid { .. }));
        } else {
            assert!(matches!(error, RewriteError::Resource { .. }));
        }
        assert_eq!(before, fixture.originals()?);
        fixture.no_outputs()?;
    }
    Ok(())
}

/// Streaming fixtures with distinct incompressible wide values, not a repeated
/// constant that disappears into one dictionary entry. Run outside Cargo for RSS.
#[tokio::test]
#[ignore = "native rewrite benchmark; use bench_update_prepare.ps1 -Stage rewrite"]
async fn rewrite_memory_benchmark() -> TestResult {
    fn parameter(name: &str, default: usize) -> TestResult<usize> {
        Ok(std::env::var(name)
            .ok()
            .map(|s| s.parse())
            .transpose()?
            .unwrap_or(default))
    }
    fn data(
        keys: Vec<i64>,
        width: usize,
        entities: usize,
        updated: bool,
    ) -> std::result::Result<RecordBatch, arrow::error::ArrowError> {
        let values: ArrayRef = Arc::new(Int64Array::from(
            keys.iter()
                .map(|key| if updated { -key - 1 } else { *key })
                .collect::<Vec<_>>(),
        ));
        let binary: ArrayRef = Arc::new(BinaryArray::from_iter_values(keys.iter().map(|key| {
            let mut seed = (*key as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15)
                ^ if updated { 0x1122334455667788 } else { 0 };
            let mut bytes = Vec::with_capacity(width);
            for _ in 0..width.div_ceil(8) {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                bytes.extend_from_slice(&seed.to_le_bytes());
            }
            bytes.truncate(width);
            bytes
        })));
        let mut fields = vec![
            Field::new("idx", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
            Field::new("wide", DataType::Binary, true),
        ];
        let mut arrays = vec![
            Arc::new(Int64Array::from(keys.clone())) as ArrayRef,
            values,
            binary,
        ];
        if entities > 0 {
            fields.push(Field::new("entity", DataType::Utf8, true));
            arrays.push(Arc::new(StringArray::from(
                keys.iter()
                    .map(|key| format!("E{}", *key as usize % entities))
                    .collect::<Vec<_>>(),
            )));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
    }
    let target_rows = parameter("TST_UPDATE_TARGET_ROWS", 16384)?;
    let updates = parameter("TST_UPDATE_ROWS", 4096)?;
    let width = parameter("TST_UPDATE_PAYLOAD_BYTES", 0)?;
    let entities = parameter("TST_UPDATE_ENTITIES", 0)?;
    let selected_wide = parameter("TST_UPDATE_SELECTED_WIDE", 0)? != 0;
    let concentrated = std::env::var("TST_UPDATE_MODE").as_deref() == Ok("concentrated");
    assert!(
        target_rows.is_power_of_two()
            && target_rows >= 4096
            && updates > 0
            && updates
                <= if concentrated {
                    target_rows / 4
                } else {
                    target_rows
                }
    );
    let empty = data(vec![], width, entities, false)?;
    let mut fixture = Fixture::empty(
        empty.schema(),
        if entities == 0 { &[] } else { &["entity"] },
    )?;
    for segment in 0..4 {
        let begin = segment * target_rows / 4;
        let end = begin + target_rows / 4;
        let batches = (begin..end).step_by(256).map(|start| {
            data(
                (start..(start + 256).min(end))
                    .map(|key| key as i64)
                    .collect(),
                width,
                entities,
                false,
            )
            .expect("benchmark schema")
        });
        fixture.add(empty.schema(), batches).await?;
    }
    let mut projection = vec![0, if selected_wide { 2 } else { 1 }];
    if entities > 0 {
        projection.push(3);
    }
    let source_schema = empty.project(&projection)?.schema();
    let logical_bytes =
        updates * (8 + if selected_wide { width } else { 8 } + if entities > 0 { 2 } else { 0 });
    println!(
        "UPDATE_BENCH_READY {}",
        serde_json::json!({"target_rows":target_rows,"updates":updates,"payload_bytes":width,"entities":entities,"selected_wide":selected_wide,"mode":if concentrated {"concentrated"} else {"shuffled"},"logical_source_bytes":logical_bytes})
    );
    let started = std::time::Instant::now();
    let input = (0..updates).step_by(256).map(|start| {
        let keys = (start..(start + 256).min(updates))
            .map(|i| {
                if concentrated {
                    i as i64
                } else {
                    ((i.wrapping_mul(104729)) & (target_rows - 1)) as i64
                }
            })
            .collect();
        data(keys, width, entities, true)?.project(&projection)
    });
    let prepared = super::super::update_prepare::prepare_updates(
        &fixture.location,
        &fixture.state,
        RecordBatchIterator::new(input, source_schema),
        &[if selected_wide {
            "wide".into()
        } else {
            "value".into()
        }],
    )
    .await?;
    let preparation_seconds = started.elapsed().as_secs_f64();
    let scratch_peak = prepared.metrics().peak_scratch_bytes;
    let preparation_key_bytes_read = prepared.metrics().key_discovery_bytes_read;
    let scratch_written = prepared.metrics().scratch_bytes_written;
    let scratch_read = prepared.metrics().scratch_bytes_read.clone();
    let rewrite_started = std::time::Instant::now();
    let staged = stage_update_replacements(&fixture.location, &fixture.state, prepared).await?;
    let rewrite_seconds = rewrite_started.elapsed().as_secs_f64();
    assert_eq!(staged.metrics.rows_updated, updates as u64);
    assert_eq!(staged.replacements.len(), if concentrated { 1 } else { 4 });
    println!(
        "UPDATE_BENCH_RESULT {}",
        serde_json::json!({"preparation_seconds":preparation_seconds,"rewrite_seconds":rewrite_seconds,"total_seconds":started.elapsed().as_secs_f64(),"metrics":staged.metrics,"peak_scratch_bytes":scratch_peak,"preparation_key_bytes_read":preparation_key_bytes_read,"scratch_bytes_written":scratch_written,"scratch_bytes_read":scratch_read.load(Ordering::Relaxed),"segments":staged.replacements.len()})
    );
    staged.close().await?;
    fixture.no_outputs()?;
    Ok(())
}
