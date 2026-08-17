//! Staging mixed-entity Parquet segments as single-entity replacements.

use std::{collections::HashSet, io::Write, path::Path};

use arrow::{array::BooleanBuilder, compute::filter_record_batch, error::ArrowError};
use futures::StreamExt;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::ParquetRecordBatchStreamBuilder,
    },
    errors::ParquetError,
    file::properties::WriterProperties,
};
use snafu::{Backtrace, Snafu};
use uuid::Uuid;

use crate::{
    coverage::{
        EntityCoverage, EntityIdentity,
        io::{CoverageError, read_entity_coverage_sidecar, write_coverage_sidecar_new_bytes},
        layout::{
            coverage_file_id_for_attempt, segment_coverage_key, segment_entity_coverage_id_v1,
        },
        serde::{EntityCoverageSerdeError, entity_coverage_to_bytes},
    },
    formats::parquet::{
        INSPECTION_BATCH_SIZE, SegmentCoverageError, compute_segment_entity_coverage,
        entity_coverage::{entity_arrays, entity_identity_at},
        logical_schema_from_parquet,
        segment_meta::segment_meta_from_parquet,
    },
    metadata::{
        logical_schema::LogicalSchema,
        schema_compat::ensure_index_matches_schema,
        segments::{FileFormat, SegmentEntityLayout, SegmentMeta},
        table_metadata::IndexSpec,
    },
    storage::{
        OutputSink, StorageError, TableLocation, normalize_relative_storage_path,
        open_new_output_sink, open_parquet_reader, remove_file_if_exists,
    },
    transaction_log::segments::SegmentError,
};

#[cfg(test)]
const MAX_OPEN_WRITERS: usize = 1;

/// One staged single-entity replacement and its verified exact coverage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedEntityReplacement {
    /// Complete identity materialized in this replacement.
    pub identity: EntityIdentity,
    /// Metadata derived from the completed staged Parquet file.
    pub meta: SegmentMeta,
    /// Exact coverage derived from the completed staged Parquet file.
    pub coverage: EntityCoverage,
}

/// Completed staged rewrite whose private objects are now caller-owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedEntityRewrite {
    /// Committed source segment path that was read without modification.
    pub source_path: String,
    /// Verified single-entity replacements in canonical identity order.
    pub replacements: Vec<StagedEntityReplacement>,
    /// Every staged data and sidecar path owned by the caller.
    pub staged_object_paths: Vec<String>,
    /// Physical rows read across all bounded source scans.
    pub rows_read: u64,
    /// Logical source rows written across all replacements.
    pub rows_written: u64,
    /// Complete identities materialized in canonical order.
    pub materialized_identities: Vec<EntityIdentity>,
}

/// Failure while staging a mixed segment rewrite.
#[derive(Debug, Snafu)]
pub enum EntityRewriteError {
    /// The committed source or table metadata violates the rewrite contract.
    #[snafu(display("Invalid mixed-segment rewrite input: {reason}"))]
    InvalidInput {
        /// Inconsistent input detail.
        reason: String,
    },

    /// A completed staged output violates a rewrite invariant.
    #[snafu(display("Invalid staged entity rewrite output: {reason}"))]
    InvalidOutput {
        /// Failed output invariant.
        reason: String,
    },

    /// Segment metadata or schema inspection failed.
    #[snafu(display("Failed to inspect Parquet segment: {source}"))]
    SegmentInspection {
        /// Existing Parquet inspection failure.
        source: SegmentError,
    },

    /// Exact entity coverage inspection failed.
    #[snafu(display("Failed to inspect exact entity coverage: {source}"))]
    CoverageInspection {
        /// Existing entity coverage inspection failure.
        source: SegmentCoverageError,
    },

    /// Coverage sidecar access failed.
    #[snafu(display("Failed to access entity coverage sidecar: {source}"))]
    CoverageSidecar {
        /// Existing coverage sidecar failure.
        source: CoverageError,
    },

    /// Entity coverage serialization failed.
    #[snafu(display("Failed to serialize staged entity coverage: {source}"))]
    CoverageSerialization {
        /// Existing coverage codec failure.
        source: EntityCoverageSerdeError,
    },

    /// Storage access failed.
    #[snafu(display("Staged entity rewrite storage failure: {source}"))]
    Storage {
        /// Existing storage failure.
        source: StorageError,
    },

    /// Parquet streaming or writing failed.
    #[snafu(display("Parquet rewrite failure at {path}: {source}"))]
    Parquet {
        /// Table-relative source or output path.
        path: String,
        /// Existing Parquet failure.
        source: ParquetError,
        /// Diagnostic backtrace.
        backtrace: Backtrace,
    },

    /// Filtering a complete record batch failed.
    #[snafu(display("Arrow row filtering failed for {path}: {source}"))]
    Arrow {
        /// Table-relative source path.
        path: String,
        /// Existing Arrow failure.
        source: ArrowError,
        /// Diagnostic backtrace.
        backtrace: Backtrace,
    },

    /// Rewrite failed and one or more private objects could not be removed.
    #[snafu(display(
        "Staged entity rewrite failed: {source}; cleanup also failed: {cleanup_errors:?}"
    ))]
    Cleanup {
        /// Primary rewrite failure.
        source: Box<EntityRewriteError>,
        /// Every private path whose cleanup failed.
        cleanup_errors: Vec<String>,
    },
}

struct SinkWriter(OutputSink);

impl Write for SinkWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.writer().write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.writer().flush()
    }
}

fn invalid_input(reason: impl Into<String>) -> EntityRewriteError {
    EntityRewriteError::InvalidInput {
        reason: reason.into(),
    }
}

fn invalid_output(reason: impl Into<String>) -> EntityRewriteError {
    EntityRewriteError::InvalidOutput {
        reason: reason.into(),
    }
}

fn canonical_path(path: &str, description: &str) -> Result<(), EntityRewriteError> {
    let (canonical, _) = normalize_relative_storage_path(Path::new(path))
        .map_err(|error| invalid_input(format!("invalid {description} path {path:?}: {error}")))?;
    if canonical != path {
        return Err(invalid_input(format!(
            "{description} path {path:?} is not canonical; expected {canonical:?}"
        )));
    }
    Ok(())
}

async fn cleanup_created(location: &TableLocation, created_paths: &[String]) -> Vec<String> {
    let mut errors = Vec::new();
    for path in created_paths.iter().rev() {
        if let Err(error) = remove_file_if_exists(location.as_ref(), Path::new(path)).await {
            errors.push(format!("{path}: {error}"));
        }
    }
    errors
}

async fn stage_identity_data(
    location: &TableLocation,
    source_path: &str,
    index: &IndexSpec,
    identity: &EntityIdentity,
    output_path: &str,
    created_paths: &mut Vec<String>,
) -> Result<(u64, u64), EntityRewriteError> {
    let source_rel = Path::new(source_path);
    let mut metadata_file = open_parquet_reader(location.as_ref(), source_rel)
        .await
        .map_err(|source| EntityRewriteError::Storage { source })?;
    let metadata =
        ArrowReaderMetadata::load_async(&mut metadata_file, ArrowReaderOptions::default())
            .await
            .map_err(|source| EntityRewriteError::Parquet {
                path: source_path.to_string(),
                source,
                backtrace: Backtrace::capture(),
            })?;
    let schema = metadata.schema().clone();
    drop(metadata_file);

    let source_file = open_parquet_reader(location.as_ref(), source_rel)
        .await
        .map_err(|source| EntityRewriteError::Storage { source })?;
    let mut reader = ParquetRecordBatchStreamBuilder::new_with_metadata(source_file, metadata)
        .with_batch_size(INSPECTION_BATCH_SIZE)
        .build()
        .map_err(|source| EntityRewriteError::Parquet {
            path: source_path.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })?;

    let sink = open_new_output_sink(location.as_ref(), Path::new(output_path))
        .await
        .map_err(|source| EntityRewriteError::Storage { source })?;
    created_paths.push(output_path.to_string());
    let mut writer = ArrowWriter::try_new(
        SinkWriter(sink),
        schema,
        Some(WriterProperties::builder().build()),
    )
    .map_err(|source| EntityRewriteError::Parquet {
        path: output_path.to_string(),
        source,
        backtrace: Backtrace::capture(),
    })?;

    let mut rows_read = 0u64;
    let mut rows_written = 0u64;
    while let Some(batch) = reader.next().await {
        let batch = batch.map_err(|source| EntityRewriteError::Parquet {
            path: source_path.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })?;
        let entities = entity_arrays(&batch, source_path, &index.entity_columns)
            .map_err(|source| EntityRewriteError::CoverageInspection { source })?;
        let mut mask = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            mask.append_value(
                entity_identity_at(&entities, row, source_path)
                    .map_err(|source| EntityRewriteError::CoverageInspection { source })?
                    == *identity,
            );
        }
        rows_read = rows_read
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| invalid_output("rows-read counter overflow"))?;
        let filtered = filter_record_batch(&batch, &mask.finish()).map_err(|source| {
            EntityRewriteError::Arrow {
                path: source_path.to_string(),
                source,
                backtrace: Backtrace::capture(),
            }
        })?;
        if filtered.num_rows() == 0 {
            continue;
        }
        rows_written = rows_written
            .checked_add(filtered.num_rows() as u64)
            .ok_or_else(|| invalid_output("rows-written counter overflow"))?;
        writer
            .write(&filtered)
            .map_err(|source| EntityRewriteError::Parquet {
                path: output_path.to_string(),
                source,
                backtrace: Backtrace::capture(),
            })?;
    }

    let sink = writer
        .into_inner()
        .map_err(|source| EntityRewriteError::Parquet {
            path: output_path.to_string(),
            source,
            backtrace: Backtrace::capture(),
        })?
        .0;
    sink.finish()
        .await
        .map_err(|source| EntityRewriteError::Storage { source })?;
    Ok((rows_read, rows_written))
}

async fn validate_source(
    location: &TableLocation,
    table_schema: &LogicalSchema,
    index: &IndexSpec,
    source: &SegmentMeta,
) -> Result<EntityCoverage, EntityRewriteError> {
    index
        .validate()
        .map_err(|error| invalid_input(format!("invalid ordered index: {error}")))?;
    ensure_index_matches_schema(table_schema, index)
        .map_err(|error| invalid_input(format!("ordered index does not match schema: {error}")))?;
    if index.entity_columns.is_empty() {
        return Err(invalid_input("table has no entity columns"));
    }
    if source.format != FileFormat::Parquet {
        return Err(invalid_input("source is not Parquet"));
    }
    if source.entity_layout != SegmentEntityLayout::Mixed {
        return Err(invalid_input(format!(
            "source {} is not classified as Mixed",
            source.path
        )));
    }
    canonical_path(&source.path, "source segment")?;
    source
        .validate_bounds(&index.kind)
        .map_err(|error| invalid_input(error.to_string()))?;
    let coverage_path = source
        .coverage_path
        .as_deref()
        .ok_or_else(|| invalid_input("source has no committed entity-coverage sidecar"))?;
    canonical_path(coverage_path, "source coverage")?;

    let source_schema = logical_schema_from_parquet(location, Path::new(&source.path))
        .await
        .map_err(|source| EntityRewriteError::SegmentInspection { source })?;
    if source_schema != *table_schema {
        return Err(invalid_input(format!(
            "source schema does not exactly match the committed table schema: source {source_schema:?}, table {table_schema:?}"
        )));
    }

    let (actual_meta, _) = segment_meta_from_parquet(location, Path::new(&source.path), index)
        .await
        .map_err(|source| EntityRewriteError::SegmentInspection { source })?;
    let file_size_matches = source
        .file_size
        .is_none_or(|expected| actual_meta.file_size == Some(expected));
    if actual_meta.index_min != source.index_min
        || actual_meta.index_max != source.index_max
        || actual_meta.row_count != source.row_count
        || !file_size_matches
    {
        return Err(invalid_input(format!(
            "source metadata does not match the committed Parquet file at {}",
            source.path
        )));
    }

    let committed_coverage = read_entity_coverage_sidecar(location, Path::new(coverage_path))
        .await
        .map_err(|source| EntityRewriteError::CoverageSidecar { source })?;
    if committed_coverage.identity_count() < 2 {
        return Err(invalid_input(
            "Mixed source coverage must contain at least two identities",
        ));
    }
    for (identity, coverage) in committed_coverage.iter() {
        if identity.components().len() != index.entity_columns.len() {
            return Err(invalid_input(format!(
                "source identity {identity:?} has {} components, expected {}",
                identity.components().len(),
                index.entity_columns.len()
            )));
        }
        if coverage.is_empty() {
            return Err(invalid_input(format!(
                "source identity {identity:?} has no covered ordered-index bucket"
            )));
        }
    }

    let actual_coverage = compute_segment_entity_coverage(location, Path::new(&source.path), index)
        .await
        .map_err(|source| EntityRewriteError::CoverageInspection { source })?;
    if actual_coverage != committed_coverage {
        return Err(invalid_input(
            "committed source coverage does not match the source Parquet rows",
        ));
    }
    Ok(committed_coverage)
}

async fn rewrite_inner(
    location: &TableLocation,
    table_schema: &LogicalSchema,
    index: &IndexSpec,
    source: &SegmentMeta,
    created_paths: &mut Vec<String>,
) -> Result<StagedEntityRewrite, EntityRewriteError> {
    let source_coverage = validate_source(location, table_schema, index, source).await?;
    let attempt_id = Uuid::new_v4();
    let mut replacements = Vec::with_capacity(source_coverage.identity_count());
    let mut materialized_identities = Vec::with_capacity(source_coverage.identity_count());
    let mut output_coverage = EntityCoverage::empty();
    let mut rows_read = 0u64;
    let mut rows_written = 0u64;

    // ponytail: one writer and one source scan per identity bound handles and
    // memory; batch identities only if repeated scan cost becomes material.
    for (ordinal, (identity, expected_coverage)) in source_coverage.iter().enumerate() {
        let data_path = format!("data/_staged/entity-rewrite/{attempt_id}/{ordinal:010}.parquet");
        let (identity_rows_read, identity_rows_written) = stage_identity_data(
            location,
            &source.path,
            index,
            identity,
            &data_path,
            created_paths,
        )
        .await?;
        rows_read = rows_read
            .checked_add(identity_rows_read)
            .ok_or_else(|| invalid_output("rows-read counter overflow"))?;
        rows_written = rows_written
            .checked_add(identity_rows_written)
            .ok_or_else(|| invalid_output("rows-written counter overflow"))?;

        let output_schema = logical_schema_from_parquet(location, Path::new(&data_path))
            .await
            .map_err(|source| EntityRewriteError::SegmentInspection { source })?;
        if output_schema != *table_schema {
            return Err(invalid_output(format!(
                "replacement {data_path} changed the committed schema"
            )));
        }
        let (mut meta, _) = segment_meta_from_parquet(location, Path::new(&data_path), index)
            .await
            .map_err(|source| EntityRewriteError::SegmentInspection { source })?;
        if meta.row_count != identity_rows_written || meta.row_count == 0 {
            return Err(invalid_output(format!(
                "replacement {data_path} row count {} does not match written row count {identity_rows_written}",
                meta.row_count
            )));
        }

        let coverage = compute_segment_entity_coverage(location, Path::new(&data_path), index)
            .await
            .map_err(|source| EntityRewriteError::CoverageInspection { source })?;
        let mut expected = EntityCoverage::empty();
        expected.union_coverage(identity.clone(), expected_coverage.clone());
        if coverage != expected {
            return Err(invalid_output(format!(
                "replacement {data_path} coverage does not match identity {identity:?}"
            )));
        }
        if output_coverage.intersection_cardinality(&coverage) != 0 {
            return Err(invalid_output(format!(
                "replacement {data_path} overlaps an earlier replacement"
            )));
        }

        let coverage_bytes = entity_coverage_to_bytes(&coverage)
            .map_err(|source| EntityRewriteError::CoverageSerialization { source })?;
        let coverage_id = coverage_file_id_for_attempt(
            &segment_entity_coverage_id_v1(index, &coverage_bytes),
            &attempt_id,
        );
        let coverage_path = segment_coverage_key(&coverage_id)
            .map_err(|error| invalid_output(error.to_string()))?;
        let sidecar_write =
            write_coverage_sidecar_new_bytes(location, Path::new(&coverage_path), &coverage_bytes)
                .await;
        if let Err(source) = sidecar_write {
            if matches!(
                &source,
                CoverageError::Storage {
                    source: StorageError::CleanupFailed { .. }
                }
            ) {
                created_paths.push(coverage_path);
            }
            return Err(EntityRewriteError::CoverageSidecar { source });
        }
        created_paths.push(coverage_path.clone());
        let persisted_coverage = read_entity_coverage_sidecar(location, Path::new(&coverage_path))
            .await
            .map_err(|source| EntityRewriteError::CoverageSidecar { source })?;
        if persisted_coverage != coverage {
            return Err(invalid_output(format!(
                "replacement sidecar {coverage_path} does not match derived coverage"
            )));
        }

        meta.entity_layout = SegmentEntityLayout::Single(identity.clone());
        meta.coverage_path = Some(coverage_path);
        output_coverage.union_inplace(&coverage);
        materialized_identities.push(identity.clone());
        replacements.push(StagedEntityReplacement {
            identity: identity.clone(),
            meta,
            coverage,
        });
    }

    if replacements.len() != source_coverage.identity_count() {
        return Err(invalid_output(format!(
            "materialized {} outputs for {} source identities",
            replacements.len(),
            source_coverage.identity_count()
        )));
    }
    if rows_written != source.row_count {
        return Err(invalid_output(format!(
            "wrote {rows_written} rows from a source containing {} rows",
            source.row_count
        )));
    }
    if output_coverage != source_coverage {
        return Err(invalid_output(
            "replacement coverage union does not equal committed source coverage",
        ));
    }
    let unique_paths = created_paths.iter().collect::<HashSet<_>>();
    if unique_paths.len() != created_paths.len() {
        return Err(invalid_output("staged object paths are not unique"));
    }

    Ok(StagedEntityRewrite {
        source_path: source.path.clone(),
        replacements,
        staged_object_paths: created_paths.clone(),
        rows_read,
        rows_written,
        materialized_identities,
    })
}

/// Rewrite one committed mixed Parquet segment into verified staged
/// single-entity replacements without changing table state.
///
/// This implementation intentionally opens one output writer at a time. Each
/// source scan streams complete record batches, so memory and open handles are
/// bounded independently of identity cardinality.
///
/// # Errors
///
/// Returns [`EntityRewriteError`] when input validation, reading, writing,
/// output verification, sidecar creation, or pre-return cleanup fails.
pub async fn rewrite_mixed_parquet_segment(
    location: &TableLocation,
    table_schema: &LogicalSchema,
    index: &IndexSpec,
    source: &SegmentMeta,
) -> Result<StagedEntityRewrite, EntityRewriteError> {
    let mut created_paths = Vec::new();
    match rewrite_inner(location, table_schema, index, source, &mut created_paths).await {
        Ok(rewrite) => Ok(rewrite),
        Err(source) => {
            let cleanup_errors = cleanup_created(location, &created_paths).await;
            if cleanup_errors.is_empty() {
                Err(source)
            } else {
                Err(EntityRewriteError::Cleanup {
                    source: Box::new(source),
                    cleanup_errors,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, fs::File};

    use arrow::{
        array::{Float64Array, StringArray, TimestampMillisecondArray},
        datatypes::TimeUnit,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::TempDir;

    use crate::{
        coverage::{io::write_coverage_sidecar_new_bytes, serde::entity_coverage_to_bytes},
        table::test_util::{make_table_meta_with_unit, write_arrow_parquet_with_unit},
        transaction_log::TableKind,
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn read_rows(path: &Path) -> TestResult<Vec<(i64, String, f64)>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?;
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch?;
            let timestamps = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("timestamp column");
            let symbols = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("symbol column");
            let prices = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("price column");
            for row in 0..batch.num_rows() {
                rows.push((
                    timestamps.value(row),
                    symbols.value(row).to_string(),
                    prices.value(row),
                ));
            }
        }
        Ok(rows)
    }

    #[tokio::test]
    async fn mixed_rewrite_stages_one_bounded_output_per_identity() -> TestResult {
        let temp = TempDir::new()?;
        let location = TableLocation::local(temp.path());
        let source_path = "data/mixed.parquet";
        let timestamps = [1_000, 2_000, 3_000, 4_000, 5_000, 6_000];
        let symbols = [
            "tenant-secret-b",
            "tenant-secret-a",
            "tenant-secret-b",
            "tenant-secret-c",
            "tenant-secret-a",
            "tenant-secret-c",
        ];
        let prices = [10.0, 20.0, 11.0, 30.0, 21.0, 31.0];
        write_arrow_parquet_with_unit(
            &temp.path().join(source_path),
            TimeUnit::Millisecond,
            &timestamps.map(Some),
            &symbols,
            &prices,
        )?;

        let table_meta = make_table_meta_with_unit(
            crate::metadata::logical_schema::LogicalTimestampUnit::Millis,
        );
        let TableKind::TimeSeries(index) = &table_meta.kind else {
            unreachable!("test metadata is time-series");
        };
        let table_schema = table_meta
            .logical_schema
            .as_ref()
            .expect("test table schema");
        let source_coverage =
            compute_segment_entity_coverage(&location, Path::new(source_path), index).await?;
        assert!(source_coverage.identity_count() > MAX_OPEN_WRITERS);
        let source_coverage_path = "_coverage/segments/source.roar";
        write_coverage_sidecar_new_bytes(
            &location,
            Path::new(source_coverage_path),
            &entity_coverage_to_bytes(&source_coverage)?,
        )
        .await?;
        let (mut source, _) =
            segment_meta_from_parquet(&location, Path::new(source_path), index).await?;
        source.entity_layout = SegmentEntityLayout::Mixed;
        source.coverage_path = Some(source_coverage_path.to_string());

        let rewrite =
            rewrite_mixed_parquet_segment(&location, table_schema, index, &source).await?;

        assert_eq!(rewrite.source_path, source_path);
        assert_eq!(rewrite.replacements.len(), 3);
        assert_eq!(rewrite.rows_read, source.row_count * 3);
        assert_eq!(rewrite.rows_written, source.row_count);
        assert_eq!(rewrite.staged_object_paths.len(), 6);
        assert!(temp.path().join(source_path).exists());
        assert!(temp.path().join(source_coverage_path).exists());

        let mut actual = BTreeMap::new();
        for replacement in &rewrite.replacements {
            assert_eq!(
                replacement.meta.entity_layout,
                SegmentEntityLayout::Single(replacement.identity.clone())
            );
            assert_eq!(replacement.coverage.identity_count(), 1);
            assert_eq!(
                read_entity_coverage_sidecar(
                    &location,
                    Path::new(
                        replacement
                            .meta
                            .coverage_path
                            .as_deref()
                            .expect("replacement coverage path")
                    )
                )
                .await?,
                replacement.coverage
            );
            assert!(rewrite.staged_object_paths.contains(&replacement.meta.path));
            for secret in ["tenant-secret-a", "tenant-secret-b", "tenant-secret-c"] {
                assert!(!replacement.meta.path.contains(secret));
            }
            actual.insert(
                replacement.identity.components()[0].clone(),
                read_rows(&temp.path().join(&replacement.meta.path))?,
            );
        }

        assert_eq!(
            actual["tenant-secret-a"],
            vec![
                (2_000, "tenant-secret-a".to_string(), 20.0),
                (5_000, "tenant-secret-a".to_string(), 21.0),
            ]
        );
        assert_eq!(
            actual["tenant-secret-b"],
            vec![
                (1_000, "tenant-secret-b".to_string(), 10.0),
                (3_000, "tenant-secret-b".to_string(), 11.0),
            ]
        );
        assert_eq!(
            actual["tenant-secret-c"],
            vec![
                (4_000, "tenant-secret-c".to_string(), 30.0),
                (6_000, "tenant-secret-c".to_string(), 31.0),
            ]
        );
        Ok(())
    }
}
