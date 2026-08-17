//! Coverage state helpers for `TimeSeriesTable`.
//!
//! This module reads table coverage bitmaps persisted alongside
//! the table. It is responsible for:
//! - Loading coverage snapshots via the transaction log pointer and enforcing
//!   bucket compatibility.
//! - Falling back to unioning segment coverage sidecars when the snapshot
//!   pointer is missing or unreadable (strict vs recovery modes).

use std::{collections::BTreeMap, path::Path};

use log::warn;

use crate::{
    coverage::{
        Coverage, EntityCoverage, EntityIdentity, EntityValue,
        io::{CoverageError, read_coverage_sidecar, read_entity_coverage_sidecar},
    },
    metadata::schema_compat::{ensure_entity_identity_matches_schema, require_table_schema},
    transaction_log::table_state::TableCoveragePointer,
};

use super::{TimeSeriesTable, error::TableError};

fn ensure_entity_coverage_identity_schema(
    coverage: &EntityCoverage,
    table: &TimeSeriesTable,
) -> Result<(), CoverageError> {
    let schema = require_table_schema(&table.state().table_meta)
        .map_err(|source| CoverageError::EntityIdentitySchema { source })?;
    for (identity, _) in coverage.iter() {
        ensure_entity_identity_matches_schema(schema, table.index_spec(), identity)
            .map_err(|source| CoverageError::EntityIdentitySchema { source })?;
    }
    Ok(())
}

impl TimeSeriesTable {
    fn ensure_global_coverage_query(&self) -> Result<(), TableError> {
        if self.index_spec().entity_columns.is_empty() {
            Ok(())
        } else {
            Err(TableError::EntityIdentityRequired {
                entity_columns: self.index_spec().entity_columns.clone(),
            })
        }
    }

    fn resolve_entity_identity(
        &self,
        components: &[(&str, EntityValue)],
    ) -> Result<EntityIdentity, TableError> {
        let entity_columns = &self.index_spec().entity_columns;
        if entity_columns.is_empty() {
            return Err(TableError::EntityIdentityNotConfigured);
        }

        let mut provided = BTreeMap::new();
        for (column, value) in components {
            if !entity_columns.iter().any(|expected| expected == *column) {
                return Err(TableError::UnexpectedEntityIdentityColumn {
                    column: (*column).to_string(),
                });
            }
            if provided.insert(*column, value).is_some() {
                return Err(TableError::DuplicateEntityIdentityColumn {
                    column: (*column).to_string(),
                });
            }
        }

        let ordered = entity_columns
            .iter()
            .map(|column| {
                provided
                    .get(column.as_str())
                    .map(|value| (**value).clone())
                    .ok_or_else(|| TableError::MissingEntityIdentityColumn {
                        column: column.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let identity = EntityIdentity::try_new(ordered)
            .map_err(|_| TableError::EntityIdentityNotConfigured)?;
        let schema = self.state().table_meta.logical_schema.as_ref().ok_or(
            TableError::MissingCanonicalSchema {
                version: self.state().version,
            },
        )?;
        ensure_entity_identity_matches_schema(schema, self.index_spec(), &identity)
            .map_err(|source| TableError::SchemaCompatibility { source })?;
        Ok(identity)
    }

    async fn read_validated_entity_coverage_sidecar(
        &self,
        path: &Path,
    ) -> Result<EntityCoverage, CoverageError> {
        let coverage = read_entity_coverage_sidecar(self.location(), path).await?;
        ensure_entity_coverage_identity_schema(&coverage, self)?;
        Ok(coverage)
    }

    /// Rebuild table coverage by reading each segment's coverage sidecar.
    ///
    /// This is used as a fallback when the table snapshot coverage is missing or
    /// unreadable. Requires every segment to have a `coverage_path`.
    pub(crate) async fn recover_table_coverage_from_segments(
        &self,
    ) -> Result<Coverage, TableError> {
        let mut acc = Coverage::empty();

        for seg in self.state().segments.values() {
            let path = seg.coverage_path.as_ref().ok_or_else(|| {
                TableError::ExistingSegmentMissingCoverage {
                    path: seg.path.clone(),
                }
            })?;

            let cov = read_coverage_sidecar(self.location(), Path::new(path))
                .await
                .map_err(|source| TableError::SegmentCoverageSidecarRead {
                    path: seg.path.clone(),
                    coverage_path: path.clone(),
                    source: Box::new(source),
                })?;

            // Prefer an in-place union to avoid repeated allocations.
            acc.union_inplace(&cov);
        }

        Ok(acc)
    }

    pub(crate) async fn recover_table_entity_coverage_from_segments(
        &self,
    ) -> Result<EntityCoverage, TableError> {
        let mut acc = EntityCoverage::empty();

        for seg in self.state().segments.values() {
            let path = seg.coverage_path.as_ref().ok_or_else(|| {
                TableError::ExistingSegmentMissingCoverage {
                    path: seg.path.clone(),
                }
            })?;

            let coverage = self
                .read_validated_entity_coverage_sidecar(Path::new(path))
                .await
                .map_err(|source| TableError::SegmentCoverageSidecarRead {
                    path: seg.path.clone(),
                    coverage_path: path.clone(),
                    source: Box::new(source),
                })?;
            acc.union_inplace(&coverage);
        }

        Ok(acc)
    }

    fn ensure_table_coverage_index_matches(
        &self,
        ptr: &TableCoveragePointer,
    ) -> Result<(), TableError> {
        let expected = self.index_spec().kind.clone();
        if ptr.index_kind != expected {
            return Err(TableError::TableCoverageIndexKindMismatch {
                expected,
                actual: ptr.index_kind.clone(),
                pointer_version: ptr.version,
            });
        }
        Ok(())
    }

    /// Load table coverage using the snapshot pointer only.
    ///
    /// - If there is no snapshot pointer:
    ///   - If table has zero segments: returns empty coverage.
    ///   - Else: returns MissingTableCoveragePointer (strict mode).
    /// - If snapshot exists but is missing/corrupt: returns the snapshot read error.
    pub async fn load_table_coverage_snapshot_only(&self) -> Result<Coverage, TableError> {
        match &self.state().table_coverage {
            None => {
                if self.state().segments.is_empty() {
                    return Ok(Coverage::empty());
                }
                Err(TableError::MissingTableCoveragePointer)
            }
            Some(ptr) => {
                self.ensure_table_coverage_index_matches(ptr)?;
                read_coverage_sidecar(self.location(), Path::new(&ptr.coverage_path))
                    .await
                    .map_err(|source| TableError::CoverageSidecar { source })
            }
        }
    }

    pub(crate) async fn load_table_entity_coverage_snapshot_only(
        &self,
    ) -> Result<EntityCoverage, TableError> {
        match &self.state().table_coverage {
            None => {
                if self.state().segments.is_empty() {
                    return Ok(EntityCoverage::empty());
                }
                Err(TableError::MissingTableCoveragePointer)
            }
            Some(ptr) => {
                self.ensure_table_coverage_index_matches(ptr)?;
                self.read_validated_entity_coverage_sidecar(Path::new(&ptr.coverage_path))
                    .await
                    .map_err(|source| TableError::CoverageSidecar { source })
            }
        }
    }

    /// Load table coverage for read paths (no writes).
    ///
    /// - If snapshot pointer is absent:
    ///   - If table has zero segments: returns empty coverage.
    ///   - Else: recovers by unioning segment sidecars.
    /// - If snapshot pointer exists but snapshot is missing/corrupt:
    ///   - Recovers by unioning segment sidecars.
    pub(crate) async fn load_table_snapshot_coverage_readonly(
        &self,
    ) -> Result<Coverage, TableError> {
        match &self.state().table_coverage {
            None => {
                if self.state().segments.is_empty() {
                    return Ok(Coverage::empty());
                }
                self.recover_table_coverage_from_segments().await
            }
            Some(ptr) => {
                self.ensure_table_coverage_index_matches(ptr)?;

                match read_coverage_sidecar(self.location(), Path::new(&ptr.coverage_path)).await {
                    Ok(cov) => Ok(cov),
                    Err(snapshot_err) => {
                        warn!(
                            "Failed to read table coverage snapshot at {} (version {}): {:?}. \
                             Attempting recovery from segment sidecars (readonly).",
                            ptr.coverage_path, ptr.version, snapshot_err
                        );
                        self.recover_table_coverage_from_segments().await
                    }
                }
            }
        }
    }

    pub(crate) async fn load_table_entity_snapshot_coverage_readonly(
        &self,
    ) -> Result<EntityCoverage, TableError> {
        match &self.state().table_coverage {
            None => {
                if self.state().segments.is_empty() {
                    return Ok(EntityCoverage::empty());
                }
                self.recover_table_entity_coverage_from_segments().await
            }
            Some(ptr) => {
                self.ensure_table_coverage_index_matches(ptr)?;
                match self.load_table_entity_coverage_snapshot_only().await {
                    Ok(coverage) => Ok(coverage),
                    Err(snapshot_err) => {
                        warn!(
                            "Failed to read entity coverage snapshot at {} (version {}): {:?}. \
                             Attempting recovery from segment sidecars (readonly).",
                            ptr.coverage_path, ptr.version, snapshot_err
                        );
                        self.recover_table_entity_coverage_from_segments().await
                    }
                }
            }
        }
    }
}
// Coverage query APIs for TimeSeriesTable.
//
// These APIs:
// - derive an inclusive bucket range from an ordered-index range (half-open [start, end))
// - load table coverage (readonly recovery)
// - reuse crate::coverage APIs (coverage_ratio, max_gap_len, last_window_at_or_before)
use std::ops::RangeInclusive;

use snafu::ResultExt;

use crate::{
    coverage::Bucket,
    coverage::bucket::{bucket_for_exclusive_end, bucket_range},
    metadata::table_metadata::{IndexValue, validate_index_range},
    table::error::{CoverageBucketSnafu, InvalidRangeSnafu},
};

impl TimeSeriesTable {
    fn bucket_range_for_index_range<S, E>(
        &self,
        start: S,
        end: E,
    ) -> Result<RangeInclusive<Bucket>, TableError>
    where
        S: Into<IndexValue>,
        E: Into<IndexValue>,
    {
        let start = start.into();
        let end = end.into();
        validate_index_range(&self.index_spec().kind, &start, &end).context(InvalidRangeSnafu)?;
        bucket_range(&self.index_spec().kind, &start, &end).context(CoverageBucketSnafu)
    }

    // ---- public query APIs ----

    /// Coverage ratio in [0.0, 1.0] for the half-open index range [start, end).
    ///
    /// This identity-free query is only valid for tables without configured
    /// entity columns. It uses the table-level coverage snapshot, with readonly
    /// recovery from segments if needed.
    ///
    /// # Errors
    /// Returns [`TableError::InvalidRange`] when the endpoints do not match the
    /// table index or `start >= end`, [`TableError::EntityIdentityRequired`]
    /// when the table has entity columns, and contextual coverage errors when
    /// the snapshot cannot be loaded or the range cannot be bucketed.
    ///
    /// # Examples
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_format::{storage::TableLocation, table::TimeSeriesTable};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_format::table::TableError> {
    /// let start = Utc.timestamp_opt(0, 0).single().unwrap();
    /// let end = Utc.timestamp_opt(120, 0).single().unwrap();
    /// let ratio = table.coverage_ratio_for_range(start, end).await?;
    /// # let _ = ratio;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn coverage_ratio_for_range<S, E>(&self, start: S, end: E) -> Result<f64, TableError>
    where
        S: Into<IndexValue>,
        E: Into<IndexValue>,
    {
        self.ensure_global_coverage_query()?;
        let range = self.bucket_range_for_index_range(start, end)?;
        let cov = self.load_table_snapshot_coverage_readonly().await?;
        Ok(cov.coverage_ratio(&range))
    }

    /// Coverage ratio in `[0.0, 1.0]` for one entity over `[start, end)`.
    ///
    /// Entity components are supplied by column name and canonicalized into the
    /// configured entity-column order. Coverage from other identities is never
    /// included. A complete identity not present in the table has zero coverage.
    ///
    /// # Errors
    /// Returns a typed entity identity error for missing, duplicate, unexpected,
    /// or unconfigured entity columns. Range and sidecar errors retain the same
    /// behavior as [`TimeSeriesTable::coverage_ratio_for_range`].
    ///
    /// # Examples
    /// If entities `A` and `B` both have data in the same bucket, their coverage
    /// is still queried independently:
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_format::{coverage::EntityValue, table::TimeSeriesTable};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_format::table::TableError> {
    /// let start = Utc.timestamp_opt(0, 0).single().unwrap();
    /// let end = Utc.timestamp_opt(120, 0).single().unwrap();
    /// let a = table
    ///     .coverage_ratio_for_entity_range(&[("symbol", EntityValue::from("A"))], start, end)
    ///     .await?;
    /// let b = table
    ///     .coverage_ratio_for_entity_range(&[("symbol", EntityValue::from("B"))], start, end)
    ///     .await?;
    /// # let _ = (a, b);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn coverage_ratio_for_entity_range<S, E>(
        &self,
        entity: &[(&str, EntityValue)],
        start: S,
        end: E,
    ) -> Result<f64, TableError>
    where
        S: Into<IndexValue>,
        E: Into<IndexValue>,
    {
        let identity = self.resolve_entity_identity(entity)?;
        let range = self.bucket_range_for_index_range(start, end)?;
        let coverage = self.load_table_entity_snapshot_coverage_readonly().await?;
        Ok(coverage
            .get(&identity)
            .map_or(0.0, |coverage| coverage.coverage_ratio(&range)))
    }

    /// Maximum contiguous missing run length in buckets for `[start, end)`.
    ///
    /// This identity-free query is only valid for tables without configured
    /// entity columns.
    ///
    /// # Errors
    /// Returns [`TableError::InvalidRange`] when the endpoints do not match the
    /// table index or `start >= end`, [`TableError::EntityIdentityRequired`]
    /// when the table has entity columns, and contextual coverage errors when
    /// the snapshot cannot be loaded or the range cannot be bucketed.
    ///
    /// # Examples
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_format::{storage::TableLocation, table::TimeSeriesTable};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_format::table::TableError> {
    /// let start = Utc.timestamp_opt(0, 0).single().unwrap();
    /// let end = Utc.timestamp_opt(180, 0).single().unwrap();
    /// let gap = table.max_gap_len_for_range(start, end).await?;
    /// # let _ = gap;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn max_gap_len_for_range<S, E>(&self, start: S, end: E) -> Result<u128, TableError>
    where
        S: Into<IndexValue>,
        E: Into<IndexValue>,
    {
        self.ensure_global_coverage_query()?;
        let range = self.bucket_range_for_index_range(start, end)?;
        let cov = self.load_table_snapshot_coverage_readonly().await?;
        Ok(cov.max_gap_len(&range))
    }

    /// Maximum contiguous missing run length for one entity over `[start, end)`.
    ///
    /// Entity components are supplied by column name and canonicalized into the
    /// configured entity-column order. Other entities never fill this entity's
    /// gaps. A complete identity not present in the table is missing for the
    /// entire requested range.
    ///
    /// # Errors
    /// Returns a typed entity identity error for missing, duplicate, unexpected,
    /// or unconfigured entity columns. It returns [`TableError::InvalidRange`]
    /// for invalid half-open range endpoints and contextual coverage errors when
    /// the snapshot cannot be loaded or the range cannot be bucketed.
    pub async fn max_gap_len_for_entity_range<S, E>(
        &self,
        entity: &[(&str, EntityValue)],
        start: S,
        end: E,
    ) -> Result<u128, TableError>
    where
        S: Into<IndexValue>,
        E: Into<IndexValue>,
    {
        let identity = self.resolve_entity_identity(entity)?;
        let range = self.bucket_range_for_index_range(start, end)?;
        let coverage = self.load_table_entity_snapshot_coverage_readonly().await?;
        Ok(coverage.get(&identity).map_or_else(
            || Coverage::range_cardinality(&range),
            |c| c.max_gap_len(&range),
        ))
    }

    /// Return the last fully covered contiguous window of `window_len_buckets`
    /// ending before the exclusive ordered-index endpoint.
    ///
    /// Notes:
    /// - This returns a bucket-id RangeInclusive in the 64-bit bucket domain.
    /// - Returns `None` when `window_len_buckets == 0` or when no fully covered window is found.
    /// - This identity-free query is only valid for tables without configured entity columns.
    ///
    /// # Errors
    /// Returns [`TableError::InvalidRange`] when `end` does not match the table
    /// index, [`TableError::EntityIdentityRequired`] when the table has entity
    /// columns, and contextual coverage errors when the endpoint cannot be
    /// bucketed or the snapshot cannot be loaded.
    ///
    /// # Examples
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_format::{storage::TableLocation, table::TimeSeriesTable};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_format::table::TableError> {
    /// let ts_end = Utc.timestamp_opt(360, 0).single().unwrap(); // end of bucket 5
    /// let window = table.last_fully_covered_window(ts_end, 2).await?;
    /// # let _ = window;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn last_fully_covered_window<E>(
        &self,
        end: E,
        window_len_buckets: u64,
    ) -> Result<Option<RangeInclusive<Bucket>>, TableError>
    where
        E: Into<IndexValue>,
    {
        self.ensure_global_coverage_query()?;
        let end = end.into();
        end.validate_kind(&self.index_spec().kind)
            .context(InvalidRangeSnafu)?;
        if window_len_buckets == 0 {
            return Ok(None);
        }

        let end_bucket =
            bucket_for_exclusive_end(&self.index_spec().kind, &end).context(CoverageBucketSnafu)?;
        let cov = self.load_table_snapshot_coverage_readonly().await?;
        Ok(cov.last_window_at_or_before(end_bucket, window_len_buckets))
    }

    /// Return one entity's last fully covered contiguous window ending before
    /// the exclusive ordered-index endpoint.
    ///
    /// Entity components are supplied by column name and canonicalized into the
    /// configured entity-column order. Other entities cannot contribute buckets
    /// to the window. A complete identity not present in the table returns
    /// `None`, as does a zero-length window.
    ///
    /// # Errors
    /// Returns a typed entity identity error for missing, duplicate, unexpected,
    /// or unconfigured entity columns. It returns [`TableError::InvalidRange`]
    /// when `end` does not match the table index and contextual coverage errors
    /// when the endpoint cannot be bucketed or the snapshot cannot be loaded.
    pub async fn last_fully_covered_window_for_entity<E>(
        &self,
        entity: &[(&str, EntityValue)],
        end: E,
        window_len_buckets: u64,
    ) -> Result<Option<RangeInclusive<Bucket>>, TableError>
    where
        E: Into<IndexValue>,
    {
        let identity = self.resolve_entity_identity(entity)?;
        let end = end.into();
        end.validate_kind(&self.index_spec().kind)
            .context(InvalidRangeSnafu)?;
        if window_len_buckets == 0 {
            return Ok(None);
        }

        let end_bucket =
            bucket_for_exclusive_end(&self.index_spec().kind, &end).context(CoverageBucketSnafu)?;
        let coverage = self.load_table_entity_snapshot_coverage_readonly().await?;
        Ok(coverage
            .get(&identity)
            .and_then(|coverage| coverage.last_window_at_or_before(end_bucket, window_len_buckets)))
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, path::Path};

    use super::*;
    use crate::{
        coverage::{
            Coverage, EntityCoverage, EntityIdentity, EntityValue,
            bucket::{BucketError, bucket_id},
            io::{write_coverage_sidecar_atomic, write_coverage_sidecar_new_bytes},
            serde::entity_coverage_to_bytes,
        },
        metadata::logical_schema::{LogicalDataType, LogicalField, LogicalSchema},
        metadata::schema_compat::SchemaCompatibilityError,
        metadata::table_metadata::{
            IndexKind, IndexSpec, IndexValueError, TableKind, TableMeta, TimeBucket,
        },
        storage::TableLocation,
        table::test_util::{
            TestResult, TestRow, make_basic_table_meta, make_int32_entity_table_meta, utc_datetime,
            write_int32_entity_parquet, write_test_parquet,
        },
    };
    use chrono::{DateTime, TimeZone, Utc};
    use tempfile::TempDir;

    type HelperResult<T> = Result<T, Box<dyn std::error::Error>>;

    fn ts_from_secs(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0)
            .single()
            .expect("valid timestamp")
    }

    async fn make_table() -> HelperResult<(TempDir, TimeSeriesTable)> {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut meta = make_basic_table_meta();
        let TableKind::TimeSeries(index) = &mut meta.kind else {
            unreachable!("test metadata is time series")
        };
        index.entity_columns.clear();
        let table = TimeSeriesTable::create(location, meta).await?;
        Ok((tmp, table))
    }

    async fn table_with_index_coverage(
        kind: IndexKind,
        coverage: Coverage,
    ) -> HelperResult<(TempDir, TimeSeriesTable)> {
        let tmp = TempDir::new()?;
        let mut table = TimeSeriesTable::create(
            TableLocation::local(tmp.path()),
            TableMeta::new_time_series(IndexSpec {
                column: "index".to_string(),
                entity_columns: Vec::new(),
                kind: kind.clone(),
            }),
        )
        .await?;
        let coverage_path = "_coverage/table/query-test.roar";
        write_coverage_sidecar_atomic(table.location(), Path::new(coverage_path), &coverage)
            .await?;
        let version = table.state().version;
        table.state_mut().table_coverage = Some(TableCoveragePointer {
            index_kind: kind,
            coverage_path: coverage_path.to_string(),
            version,
        });
        Ok((tmp, table))
    }

    async fn append_segment(
        table: &mut TimeSeriesTable,
        tmp: &TempDir,
        rel_path: &str,
        rows: &[TestRow],
    ) -> HelperResult<()> {
        let abs = tmp.path().join(rel_path);
        write_test_parquet(&abs, true, false, rows)?;
        table.append_parquet_segment(rel_path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn entity_sidecar_identity_schema_is_validated_for_snapshots_and_recovery() -> TestResult
    {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location.clone(), make_basic_table_meta()).await?;
        let mut wrong_arity = EntityCoverage::empty();
        wrong_arity.union_coverage(
            EntityIdentity::try_new(vec!["A".into(), "X".into()])?,
            Coverage::from_iter([0]),
        );
        let wrong_arity_bytes = entity_coverage_to_bytes(&wrong_arity)?;
        let snapshot_path = "_coverage/table/wrong-arity.roar";
        write_coverage_sidecar_new_bytes(&location, Path::new(snapshot_path), &wrong_arity_bytes)
            .await?;
        let version = table.state().version;
        let index_kind = table.index_spec().kind.clone();
        table.state_mut().table_coverage = Some(TableCoveragePointer {
            index_kind,
            coverage_path: snapshot_path.to_string(),
            version,
        });

        let snapshot_error = table
            .load_table_entity_coverage_snapshot_only()
            .await
            .expect_err("snapshot identity arity must match the table");
        assert!(matches!(
            snapshot_error,
            TableError::CoverageSidecar {
                source: CoverageError::EntityIdentitySchema {
                    source: SchemaCompatibilityError::EntityIdentityArityMismatch {
                        expected: 1,
                        actual: 2,
                    },
                },
            }
        ));

        let mut wrong_type = EntityCoverage::empty();
        wrong_type.union_coverage(
            EntityIdentity::try_new(vec![EntityValue::Int32(1)])?,
            Coverage::from_iter([0]),
        );
        let wrong_type_bytes = entity_coverage_to_bytes(&wrong_type)?;
        table.state_mut().table_coverage = None;
        let segment_path = "data/entity-arity.parquet";
        append_segment(
            &mut table,
            &tmp,
            segment_path,
            &[TestRow {
                ts_millis: 1_000,
                symbol: "A",
                price: 1.0,
            }],
        )
        .await?;
        let segment_coverage_path = table
            .state()
            .segments
            .get(segment_path)
            .and_then(|segment| segment.coverage_path.clone())
            .expect("segment coverage path");
        tokio::fs::write(tmp.path().join(&segment_coverage_path), &wrong_type_bytes).await?;

        let recovery_error = table
            .recover_table_entity_coverage_from_segments()
            .await
            .expect_err("segment identity type must match the table schema");
        assert!(matches!(
            recovery_error,
            TableError::SegmentCoverageSidecarRead {
                path,
                coverage_path,
                source,
            } if path == segment_path
                && coverage_path == segment_coverage_path
                && matches!(
                    &*source,
                    CoverageError::EntityIdentitySchema {
                        source: SchemaCompatibilityError::EntityIdentityTypeMismatch {
                            column,
                            expected: crate::metadata::logical_schema::LogicalDataType::Utf8,
                            actual: "int32",
                        },
                    }
                    if column == "symbol"
                )
        ));
        Ok(())
    }

    #[tokio::test]
    async fn entity_coverage_ratio_selects_only_the_requested_identity() -> TestResult {
        let tmp = TempDir::new()?;
        let location = TableLocation::local(tmp.path());
        let mut table = TimeSeriesTable::create(location, make_basic_table_meta()).await?;
        append_segment(
            &mut table,
            &tmp,
            "data/entity-ratio.parquet",
            &[
                TestRow {
                    ts_millis: 1_000,
                    symbol: "A",
                    price: 1.0,
                },
                TestRow {
                    ts_millis: 61_000,
                    symbol: "B",
                    price: 2.0,
                },
            ],
        )
        .await?;
        let state_before = table.state().clone();
        let start = ts_from_secs(0);
        let end = ts_from_secs(120);

        assert_eq!(
            table
                .coverage_ratio_for_entity_range(&[("symbol", EntityValue::from("A"))], start, end)
                .await?,
            0.5
        );
        assert_eq!(
            table
                .coverage_ratio_for_entity_range(&[("symbol", EntityValue::from("B"))], start, end)
                .await?,
            0.5
        );
        assert_eq!(
            table
                .coverage_ratio_for_entity_range(
                    &[("symbol", EntityValue::from("unseen"))],
                    start,
                    end,
                )
                .await?,
            0.0
        );
        assert!(matches!(
            table.coverage_ratio_for_range(start, end).await,
            Err(TableError::EntityIdentityRequired { entity_columns })
                if entity_columns == ["symbol"]
        ));
        assert_eq!(table.state(), &state_before);
        Ok(())
    }

    #[tokio::test]
    async fn numeric_entity_queries_require_the_exact_scalar_type() -> TestResult {
        let tmp = TempDir::new()?;
        let mut table = TimeSeriesTable::create(
            TableLocation::local(tmp.path()),
            make_int32_entity_table_meta(),
        )
        .await?;
        let path = "data/numeric-coverage.parquet";
        write_int32_entity_parquet(
            &tmp.path().join(path),
            &[1_000, 61_000],
            &[-1, i32::MAX],
            &[10.0, 20.0],
        )?;
        table.append_parquet_segment(path).await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(120);

        assert_eq!(
            table
                .coverage_ratio_for_entity_range(
                    &[("device_id", EntityValue::Int32(-1))],
                    start,
                    end,
                )
                .await?,
            0.5
        );
        assert_eq!(
            table
                .coverage_ratio_for_entity_range(
                    &[("device_id", EntityValue::Int32(42))],
                    start,
                    end,
                )
                .await?,
            0.0
        );
        assert!(matches!(
            table
                .coverage_ratio_for_entity_range(
                    &[("device_id", EntityValue::Int64(-1))],
                    start,
                    end,
                )
                .await,
            Err(TableError::SchemaCompatibility {
                source: SchemaCompatibilityError::EntityIdentityTypeMismatch {
                    column,
                    expected: LogicalDataType::Int32,
                    actual: "int64",
                },
            }) if column == "device_id"
        ));
        assert!(matches!(
            table
                .coverage_ratio_for_entity_range(
                    &[("device_id", EntityValue::from("-1"))],
                    start,
                    end,
                )
                .await,
            Err(TableError::SchemaCompatibility {
                source: SchemaCompatibilityError::EntityIdentityTypeMismatch {
                    column,
                    expected: LogicalDataType::Int32,
                    actual: "utf8",
                },
            }) if column == "device_id"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn entity_gap_and_window_queries_are_isolated_and_recover_readonly() -> TestResult {
        let tmp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(tmp.path()), make_basic_table_meta())
                .await?;
        append_segment(
            &mut table,
            &tmp,
            "data/entity-gaps.parquet",
            &[
                TestRow {
                    ts_millis: 1_000,
                    symbol: "A",
                    price: 1.0,
                },
                TestRow {
                    ts_millis: 181_000,
                    symbol: "A",
                    price: 2.0,
                },
                TestRow {
                    ts_millis: 1_000,
                    symbol: "B",
                    price: 3.0,
                },
                TestRow {
                    ts_millis: 61_000,
                    symbol: "B",
                    price: 4.0,
                },
                TestRow {
                    ts_millis: 121_000,
                    symbol: "B",
                    price: 5.0,
                },
            ],
        )
        .await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(240);

        assert_eq!(
            table
                .coverage_ratio_for_entity_range(&[("symbol", EntityValue::from("A"))], start, end)
                .await?,
            0.5
        );
        assert_eq!(
            table
                .coverage_ratio_for_entity_range(&[("symbol", EntityValue::from("B"))], start, end)
                .await?,
            0.75
        );
        assert_eq!(
            table
                .max_gap_len_for_entity_range(&[("symbol", EntityValue::from("A"))], start, end)
                .await?,
            2
        );
        assert_eq!(
            table
                .max_gap_len_for_entity_range(&[("symbol", EntityValue::from("B"))], start, end)
                .await?,
            1
        );
        assert_eq!(
            table
                .last_fully_covered_window_for_entity(
                    &[("symbol", EntityValue::from("A"))],
                    end,
                    2,
                )
                .await?,
            None
        );
        assert_eq!(
            table
                .last_fully_covered_window_for_entity(
                    &[("symbol", EntityValue::from("B"))],
                    end,
                    2,
                )
                .await?,
            Some(0x8000_0000_0000_0001..=0x8000_0000_0000_0002)
        );

        let snapshot_path = table
            .state()
            .table_coverage
            .as_ref()
            .expect("snapshot pointer")
            .coverage_path
            .clone();
        let state_before = table.state().clone();
        tokio::fs::remove_file(tmp.path().join(snapshot_path)).await?;

        assert_eq!(
            table
                .coverage_ratio_for_entity_range(&[("symbol", EntityValue::from("A"))], start, end)
                .await?,
            0.5
        );
        assert_eq!(
            table
                .coverage_ratio_for_entity_range(&[("symbol", EntityValue::from("B"))], start, end)
                .await?,
            0.75
        );
        assert_eq!(
            table
                .max_gap_len_for_entity_range(&[("symbol", EntityValue::from("A"))], start, end)
                .await?,
            2
        );
        assert_eq!(
            table
                .last_fully_covered_window_for_entity(
                    &[("symbol", EntityValue::from("B"))],
                    end,
                    2,
                )
                .await?,
            Some(0x8000_0000_0000_0001..=0x8000_0000_0000_0002)
        );
        assert_eq!(
            table
                .max_gap_len_for_entity_range(
                    &[("symbol", EntityValue::from("unseen"))],
                    start,
                    end,
                )
                .await?,
            4
        );
        assert_eq!(
            table
                .last_fully_covered_window_for_entity(
                    &[("symbol", EntityValue::from("unseen"))],
                    end,
                    1,
                )
                .await?,
            None
        );
        assert_eq!(
            table
                .last_fully_covered_window_for_entity(
                    &[("symbol", EntityValue::from("unseen"))],
                    end,
                    0,
                )
                .await?,
            None
        );

        assert!(matches!(
            table.coverage_ratio_for_range(start, end).await,
            Err(TableError::EntityIdentityRequired { entity_columns })
                if entity_columns == ["symbol"]
        ));
        assert!(matches!(
            table.max_gap_len_for_range(start, end).await,
            Err(TableError::EntityIdentityRequired { entity_columns })
                if entity_columns == ["symbol"]
        ));
        assert!(matches!(
            table.last_fully_covered_window(end, 0).await,
            Err(TableError::EntityIdentityRequired { entity_columns })
                if entity_columns == ["symbol"]
        ));
        assert_eq!(table.state(), &state_before);
        Ok(())
    }

    #[tokio::test]
    async fn entity_identity_input_is_validated_and_canonicalized() -> TestResult {
        let tmp = TempDir::new()?;
        let mut meta = make_basic_table_meta();
        let TableKind::TimeSeries(index) = &mut meta.kind else {
            unreachable!("test metadata is time series")
        };
        index.entity_columns = vec!["symbol".to_string(), "venue".to_string()];
        let mut fields = meta
            .logical_schema
            .as_ref()
            .expect("test schema")
            .columns()
            .to_vec();
        fields.push(LogicalField {
            name: "venue".to_string(),
            data_type: LogicalDataType::Utf8,
            nullable: false,
        });
        meta.logical_schema = Some(LogicalSchema::new(fields)?);
        let table = TimeSeriesTable::create(TableLocation::local(tmp.path()), meta).await?;

        let identity = table.resolve_entity_identity(&[
            ("venue", EntityValue::from("X")),
            ("symbol", EntityValue::from("A")),
        ])?;
        assert_eq!(
            identity.components(),
            [EntityValue::from("A"), EntityValue::from("X")]
        );
        let start = ts_from_secs(0);
        let end = ts_from_secs(60);
        assert!(matches!(
            table
                .coverage_ratio_for_entity_range(&[("venue", EntityValue::from("X"))], start, end)
                .await,
            Err(TableError::MissingEntityIdentityColumn { column }) if column == "symbol"
        ));
        assert!(matches!(
            table
                .coverage_ratio_for_entity_range(
                    &[
                        ("device", EntityValue::from("A")),
                        ("venue", EntityValue::from("X")),
                    ],
                    start,
                    end,
                )
                .await,
            Err(TableError::UnexpectedEntityIdentityColumn { column }) if column == "device"
        ));
        assert!(matches!(
            table
                .coverage_ratio_for_entity_range(
                    &[
                        ("symbol", EntityValue::from("A")),
                        ("symbol", EntityValue::from("B")),
                        ("venue", EntityValue::from("X")),
                    ],
                    start,
                    end,
                )
                .await,
            Err(TableError::DuplicateEntityIdentityColumn { column }) if column == "symbol"
        ));

        let (_tmp, global_table) = make_table().await?;
        assert!(matches!(
            global_table
                .coverage_ratio_for_entity_range(&[("symbol", EntityValue::from("A"))], start, end)
                .await,
            Err(TableError::EntityIdentityNotConfigured)
        ));
        Ok(())
    }

    async fn table_with_sparse_coverage() -> HelperResult<(TempDir, TimeSeriesTable)> {
        // Buckets covered: 0, 1, 3 (gap at 2).
        let (tmp, mut table) = make_table().await?;
        append_segment(
            &mut table,
            &tmp,
            "data/sparse.parquet",
            &[
                TestRow {
                    ts_millis: 1_000,
                    symbol: "A",
                    price: 1.0,
                },
                TestRow {
                    ts_millis: 61_000,
                    symbol: "A",
                    price: 2.0,
                },
                TestRow {
                    ts_millis: 180_000,
                    symbol: "A",
                    price: 3.0,
                },
            ],
        )
        .await?;
        Ok((tmp, table))
    }

    async fn table_with_contiguous_run() -> HelperResult<(TempDir, TimeSeriesTable)> {
        // Buckets covered: 4 and 5 (contiguous run).
        let (tmp, mut table) = make_table().await?;
        append_segment(
            &mut table,
            &tmp,
            "data/window.parquet",
            &[
                TestRow {
                    ts_millis: 240_000,
                    symbol: "A",
                    price: 1.0,
                },
                TestRow {
                    ts_millis: 300_000,
                    symbol: "A",
                    price: 2.0,
                },
            ],
        )
        .await?;
        Ok((tmp, table))
    }

    #[tokio::test]
    async fn bucket_range_rejects_invalid_range() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let ts = utc_datetime(2024, 1, 1, 0, 0, 0);

        let err = table
            .bucket_range_for_index_range(ts, ts)
            .expect_err("start >= end should be invalid");
        assert!(matches!(err, TableError::InvalidRange { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn bucket_range_uses_64_bit_signed_timestamp_mapping() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(180); // covers buckets 0,1,2 with 1-minute bucket spec

        let range = table.bucket_range_for_index_range(start, end)?;
        assert_eq!(range, 0x8000_0000_0000_0000..=0x8000_0000_0000_0002);
        Ok(())
    }

    #[tokio::test]
    async fn signed_coverage_queries_handle_gaps_extremes_and_last_window() -> TestResult {
        let kind = IndexKind::Int64 {
            bucket_width: NonZeroU64::new(10).unwrap(),
        };
        let coverage: Coverage = [-10i64, 0, 10]
            .into_iter()
            .map(|value| bucket_id(&kind, &value.into()).unwrap())
            .collect();
        let huge_gap = u128::from(
            bucket_id(&kind, &(-10i64).into()).unwrap()
                - bucket_id(&kind, &i64::MIN.into()).unwrap(),
        )
        .max(u128::from(
            bucket_for_exclusive_end(&kind, &i64::MAX.into()).unwrap()
                - bucket_id(&kind, &10i64.into()).unwrap(),
        ));
        let (_tmp, table) = table_with_index_coverage(kind, coverage).await?;

        assert_eq!(table.coverage_ratio_for_range(-20i64, 30i64).await?, 0.6);
        assert_eq!(table.max_gap_len_for_range(-20i64, 30i64).await?, 1);
        assert_eq!(table.max_gap_len_for_range(-10i64, 0i64).await?, 0);
        assert_eq!(table.max_gap_len_for_range(-50i64, -20i64).await?, 3);
        assert_eq!(
            table.max_gap_len_for_range(i64::MIN, i64::MAX).await?,
            huge_gap
        );

        let window = table
            .last_fully_covered_window(10i64, 2)
            .await?
            .expect("signed window across zero");
        assert_eq!(
            window,
            bucket_id(&table.index_spec().kind, &(-10i64).into()).unwrap()
                ..=bucket_id(&table.index_spec().kind, &0i64.into()).unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn unsigned_coverage_queries_preserve_large_values_and_boundaries() -> TestResult {
        let kind = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        };
        let start = i64::MAX as u64 + 1;
        let coverage: Coverage = [start, start + 1, u64::MAX - 2, u64::MAX - 1]
            .into_iter()
            .collect();
        let (_tmp, table) = table_with_index_coverage(kind, coverage).await?;

        let requested = u128::from(u64::MAX) - u128::from(start);
        let ratio = table.coverage_ratio_for_range(start, u64::MAX).await?;
        assert!((ratio - 4.0 / requested as f64).abs() < f64::EPSILON);
        assert_eq!(
            table.max_gap_len_for_range(start, u64::MAX).await?,
            u128::from(u64::MAX) - u128::from(start) - 4
        );
        assert_eq!(
            table.last_fully_covered_window(start + 2, 2).await?,
            Some(start..=start + 1)
        );
        assert_eq!(
            table.last_fully_covered_window(u64::MAX, 2).await?,
            Some(u64::MAX - 2..=u64::MAX - 1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_uses_snapshot_when_present() -> TestResult {
        let (_tmp, table) = table_with_sparse_coverage().await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(240); // buckets 0,1,2,3 expected

        let ratio = table.coverage_ratio_for_range(start, end).await?;
        assert!((ratio - 0.75).abs() < 1e-12);
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_recovers_when_snapshot_missing() -> TestResult {
        let (_tmp, mut table) = table_with_sparse_coverage().await?;
        table.state_mut().table_coverage = None;

        let ratio = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await?;
        assert!((ratio - 0.75).abs() < 1e-12);
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_errors_when_recovery_missing_segment_coverage_path() -> TestResult {
        let (_tmp, mut table) = table_with_sparse_coverage().await?;
        table.state_mut().table_coverage = None;
        let segment = table
            .state_mut()
            .segments
            .values_mut()
            .next()
            .expect("segment present");
        let segment_path = segment.path.clone();
        segment.coverage_path = None;

        let err = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await
            .expect_err("missing segment coverage_path should bubble up");
        assert!(matches!(
            err,
            TableError::ExistingSegmentMissingCoverage { path } if path == segment_path
        ));
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_errors_on_bucket_mismatch() -> TestResult {
        let (_tmp, mut table) = table_with_sparse_coverage().await?;
        let mut ptr = table
            .state()
            .table_coverage
            .clone()
            .expect("snapshot pointer present");
        ptr.index_kind = IndexKind::Timestamp {
            bucket: TimeBucket::Hours(1),
            timezone: None,
        };
        table.state_mut().table_coverage = Some(ptr.clone());

        let err = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await
            .expect_err("mismatched bucket spec should error");

        match err {
            TableError::TableCoverageIndexKindMismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, table.index_spec().kind);
                assert_eq!(actual, ptr.index_kind);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_handles_empty_table() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let ratio = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(60))
            .await?;
        assert_eq!(ratio, 0.0);
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_handles_bucket_ids_above_u32() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(((u32::MAX as i64) + 3) * 60);

        let ratio = table.coverage_ratio_for_range(start, end).await?;
        assert_eq!(ratio, 0.0);
        Ok(())
    }

    #[tokio::test]
    async fn max_gap_len_reports_missing_run() -> TestResult {
        let (_tmp, table) = table_with_sparse_coverage().await?;
        let gap = table
            .max_gap_len_for_range(ts_from_secs(0), ts_from_secs(240))
            .await?;
        assert_eq!(gap, 1);
        Ok(())
    }

    #[tokio::test]
    async fn last_window_returns_none_for_zero_length() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let res = table.last_fully_covered_window(ts_from_secs(0), 0).await?;
        assert!(res.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn last_window_respects_half_open_end_and_run_length() -> TestResult {
        let (_tmp, table) = table_with_contiguous_run().await?;
        let ts_end = ts_from_secs(360); // exactly at the start of bucket 6

        let win = table
            .last_fully_covered_window(ts_end, 2)
            .await?
            .expect("window should be present");
        assert_eq!(win, 0x8000_0000_0000_0004..=0x8000_0000_0000_0005);

        let none = table.last_fully_covered_window(ts_end, 3).await?;
        assert!(none.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn coverage_queries_validate_before_reading_coverage() -> TestResult {
        let tmp = TempDir::new()?;
        let kind = IndexKind::UInt64 {
            bucket_width: NonZeroU64::new(1).unwrap(),
        };
        let meta = TableMeta::new_time_series(IndexSpec {
            column: "offset".to_string(),
            entity_columns: Vec::new(),
            kind: kind.clone(),
        });
        let mut table = TimeSeriesTable::create(TableLocation::local(tmp.path()), meta).await?;
        let version = table.state().version;
        table.state_mut().table_coverage = Some(TableCoveragePointer {
            index_kind: kind,
            coverage_path: "_coverage/table/missing.roar".to_string(),
            version,
        });

        let error = table
            .last_fully_covered_window(ts_from_secs(1), 1)
            .await
            .expect_err("endpoint domain must match the table index");
        assert!(matches!(
            error,
            TableError::InvalidRange {
                source: IndexValueError::KindMismatch {
                    expected: "uint64",
                    actual: "timestamp"
                }
            }
        ));

        assert!(matches!(
            table.coverage_ratio_for_range(1u64, 1u64).await,
            Err(TableError::InvalidRange {
                source: IndexValueError::InvalidRange { .. }
            })
        ));
        assert!(matches!(
            table.max_gap_len_for_range(2u64, 1u64).await,
            Err(TableError::InvalidRange {
                source: IndexValueError::InvalidRange { .. }
            })
        ));
        assert!(matches!(
            table.coverage_ratio_for_range(0u64, 1i64).await,
            Err(TableError::InvalidRange {
                source: IndexValueError::KindMismatch { .. }
            })
        ));
        assert_eq!(table.last_fully_covered_window(0u64, 0).await?, None);
        assert!(matches!(
            table.last_fully_covered_window(0u64, 1).await,
            Err(TableError::CoverageBucket {
                source: BucketError::RangeEndUnderflow { .. }
            })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn last_window_errors_when_recovery_fails() -> TestResult {
        let (_tmp, mut table) = table_with_contiguous_run().await?;
        table.state_mut().table_coverage = None;
        let segment = table
            .state_mut()
            .segments
            .values_mut()
            .next()
            .expect("segment present");
        let segment_path = segment.path.clone();
        segment.coverage_path = None;

        let err = table
            .last_fully_covered_window(ts_from_secs(360), 1)
            .await
            .expect_err("missing coverage_path should bubble up");
        assert!(matches!(
            err,
            TableError::ExistingSegmentMissingCoverage { path } if path == segment_path
        ));
        Ok(())
    }
}
