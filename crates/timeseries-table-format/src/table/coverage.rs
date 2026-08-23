//! Coverage state helpers for `TimeSeriesTable`.
//!
//! This module reads table coverage bitmaps persisted alongside
//! the table. It is responsible for:
//! - Loading coverage snapshots via the transaction log pointer and enforcing
//!   index granularity compatibility.
//! - Falling back to unioning segment coverage sidecars when the snapshot
//!   pointer is missing or unreadable (strict vs recovery modes).

mod error;

pub use error::CoverageQueryError;

use std::{collections::BTreeMap, path::Path};

use snafu::{Backtrace, OptionExt, ResultExt};

use crate::{
    coverage::{
        Coverage, EntityCoverage, EntityIdentity, EntityValue,
        io::{CoverageSidecarError, read_coverage_sidecar, read_entity_coverage_sidecar},
    },
    metadata::schema_compat::{ensure_entity_identity_matches_schema, require_table_schema},
    metadata::table_metadata::IndexKind,
    transaction_log::table_state::TableCoveragePointer,
};

use self::error as query_error;
use super::{
    TimeSeriesTable,
    error::{AppendError, TableError},
};

pub(crate) trait CoverageRecoveryError: Sized {
    fn missing_segment_coverage_path(segment_path: String) -> Self;

    fn segment_coverage_read_failed(
        segment_path: String,
        coverage_path: String,
        source: CoverageSidecarError,
    ) -> Self;

    fn coverage_index_mismatch(
        expected: IndexKind,
        actual: IndexKind,
        pointer_version: u64,
    ) -> Self;
}

impl CoverageRecoveryError for AppendError {
    fn missing_segment_coverage_path(segment_path: String) -> Self {
        Self::ExistingSegmentMissingCoverageMetadata { segment_path }
    }

    fn segment_coverage_read_failed(
        segment_path: String,
        coverage_path: String,
        source: CoverageSidecarError,
    ) -> Self {
        Self::ExistingSegmentCoverageSidecarRead {
            segment_path,
            coverage_path,
            source: Box::new(source),
        }
    }

    fn coverage_index_mismatch(
        expected: IndexKind,
        actual: IndexKind,
        pointer_version: u64,
    ) -> Self {
        Self::CoverageSnapshotIndexKindMismatch {
            expected,
            actual,
            pointer_version,
        }
    }
}

impl CoverageRecoveryError for CoverageQueryError {
    fn missing_segment_coverage_path(segment_path: String) -> Self {
        Self::ExistingSegmentMissingCoverageMetadata {
            segment_path,
            backtrace: Box::new(Backtrace::capture()),
        }
    }

    fn segment_coverage_read_failed(
        segment_path: String,
        coverage_path: String,
        source: CoverageSidecarError,
    ) -> Self {
        Self::SegmentCoverageSidecarRead {
            segment_path,
            coverage_path,
            source: Box::new(source),
        }
    }

    fn coverage_index_mismatch(
        expected: IndexKind,
        actual: IndexKind,
        pointer_version: u64,
    ) -> Self {
        Self::TableCoverageIndexKindMismatch {
            expected,
            actual,
            pointer_version,
            backtrace: Box::new(Backtrace::capture()),
        }
    }
}

fn ensure_entity_coverage_identity_schema(
    coverage: &EntityCoverage,
    table: &TimeSeriesTable,
) -> Result<(), CoverageSidecarError> {
    let schema =
        require_table_schema(&table.state().table_meta).map_err(CoverageSidecarError::from)?;
    for (identity, _) in coverage.iter() {
        ensure_entity_identity_matches_schema(schema, table.index_spec(), identity)
            .map_err(CoverageSidecarError::from)?;
    }
    Ok(())
}

impl TimeSeriesTable {
    fn ensure_global_coverage_query(&self) -> Result<(), CoverageQueryError> {
        if self.index_spec().entity_columns.is_empty() {
            Ok(())
        } else {
            query_error::EntityIdentityRequiredSnafu {
                entity_columns: self.index_spec().entity_columns.clone(),
            }
            .fail()
        }
    }

    fn resolve_entity_identity(
        &self,
        components: &[(&str, EntityValue)],
    ) -> Result<EntityIdentity, CoverageQueryError> {
        let entity_columns = &self.index_spec().entity_columns;
        if entity_columns.is_empty() {
            return query_error::EntityIdentityNotConfiguredSnafu.fail();
        }

        let mut provided = BTreeMap::new();
        for (column, value) in components {
            if !entity_columns.iter().any(|expected| expected == *column) {
                return query_error::UnexpectedEntityIdentityColumnSnafu {
                    column: (*column).to_string(),
                }
                .fail();
            }
            if provided.insert(*column, value).is_some() {
                return query_error::DuplicateEntityIdentityColumnSnafu {
                    column: (*column).to_string(),
                }
                .fail();
            }
        }

        let ordered = entity_columns
            .iter()
            .map(|column| {
                provided
                    .get(column.as_str())
                    .map(|value| (**value).clone())
                    .context(query_error::MissingEntityIdentityColumnSnafu {
                        column: column.clone(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let identity =
            EntityIdentity::try_new(ordered).context(query_error::InvalidEntityIdentitySnafu)?;
        let schema = require_table_schema(&self.state().table_meta)
            .context(query_error::SchemaCompatibilitySnafu)?;
        ensure_entity_identity_matches_schema(schema, self.index_spec(), &identity)
            .context(query_error::SchemaCompatibilitySnafu)?;
        Ok(identity)
    }

    async fn read_and_validate_entity_coverage_sidecar(
        &self,
        path: &Path,
    ) -> Result<EntityCoverage, CoverageSidecarError> {
        let coverage = read_entity_coverage_sidecar(self.location(), path).await?;
        ensure_entity_coverage_identity_schema(&coverage, self)?;
        Ok(coverage)
    }

    /// Rebuild table coverage by reading each segment's coverage sidecar.
    ///
    /// This is used as a fallback when the table snapshot coverage is missing or
    /// unreadable. Requires every segment to have a `coverage_path`.
    pub(crate) async fn recover_global_coverage_from_segments<E>(&self) -> Result<Coverage, E>
    where
        E: CoverageRecoveryError,
    {
        let mut acc = Coverage::empty();

        for seg in self.state().segments.values() {
            let path = seg
                .coverage_path
                .as_ref()
                .ok_or_else(|| E::missing_segment_coverage_path(seg.path.clone()))?;

            let cov = read_coverage_sidecar(self.location(), Path::new(path))
                .await
                .map_err(|source| {
                    E::segment_coverage_read_failed(seg.path.clone(), path.clone(), source)
                })?;

            // Prefer an in-place union to avoid repeated allocations.
            acc.union_inplace(&cov);
        }

        Ok(acc)
    }

    pub(crate) async fn recover_entity_coverage_from_segments<E>(&self) -> Result<EntityCoverage, E>
    where
        E: CoverageRecoveryError,
    {
        let mut acc = EntityCoverage::empty();

        for seg in self.state().segments.values() {
            let path = seg
                .coverage_path
                .as_ref()
                .ok_or_else(|| E::missing_segment_coverage_path(seg.path.clone()))?;

            let coverage = self
                .read_and_validate_entity_coverage_sidecar(Path::new(path))
                .await
                .map_err(|source| {
                    E::segment_coverage_read_failed(seg.path.clone(), path.clone(), source)
                })?;
            acc.union_inplace(&coverage);
        }

        Ok(acc)
    }

    fn validate_coverage_pointer_index_kind<E>(&self, ptr: &TableCoveragePointer) -> Result<(), E>
    where
        E: CoverageRecoveryError,
    {
        let expected = self.index_spec().kind.clone();
        if ptr.index_kind != expected {
            return Err(E::coverage_index_mismatch(
                expected,
                ptr.index_kind.clone(),
                ptr.version,
            ));
        }
        Ok(())
    }

    async fn load_global_coverage_snapshot_for_query(
        &self,
    ) -> Result<Coverage, CoverageQueryError> {
        match &self.state().table_coverage {
            None => {
                if self.state().segments.is_empty() {
                    return Ok(Coverage::empty());
                }
                query_error::MissingTableCoveragePointerSnafu.fail()
            }
            Some(ptr) => {
                self.validate_coverage_pointer_index_kind::<CoverageQueryError>(ptr)?;
                read_coverage_sidecar(self.location(), Path::new(&ptr.coverage_path))
                    .await
                    .context(query_error::CoverageSnapshotReadSnafu {
                        coverage_path: ptr.coverage_path.clone(),
                    })
            }
        }
    }

    /// Load table coverage using the snapshot pointer only.
    ///
    /// - If there is no snapshot pointer:
    ///   - If table has zero segments: returns empty coverage.
    ///   - Else: returns MissingTableCoveragePointer (strict mode).
    /// - If snapshot exists but is missing/corrupt: returns the snapshot read error.
    pub async fn load_table_coverage_snapshot_only(&self) -> Result<Coverage, TableError> {
        self.load_global_coverage_snapshot_for_query()
            .await
            .context(super::error::CoverageQuerySnafu)
    }
    /// Load table coverage for read paths (no writes).
    ///
    /// - If snapshot pointer is absent:
    ///   - If table has zero segments: returns empty coverage.
    ///   - Else: recovers by unioning segment sidecars.
    /// - If snapshot pointer exists but snapshot is missing/corrupt:
    ///   - Recovers by unioning segment sidecars.
    pub(crate) async fn load_global_coverage_with_recovery<E>(&self) -> Result<Coverage, E>
    where
        E: CoverageRecoveryError,
    {
        match &self.state().table_coverage {
            None => {
                if self.state().segments.is_empty() {
                    return Ok(Coverage::empty());
                }
                self.recover_global_coverage_from_segments::<E>().await
            }
            Some(ptr) => {
                self.validate_coverage_pointer_index_kind::<E>(ptr)?;

                match read_coverage_sidecar(self.location(), Path::new(&ptr.coverage_path)).await {
                    Ok(cov) => Ok(cov),
                    Err(snapshot_err) => {
                        tracing::warn!(
                            name: "coverage.recover",
                            coverage_mode = "global",
                            snapshot_version = ptr.version,
                            coverage_path = %ptr.coverage_path,
                            error = %snapshot_err,
                            recovery_source = "segment_sidecars",
                            "Failed to read table coverage snapshot; attempting read-only recovery from segment sidecars"
                        );
                        let coverage = self.recover_global_coverage_from_segments::<E>().await?;
                        tracing::debug!(
                            name: "coverage.recover",
                            coverage_mode = "global",
                            snapshot_version = ptr.version,
                            coverage_path = %ptr.coverage_path,
                            recovery_source = "segment_sidecars",
                            covered_index_interval_count = coverage.cardinality(),
                            outcome = "succeeded",
                            "Recovered table coverage from segment sidecars"
                        );
                        Ok(coverage)
                    }
                }
            }
        }
    }

    pub(crate) async fn load_entity_coverage_with_recovery<E>(&self) -> Result<EntityCoverage, E>
    where
        E: CoverageRecoveryError,
    {
        match &self.state().table_coverage {
            None => {
                if self.state().segments.is_empty() {
                    return Ok(EntityCoverage::empty());
                }
                self.recover_entity_coverage_from_segments::<E>().await
            }
            Some(ptr) => {
                self.validate_coverage_pointer_index_kind::<E>(ptr)?;
                match self
                    .read_and_validate_entity_coverage_sidecar(Path::new(&ptr.coverage_path))
                    .await
                {
                    Ok(coverage) => Ok(coverage),
                    Err(snapshot_err) => {
                        tracing::warn!(
                            name: "coverage.recover",
                            coverage_mode = "entity",
                            snapshot_version = ptr.version,
                            coverage_path = %ptr.coverage_path,
                            error = %snapshot_err,
                            recovery_source = "segment_sidecars",
                            "Failed to read entity coverage snapshot; attempting read-only recovery from segment sidecars"
                        );
                        let coverage = self.recover_entity_coverage_from_segments::<E>().await?;
                        tracing::debug!(
                            name: "coverage.recover",
                            coverage_mode = "entity",
                            snapshot_version = ptr.version,
                            coverage_path = %ptr.coverage_path,
                            recovery_source = "segment_sidecars",
                            coverage_identity_count = coverage.identity_count(),
                            outcome = "succeeded",
                            "Recovered entity coverage from segment sidecars"
                        );
                        Ok(coverage)
                    }
                }
            }
        }
    }

}

// Coverage query APIs for TimeSeriesTable.
//
// These APIs:
// - derive an inclusive index interval ID range from `[start, end)`
// - load table coverage (readonly recovery)
// - reuse crate::coverage APIs (coverage_ratio, max_gap_len, last_window_at_or_before)
use std::ops::RangeInclusive;

use crate::{
    coverage::IndexIntervalId,
    coverage::index_interval::{index_interval_id_for_exclusive_end, index_interval_id_range},
    metadata::table_metadata::{IndexValue, validate_index_range},
    table::error::CoverageQuerySnafu,
};

impl TimeSeriesTable {
    fn interval_ids_for_query_range<S, E>(
        &self,
        start: S,
        end: E,
    ) -> Result<RangeInclusive<IndexIntervalId>, CoverageQueryError>
    where
        S: Into<IndexValue>,
        E: Into<IndexValue>,
    {
        let start = start.into();
        let end = end.into();
        validate_index_range(&self.index_spec().kind, &start, &end)
            .context(query_error::InvalidRangeSnafu)?;
        index_interval_id_range(&self.index_spec().kind, &start, &end)
            .context(query_error::IndexIntervalMappingSnafu)
    }

    // ---- public query APIs ----

    /// Coverage ratio in [0.0, 1.0] for the half-open index range [start, end).
    ///
    /// This identity-free query is only valid for tables without configured
    /// entity columns. It uses the table-level coverage snapshot, with readonly
    /// recovery from segments if needed.
    ///
    /// # Errors
    /// Returns [`TableError::CoverageQuery`] containing
    /// [`CoverageQueryError::InvalidRange`] when the endpoints do not match the
    /// table index or `start >= end`, or
    /// [`CoverageQueryError::EntityIdentityRequired`] when the table has entity
    /// columns. Snapshot and interval mapping failures retain their typed
    /// sources in the same operation error.
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
        async {
            self.ensure_global_coverage_query()?;
            let range = self.interval_ids_for_query_range(start, end)?;
            let cov = self
                .load_global_coverage_with_recovery::<CoverageQueryError>()
                .await?;
            Ok(cov.coverage_ratio(&range))
        }
        .await
        .context(CoverageQuerySnafu)
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
    /// If entities `A` and `B` both have data in the same interval, their coverage
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
        async {
            let identity = self.resolve_entity_identity(entity)?;
            let range = self.interval_ids_for_query_range(start, end)?;
            let coverage = self
                .load_entity_coverage_with_recovery::<CoverageQueryError>()
                .await?;
            Ok(coverage
                .get(&identity)
                .map_or(0.0, |coverage| coverage.coverage_ratio(&range)))
        }
        .await
        .context(CoverageQuerySnafu)
    }

    /// Maximum contiguous missing run length in index intervals for `[start, end)`.
    ///
    /// This identity-free query is only valid for tables without configured
    /// entity columns.
    ///
    /// # Errors
    /// Returns [`TableError::CoverageQuery`] containing
    /// [`CoverageQueryError::InvalidRange`] when the endpoints do not match the
    /// table index or `start >= end`, or
    /// [`CoverageQueryError::EntityIdentityRequired`] when the table has entity
    /// columns. Snapshot and interval mapping failures retain their typed
    /// sources in the same operation error.
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
        async {
            self.ensure_global_coverage_query()?;
            let range = self.interval_ids_for_query_range(start, end)?;
            let cov = self
                .load_global_coverage_with_recovery::<CoverageQueryError>()
                .await?;
            Ok(cov.max_gap_len(&range))
        }
        .await
        .context(CoverageQuerySnafu)
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
    /// or unconfigured entity columns. It returns
    /// [`CoverageQueryError::InvalidRange`] inside [`TableError::CoverageQuery`]
    /// for invalid half-open range endpoints and retains typed snapshot and
    /// interval mapping sources in the same operation error.
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
        async {
            let identity = self.resolve_entity_identity(entity)?;
            let range = self.interval_ids_for_query_range(start, end)?;
            let coverage = self
                .load_entity_coverage_with_recovery::<CoverageQueryError>()
                .await?;
            Ok(coverage.get(&identity).map_or_else(
                || Coverage::range_cardinality(&range),
                |c| c.max_gap_len(&range),
            ))
        }
        .await
        .context(CoverageQuerySnafu)
    }

    /// Return the last fully covered contiguous window of `window_len_intervals`
    /// ending before the exclusive ordered-index endpoint.
    ///
    /// Notes:
    /// - This returns inclusive index interval IDs in their 64-bit domain.
    /// - Returns `None` for a zero-length window or when no complete window exists.
    /// - This identity-free query is only valid for tables without configured entity columns.
    ///
    /// # Errors
    /// Returns [`TableError::CoverageQuery`] containing
    /// [`CoverageQueryError::InvalidRange`] when `end` does not match the table
    /// index or [`CoverageQueryError::EntityIdentityRequired`] when the table
    /// has entity columns. Endpoint mapping and snapshot failures retain their
    /// typed sources in the same operation error.
    ///
    /// # Examples
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// # use timeseries_table_format::{storage::TableLocation, table::TimeSeriesTable};
    /// # async fn demo(table: &TimeSeriesTable) -> Result<(), timeseries_table_format::table::TableError> {
    /// let ts_end = Utc.timestamp_opt(360, 0).single().unwrap(); // end of interval 5
    /// let window = table.last_fully_covered_window(ts_end, 2).await?;
    /// # let _ = window;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn last_fully_covered_window<E>(
        &self,
        end: E,
        window_len_intervals: u64,
    ) -> Result<Option<RangeInclusive<IndexIntervalId>>, TableError>
    where
        E: Into<IndexValue>,
    {
        async {
            self.ensure_global_coverage_query()?;
            let end = end.into();
            end.validate_kind(&self.index_spec().kind)
                .context(query_error::InvalidRangeSnafu)?;
            if window_len_intervals == 0 {
                return Ok(None);
            }

            let end_index_interval_id =
                index_interval_id_for_exclusive_end(&self.index_spec().kind, &end)
                    .context(query_error::IndexIntervalMappingSnafu)?;
            let cov = self
                .load_global_coverage_with_recovery::<CoverageQueryError>()
                .await?;
            Ok(cov.last_window_at_or_before(end_index_interval_id, window_len_intervals))
        }
        .await
        .context(CoverageQuerySnafu)
    }

    /// Return one entity's last fully covered contiguous window ending before
    /// the exclusive ordered-index endpoint.
    ///
    /// Entity components are supplied by column name and canonicalized into the
    /// configured entity-column order. Other entities cannot contribute intervals
    /// to the window. A complete identity not present in the table returns
    /// `None`, as does a zero-length window.
    ///
    /// # Errors
    /// Returns a typed entity identity error for missing, duplicate, unexpected,
    /// or unconfigured entity columns. It returns
    /// [`CoverageQueryError::InvalidRange`] inside [`TableError::CoverageQuery`]
    /// when `end` does not match the table index and retains typed endpoint
    /// mapping and snapshot sources in the same operation error.
    pub async fn last_fully_covered_window_for_entity<E>(
        &self,
        entity: &[(&str, EntityValue)],
        end: E,
        window_len_intervals: u64,
    ) -> Result<Option<RangeInclusive<IndexIntervalId>>, TableError>
    where
        E: Into<IndexValue>,
    {
        async {
            let identity = self.resolve_entity_identity(entity)?;
            let end = end.into();
            end.validate_kind(&self.index_spec().kind)
                .context(query_error::InvalidRangeSnafu)?;
            if window_len_intervals == 0 {
                return Ok(None);
            }

            let end_index_interval_id =
                index_interval_id_for_exclusive_end(&self.index_spec().kind, &end)
                    .context(query_error::IndexIntervalMappingSnafu)?;
            let coverage = self
                .load_entity_coverage_with_recovery::<CoverageQueryError>()
                .await?;
            Ok(coverage.get(&identity).and_then(|coverage| {
                coverage.last_window_at_or_before(end_index_interval_id, window_len_intervals)
            }))
        }
        .await
        .context(CoverageQuerySnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, error::Error as _, num::NonZeroU64, path::Path};

    use super::*;
    use crate::{
        coverage::{
            Coverage, EntityCoverage, EntityIdentity, EntityValue,
            index_interval::{IndexIntervalMappingError, index_interval_id_for_value},
            io::{
                CoverageSidecarError, write_coverage_sidecar_atomic,
                write_coverage_sidecar_new_bytes,
            },
            serde::CoverageCodecError,
            serde::entity_coverage_to_bytes,
        },
        metadata::logical_schema::{LogicalDataType, LogicalField, LogicalSchema},
        metadata::schema_compat::SchemaCompatibilityError,
        metadata::table_metadata::{
            IndexKind, IndexSpec, IndexValueError, TableKind, TableMeta, TimeIndexGranularity,
        },
        storage::{StorageError, TableLocation},
        table::test_util::{
            TestResult, TestRow, TraceCapture, append_parquet_fixture, make_basic_table_meta,
            make_int32_entity_table_meta, utc_datetime, write_int32_entity_parquet,
            write_test_parquet,
        },
    };
    use chrono::{DateTime, TimeZone, Utc};
    use snafu::ErrorCompat;
    use tempfile::TempDir;

    type HelperResult<T> = Result<T, Box<dyn std::error::Error>>;

    fn ts_from_secs(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0)
            .single()
            .expect("valid timestamp")
    }

    fn coverage_query_source(error: &TableError) -> &CoverageQueryError {
        error
            .source()
            .and_then(|source| source.downcast_ref::<CoverageQueryError>())
            .expect("coverage query source")
    }

    fn coverage_sidecar_source(error: &CoverageQueryError) -> &CoverageSidecarError {
        error
            .source()
            .and_then(|source| source.downcast_ref::<Box<CoverageSidecarError>>())
            .map(Box::as_ref)
            .expect("coverage sidecar source")
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

    #[tokio::test]
    async fn strict_snapshot_missing_preserves_storage_source_and_backtrace() -> TestResult {
        let (_tmp, mut table) = make_table().await?;
        let coverage_path = "_coverage/table/missing.roar";
        table.state_mut().table_coverage = Some(TableCoveragePointer {
            index_kind: table.index_spec().kind.clone(),
            coverage_path: coverage_path.to_string(),
            version: table.state().version,
        });

        let error = table
            .load_table_coverage_snapshot_only()
            .await
            .expect_err("missing snapshot must fail");
        let query = coverage_query_source(&error);
        assert!(matches!(
            query,
            CoverageQueryError::CoverageSidecar { path, .. } if path == coverage_path
        ));
        let sidecar = coverage_sidecar_source(query);
        let storage = sidecar
            .source()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("storage source");
        assert!(matches!(storage, StorageError::NotFound { .. }));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            ErrorCompat::backtrace(storage).expect("storage backtrace"),
        ));
        Ok(())
    }

    #[tokio::test]
    async fn strict_snapshot_corruption_preserves_codec_source_and_backtrace() -> TestResult {
        let (tmp, mut table) = make_table().await?;
        let coverage_path = "_coverage/table/corrupt.roar";
        let absolute = tmp.path().join(coverage_path);
        std::fs::create_dir_all(absolute.parent().expect("coverage parent"))?;
        std::fs::write(&absolute, b"not a bitmap")?;
        table.state_mut().table_coverage = Some(TableCoveragePointer {
            index_kind: table.index_spec().kind.clone(),
            coverage_path: coverage_path.to_string(),
            version: table.state().version,
        });

        let error = table
            .load_table_coverage_snapshot_only()
            .await
            .expect_err("corrupt snapshot must fail");
        let query = coverage_query_source(&error);
        let sidecar = coverage_sidecar_source(query);
        let codec = sidecar
            .source()
            .and_then(|source| source.downcast_ref::<CoverageCodecError>())
            .expect("codec source");
        assert!(matches!(
            codec,
            CoverageCodecError::BitmapDeserialization { .. }
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            ErrorCompat::backtrace(codec).expect("codec backtrace"),
        ));
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn strict_snapshot_permission_denied_preserves_storage_source() -> TestResult {
        use std::os::unix::fs::PermissionsExt;

        let (tmp, mut table) = make_table().await?;
        let coverage_path = "_coverage/table/denied.roar";
        let absolute = tmp.path().join(coverage_path);
        write_coverage_sidecar_atomic(
            table.location(),
            Path::new(coverage_path),
            &Coverage::empty(),
        )
        .await?;
        table.state_mut().table_coverage = Some(TableCoveragePointer {
            index_kind: table.index_spec().kind.clone(),
            coverage_path: coverage_path.to_string(),
            version: table.state().version,
        });
        let original_permissions = std::fs::metadata(&absolute)?.permissions();
        let mut denied_permissions = original_permissions.clone();
        denied_permissions.set_mode(0o0);
        std::fs::set_permissions(&absolute, denied_permissions)?;

        let error = table
            .load_table_coverage_snapshot_only()
            .await
            .expect_err("permission-denied snapshot must fail");
        std::fs::set_permissions(&absolute, original_permissions)?;

        let query = coverage_query_source(&error);
        let sidecar = coverage_sidecar_source(query);
        let storage = sidecar
            .source()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("storage source");
        assert!(matches!(storage, StorageError::OtherIo { .. }));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            ErrorCompat::backtrace(storage).expect("storage backtrace"),
        ));
        Ok(())
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
    ) -> HelperResult<String> {
        let abs = tmp.path().join(rel_path);
        write_test_parquet(&abs, true, false, rows)?;
        let existing_paths = table
            .state()
            .segments
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        append_parquet_fixture(table, rel_path).await?;
        table
            .state()
            .segments
            .keys()
            .find(|path| !existing_paths.contains(*path))
            .cloned()
            .ok_or_else(|| "append did not add a segment".into())
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
            .read_and_validate_entity_coverage_sidecar(Path::new(snapshot_path))
            .await
            .expect_err("snapshot identity arity must match the table");
        assert!(matches!(
            snapshot_error,
            CoverageSidecarError::EntityIdentitySchema { source, .. } if matches!(
                source.as_ref(),
                SchemaCompatibilityError::EntityIdentityArityMismatch {
                    expected: 1,
                    actual: 2,
                }
            )
        ));

        let mut wrong_type = EntityCoverage::empty();
        wrong_type.union_coverage(
            EntityIdentity::try_new(vec![EntityValue::Int32(1)])?,
            Coverage::from_iter([0]),
        );
        let wrong_type_bytes = entity_coverage_to_bytes(&wrong_type)?;
        table.state_mut().table_coverage = None;
        let segment_path = "data/entity-arity.parquet";
        let committed_segment_path = append_segment(
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
            .get(&committed_segment_path)
            .and_then(|segment| segment.coverage_path.clone())
            .expect("segment coverage path");
        tokio::fs::write(tmp.path().join(&segment_coverage_path), &wrong_type_bytes).await?;

        let recovery_error = table
            .recover_entity_coverage_from_segments::<CoverageQueryError>()
            .await
            .expect_err("segment identity type must match the table schema");
        assert!(matches!(
            recovery_error,
            TableError::SegmentCoverageSidecarRead {
                path,
                coverage_path,
                source,
            } if path == committed_segment_path
                && coverage_path == segment_coverage_path
                && matches!(
                    &*source,
                    CoverageSidecarError::EntityIdentitySchema {
                        source,
                        ..
                    }
                    if matches!(
                        source.as_ref(),
                        SchemaCompatibilityError::EntityIdentityTypeMismatch {
                            column,
                            expected: crate::metadata::logical_schema::LogicalDataType::Utf8,
                            actual: "int32",
                        } if column == "symbol"
                    )
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
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::EntityIdentityRequired { entity_columns, .. }
            })
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
        append_parquet_fixture(&mut table, path).await?;
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
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::SchemaCompatibility { source, .. }
            }) if matches!(
                source.as_ref(),
                SchemaCompatibilityError::EntityIdentityTypeMismatch {
                    column,
                    expected: LogicalDataType::Int32,
                    actual: "int64",
                } if column == "device_id"
            )
        ));
        assert!(matches!(
            table
                .coverage_ratio_for_entity_range(
                    &[("device_id", EntityValue::from("-1"))],
                    start,
                    end,
                )
                .await,
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::SchemaCompatibility { source, .. }
            }) if matches!(
                source.as_ref(),
                SchemaCompatibilityError::EntityIdentityTypeMismatch {
                    column,
                    expected: LogicalDataType::Int32,
                    actual: "utf8",
                } if column == "device_id"
            )
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
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::EntityIdentityRequired { entity_columns, .. }
            })
                if entity_columns == ["symbol"]
        ));
        assert!(matches!(
            table.max_gap_len_for_range(start, end).await,
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::EntityIdentityRequired { entity_columns, .. }
            })
                if entity_columns == ["symbol"]
        ));
        assert!(matches!(
            table.last_fully_covered_window(end, 0).await,
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::EntityIdentityRequired { entity_columns, .. }
            })
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
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::MissingEntityIdentityColumn { column, .. }
            }) if column == "symbol"
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
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::UnexpectedEntityIdentityColumn { column, .. }
            }) if column == "device"
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
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::DuplicateEntityIdentityColumn { column, .. }
            }) if column == "symbol"
        ));

        let (_tmp, global_table) = make_table().await?;
        assert!(matches!(
            global_table
                .coverage_ratio_for_entity_range(&[("symbol", EntityValue::from("A"))], start, end)
                .await,
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::EntityIdentityNotConfigured { .. }
            })
        ));
        Ok(())
    }

    async fn table_with_sparse_coverage() -> HelperResult<(TempDir, TimeSeriesTable)> {
        // Interval IDs covered: 0, 1, and 3 (gap at 2).
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
        // Interval IDs covered: 4 and 5 (contiguous run).
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
    async fn index_interval_id_range_rejects_invalid_index_range() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let ts = utc_datetime(2024, 1, 1, 0, 0, 0);

        let err = table
            .interval_ids_for_query_range(ts, ts)
            .expect_err("start >= end should be invalid");
        assert!(matches!(err, CoverageQueryError::InvalidRange { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn index_interval_id_range_uses_64_bit_signed_timestamp_mapping() -> TestResult {
        let (_tmp, table) = make_table().await?;
        let start = ts_from_secs(0);
        let end = ts_from_secs(180); // covers the first three one-minute intervals

        let range = table.interval_ids_for_query_range(start, end)?;
        assert_eq!(range, 0x8000_0000_0000_0000..=0x8000_0000_0000_0002);
        Ok(())
    }

    #[tokio::test]
    async fn signed_coverage_queries_handle_gaps_extremes_and_last_window() -> TestResult {
        let kind = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(10).unwrap(),
        };
        let coverage: Coverage = [-10i64, 0, 10]
            .into_iter()
            .map(|value| index_interval_id_for_value(&kind, &value.into()).unwrap())
            .collect();
        let huge_gap = u128::from(
            index_interval_id_for_value(&kind, &(-10i64).into()).unwrap()
                - index_interval_id_for_value(&kind, &i64::MIN.into()).unwrap(),
        )
        .max(u128::from(
            index_interval_id_for_exclusive_end(&kind, &i64::MAX.into()).unwrap()
                - index_interval_id_for_value(&kind, &10i64.into()).unwrap(),
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
            index_interval_id_for_value(&table.index_spec().kind, &(-10i64).into()).unwrap()
                ..=index_interval_id_for_value(&table.index_spec().kind, &0i64.into()).unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn unsigned_coverage_queries_preserve_large_values_and_boundaries() -> TestResult {
        let kind = IndexKind::UInt64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
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
        let end = ts_from_secs(240); // interval IDs 0, 1, 2, and 3 are expected

        let ratio = table.coverage_ratio_for_range(start, end).await?;
        assert!((ratio - 0.75).abs() < 1e-12);
        Ok(())
    }

    fn assert_recovery_events(
        capture: &TraceCapture,
        pointer: &TableCoveragePointer,
        mode: &str,
        count_field: &str,
        count: &str,
        forbidden_values: &[&str],
    ) {
        let recovery_events: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| event.name == "coverage.recover")
            .collect();
        assert_eq!(recovery_events.len(), 2);

        let warning = recovery_events
            .iter()
            .find(|event| event.level == tracing::Level::WARN)
            .expect("recovery warning");
        assert_eq!(
            warning.fields.get("coverage_mode").map(String::as_str),
            Some(mode)
        );
        assert_eq!(
            warning.fields.get("snapshot_version"),
            Some(&pointer.version.to_string())
        );
        assert_eq!(
            warning.fields.get("coverage_path"),
            Some(&pointer.coverage_path)
        );
        assert_eq!(
            warning.fields.get("recovery_source").map(String::as_str),
            Some("segment_sidecars")
        );
        assert!(
            warning
                .fields
                .get("message")
                .is_some_and(|message| message.contains("attempting read-only recovery"))
        );
        assert!(
            warning
                .fields
                .get("error")
                .is_some_and(|error| error.contains(&pointer.coverage_path))
        );

        let completion = recovery_events
            .iter()
            .find(|event| event.level == tracing::Level::DEBUG)
            .expect("recovery completion");
        assert_eq!(
            completion.fields.get("outcome").map(String::as_str),
            Some("succeeded")
        );
        assert_eq!(
            completion.fields.get(count_field).map(String::as_str),
            Some(count)
        );

        for value in recovery_events
            .iter()
            .flat_map(|event| event.fields.values())
        {
            for forbidden in forbidden_values
                .iter()
                .copied()
                .chain(["LogicalSchema", "RecordBatch"])
            {
                assert!(
                    !value.contains(forbidden),
                    "diagnostic value contains sensitive data '{forbidden}': {value}"
                );
            }
        }
    }

    #[tokio::test]
    async fn global_coverage_snapshot_recovery_emits_safe_structured_events() -> TestResult {
        let (tmp, table) = table_with_sparse_coverage().await?;
        let pointer = table
            .state()
            .table_coverage
            .as_ref()
            .expect("snapshot pointer")
            .clone();
        tokio::fs::remove_file(tmp.path().join(&pointer.coverage_path)).await?;
        let capture = TraceCapture::default();

        let ratio = capture
            .run(table.coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240)))
            .await?;

        assert!((ratio - 0.75).abs() < 1e-12);
        let table_root = tmp.path().display().to_string();
        assert_recovery_events(
            &capture,
            &pointer,
            "global",
            "covered_index_interval_count",
            "3",
            &[&table_root],
        );
        Ok(())
    }

    #[tokio::test]
    async fn entity_coverage_snapshot_recovery_emits_safe_structured_events() -> TestResult {
        const SENSITIVE_ENTITY: &str = "sensitive-entity-value";
        let tmp = TempDir::new()?;
        let mut table =
            TimeSeriesTable::create(TableLocation::local(tmp.path()), make_basic_table_meta())
                .await?;
        append_segment(
            &mut table,
            &tmp,
            "data/sensitive.parquet",
            &[TestRow {
                ts_millis: 1_000,
                symbol: SENSITIVE_ENTITY,
                price: 987_654.25,
            }],
        )
        .await?;
        let pointer = table
            .state()
            .table_coverage
            .as_ref()
            .expect("snapshot pointer")
            .clone();
        tokio::fs::remove_file(tmp.path().join(&pointer.coverage_path)).await?;
        let capture = TraceCapture::default();

        let ratio = capture
            .run(table.coverage_ratio_for_entity_range(
                &[("symbol", EntityValue::from(SENSITIVE_ENTITY))],
                ts_from_secs(0),
                ts_from_secs(60),
            ))
            .await?;

        assert_eq!(ratio, 1.0);
        let table_root = tmp.path().display().to_string();
        assert_recovery_events(
            &capture,
            &pointer,
            "entity",
            "coverage_identity_count",
            "1",
            &[&table_root, SENSITIVE_ENTITY, "987654.25"],
        );
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
            TableError::CoverageQuery {
                source: CoverageQueryError::ExistingSegmentMissingCoverage { path, .. }
            } if path == segment_path
        ));
        Ok(())
    }

    #[tokio::test]
    async fn coverage_ratio_errors_on_index_granularity_mismatch() -> TestResult {
        let (_tmp, mut table) = table_with_sparse_coverage().await?;
        let mut ptr = table
            .state()
            .table_coverage
            .clone()
            .expect("snapshot pointer present");
        ptr.index_kind = IndexKind::Timestamp {
            index_granularity: TimeIndexGranularity::Hours(1),
            timezone: None,
        };
        table.state_mut().table_coverage = Some(ptr.clone());

        let err = table
            .coverage_ratio_for_range(ts_from_secs(0), ts_from_secs(240))
            .await
            .expect_err("mismatched index granularity should error");

        match err {
            TableError::CoverageQuery {
                source:
                    CoverageQueryError::TableCoverageIndexKindMismatch {
                        expected, actual, ..
                    },
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
    async fn coverage_ratio_handles_interval_ids_above_u32() -> TestResult {
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
        let ts_end = ts_from_secs(360); // exactly at the start of interval 6

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
            index_granularity: NonZeroU64::new(1).unwrap(),
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
        let query = coverage_query_source(&error);
        assert!(matches!(
            query,
            CoverageQueryError::InvalidRange {
                source: IndexValueError::KindMismatch {
                    expected: "uint64",
                    actual: "timestamp"
                },
                ..
            }
        ));
        assert!(matches!(
            query.source(),
            Some(source) if source.downcast_ref::<IndexValueError>().is_some()
        ));
        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("table backtrace"),
            ErrorCompat::backtrace(query).expect("coverage query backtrace"),
        ));

        assert!(matches!(
            table.coverage_ratio_for_range(1u64, 1u64).await,
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::InvalidRange {
                    source: IndexValueError::InvalidRange { .. },
                    ..
                }
            })
        ));
        assert!(matches!(
            table.max_gap_len_for_range(2u64, 1u64).await,
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::InvalidRange {
                    source: IndexValueError::InvalidRange { .. },
                    ..
                }
            })
        ));
        assert!(matches!(
            table.coverage_ratio_for_range(0u64, 1i64).await,
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::InvalidRange {
                    source: IndexValueError::KindMismatch { .. },
                    ..
                }
            })
        ));
        assert_eq!(table.last_fully_covered_window(0u64, 0).await?, None);
        assert!(matches!(
            table.last_fully_covered_window(0u64, 1).await,
            Err(TableError::CoverageQuery {
                source: CoverageQueryError::IndexIntervalMapping {
                    source: IndexIntervalMappingError::RangeEndUnderflow { .. },
                    ..
                }
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
            TableError::CoverageQuery {
                source: CoverageQueryError::ExistingSegmentMissingCoverage { path, .. }
            } if path == segment_path
        ));
        Ok(())
    }
}
