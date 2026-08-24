//! Errors owned by an append operation.

use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use snafu::{Backtrace, Snafu};

use crate::{
    coverage::{
        EntityIdentity, IndexIntervalId,
        index_interval::{IndexInterval, IndexIntervalMappingError},
        io::CoverageSidecarError,
    },
    formats::parquet::SegmentCoverageError,
    metadata::{
        logical_schema::ArrowToLogicalSchemaError, protocol::TableProtocolError,
        schema_compat::SchemaCompatibilityError,
    },
    storage::StorageError,
    transaction_log::{CommitError, segments::SegmentError},
};

/// Errors owned by an append operation.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[non_exhaustive]
pub enum AppendError {
    /// The table protocol does not permit this client to append.
    #[snafu(context(false), display("Table protocol error: {source}"))]
    Protocol {
        /// Complete table protocol failure.
        #[snafu(source)]
        source: TableProtocolError,
        /// Backtrace captured at the append boundary.
        backtrace: Backtrace,
    },

    /// An Arrow input reader or batch normalization failed.
    #[snafu(display("Arrow input error: {source}"))]
    ArrowInput {
        /// Arrow error returned while reading or normalizing a batch.
        #[snafu(source)]
        source: ArrowError,
        /// Backtrace captured at the append input boundary.
        backtrace: Backtrace,
    },

    /// An Arrow schema could not be represented by the table logical schema.
    #[snafu(display("Invalid append input schema: {source}"))]
    ArrowToLogicalSchema {
        /// Arrow-to-logical schema conversion failure.
        #[snafu(source)]
        source: ArrowToLogicalSchemaError,
        /// Backtrace captured at the append schema boundary.
        backtrace: Backtrace,
    },

    /// An append input contained no rows.
    #[snafu(display("Cannot append an empty Arrow input"))]
    EmptyInput,

    /// A configured Parquet row group cannot contain zero rows.
    #[snafu(display(
        "Invalid maximum rows per Parquet row group: {max_rows_per_row_group}; expected a positive value"
    ))]
    InvalidMaxRowsPerRowGroup {
        /// Rejected per-append row-group limit.
        max_rows_per_row_group: usize,
    },

    /// A configured Parquet row group cannot contain zero estimated bytes.
    #[snafu(display(
        "Invalid maximum bytes per Parquet row group: {max_bytes_per_row_group}; expected a positive value"
    ))]
    InvalidMaxBytesPerRowGroup {
        /// Rejected per-append row-group byte limit.
        max_bytes_per_row_group: usize,
    },

    /// Validating the incoming and registered table schemas failed.
    #[snafu(context(false), display("Schema validation failed: {source}"))]
    SchemaValidation {
        /// Complete schema compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)), backtrace)]
        source: Box<SchemaCompatibilityError>,
    },

    /// A generated segment is incompatible with the table schema.
    #[snafu(display(
        "Generated segment {segment_path} is incompatible with the table schema: {source}"
    ))]
    GeneratedSegmentSchemaCompatibility {
        /// Generated table-relative segment path.
        segment_path: String,
        /// Complete schema compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)), backtrace)]
        source: Box<SchemaCompatibilityError>,
    },

    /// Table state lacks the canonical schema required by append.
    #[snafu(display(
        "Table has no logical_schema at version {version}; cannot append without a canonical schema"
    ))]
    MissingCanonicalTableSchema {
        /// Transaction log version missing a canonical logical schema.
        version: u64,
    },

    /// Reading metadata from the generated segment failed.
    #[snafu(context(false), display("Generated segment metadata error: {source}"))]
    SegmentMetadata {
        /// Complete segment metadata failure.
        #[snafu(source(from(SegmentError, Box::new)), backtrace)]
        source: Box<SegmentError>,
    },

    /// Streaming the append input into Parquet failed.
    #[snafu(display("Parquet write error: {source}"))]
    ParquetWrite {
        /// Parquet writer failure.
        #[snafu(source)]
        source: ParquetError,
        /// Backtrace captured at the append writer boundary.
        backtrace: Backtrace,
    },

    /// Deriving ordered-index coverage from the generated segment failed.
    #[snafu(context(false), display("Segment coverage error: {source}"))]
    GeneratedSegmentCoverage {
        /// Complete segment coverage derivation failure.
        #[snafu(source(from(SegmentCoverageError, Box::new)), backtrace)]
        source: Box<SegmentCoverageError>,
    },

    /// An ordered-index interval could not be reconstructed for a diagnostic.
    #[snafu(context(false), display("Index interval mapping failed: {source}"))]
    IndexIntervalMapping {
        /// Complete interval mapping failure.
        #[snafu(source)]
        source: IndexIntervalMappingError,
        /// Backtrace captured at the append boundary.
        backtrace: Backtrace,
    },

    /// A coverage sidecar could not be prepared or written.
    #[snafu(context(false), display("Coverage sidecar error: {source}"))]
    CoverageSidecar {
        /// Complete coverage sidecar failure.
        #[snafu(source(from(CoverageSidecarError, Box::new)), backtrace)]
        source: Box<CoverageSidecarError>,
    },

    /// The generated segment overlaps coverage already persisted by the table.
    #[snafu(display(
        "Ordered-index interval overlap while appending {segment_path}: {overlap_count} overlapping identity/index interval pairs (example_identity={example_identity:?}, example_index_interval={example_index_interval})"
    ))]
    PersistedIndexIntervalOverlap {
        /// Relative path of the generated segment.
        segment_path: String,
        /// Number of overlapping identity/index interval pairs.
        overlap_count: u128,
        /// First overlapping identity, or `None` for a table-wide index.
        example_identity: Option<EntityIdentity>,
        /// Internal ID of the example interval.
        example_index_interval_id: IndexIntervalId,
        /// Logical ordered-index interval represented by the example ID.
        example_index_interval: Box<IndexInterval>,
    },

    /// An entity-aware generated segment produced no entity coverage.
    #[snafu(display("No entity coverage derived while appending segment {segment_path}"))]
    EmptySegmentEntityCoverage {
        /// Relative path of the generated segment.
        segment_path: String,
    },

    /// One entity has rows but no usable ordered-index coverage.
    #[snafu(display(
        "Entity {identity:?} in segment {segment_path} has no non-null ordered-index values"
    ))]
    EntityWithoutIndexCoverage {
        /// Relative path of the generated segment.
        segment_path: String,
        /// Complete identity whose rows all have null ordered-index values.
        identity: EntityIdentity,
    },

    /// An existing segment lacks coverage metadata required by append.
    #[snafu(display(
        "Cannot append because existing segment {segment_path} is missing coverage_path"
    ))]
    ExistingSegmentMissingCoverageMetadata {
        /// Canonical segment path missing coverage metadata.
        segment_path: String,
    },

    /// Reading one existing segment's coverage sidecar during recovery failed.
    #[snafu(display(
        "Cannot recover append coverage: failed to read segment {segment_path} coverage at {coverage_path}: {source}"
    ))]
    ExistingSegmentCoverageSidecarRead {
        /// Canonical path of the segment whose sidecar could not be read.
        segment_path: String,
        /// Path of the failed coverage sidecar.
        coverage_path: String,
        /// Complete coverage sidecar failure.
        #[snafu(source(from(CoverageSidecarError, Box::new)), backtrace)]
        source: Box<CoverageSidecarError>,
    },

    /// Direct storage access for an append artifact failed.
    #[snafu(context(false), display("Storage error: {source}"))]
    Storage {
        /// Complete storage failure.
        #[snafu(source, backtrace)]
        source: StorageError,
    },

    /// Publishing the append transaction failed with a definite outcome.
    #[snafu(context(false), display("Commit error: {source}"))]
    Commit {
        /// Complete transaction-log failure.
        #[snafu(source, backtrace)]
        source: CommitError,
    },

    /// The append transaction may have committed, so its artifacts were preserved.
    #[snafu(display(
        "Commit outcome is ambiguous; generated Parquet path {segment_path} was preserved: {source}"
    ))]
    CommitAmbiguous {
        /// Generated table-relative Parquet path that was preserved.
        segment_path: String,
        /// Ambiguous transaction-log failure.
        #[snafu(source(from(CommitError, Box::new)), backtrace)]
        source: Box<CommitError>,
    },

    /// Append failed and one or more attempt-owned artifacts could not be removed.
    #[snafu(display(
        "{source}; artifact rollback also failed: [{}]",
        cleanup_errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    ))]
    Rollback {
        /// Original append failure that triggered rollback.
        #[snafu(source, backtrace)]
        source: Box<AppendError>,
        /// Typed cleanup failure for every artifact that could not be removed.
        cleanup_errors: Vec<StorageError>,
    },
}
