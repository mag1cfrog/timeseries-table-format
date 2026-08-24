use snafu::{Backtrace, Snafu};

use crate::{
    coverage::{
        EntityIdentityError, index_interval::IndexIntervalMappingError, io::CoverageSidecarError,
    },
    metadata::{index::IndexValueError, schema_compat::SchemaCompatibilityError},
};

/// Errors from table coverage queries and read-only coverage recovery.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
#[non_exhaustive]
pub enum CoverageQueryError {
    /// The requested half-open ordered-index range is invalid.
    #[snafu(display("Invalid coverage query range: {source}"))]
    InvalidRange {
        /// Complete range validation error.
        source: IndexValueError,
        /// Backtrace captured at the coverage query boundary.
        backtrace: Box<Backtrace>,
    },

    /// An ordered value could not be mapped to its index interval ID.
    #[snafu(display("Coverage query index interval mapping failed: {source}"))]
    IndexIntervalMapping {
        /// Complete interval mapping error.
        source: IndexIntervalMappingError,
        /// Backtrace captured at the coverage query boundary.
        backtrace: Box<Backtrace>,
    },

    /// Constructing a canonical entity identity failed.
    #[snafu(display("Invalid coverage query entity identity: {source}"))]
    InvalidEntityIdentity {
        /// Complete entity identity construction error.
        source: EntityIdentityError,
        /// Backtrace captured at the coverage query boundary.
        backtrace: Box<Backtrace>,
    },

    /// The query identity is incompatible with the table schema.
    #[snafu(display("Coverage query schema compatibility error: {source}"))]
    SchemaCompatibility {
        /// Complete schema compatibility error.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)), backtrace)]
        source: Box<SchemaCompatibilityError>,
    },

    /// An identity-free query was used on an entity-aware table.
    #[snafu(display(
        "Entity identity is required for coverage queries; configured entity columns: {entity_columns:?}"
    ))]
    EntityIdentityRequired {
        /// Entity columns that require values from the caller.
        entity_columns: Vec<String>,
        /// Backtrace captured at the coverage query boundary.
        backtrace: Box<Backtrace>,
    },

    /// An entity-aware query was used on a table with global coverage.
    #[snafu(display("Table has no configured entity columns"))]
    EntityIdentityNotConfigured {
        /// Backtrace captured at the coverage query boundary.
        backtrace: Box<Backtrace>,
    },

    /// A required entity column has no caller-provided value.
    #[snafu(display("Missing entity identity component for column {column}"))]
    MissingEntityIdentityColumn {
        /// Configured entity column missing from the caller input.
        column: String,
        /// Backtrace captured at the coverage query boundary.
        backtrace: Box<Backtrace>,
    },

    /// Caller input repeats one entity column.
    #[snafu(display("Duplicate entity identity component for column {column}"))]
    DuplicateEntityIdentityColumn {
        /// Repeated entity column name.
        column: String,
        /// Backtrace captured at the coverage query boundary.
        backtrace: Box<Backtrace>,
    },

    /// Caller input contains a column outside the entity identity.
    #[snafu(display("Unexpected entity identity component for column {column}"))]
    UnexpectedEntityIdentityColumn {
        /// Unknown entity column name.
        column: String,
        /// Backtrace captured at the coverage query boundary.
        backtrace: Box<Backtrace>,
    },

    /// Reading a table coverage snapshot sidecar failed.
    #[snafu(display("Failed to read table coverage sidecar {coverage_path}: {source}"))]
    CoverageSnapshotRead {
        /// Table-relative sidecar path.
        coverage_path: String,
        /// Complete sidecar error, including storage or codec detail.
        #[snafu(source(from(CoverageSidecarError, Box::new)), backtrace)]
        source: Box<CoverageSidecarError>,
    },

    /// An existing segment has no coverage sidecar path.
    #[snafu(display("Existing segment {segment_path} is missing coverage_path"))]
    ExistingSegmentMissingCoverageMetadata {
        /// Canonical segment path missing a coverage path.
        segment_path: String,
        /// Backtrace captured at the coverage query boundary.
        backtrace: Box<Backtrace>,
    },

    /// Reading a segment sidecar failed during read-only recovery.
    #[snafu(display(
        "Failed to recover table coverage from segment {segment_path} sidecar {coverage_path}: {source}"
    ))]
    SegmentCoverageSidecarRead {
        /// Canonical segment path.
        segment_path: String,
        /// Table-relative sidecar path.
        coverage_path: String,
        /// Complete sidecar error, including storage or codec detail.
        #[snafu(source(from(CoverageSidecarError, Box::new)), backtrace)]
        source: Box<CoverageSidecarError>,
    },

    /// Table state has segments but no coverage snapshot pointer.
    #[snafu(display("Table has segments but no table coverage snapshot pointer"))]
    MissingTableCoveragePointer {
        /// Backtrace captured at the coverage query boundary.
        backtrace: Box<Backtrace>,
    },
}
