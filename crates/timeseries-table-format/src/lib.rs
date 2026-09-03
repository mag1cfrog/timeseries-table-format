//! # timeseries-table-format
//!
//! Append-only time-series table format with gap/overlap tracking.
//!
//! This crate is the canonical library for the table engine and optional integrations.
//!
//! ## Features
//!
//! - `datafusion` (default): Enables DataFusion integration for SQL queries
//!
//! ## Quick start
//!
//! Open an existing table (async; returns a `Future`):
//!
//! ```rust
//! use timeseries_table_format::{TableLocation, TimeSeriesTable};
//!
//! let location = TableLocation::local("./my_table");
//! let _open = TimeSeriesTable::open(location);
//! ```
//!
//! Or import the stable, supported surface via the prelude:
//!
//! ```rust
//! use timeseries_table_format::prelude::*;
//! ```
//!
//! ## Observability
//!
//! The crate emits backend-neutral structured diagnostics through [`tracing`]
//! but never installs a subscriber. Embedding applications own filtering,
//! formatting, and export. When no tracing subscriber is active, event-style
//! diagnostics are forwarded to the standard [`log`](https://docs.rs/log)
//! facade for applications that install a logger.
//!
//! The initial operation names include `table.open`, `table.create`,
//! `table.refresh`, `table.append`, `table.optimize`, `table.vacuum`, `table.scan.plan`,
//! `transaction.commit`, and `coverage.recover`. Diagnostics exclude SQL,
//! entity identities, record values, complete schemas, credentials, and
//! environment variables. `table.scan.plan` covers physical plan construction
//! only; DataFusion remains the source of query execution metrics.

pub mod coverage;
#[cfg(feature = "datafusion")]
pub mod datafusion;
pub(crate) mod formats;
pub mod metadata;
/// Convenience prelude with the stable, supported surface.
pub mod prelude;
pub mod storage;
pub mod table;
pub mod transaction_log;

pub use metadata::index::{
    IndexKind, IndexSpec, IndexSpecError, IndexValue, IndexValueError,
    ParseTimeIndexGranularityError, TimeIndexGranularity, validate_index_range,
};
pub use metadata::logical_schema::{LogicalDataType, LogicalField, LogicalSchema};
pub use metadata::protocol::TableProtocolError;
pub use metadata::table::TableMeta;
pub use storage::TableLocation;
pub use table::{
    AppendError, AppendReport, CoverageQueryError, CreateTableError, OpenTableError, OptimizeError,
    OptimizeReport, ScanError, TableError, TableStateAccessError, TimeSeriesTable, VacuumArtifact,
    VacuumArtifactDisposition, VacuumArtifactReason, VacuumError, VacuumMode, VacuumReport,
    append::{AppendRequest, IntoRecordBatchReader, ParquetCompression},
};

/// DataFusion table provider (enabled by default).
#[cfg(feature = "datafusion")]
pub use datafusion::TsTableProvider;
