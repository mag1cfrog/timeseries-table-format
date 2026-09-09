//! High-level operations on [`TimeSeriesTable`](super::TimeSeriesTable).

mod add_columns;
pub mod append;
mod coverage;
mod create;
mod open;
mod optimize;
mod scan;
mod state_access;
pub(crate) mod update_prepare;
pub(crate) mod update_rewrite;
mod update_rows;
mod vacuum;

pub use add_columns::AddColumnsError;
pub use append::{AppendReport, error::AppendError};
pub use coverage::CoverageQueryError;
pub use create::CreateTableError;
pub use open::OpenTableError;
pub use optimize::{OptimizeError, OptimizeReport};
pub use scan::ScanError;
pub use state_access::TableStateAccessError;
pub use update_prepare::{
    KeyValue as UpdateKeyValue, KeyViolation as UpdateKeyViolation,
    PrepareError as UpdatePreparationError, UpdateKey,
};
pub use update_rewrite::RewriteError as UpdateRewriteError;
pub use update_rows::{UpdateRowsError, UpdateRowsReport};
pub use vacuum::{
    VacuumArtifact, VacuumArtifactDisposition, VacuumArtifactReason, VacuumError, VacuumMode,
    VacuumReport,
};
