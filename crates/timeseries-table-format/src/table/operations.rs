//! High-level operations on [`TimeSeriesTable`](super::TimeSeriesTable).

mod add_columns;
pub mod append;
mod coverage;
mod create;
mod open;
mod optimize;
mod scan;
mod state_access;
mod vacuum;

pub use add_columns::AddColumnsError;
pub use append::{AppendReport, error::AppendError};
pub use coverage::CoverageQueryError;
pub use create::CreateTableError;
pub use open::OpenTableError;
pub use optimize::{OptimizeError, OptimizeReport};
pub use scan::ScanError;
pub use state_access::TableStateAccessError;
pub use vacuum::{
    VacuumArtifact, VacuumArtifactDisposition, VacuumArtifactReason, VacuumError, VacuumMode,
    VacuumReport,
};
