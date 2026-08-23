//! High-level operations on [`TimeSeriesTable`](super::TimeSeriesTable).

pub mod append;
mod append_schema;
mod coverage;
mod create;
mod open;
mod optimize;
mod scan;
mod state_access;

pub use append::error::AppendError;
pub use coverage::CoverageQueryError;
pub use create::CreateTableError;
pub use open::OpenTableError;
pub use optimize::{OptimizeError, OptimizeReport};
pub use scan::ScanError;
pub use state_access::TableStateAccessError;
