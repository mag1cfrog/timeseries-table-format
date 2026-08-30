use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use std::path::PathBuf;
use timeseries_table_format::metadata::index::ParseTimeIndexGranularityError;
use timeseries_table_format::table::TableError;

use snafu::Snafu;

pub type CliResult<T> = std::result::Result<T, CliError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum CliError {
    #[snafu(display("Failed to initialize CLI diagnostics: {reason}"))]
    DiagnosticsInitialization { reason: String },

    #[snafu(display("Invalid --index-granularity '{spec}' for --index-type timestamp: {source}"))]
    InvalidTimeIndexGranularity {
        spec: String,
        source: ParseTimeIndexGranularityError,
    },

    #[snafu(display("Invalid {option} for --index-type {index_type}: {reason}"))]
    InvalidIndexOption {
        option: &'static str,
        index_type: &'static str,
        reason: String,
    },

    #[snafu(display(
        "Failed to create timeseries table at {table}. \
         Ensure the directory is writable."
    ))]
    CreateTable {
        table: String,
        #[snafu(source(from(TableError, Box::new)), backtrace)]
        source: Box<TableError>,
    },

    #[snafu(display(
        "Failed to open time-series table at {table}. \
         Ensure it is a valid timeseries-table-format table."
    ))]
    OpenTable {
        table: String,
        #[snafu(source(from(TableError, Box::new)), backtrace)]
        source: Box<TableError>,
    },

    #[snafu(display("Failed to read Parquet source {path}: {source}"))]
    ReadParquetSource { path: String, source: ParquetError },

    #[snafu(display(
        "Append failed for Parquet source {parquet} into table {table}. \
         Ensure schema matches the table and the parquet is valid: {source}"
    ))]
    AppendSegment {
        table: String,
        parquet: String,
        #[snafu(source(from(TableError, Box::new)), backtrace)]
        source: Box<TableError>,
    },

    #[snafu(display("Entity-layout optimization failed for table {table}: {source}"))]
    OptimizeTable {
        table: String,
        #[snafu(source(from(TableError, Box::new)), backtrace)]
        source: Box<TableError>,
    },

    #[snafu(display("Vacuum failed for table {table}: {source}"))]
    VacuumTable {
        table: String,
        #[snafu(source(from(TableError, Box::new)), backtrace)]
        source: Box<TableError>,
    },

    #[snafu(display("Internal path error: {message}"))]
    PathInvariantNoSource {
        message: String,
        path: Option<PathBuf>,
    },

    #[snafu(display("Storage error: {source}"))]
    Storage {
        #[snafu(backtrace)]
        source: timeseries_table_format::storage::StorageError,
    },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: ArrowError },

    #[snafu(display(
        "CSV output does not support nested field '{field}' with type {data_type}. \
         Use --format jsonl instead."
    ))]
    CsvUnsupportedType { field: String, data_type: String },
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use arrow::error::ArrowError;
    use snafu::{Backtrace, ErrorCompat};
    use timeseries_table_format::{
        coverage::CoverageSidecarError,
        storage::StorageLocation,
        table::{AppendError, CoverageQueryError, OpenTableError, TableError},
    };

    use super::CliError;

    #[test]
    fn datafusion_cli_error_preserves_coverage_query_hierarchy() {
        let storage = StorageLocation::parse("").expect_err("empty location must fail");
        let error = CliError::DataFusion {
            source: datafusion::error::DataFusionError::External(Box::new(
                TableError::CoverageQuery {
                    source: CoverageQueryError::CoverageSnapshotRead {
                        coverage_path: "_coverage/table/missing.roar".to_string(),
                        source: Box::new(CoverageSidecarError::Storage { source: storage }),
                    },
                },
            )),
        };

        let datafusion = error
            .source()
            .and_then(|source| source.downcast_ref::<datafusion::error::DataFusionError>())
            .expect("DataFusion source");
        let table = datafusion
            .source()
            .and_then(|source| source.downcast_ref::<TableError>())
            .expect("table source");
        assert!(matches!(
            table.source()
                .and_then(|source| source.downcast_ref::<CoverageQueryError>()),
            Some(CoverageQueryError::CoverageSnapshotRead {
                coverage_path: path,
                ..
            })
                if path == "_coverage/table/missing.roar"
        ));
    }

    #[test]
    fn table_cli_error_delegates_the_table_backtrace() {
        let table = TableError::Append {
            source: AppendError::ArrowInput {
                source: ArrowError::ComputeError("input failed".to_string()),
                backtrace: Backtrace::capture(),
            },
        };
        let error = CliError::AppendSegment {
            table: "table".to_string(),
            parquet: "input.parquet".to_string(),
            source: Box::new(table),
        };
        let table = error
            .source()
            .and_then(|source| source.downcast_ref::<Box<TableError>>())
            .expect("table source");
        let table_backtrace = ErrorCompat::backtrace(table).expect("table backtrace");

        assert!(std::ptr::eq(
            ErrorCompat::backtrace(&error).expect("CLI backtrace"),
            table_backtrace
        ));
    }

    #[test]
    fn open_error_message_does_not_name_an_obsolete_format_version() {
        let error = CliError::OpenTable {
            table: "table".to_string(),
            source: Box::new(TableError::Open {
                source: OpenTableError::EmptyTable,
            }),
        };
        let message = error.to_string();

        assert!(message.contains("Failed to open time-series table"));
        assert!(!message.contains("v0.1"));
    }
}
