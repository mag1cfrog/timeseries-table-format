use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use std::path::PathBuf;
use timeseries_table_format::metadata::table_metadata::ParseTimeIndexGranularityError;
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
        #[snafu(source(from(TableError, Box::new)))]
        source: Box<TableError>,
    },

    #[snafu(display(
        "Failed to open v0.1 table at {table}. \
         Ensure it is a valid timeseries-table-format table (v0.1 log format)."
    ))]
    OpenTable {
        table: String,
        #[snafu(source(from(TableError, Box::new)))]
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
        #[snafu(source(from(TableError, Box::new)))]
        source: Box<TableError>,
    },

    #[snafu(display("Entity-layout optimization failed for table {table}: {source}"))]
    OptimizeTable {
        table: String,
        #[snafu(source(from(TableError, Box::new)))]
        source: Box<TableError>,
    },

    #[snafu(display("Internal path error: {message}"))]
    PathInvariantNoSource {
        message: String,
        path: Option<PathBuf>,
    },

    #[snafu(display("Storage error: {source}"))]
    Storage {
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

    use timeseries_table_format::{
        coverage::io::CoverageSidecarError,
        storage::StorageLocation,
        table::{CoverageQueryError, TableError},
    };

    use super::CliError;

    #[test]
    fn datafusion_cli_error_preserves_coverage_query_hierarchy() {
        let storage = StorageLocation::parse("").expect_err("empty location must fail");
        let error = CliError::DataFusion {
            source: datafusion::error::DataFusionError::External(Box::new(
                TableError::CoverageQuery {
                    source: CoverageQueryError::CoverageSidecar {
                        path: "_coverage/table/missing.roar".to_string(),
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
            Some(CoverageQueryError::CoverageSidecar { path, .. })
                if path == "_coverage/table/missing.roar"
        ));
    }
}
