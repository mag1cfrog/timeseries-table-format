use std::path::PathBuf;
use timeseries_table_core::{ParseTimeBucketError, TableError};

use snafu::Snafu;

pub type CliResult<T> = std::result::Result<T, CliError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum CliError {
    #[snafu(display("Invalid --bucket '{spec}': {source}"))]
    InvalidBucket {
        spec: String,
        source: ParseTimeBucketError,
    },

    #[snafu(display("Table root not found or not accessible: {path}"))]
    TableRootMissing {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Parquet file not found or not accessible: {path}"))]
    ParquetMissing {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to create directory: {path}"))]
    CreateDirAll {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to copy parquet into table: {src} -> {dst}"))]
    CopyParquet {
        src: String,
        dst: String,
        source: std::io::Error,
    },

    #[snafu(display("Parquet path has no filename: {path}"))]
    ParquetNoFilename { path: String },

    #[snafu(display(
        "Refusing to overwrite existing file: {path}. \
         Remove it or rename the input parquet."
    ))]
    DestAlreadyExists { path: String },

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
         Ensure it is a valid timeseries-table-core table (v0.1 log format)."
    ))]
    OpenTable {
        table: String,
        #[snafu(source(from(TableError, Box::new)))]
        source: Box<TableError>,
    },

    #[snafu(display(
        "Append failed for table {table}. \
         Ensure schema matches the table and the parquet is valid."
    ))]
    AppendSegment {
        table: String,
        #[snafu(source(from(TableError, Box::new)))]
        source: Box<TableError>,
    },

    #[snafu(display("Internal path error: {message}"))]
    PathInvariant {
        message: String,
        path: Option<PathBuf>,
        source: std::io::Error,
    },

    #[snafu(display("Internal path error: {message}"))]
    PathInvariantNoSource {
        message: String,
        path: Option<PathBuf>,
    },
}
