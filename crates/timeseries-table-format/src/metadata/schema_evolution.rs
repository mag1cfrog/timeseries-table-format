//! Validation of top-level nullable-column additions, without data-file IO.

use parquet::arrow::{
    ARROW_SCHEMA_META_KEY, ArrowSchemaConverter, encode_arrow_schema, parquet_to_arrow_schema,
};
use parquet::file::metadata::KeyValue;
use snafu::Snafu;

use super::{
    logical_schema::{
        ArrowToLogicalSchemaError, LogicalField, LogicalSchema, LogicalSchemaValidationError,
        LogicalToArrowSchemaError,
    },
    protocol::SCHEMA_ADD_COLUMNS_FEATURE,
    schema_compat::{SchemaCompatibilityError, ensure_index_spec_matches_schema},
    table::{TableKind, TableMeta},
};

/// A requested or persisted schema change violates the nullable-addition contract.
#[derive(Debug, Snafu)]
#[snafu(module)]
#[non_exhaustive]
pub enum SchemaEvolutionError {
    /// Column addition requires an established canonical schema.
    #[snafu(display("Column addition requires an established canonical schema"))]
    MissingSchema,
    /// At least one new column is required.
    #[snafu(display("Column addition requires at least one column"))]
    EmptyAddition,
    /// Top-level names must contain a non-whitespace character.
    #[snafu(display("Column name must not be empty or whitespace-only: {column:?}"))]
    EmptyName {
        /// Rejected column name, without normalization.
        column: String,
    },
    /// New columns must be nullable, even on an empty table.
    #[snafu(display("New column {column:?} must be nullable"))]
    NonNullableColumn {
        /// Rejected column name.
        column: String,
    },
    /// Existing fields must remain identical and in their original order.
    #[snafu(display("Schema evolution must preserve all existing fields and their order"))]
    ExistingFieldsChanged,
    /// Table kind and all ordered-index/entity-key definitions are immutable.
    #[snafu(display("Schema evolution must preserve the table kind and key definitions"))]
    TableKindOrKeysChanged,
    /// An addition was persisted without its required reader feature.
    #[snafu(display(
        "Nullable-column addition requires reader feature schema_add_columns in the same metadata update"
    ))]
    MissingReaderFeature,
    /// The fields contain duplicates or invalid nested definitions.
    #[snafu(context(false), display("Invalid logical schema: {source}"))]
    InvalidSchema {
        /// Original logical schema validation failure.
        source: LogicalSchemaValidationError,
    },
    /// A type cannot be represented by the Arrow writer/reader.
    #[snafu(
        context(false),
        display("Schema cannot be converted to Arrow: {source}")
    )]
    ArrowConversion {
        /// Original conversion failure.
        #[snafu(backtrace)]
        source: LogicalToArrowSchemaError,
    },
    /// A converted schema cannot be represented by the logical model.
    #[snafu(
        context(false),
        display("Schema cannot round-trip from Arrow: {source}")
    )]
    LogicalConversion {
        /// Original conversion failure.
        source: ArrowToLogicalSchemaError,
    },
    /// The Parquet schema conversion rejected a type or its parameters.
    #[snafu(
        context(false),
        display("Schema cannot round-trip through Parquet: {source}")
    )]
    ParquetConversion {
        /// Original Parquet schema conversion failure.
        source: parquet::errors::ParquetError,
    },
    /// Conversion would change field names, types, or nullability.
    #[snafu(display("Column definitions must round-trip exactly through Arrow and Parquet"))]
    InexactRoundTrip,
    /// The proposed schema is incompatible with the table keys.
    #[snafu(
        context(false),
        display("Schema is incompatible with table keys: {source}")
    )]
    KeySchema {
        /// Original key/schema compatibility failure.
        #[snafu(source(from(SchemaCompatibilityError, Box::new)), backtrace)]
        source: Box<SchemaCompatibilityError>,
    },
}

/// Validate only schema metadata, using the same embedded Arrow schema as our writer.
fn validate_schema_addition(
    schema: &LogicalSchema,
    added_fields: &[LogicalField],
) -> Result<(), SchemaEvolutionError> {
    for field in added_fields {
        if field.name.trim().is_empty() {
            return Err(SchemaEvolutionError::EmptyName {
                column: field.name.clone(),
            });
        }
        if !field.nullable {
            return Err(SchemaEvolutionError::NonNullableColumn {
                column: field.name.clone(),
            });
        }
    }
    // Replay may supply a deserialized schema that bypassed LogicalSchema::new.
    // Activating historical alignment requires the entire canonical schema to
    // be representable, including fields that predate this addition.
    let logical = LogicalSchema::new(schema.columns().to_vec())?;
    let arrow = logical.to_arrow_schema()?;
    let parquet = ArrowSchemaConverter::new().convert(&arrow)?;
    let metadata = vec![KeyValue::new(
        ARROW_SCHEMA_META_KEY.to_string(),
        encode_arrow_schema(&arrow),
    )];
    let restored = parquet_to_arrow_schema(&parquet, Some(&metadata))?;
    if LogicalSchema::try_from_arrow_schema(&restored)? != logical {
        return Err(SchemaEvolutionError::InexactRoundTrip);
    }
    Ok(())
}

impl TableMeta {
    /// Build a validated metadata replacement without changing this snapshot.
    pub(crate) fn with_added_columns(
        &self,
        columns: Vec<LogicalField>,
    ) -> Result<Self, SchemaEvolutionError> {
        let schema = self
            .logical_schema
            .as_ref()
            .ok_or(SchemaEvolutionError::MissingSchema)?;
        if columns.is_empty() {
            return Err(SchemaEvolutionError::EmptyAddition);
        }
        let mut fields = schema.columns().to_vec();
        fields.extend(columns);
        let mut next = self.clone();
        next.logical_schema = Some(LogicalSchema::new(fields)?);
        next.required_reader_features
            .insert(SCHEMA_ADD_COLUMNS_FEATURE.to_string());
        self.ensure_valid_schema_transition_to(&next)?;
        Ok(next)
    }

    /// Validate keys and schema changes; protocol monotonicity is checked separately.
    pub(crate) fn ensure_valid_schema_transition_to(
        &self,
        next: &Self,
    ) -> Result<(), SchemaEvolutionError> {
        if self.kind != next.kind {
            return Err(SchemaEvolutionError::TableKindOrKeysChanged);
        }
        if let Some(schema) = &next.logical_schema
            && let TableKind::TimeSeries(index) = &next.kind
        {
            ensure_index_spec_matches_schema(schema, index)?;
        }
        let Some(previous) = &self.logical_schema else {
            // First append may establish the initial schema without evolution.
            return Ok(());
        };
        let proposed = next
            .logical_schema
            .as_ref()
            .ok_or(SchemaEvolutionError::ExistingFieldsChanged)?;
        if !proposed.columns().starts_with(previous.columns()) {
            return Err(SchemaEvolutionError::ExistingFieldsChanged);
        }
        let additions = &proposed.columns()[previous.columns().len()..];
        if !additions.is_empty() {
            if !next
                .required_reader_features
                .contains(SCHEMA_ADD_COLUMNS_FEATURE)
            {
                return Err(SchemaEvolutionError::MissingReaderFeature);
            }
            validate_schema_addition(proposed, additions)?;
        }
        Ok(())
    }
}
