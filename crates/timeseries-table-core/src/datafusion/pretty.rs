//! Pretty-print helpers for Arrow `RecordBatch` output.
//!
//! DataFusion's `DataFrame::show()` prints `f32`/`f64` values using their full binary-float
//! representation, which can result in distracting artifacts like `115.38499999999999`.
//!
//! This module provides a small wrapper around Arrow's pretty printer that formats floating point
//! values with a fixed maximum number of decimal places and trims trailing zeros.

use arrow::{
    array::{Array, Float32Array, Float64Array, RecordBatch},
    datatypes::DataType,
    error::ArrowError,
    util::{
        display::{
            ArrayFormatter, ArrayFormatterFactory, DisplayIndex, FormatOptions, FormatResult,
        },
        pretty::pretty_format_batches_with_options,
    },
};

/// Default maximum number of decimal places used for `f32`/`f64` values.
pub const DEFAULT_FLOAT_MAX_DECIMALS: usize = 6;

#[derive(Debug)]
struct CompactFloatFormatterFactory {
    max_decimals: usize,
}

impl CompactFloatFormatterFactory {
    fn new(max_decimals: usize) -> Self {
        Self {
            max_decimals: max_decimals.min(15),
        }
    }
}

#[derive(Debug)]
struct CompactFloat64Formatter<'a> {
    array: &'a Float64Array,
    null: &'a str,
    max_decimals: usize,
}

impl DisplayIndex for CompactFloat64Formatter<'_> {
    fn write(&self, idx: usize, f: &mut dyn std::fmt::Write) -> FormatResult {
        if self.array.is_null(idx) {
            write!(f, "{}", self.null)?;
            return Ok(());
        }

        let value = self.array.value(idx);
        let s = format_compact_float(value, self.max_decimals);
        write!(f, "{s}")?;
        Ok(())
    }
}

#[derive(Debug)]
struct CompactFloat32Formatter<'a> {
    array: &'a Float32Array,
    null: &'a str,
    max_decimals: usize,
}

impl DisplayIndex for CompactFloat32Formatter<'_> {
    fn write(&self, idx: usize, f: &mut dyn std::fmt::Write) -> FormatResult {
        if self.array.is_null(idx) {
            write!(f, "{}", self.null)?;
            return Ok(());
        }

        let value = self.array.value(idx) as f64;
        let s = format_compact_float(value, self.max_decimals);
        write!(f, "{s}")?;
        Ok(())
    }
}

impl ArrayFormatterFactory for CompactFloatFormatterFactory {
    fn create_array_formatter<'formatter>(
        &self,
        array: &'formatter dyn Array,
        options: &FormatOptions<'formatter>,
        _field: Option<&'formatter arrow::datatypes::Field>,
    ) -> Result<Option<ArrayFormatter<'formatter>>, ArrowError> {
        match array.data_type() {
            DataType::Float64 => {
                let array = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        ArrowError::CastError("expected Float64Array for Float64".to_string())
                    })?;

                let display_index = Box::new(CompactFloat64Formatter {
                    array,
                    null: options.null(),
                    max_decimals: self.max_decimals,
                });
                Ok(Some(ArrayFormatter::new(display_index, options.safe())))
            }
            DataType::Float32 => {
                let array = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        ArrowError::CastError("expected Float32Array for Float32".to_string())
                    })?;

                let display_index = Box::new(CompactFloat32Formatter {
                    array,
                    null: options.null(),
                    max_decimals: self.max_decimals,
                });
                Ok(Some(ArrayFormatter::new(display_index, options.safe())))
            }
            _ => Ok(None),
        }
    }
}

fn format_compact_float(value: f64, max_decimals: usize) -> String {
    if !value.is_finite() {
        return value.to_string();
    }

    let prec = max_decimals.min(15);
    let mut s = format!("{value:.prec$}", prec = prec);
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }

    if s == "-0" {
        s.clear();
        s.push('0');
    }

    s
}

/// Pretty-format Arrow record batches, using compact `f32`/`f64` formatting.
///
/// This is intended for human-facing output (README examples, CLI previews, logs), not for
/// round-tripping numeric values.
pub fn pretty_format_batches_compact_floats(batches: &[RecordBatch]) -> Result<String, ArrowError> {
    pretty_format_batches_compact_floats_with_max_decimals(batches, DEFAULT_FLOAT_MAX_DECIMALS)
}

/// Same as [`pretty_format_batches_compact_floats`], but allows setting the maximum decimal places.
pub fn pretty_format_batches_compact_floats_with_max_decimals(
    batches: &[RecordBatch],
    max_decimals: usize,
) -> Result<String, ArrowError> {
    let factory = CompactFloatFormatterFactory::new(max_decimals);
    let options = FormatOptions::default().with_formatter_factory(Some(&factory));
    let rendered = pretty_format_batches_with_options(batches, &options)?;
    Ok(rendered.to_string())
}
