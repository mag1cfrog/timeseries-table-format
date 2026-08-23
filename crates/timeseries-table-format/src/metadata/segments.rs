//! Segment formats and per-file metadata recorded in table metadata.
//!
//! This module contains **pure** data types + non-IO validation/decoding errors.
//! Any functions that touch storage backends (filesystem, object store, etc.)
//! must live outside `metadata/` (for example under `transaction_log` or
//! format-specific helpers).

use parquet::errors::ParquetError;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, prelude::*};

use crate::{
    coverage::EntityIdentity,
    metadata::{
        logical_schema::{ArrowToLogicalSchemaError, LogicalSchemaValidationError},
        table_metadata::{IndexKind, IndexValue, IndexValueError},
    },
};

/// Supported on-disk file formats for segments.
///
/// In v0.1, only `Parquet` is implemented, but the enum keeps the metadata model
/// open to other formats in future versions.
///
/// JSON layout example: `"format": "parquet"`
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    /// Apache Parquet columnar format.
    #[default]
    Parquet,
    // Future:
    // Orc,
    // Avro,
    // Csv,
}

/// Entity distribution within one physical segment.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SegmentEntityLayout {
    /// The table has no configured entity columns.
    NotApplicable,
    /// Every row belongs to one complete entity identity.
    Single(EntityIdentity),
    /// Rows belong to more than one complete entity identity.
    Mixed,
}

/// Metadata about a single physical segment.
///
/// In v0.1, a "segment" corresponds to one stored data object.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentMeta {
    /// Canonical file path relative to the table root and the segment identity.
    pub path: String,

    /// File format for this segment.
    pub format: FileFormat,

    /// Exact entity distribution derived before the segment is committed.
    pub entity_layout: SegmentEntityLayout,

    /// Minimum observed ordered-index value in this segment (inclusive).
    pub index_min: IndexValue,

    /// Maximum observed ordered-index value in this segment (inclusive).
    pub index_max: IndexValue,

    /// Number of rows in this segment.
    pub row_count: u64,

    /// Optional file size in bytes at the time metadata was captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,

    /// Coverage sidecar pointer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coverage_path: Option<String>,
}

/// Exact Parquet ordered-index column validation failure.
#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
#[snafu(display(
    "Invalid ordered-index column {column} in segment at {path}: expected {expected_domain}, observed {observed_type}"
))]
pub struct ParquetIndexColumnError {
    /// Normalized path to the segment file.
    pub path: String,
    /// Registered top-level ordered-index column.
    pub column: String,
    /// Registered ordered-index domain.
    pub expected_domain: &'static str,
    /// Observed Parquet column shape and annotations.
    pub observed_type: String,
}

impl SegmentMeta {
    /// Set the coverage sidecar path for this segment metadata.
    pub fn with_coverage_path(mut self, path: impl Into<String>) -> Self {
        self.coverage_path = Some(path.into());
        self
    }

    /// Validate the segment's inclusive ordered-index bounds.
    ///
    /// # Errors
    /// Returns [`SegmentMetaError::InvalidIndexBounds`] when either bound has
    /// the wrong domain or the minimum is greater than the maximum.
    pub fn validate_bounds(&self, kind: &IndexKind) -> Result<(), SegmentMetaError> {
        self.index_min.validate_kind(kind).map_err(|source| {
            SegmentMetaError::InvalidIndexBounds {
                path: self.path.clone(),
                source,
            }
        })?;
        self.index_max.validate_kind(kind).map_err(|source| {
            SegmentMetaError::InvalidIndexBounds {
                path: self.path.clone(),
                source,
            }
        })?;
        if self
            .index_min
            .compare(&self.index_max)
            .map_err(|source| SegmentMetaError::InvalidIndexBounds {
                path: self.path.clone(),
                source,
            })?
            .is_gt()
        {
            return Err(SegmentMetaError::InvalidIndexBounds {
                path: self.path.clone(),
                source: IndexValueError::InvalidBounds {
                    min: self.index_min.clone(),
                    max: self.index_max.clone(),
                },
            });
        }
        Ok(())
    }
}

/// Errors that can occur while validating or decoding segment metadata.
///
/// This enum intentionally contains **no storage backend errors**. IO-related
/// errors should be wrapped at the IO boundary (for example, in
/// [`crate::transaction_log::SegmentError`]).
#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum SegmentMetaError {
    /// Persisted segment bounds violate the table's ordered-index domain.
    #[snafu(display("Invalid ordered-index bounds in segment at {path}: {source}"))]
    InvalidIndexBounds {
        /// Segment path containing invalid bounds.
        path: String,
        /// Domain or ordering failure.
        source: IndexValueError,
    },

    /// The file is too short to be a valid Parquet file.
    #[snafu(display("Segment file too short to be valid Parquet: {path}"))]
    TooShort {
        /// The path to the file that was too short.
        path: String,
    },

    /// Parquet reader / metadata failure.
    #[snafu(display("Error reading Parquet metadata for segment at {path}: {source}"))]
    ParquetRead {
        /// The path to the file that caused the Parquet read failure.
        path: String,
        /// Underlying parquet error that caused this failure.
        source: ParquetError,
        /// Diagnostic backtrace for this error.
        backtrace: Backtrace,
    },

    /// A parallel row-group inspection task failed before returning its typed result.
    #[snafu(display("Row-group inspection task failed for segment at {path}: {source}"))]
    RowGroupTask {
        /// Segment path being inspected.
        path: String,
        /// Tokio task failure, including panic or cancellation details.
        source: tokio::task::JoinError,
        /// Backtrace captured while joining the row-group task.
        backtrace: Backtrace,
    },

    /// The registered ordered-index column is missing or incompatible.
    #[snafu(transparent)]
    OrderedIndexColumn {
        /// Exact registered and observed Parquet column details.
        source: ParquetIndexColumnError,
    },

    /// Statistics exist but are not well-shaped (wrong length / unexpected type).
    #[snafu(display(
        "Parquet statistics shape invalid for {column} in segment at {path}: {detail}"
    ))]
    ParquetStatsShape {
        /// The path to the file with malformed Parquet statistics.
        path: String,
        /// The column whose statistics are malformed.
        column: String,
        /// Details about how the statistics are malformed.
        detail: String,
    },

    /// The file contains no non-null value for the registered ordered index.
    #[snafu(display(
        "No observed {expected_domain} value for ordered-index column {column} in segment at {path}"
    ))]
    NoObservedIndexValue {
        /// Path to the segment file.
        path: String,
        /// Registered ordered-index column.
        column: String,
        /// Registered ordered-index domain.
        expected_domain: &'static str,
    },

    /// Failed to derive a valid LogicalSchema from the Parquet file.
    #[snafu(display("Invalid logical schema derived from Parquet at {path}: {source}"))]
    LogicalSchemaInvalid {
        /// The path to the file without a valid LogicalSchema.
        path: String,
        /// Underlying logical schema error that triggered this failure.
        #[snafu(source(from(LogicalSchemaValidationError, Box::new)))]
        source: Box<LogicalSchemaValidationError>,
        /// Backtrace captured with segment path context.
        backtrace: Backtrace,
    },

    /// An embedded Arrow schema cannot be represented by the logical schema model.
    #[snafu(display("Invalid Arrow schema derived from Parquet at {path}: {source}"))]
    ArrowToLogicalSchema {
        /// Segment path containing the embedded Arrow schema.
        path: String,
        /// Exact Arrow-to-logical conversion failure.
        #[snafu(source(from(ArrowToLogicalSchemaError, Box::new)))]
        source: Box<ArrowToLogicalSchemaError>,
        /// Backtrace captured with segment path context.
        backtrace: Backtrace,
    },
}

/// Deterministic ordering for segments by ordered-index bounds.
///
/// Ordering is by `index_min`, then `index_max`, and finally `path` as a stable
/// tie-breaker.
pub(crate) fn cmp_segment_meta_by_index(
    a: &SegmentMeta,
    b: &SegmentMeta,
) -> Result<std::cmp::Ordering, IndexValueError> {
    let min_order = a.index_min.compare(&b.index_min)?;
    if !min_order.is_eq() {
        return Ok(min_order);
    }
    let max_order = a.index_max.compare(&b.index_max)?;
    Ok(max_order.then_with(|| a.path.cmp(&b.path)))
}

/// Sort segment metadata by typed bounds, rejecting cross-domain values first.
pub(crate) fn sort_segment_meta_by_index<T>(segments: &mut [T]) -> Result<(), IndexValueError>
where
    T: std::borrow::Borrow<SegmentMeta>,
{
    let mut domain: Option<&IndexValue> = None;
    for segment in segments.iter().map(std::borrow::Borrow::borrow) {
        if segment.index_min.compare(&segment.index_max)?.is_gt() {
            return Err(IndexValueError::InvalidBounds {
                min: segment.index_min.clone(),
                max: segment.index_max.clone(),
            });
        }
        if let Some(domain) = domain {
            domain.compare(&segment.index_min)?;
            domain.compare(&segment.index_max)?;
        } else {
            domain = Some(&segment.index_min);
        }
    }

    let mut sort_error = None;
    segments.sort_unstable_by(
        |a, b| match cmp_segment_meta_by_index(a.borrow(), b.borrow()) {
            Ok(order) => order,
            Err(error) => {
                sort_error.get_or_insert(error);
                std::cmp::Ordering::Equal
            }
        },
    );
    if let Some(error) = sort_error {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use chrono::{TimeZone, Utc};

    fn seg(id: &str, ts_min: i64, ts_max: i64) -> SegmentMeta {
        SegmentMeta {
            path: format!("data/{id}.parquet"),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: IndexValue::Timestamp(Utc.timestamp_opt(ts_min, 0).single().unwrap()),
            index_max: IndexValue::Timestamp(Utc.timestamp_opt(ts_max, 0).single().unwrap()),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        }
    }

    #[test]
    fn entity_layout_json_roundtrips_are_stable() {
        let cases = [
            (SegmentEntityLayout::NotApplicable, "\"NotApplicable\""),
            (
                SegmentEntityLayout::Single(
                    EntityIdentity::try_new(vec!["us".into(), "device-1".into()]).unwrap(),
                ),
                r#"{"Single":[{"type":"utf8","value":"us"},{"type":"utf8","value":"device-1"}]}"#,
            ),
            (SegmentEntityLayout::Mixed, "\"Mixed\""),
        ];

        for (layout, expected_json) in cases {
            let json = serde_json::to_string(&layout).unwrap();
            assert_eq!(json, expected_json);
            assert_eq!(
                serde_json::from_str::<SegmentEntityLayout>(&json).unwrap(),
                layout
            );
        }
    }

    #[test]
    fn single_layout_rejects_an_empty_identity() {
        let error = serde_json::from_str::<SegmentEntityLayout>(r#"{"Single":[]}"#)
            .expect_err("empty identity must be rejected");
        assert!(error.to_string().contains("at least one component"));
    }

    #[test]
    fn ordering_is_deterministic_with_tie_breakers() {
        let mut v = vec![
            seg("c", 10, 20),
            seg("b", 10, 20),
            seg("a", 10, 30),
            seg("d", 5, 7),
        ];

        v.sort_unstable_by(|a, b| cmp_segment_meta_by_index(a, b).expect("matching domains"));

        let paths: Vec<String> = v.into_iter().map(|s| s.path).collect();
        assert_eq!(
            paths,
            vec![
                "data/d.parquet",
                "data/b.parquet",
                "data/c.parquet",
                "data/a.parquet"
            ]
        );
    }

    #[test]
    fn ordering_is_equal_for_identical_segments() {
        let a = seg("same", 10, 20);
        let b = seg("same", 10, 20);
        assert_eq!(
            cmp_segment_meta_by_index(&a, &b).unwrap(),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            cmp_segment_meta_by_index(&b, &a).unwrap(),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn ordering_primary_key_ts_min_dominates() {
        let mut v = vec![seg("z", 20, 30), seg("a", 10, 50), seg("m", 15, 10)];

        v.sort_unstable_by(|a, b| cmp_segment_meta_by_index(a, b).expect("matching domains"));

        let paths: Vec<String> = v.into_iter().map(|s| s.path).collect();
        assert_eq!(
            paths,
            vec!["data/a.parquet", "data/m.parquet", "data/z.parquet"]
        );
    }

    #[test]
    fn ordering_uses_path_as_final_tie_breaker() {
        let mut v = vec![seg("b", 10, 20), seg("a", 10, 20), seg("c", 10, 20)];

        v.sort_unstable_by(|a, b| cmp_segment_meta_by_index(a, b).expect("matching domains"));

        let paths: Vec<String> = v.into_iter().map(|s| s.path).collect();
        assert_eq!(
            paths,
            vec!["data/a.parquet", "data/b.parquet", "data/c.parquet"]
        );
    }

    #[test]
    fn segment_bounds_validate_domain_and_native_order() {
        let signed = IndexKind::Int64 {
            index_granularity: NonZeroU64::new(1).unwrap(),
        };
        let valid = SegmentMeta {
            path: "data/valid.parquet".to_string(),
            format: FileFormat::Parquet,
            entity_layout: SegmentEntityLayout::NotApplicable,
            index_min: IndexValue::Int64(i64::MIN),
            index_max: IndexValue::Int64(i64::MAX),
            row_count: 1,
            file_size: None,
            coverage_path: None,
        };
        valid.validate_bounds(&signed).unwrap();

        let reversed = SegmentMeta {
            index_min: IndexValue::Int64(1),
            index_max: IndexValue::Int64(0),
            ..valid.clone()
        };
        assert!(matches!(
            reversed.validate_bounds(&signed),
            Err(SegmentMetaError::InvalidIndexBounds {
                source: IndexValueError::InvalidBounds { .. },
                ..
            })
        ));

        let wrong_domain = SegmentMeta {
            index_min: IndexValue::UInt64(0),
            index_max: IndexValue::UInt64(u64::MAX),
            ..valid
        };
        assert!(matches!(
            wrong_domain.validate_bounds(&signed),
            Err(SegmentMetaError::InvalidIndexBounds {
                source: IndexValueError::KindMismatch { .. },
                ..
            })
        ));
    }

    #[test]
    fn integer_segment_ordering_uses_native_bounds_then_path() {
        let mut segments = vec![
            SegmentMeta {
                path: "data/z.parquet".to_string(),
                format: FileFormat::Parquet,
                entity_layout: SegmentEntityLayout::NotApplicable,
                index_min: IndexValue::UInt64(u64::MAX),
                index_max: IndexValue::UInt64(u64::MAX),
                row_count: 1,
                file_size: None,
                coverage_path: None,
            },
            SegmentMeta {
                path: "data/b.parquet".to_string(),
                format: FileFormat::Parquet,
                entity_layout: SegmentEntityLayout::NotApplicable,
                index_min: IndexValue::UInt64(0),
                index_max: IndexValue::UInt64(7),
                row_count: 1,
                file_size: None,
                coverage_path: None,
            },
            SegmentMeta {
                path: "data/a.parquet".to_string(),
                format: FileFormat::Parquet,
                entity_layout: SegmentEntityLayout::NotApplicable,
                index_min: IndexValue::UInt64(0),
                index_max: IndexValue::UInt64(7),
                row_count: 1,
                file_size: None,
                coverage_path: None,
            },
        ];
        segments.sort_unstable_by(|a, b| cmp_segment_meta_by_index(a, b).unwrap());
        assert_eq!(
            segments
                .into_iter()
                .map(|segment| segment.path)
                .collect::<Vec<_>>(),
            vec!["data/a.parquet", "data/b.parquet", "data/z.parquet"]
        );
    }

    #[test]
    fn sorting_rejects_cross_domain_segments() {
        let mut segments = vec![
            seg("timestamp", 0, 1),
            SegmentMeta {
                path: "data/integer.parquet".to_string(),
                format: FileFormat::Parquet,
                entity_layout: SegmentEntityLayout::NotApplicable,
                index_min: IndexValue::Int64(0),
                index_max: IndexValue::Int64(1),
                row_count: 1,
                file_size: None,
                coverage_path: None,
            },
        ];

        assert!(matches!(
            sort_segment_meta_by_index(&mut segments),
            Err(IndexValueError::DomainMismatch { .. })
        ));
    }

    #[test]
    fn segment_json_preserves_integer_bound_extremes() {
        for (minimum, maximum) in [
            (IndexValue::Int64(i64::MIN), IndexValue::Int64(i64::MAX)),
            (IndexValue::UInt64(0), IndexValue::UInt64(u64::MAX)),
        ] {
            let segment = SegmentMeta {
                path: "data/extremes.parquet".to_string(),
                format: FileFormat::Parquet,
                entity_layout: SegmentEntityLayout::NotApplicable,
                index_min: minimum,
                index_max: maximum,
                row_count: 2,
                file_size: Some(42),
                coverage_path: Some("_coverage/segments/extremes.roar".to_string()),
            };
            let json = serde_json::to_string(&segment).unwrap();
            assert_eq!(serde_json::from_str::<SegmentMeta>(&json).unwrap(), segment);
        }
    }
}
