//! Coverage on-disk layout helpers.
//!
//! These helpers define:
//! - how coverage ids are validated
//! - how coverage sidecar keys are constructed (relative to the table root)
//! - deterministic id derivation helpers for per-segment and table snapshots
//!
//! Note: these functions return canonical slash-separated object keys. The
//! storage backend is responsible for resolving them under its table root.

use snafu::Snafu;
use uuid::Uuid;

use crate::metadata::table_metadata::{IndexKind, IndexSpec, TimeBucket};

/// Root directory for coverage data.
pub const COVERAGE_ROOT_DIR: &str = "_coverage";
/// Directory for segment coverage data.
pub const SEGMENT_COVERAGE_DIR: &str = "_coverage/segments";
/// Directory for table snapshot coverage data.
pub const TABLE_SNAPSHOT_DIR: &str = "_coverage/table";
/// File extension for coverage files.
pub const COVERAGE_EXT: &str = "roar";

/// Errors that can occur during coverage layout operations.
#[derive(Debug, Snafu)]
pub enum CoverageLayoutError {
    /// Returned when an invalid coverage ID is provided.
    #[snafu(display("Invalid coverage id: {coverage_id}"))]
    InvalidCoverageId {
        /// The invalid coverage ID.
        coverage_id: String,
    },
}

/// Validates that a coverage ID meets security and format requirements.
///
/// A valid coverage ID must:
/// - Not be empty and not exceed 128 characters
/// - Not contain path separators (`/`, `\\`) or `..` sequences
/// - Only contain ASCII alphanumeric characters, dots, underscores, and hyphens
pub fn validate_coverage_id(coverage_id: &str) -> Result<(), CoverageLayoutError> {
    if coverage_id.is_empty() || coverage_id.len() > 128 {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    // Require at least one alphanumeric
    if !coverage_id.chars().any(|c| c.is_ascii_alphanumeric()) {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    // Reject leading dot
    if coverage_id.starts_with('.') {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    // Reject any path separator and any ".." component-ish content.
    if coverage_id.contains('/') || coverage_id.contains('\\') || coverage_id.contains("..") {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    // Restrict to a conservative ASCII allowlist.
    let ok = coverage_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));

    if !ok {
        return Err(CoverageLayoutError::InvalidCoverageId {
            coverage_id: coverage_id.to_string(),
        });
    }

    Ok(())
}

/// Relative object key: `_coverage/segments/<coverage_id>.roar`
pub fn segment_coverage_key(coverage_id: &str) -> Result<String, CoverageLayoutError> {
    validate_coverage_id(coverage_id)?;
    Ok(format!(
        "{SEGMENT_COVERAGE_DIR}/{coverage_id}.{COVERAGE_EXT}"
    ))
}

/// Relative object key: `_coverage/table/<version>-<snapshot_id>.roar`
pub fn table_snapshot_key(version: u64, snapshot_id: &str) -> Result<String, CoverageLayoutError> {
    validate_coverage_id(snapshot_id)?;
    Ok(format!(
        "{TABLE_SNAPSHOT_DIR}/{version}-{snapshot_id}.{COVERAGE_EXT}"
    ))
}

fn coverage_id_v2(
    domain_prefix: &[u8],
    output_prefix: &str,
    index: &IndexSpec,
    coverage_bytes: &[u8],
) -> String {
    let mut h = blake3::Hasher::new();

    // domain separation
    h.update(domain_prefix);
    h.update(b"\0");

    h.update(index.column.as_bytes());
    h.update(b"\0");

    match &index.kind {
        IndexKind::Timestamp { bucket, timezone } => {
            h.update(b"T");
            hash_time_bucket(&mut h, bucket);
            h.update(b"\0");
            match timezone {
                Some(timezone) => {
                    h.update(b"S");
                    h.update(timezone.as_bytes());
                }
                None => {
                    h.update(b"N");
                }
            }
        }
        IndexKind::Int64 { bucket_width } => {
            h.update(b"I");
            h.update(&bucket_width.get().to_le_bytes());
        }
        IndexKind::UInt64 { bucket_width } => {
            h.update(b"U");
            h.update(&bucket_width.get().to_le_bytes());
        }
    }

    h.update(b"\0");
    h.update(coverage_bytes);

    let hex = h.finalize().to_hex();
    format!("{output_prefix}-{}", &hex[..32])
}

fn hash_time_bucket(hasher: &mut blake3::Hasher, bucket: &TimeBucket) {
    match bucket {
        TimeBucket::Seconds(n) => {
            hasher.update(b"S");
            hasher.update(&n.to_le_bytes());
        }
        TimeBucket::Minutes(n) => {
            hasher.update(b"M");
            hasher.update(&n.to_le_bytes());
        }
        TimeBucket::Hours(n) => {
            hasher.update(b"H");
            hasher.update(&n.to_le_bytes());
        }
        TimeBucket::Days(n) => {
            hasher.update(b"D");
            hasher.update(&n.to_le_bytes());
        }
    }
}

/// Deterministically derive a safe content id for segment coverage.
pub fn segment_coverage_id_v2(index: &IndexSpec, coverage_bytes: &[u8]) -> String {
    coverage_id_v2(b"segcov-v2", "segcov", index, coverage_bytes)
}

/// Deterministically derive a safe content id for table snapshot coverage.
pub fn table_coverage_id_v2(index: &IndexSpec, coverage_bytes: &[u8]) -> String {
    coverage_id_v2(b"tblcov-v2", "tblcov", index, coverage_bytes)
}

/// Add a writer-owned suffix to a deterministic coverage content id.
pub(crate) fn coverage_file_id_for_attempt(content_id: &str, attempt_id: &Uuid) -> String {
    format!("{content_id}-{attempt_id}")
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    fn timestamp_index(column: &str, bucket: TimeBucket) -> IndexSpec {
        IndexSpec {
            column: column.to_string(),
            entity_columns: Vec::new(),
            kind: IndexKind::Timestamp {
                bucket,
                timezone: None,
            },
        }
    }

    #[test]
    fn validate_coverage_id_accepts_valid_ids() {
        let long = "a".repeat(128);
        let valid_ids = ["abc", "A_B-1.2", long.as_str()];

        for id in valid_ids {
            validate_coverage_id(id).expect("valid id should pass");
        }
    }

    #[test]
    fn validate_coverage_id_rejects_empty_or_too_long() {
        let too_long = "x".repeat(129);
        assert!(validate_coverage_id("").is_err());
        assert!(validate_coverage_id(&too_long).is_err());
    }

    #[test]
    fn validate_coverage_id_rejects_path_components() {
        for id in ["a/b", "a\\b", "a..b", "..", "../etc"] {
            assert!(validate_coverage_id(id).is_err(), "id `{id}` should fail");
        }
    }

    #[test]
    fn validate_coverage_id_rejects_disallowed_chars() {
        for id in ["space id", "id*", "id@", "id$", "id:"] {
            assert!(validate_coverage_id(id).is_err(), "id `{id}` should fail");
        }
    }

    #[test]
    fn segment_coverage_key_formats_and_validates() {
        let id = "seg-001";
        let key = segment_coverage_key(id).expect("valid id");
        assert_eq!(key, "_coverage/segments/seg-001.roar");

        // Ensure validation runs
        assert!(segment_coverage_key("bad/id").is_err());
    }

    #[test]
    fn table_snapshot_key_formats() {
        let key = table_snapshot_key(42, "snap-001").expect("valid snapshot id");
        assert_eq!(key, "_coverage/table/42-snap-001.roar");
    }

    #[test]
    fn segment_coverage_id_is_deterministic_and_valid() {
        let index = timestamp_index("ts", TimeBucket::Minutes(1));
        let bytes = b"bitmap-bytes";

        let id1 = segment_coverage_id_v2(&index, bytes);
        let id2 = segment_coverage_id_v2(&index, bytes);

        assert_eq!(id1, id2, "same inputs must produce stable id");
        assert!(id1.starts_with("segcov-"));
        assert_eq!(id1.len(), "segcov-".len() + 32, "prefix + 32 hex chars");
        validate_coverage_id(&id1).expect("derived id should be valid");
    }

    #[test]
    fn segment_coverage_id_changes_with_inputs() {
        let bytes = b"bytes";

        let base_index = timestamp_index("ts", TimeBucket::Seconds(5));
        let base = segment_coverage_id_v2(&base_index, bytes);
        let different_bucket =
            segment_coverage_id_v2(&timestamp_index("ts", TimeBucket::Hours(5)), bytes);
        let different_column = segment_coverage_id_v2(
            &timestamp_index("event_time", TimeBucket::Seconds(5)),
            bytes,
        );
        let different_kind = segment_coverage_id_v2(
            &IndexSpec {
                column: "ts".to_string(),
                entity_columns: Vec::new(),
                kind: IndexKind::UInt64 {
                    bucket_width: NonZeroU64::new(5).unwrap(),
                },
            },
            bytes,
        );
        let different_integer_domain = segment_coverage_id_v2(
            &IndexSpec {
                column: "ts".to_string(),
                entity_columns: Vec::new(),
                kind: IndexKind::Int64 {
                    bucket_width: NonZeroU64::new(5).unwrap(),
                },
            },
            bytes,
        );
        let different_width = segment_coverage_id_v2(
            &IndexSpec {
                column: "ts".to_string(),
                entity_columns: Vec::new(),
                kind: IndexKind::UInt64 {
                    bucket_width: NonZeroU64::new(6).unwrap(),
                },
            },
            bytes,
        );
        let different_bytes = segment_coverage_id_v2(&base_index, b"other");

        assert_ne!(base, different_bucket, "bucket spec should affect id");
        assert_ne!(base, different_column, "index column should affect id");
        assert_ne!(base, different_kind, "index kind should affect id");
        assert_ne!(different_kind, different_integer_domain);
        assert_ne!(different_kind, different_width);
        assert_ne!(base, different_bytes, "coverage bytes should affect id");
    }

    #[test]
    fn table_coverage_id_is_deterministic_and_valid() {
        let index = timestamp_index("ts", TimeBucket::Hours(1));
        let bytes = b"table-bitmap";

        let id1 = table_coverage_id_v2(&index, bytes);
        let id2 = table_coverage_id_v2(&index, bytes);

        assert_eq!(id1, id2, "same inputs must produce stable id");
        assert!(id1.starts_with("tblcov-"));
        assert_eq!(id1.len(), "tblcov-".len() + 32, "prefix + 32 hex chars");
        validate_coverage_id(&id1).expect("derived id should be valid");
    }

    #[test]
    fn table_coverage_id_changes_with_inputs() {
        let bytes = b"bytes";

        let base_index = timestamp_index("ts", TimeBucket::Minutes(15));
        let base = table_coverage_id_v2(&base_index, bytes);
        let different_bucket =
            table_coverage_id_v2(&timestamp_index("ts", TimeBucket::Days(1)), bytes);
        let different_column = table_coverage_id_v2(
            &timestamp_index("event_time", TimeBucket::Minutes(15)),
            bytes,
        );
        let different_bytes = table_coverage_id_v2(&base_index, b"other");

        assert_ne!(base, different_bucket, "bucket spec should affect id");
        assert_ne!(base, different_column, "index column should affect id");
        assert_ne!(base, different_bytes, "coverage bytes should affect id");
    }

    #[test]
    fn coverage_file_ids_are_owned_by_the_append_attempt() {
        let content_id = "segcov-0123456789abcdef0123456789abcdef";
        let first = coverage_file_id_for_attempt(content_id, &Uuid::from_u128(1));
        let second = coverage_file_id_for_attempt(content_id, &Uuid::from_u128(2));

        assert_ne!(first, second);
        validate_coverage_id(&first).expect("first id should be valid");
        validate_coverage_id(&second).expect("second id should be valid");
    }
}
