//! Table protocol compatibility and feature negotiation.

use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer};
use snafu::prelude::*;

use crate::metadata::table::TableMeta;

/// Current table protocol version written by new tables.
///
/// This changes only when the core metadata or commit-log envelope can no
/// longer be decoded under the current protocol. Optional capabilities are
/// declared separately in the reader and writer feature sets.
pub const TABLE_PROTOCOL_VERSION: u32 = 7;

const SUPPORTED_READER_FEATURES: &[&str] = &[];
const SUPPORTED_WRITER_FEATURES: &[&str] = &[];

#[derive(Deserialize)]
pub(crate) struct RawTableProtocolRequirements {
    protocol_version: u64,
    #[serde(deserialize_with = "deserialize_required_features")]
    required_reader_features: BTreeSet<String>,
    #[serde(
        rename = "required_writer_features",
        deserialize_with = "deserialize_required_features"
    )]
    _required_writer_features: BTreeSet<String>,
}

pub(crate) fn deserialize_required_features<'de, D>(
    deserializer: D,
) -> Result<BTreeSet<String>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error as _;

    let features = Vec::<String>::deserialize(deserializer)?;
    let mut unique = BTreeSet::new();
    for feature in features {
        if !is_valid_feature_name(&feature) {
            return Err(D::Error::custom(format!(
                "invalid table feature {feature:?}; expected [a-z][a-z0-9_]*"
            )));
        }
        if !unique.insert(feature.clone()) {
            return Err(D::Error::custom(format!(
                "duplicate table feature {feature:?}"
            )));
        }
    }
    Ok(unique)
}

fn is_valid_feature_name(feature: &str) -> bool {
    let mut bytes = feature.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// A table protocol requirement or transition is not supported.
#[derive(Debug, Snafu, PartialEq, Eq)]
#[non_exhaustive]
pub enum TableProtocolError {
    /// The table uses a core protocol version this client cannot decode.
    #[snafu(display("unsupported table protocol version: expected {expected}, found {found}"))]
    UnsupportedVersion {
        /// Core protocol version supported by this client.
        expected: u32,
        /// Core protocol version required by the table.
        found: u64,
    },

    /// The table requires reader features this client does not support.
    #[snafu(display("unsupported table reader features: {features:?}"))]
    UnsupportedReaderFeatures {
        /// Unsupported feature identifiers in canonical order.
        features: Vec<String>,
    },

    /// The table requires writer features this client does not support.
    #[snafu(display("unsupported table writer features: {features:?}"))]
    UnsupportedWriterFeatures {
        /// Unsupported feature identifiers in canonical order.
        features: Vec<String>,
    },

    /// A metadata update removed previously required reader features.
    #[snafu(display("table metadata removed required reader features: {features:?}"))]
    ReaderFeaturesRemoved {
        /// Removed feature identifiers in canonical order.
        features: Vec<String>,
    },

    /// A metadata update removed previously required writer features.
    #[snafu(display("table metadata removed required writer features: {features:?}"))]
    WriterFeaturesRemoved {
        /// Removed feature identifiers in canonical order.
        features: Vec<String>,
    },

    /// A metadata update decreased the core protocol version.
    #[snafu(display("table metadata decreased protocol version from {previous} to {next}"))]
    ProtocolVersionDecreased {
        /// Protocol version before the metadata update.
        previous: u32,
        /// Protocol version after the metadata update.
        next: u32,
    },
}

impl TableMeta {
    /// Returns the on-disk table protocol version.
    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// Returns the features a client must support to read the table.
    pub fn required_reader_features(&self) -> &BTreeSet<String> {
        &self.required_reader_features
    }

    /// Returns the features a client must support to write the table.
    pub fn required_writer_features(&self) -> &BTreeSet<String> {
        &self.required_writer_features
    }

    pub(crate) fn ensure_read_compatible(&self) -> Result<(), TableProtocolError> {
        self.ensure_read_compatible_with(SUPPORTED_READER_FEATURES)
    }

    pub(crate) fn ensure_write_compatible(&self) -> Result<(), TableProtocolError> {
        self.ensure_write_compatible_with(SUPPORTED_READER_FEATURES, SUPPORTED_WRITER_FEATURES)
    }

    fn ensure_read_compatible_with(
        &self,
        supported_reader_features: &[&str],
    ) -> Result<(), TableProtocolError> {
        ensure_read_compatible(
            u64::from(self.protocol_version),
            &self.required_reader_features,
            supported_reader_features,
        )
    }

    fn ensure_write_compatible_with(
        &self,
        supported_reader_features: &[&str],
        supported_writer_features: &[&str],
    ) -> Result<(), TableProtocolError> {
        self.ensure_read_compatible_with(supported_reader_features)?;

        let unsupported =
            unsupported_features(&self.required_writer_features, supported_writer_features);
        if !unsupported.is_empty() {
            return Err(TableProtocolError::UnsupportedWriterFeatures {
                features: unsupported,
            });
        }
        Ok(())
    }

    pub(crate) fn ensure_valid_transition_to(&self, next: &Self) -> Result<(), TableProtocolError> {
        if next.protocol_version < self.protocol_version {
            return Err(TableProtocolError::ProtocolVersionDecreased {
                previous: self.protocol_version,
                next: next.protocol_version,
            });
        }

        let removed_reader_features = self
            .required_reader_features
            .difference(&next.required_reader_features)
            .cloned()
            .collect::<Vec<_>>();
        if !removed_reader_features.is_empty() {
            return Err(TableProtocolError::ReaderFeaturesRemoved {
                features: removed_reader_features,
            });
        }

        let removed_writer_features = self
            .required_writer_features
            .difference(&next.required_writer_features)
            .cloned()
            .collect::<Vec<_>>();
        if !removed_writer_features.is_empty() {
            return Err(TableProtocolError::WriterFeaturesRemoved {
                features: removed_writer_features,
            });
        }
        Ok(())
    }
}

impl RawTableProtocolRequirements {
    pub(crate) fn ensure_read_compatible(&self) -> Result<(), TableProtocolError> {
        ensure_read_compatible(
            self.protocol_version,
            &self.required_reader_features,
            SUPPORTED_READER_FEATURES,
        )
    }
}

fn ensure_read_compatible(
    protocol_version: u64,
    required_reader_features: &BTreeSet<String>,
    supported_reader_features: &[&str],
) -> Result<(), TableProtocolError> {
    if protocol_version != u64::from(TABLE_PROTOCOL_VERSION) {
        return Err(TableProtocolError::UnsupportedVersion {
            expected: TABLE_PROTOCOL_VERSION,
            found: protocol_version,
        });
    }

    let unsupported = unsupported_features(required_reader_features, supported_reader_features);
    if !unsupported.is_empty() {
        return Err(TableProtocolError::UnsupportedReaderFeatures {
            features: unsupported,
        });
    }
    Ok(())
}

fn unsupported_features(required: &BTreeSet<String>, supported: &[&str]) -> Vec<String> {
    required
        .iter()
        .filter(|feature| !supported.contains(&feature.as_str()))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::metadata::{
        index::{IndexKind, IndexSpec, TimeIndexGranularity},
        table::TableMeta,
    };

    use super::*;

    fn sample_table_meta() -> TableMeta {
        TableMeta::new_time_series(IndexSpec {
            column: "ts".to_string(),
            entity_columns: vec!["symbol".to_string()],
            kind: IndexKind::Timestamp {
                index_granularity: TimeIndexGranularity::Minutes(1),
                timezone: None,
            },
        })
    }

    #[test]
    fn table_meta_requires_valid_unique_feature_lists() {
        let valid = serde_json::to_value(sample_table_meta()).unwrap();
        let invalid_lists = [
            serde_json::Value::Null,
            serde_json::json!(["duplicate", "duplicate"]),
            serde_json::json!(["Uppercase"]),
            serde_json::json!([""]),
        ];

        for invalid in invalid_lists {
            let mut json = valid.clone();
            json["required_reader_features"] = invalid;
            assert!(serde_json::from_value::<TableMeta>(json).is_err());
        }

        for field in ["required_reader_features", "required_writer_features"] {
            let mut json = valid.clone();
            json.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<TableMeta>(json).is_err());
        }
    }

    #[test]
    fn table_protocol_compatibility_is_operation_specific() {
        let mut meta = sample_table_meta();
        meta.required_reader_features
            .extend(["reader_a".to_string(), "reader_b".to_string()]);
        meta.required_writer_features.insert("writer_a".to_string());

        assert!(
            meta.ensure_read_compatible_with(&["reader_a", "reader_b"])
                .is_ok()
        );
        assert_eq!(
            meta.ensure_read_compatible_with(&["reader_b"]),
            Err(TableProtocolError::UnsupportedReaderFeatures {
                features: vec!["reader_a".to_string()],
            })
        );
        assert_eq!(
            meta.ensure_write_compatible_with(&["reader_a", "reader_b"], &[]),
            Err(TableProtocolError::UnsupportedWriterFeatures {
                features: vec!["writer_a".to_string()],
            })
        );
        assert!(
            meta.ensure_write_compatible_with(&["reader_a", "reader_b"], &["writer_a"])
                .is_ok()
        );
        assert_eq!(
            meta.ensure_write_compatible(),
            Err(TableProtocolError::UnsupportedReaderFeatures {
                features: vec!["reader_a".to_string(), "reader_b".to_string()],
            })
        );
    }

    #[test]
    fn table_protocol_transition_requirements_are_monotonic() {
        let mut previous = sample_table_meta();
        previous
            .required_reader_features
            .extend(["reader_a".to_string(), "reader_b".to_string()]);
        previous
            .required_writer_features
            .insert("writer_a".to_string());

        let mut additive = previous.clone();
        additive
            .required_writer_features
            .insert("writer_b".to_string());
        assert!(previous.ensure_valid_transition_to(&additive).is_ok());

        let mut removed = previous.clone();
        removed.required_reader_features.clear();
        assert_eq!(
            previous.ensure_valid_transition_to(&removed),
            Err(TableProtocolError::ReaderFeaturesRemoved {
                features: vec!["reader_a".to_string(), "reader_b".to_string()],
            })
        );

        let mut removed = previous.clone();
        removed.required_writer_features.clear();
        assert_eq!(
            previous.ensure_valid_transition_to(&removed),
            Err(TableProtocolError::WriterFeaturesRemoved {
                features: vec!["writer_a".to_string()],
            })
        );

        let mut decreased = previous.clone();
        decreased.protocol_version -= 1;
        assert_eq!(
            previous.ensure_valid_transition_to(&decreased),
            Err(TableProtocolError::ProtocolVersionDecreased {
                previous: TABLE_PROTOCOL_VERSION,
                next: TABLE_PROTOCOL_VERSION - 1,
            })
        );
    }
}
