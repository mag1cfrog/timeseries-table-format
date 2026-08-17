//! Serialization and deserialization of coverage values.
//!
//! Global [`Coverage`] values keep their historical RoaringTreemap format.
//! [`EntityCoverage`] values use a separate, identified format that length
//! prefixes identity components and nested coverage payloads.
//!
//! # Global serialization format
//!
//! Coverage data is serialized to bytes using the RoaringTreemap binary format
//! (portable across platforms). The byte format is opaque and should not be
//! interpreted directly; always use [`coverage_from_bytes`] to deserialize.
//!
//! # Entity-aware V2 serialization format
//!
//! All integer fields outside nested coverage are big-endian:
//!
//! ```text
//! "TSTECOV2"
//! entity_count: u32
//! repeated entity_count times:
//!   component_count: u32
//!   repeated component_count times:
//!     component_type: u8
//!     component_value:
//!       Utf8: byte_len: u64, then UTF-8 bytes
//!       Int32: big-endian i32
//!       Int64: big-endian i64
//!       UInt64: big-endian u64
//!   nested_coverage_byte_len: u64
//!   nested_historical_roaring_treemap_bytes
//! ```
//!
//! # Example
//!
//! ```ignore
//! use timeseries_table_format::coverage::Coverage;
//! use timeseries_table_format::coverage::serde::{coverage_to_bytes, coverage_from_bytes};
//!
//! let cov = Coverage::from_iter(vec![1u64, 2, 3]);
//! let bytes = coverage_to_bytes(&cov)?;
//! let restored = coverage_from_bytes(&bytes)?;
//! assert_eq!(cov.cardinality(), restored.cardinality());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::{io::Cursor, str::Utf8Error};

use roaring::{RoaringBitmap, RoaringTreemap};
use snafu::{ResultExt, Snafu};

use crate::coverage::{Coverage, EntityCoverage, EntityIdentity, EntityIdentityError, EntityValue};

const ENTITY_COVERAGE_MAGIC: &[u8; 8] = b"TSTECOV2";
const ENTITY_VALUE_UTF8: u8 = 1;
const ENTITY_VALUE_INT32: u8 = 2;
const ENTITY_VALUE_INT64: u8 = 3;
const ENTITY_VALUE_UINT64: u8 = 4;

/// Errors that can occur during coverage serialization or deserialization.
///
/// These errors indicate I/O failures when reading or writing the RoaringTreemap
/// binary format. Callers should handle these gracefully and may retry or fall back
/// to recovering coverage from the source data.
#[derive(Debug, Snafu)]
pub enum CoverageSerdeError {
    /// I/O error during serialization of a coverage bitmap.
    #[snafu(display("Failed to serialize roaring bitmap: {source}"))]
    Serialize {
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// I/O error during deserialization of a coverage bitmap.
    #[snafu(display("Failed to deserialize roaring bitmap: {source}"))]
    Deserialize {
        /// The underlying I/O error.
        source: std::io::Error,
    },
}

/// Errors from the distinct entity-aware coverage encoding.
#[derive(Debug, Snafu)]
pub enum EntityCoverageSerdeError {
    /// An in-memory length cannot be represented by the format.
    #[snafu(display("Entity coverage {field} is too large to serialize"))]
    LengthOverflow {
        /// The field whose length overflowed.
        field: &'static str,
    },

    /// A nested bitmap could not be serialized.
    #[snafu(display("Failed to serialize nested entity coverage: {source}"))]
    SerializeCoverage {
        /// The nested global coverage error.
        source: CoverageSerdeError,
    },

    /// The payload is not entity-aware coverage.
    #[snafu(display("Invalid entity coverage payload identifier"))]
    InvalidMagic,

    /// A fixed-size field is incomplete.
    #[snafu(display("Truncated entity coverage payload"))]
    Truncated,

    /// A declared count or length is not valid for the remaining payload.
    #[snafu(display("Invalid entity coverage {field}"))]
    InvalidLength {
        /// The invalid field.
        field: &'static str,
    },

    /// An identity component is not valid UTF-8.
    #[snafu(display("Invalid entity identity string: {source}"))]
    InvalidString {
        /// The UTF-8 validation error.
        source: Utf8Error,
    },

    /// An identity component uses an unknown scalar type tag.
    #[snafu(display("Unknown entity identity value type tag: {tag}"))]
    UnknownValueType {
        /// Unrecognized encoded type tag.
        tag: u8,
    },

    /// An encoded identity is incomplete.
    #[snafu(display("Invalid entity identity: {source}"))]
    InvalidIdentity {
        /// The identity validation error.
        source: EntityIdentityError,
    },

    /// The payload contains the same identity more than once.
    #[snafu(display("Duplicate entity identity in coverage payload: {identity:?}"))]
    DuplicateIdentity {
        /// The repeated identity.
        identity: EntityIdentity,
    },

    /// A nested RoaringTreemap payload is malformed.
    #[snafu(display("Malformed nested entity coverage: {source}"))]
    MalformedCoverage {
        /// The nested global coverage error.
        source: CoverageSerdeError,
    },

    /// Bytes remain after the declared entity entries.
    #[snafu(display("Trailing bytes after entity coverage payload"))]
    TrailingBytes,
}

/// Serialize a coverage bitmap to a byte vector.
///
/// Converts the given [`Coverage`] instance to its RoaringTreemap binary representation,
/// which can be written to disk or transmitted over the network.
///
/// # Arguments
///
/// * `cov` - The coverage instance to serialize.
///
/// # Returns
///
/// A vector of bytes in RoaringTreemap binary format, or an error if serialization fails.
///
/// # Errors
///
/// Returns [`CoverageSerdeError::Serialize`] if an I/O error occurs during serialization.
pub fn coverage_to_bytes(cov: &Coverage) -> Result<Vec<u8>, CoverageSerdeError> {
    let mut out = Vec::new();
    {
        let mut w = Cursor::new(&mut out);
        cov.present()
            .serialize_into(&mut w)
            .context(SerializeSnafu)?;
    }
    Ok(out)
}

/// Deserialize a coverage bitmap from bytes.
///
/// Reconstructs a [`Coverage`] instance from bytes previously written by [`coverage_to_bytes`].
/// The byte format is the RoaringTreemap portable binary representation.
///
/// # Arguments
///
/// * `bytes` - A byte slice in RoaringTreemap binary format.
///
/// # Returns
///
/// A reconstructed [`Coverage`] instance, or an error if deserialization fails.
///
/// # Errors
///
/// Returns [`CoverageSerdeError::Deserialize`] if an I/O error occurs during deserialization
/// or if the byte sequence is not a valid RoaringTreemap.
pub fn coverage_from_bytes(bytes: &[u8]) -> Result<Coverage, CoverageSerdeError> {
    let mut r = Cursor::new(bytes);
    let present = RoaringTreemap::deserialize_from(&mut r).context(DeserializeSnafu)?;

    if r.position() != bytes.len() as u64 {
        return Err(CoverageSerdeError::Deserialize {
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "trailing bytes after roaring bitmap",
            ),
        });
    }

    Ok(Coverage::from_treemap(present))
}

/// Serialize entity-scoped coverage in canonical identity order.
///
/// # Errors
///
/// Returns [`EntityCoverageSerdeError`] if a count cannot be represented or a
/// nested coverage bitmap cannot be serialized.
pub fn entity_coverage_to_bytes(
    coverage: &EntityCoverage,
) -> Result<Vec<u8>, EntityCoverageSerdeError> {
    let entity_count = u32::try_from(coverage.identity_count()).map_err(|_| {
        EntityCoverageSerdeError::LengthOverflow {
            field: "entity count",
        }
    })?;

    let mut out = Vec::new();
    out.extend_from_slice(ENTITY_COVERAGE_MAGIC);
    out.extend_from_slice(&entity_count.to_be_bytes());

    for (identity, nested) in coverage.iter() {
        let component_count = u32::try_from(identity.components().len()).map_err(|_| {
            EntityCoverageSerdeError::LengthOverflow {
                field: "identity component count",
            }
        })?;
        out.extend_from_slice(&component_count.to_be_bytes());

        for component in identity.components() {
            match component {
                EntityValue::Utf8(value) => {
                    out.push(ENTITY_VALUE_UTF8);
                    let component_len = u64::try_from(value.len()).map_err(|_| {
                        EntityCoverageSerdeError::LengthOverflow {
                            field: "identity component length",
                        }
                    })?;
                    out.extend_from_slice(&component_len.to_be_bytes());
                    out.extend_from_slice(value.as_bytes());
                }
                EntityValue::Int32(value) => {
                    out.push(ENTITY_VALUE_INT32);
                    out.extend_from_slice(&value.to_be_bytes());
                }
                EntityValue::Int64(value) => {
                    out.push(ENTITY_VALUE_INT64);
                    out.extend_from_slice(&value.to_be_bytes());
                }
                EntityValue::UInt64(value) => {
                    out.push(ENTITY_VALUE_UINT64);
                    out.extend_from_slice(&value.to_be_bytes());
                }
            }
        }

        let nested_bytes = canonical_nested_coverage_to_bytes(nested)
            .map_err(|source| EntityCoverageSerdeError::SerializeCoverage { source })?;
        let nested_len = u64::try_from(nested_bytes.len()).map_err(|_| {
            EntityCoverageSerdeError::LengthOverflow {
                field: "nested coverage length",
            }
        })?;
        out.extend_from_slice(&nested_len.to_be_bytes());
        out.extend_from_slice(&nested_bytes);
    }

    Ok(out)
}

/// Deserialize the distinct entity-aware coverage format.
///
/// # Errors
///
/// Returns [`EntityCoverageSerdeError`] for malformed, ambiguous, truncated,
/// or non-entity-aware input.
pub fn entity_coverage_from_bytes(
    bytes: &[u8],
) -> Result<EntityCoverage, EntityCoverageSerdeError> {
    let mut remaining = bytes;
    if take(&mut remaining, ENTITY_COVERAGE_MAGIC.len())? != ENTITY_COVERAGE_MAGIC {
        return Err(EntityCoverageSerdeError::InvalidMagic);
    }

    let entity_count = read_u32(&mut remaining)? as usize;
    let mut coverage = EntityCoverage::empty();
    for _ in 0..entity_count {
        let component_count = read_u32(&mut remaining)? as usize;
        if component_count > remaining.len().saturating_sub(8) / 5 {
            return Err(EntityCoverageSerdeError::InvalidLength {
                field: "identity component count",
            });
        }

        let mut components = Vec::new();
        for _ in 0..component_count {
            let tag = take(&mut remaining, 1)?[0];
            let component = match tag {
                ENTITY_VALUE_UTF8 => {
                    let component_len = read_u64(&mut remaining)?;
                    let component_bytes =
                        take_declared(&mut remaining, component_len, "identity component length")?;
                    let component = std::str::from_utf8(component_bytes)
                        .map_err(|source| EntityCoverageSerdeError::InvalidString { source })?;
                    EntityValue::Utf8(component.to_owned())
                }
                ENTITY_VALUE_INT32 => EntityValue::Int32(read_i32(&mut remaining)?),
                ENTITY_VALUE_INT64 => EntityValue::Int64(read_i64(&mut remaining)?),
                ENTITY_VALUE_UINT64 => EntityValue::UInt64(read_u64(&mut remaining)?),
                tag => return Err(EntityCoverageSerdeError::UnknownValueType { tag }),
            };
            components.push(component);
        }

        let identity = EntityIdentity::try_new(components)
            .map_err(|source| EntityCoverageSerdeError::InvalidIdentity { source })?;
        if coverage.get(&identity).is_some() {
            return Err(EntityCoverageSerdeError::DuplicateIdentity { identity });
        }

        let nested_len = read_u64(&mut remaining)?;
        let nested_bytes = take_declared(&mut remaining, nested_len, "nested coverage length")?;
        let nested = coverage_from_bytes(nested_bytes)
            .map_err(|source| EntityCoverageSerdeError::MalformedCoverage { source })?;
        coverage.union_coverage(identity, nested);
    }

    if !remaining.is_empty() {
        return Err(EntityCoverageSerdeError::TrailingBytes);
    }
    Ok(coverage)
}

/// Serialize after removing empty partitions and construction-history-dependent
/// Roaring container choices from entity-aware nested coverage.
fn canonical_nested_coverage_to_bytes(coverage: &Coverage) -> Result<Vec<u8>, CoverageSerdeError> {
    let present = RoaringTreemap::from_bitmaps(
        coverage
            .present()
            .bitmaps()
            .filter(|(_, bitmap)| !bitmap.is_empty())
            .map(|(key, bitmap)| {
                let mut canonical = RoaringBitmap::new();
                let mut ranges = bitmap.iter();
                while let Some(range) = ranges.next_range() {
                    canonical.insert_range(range);
                }
                canonical.optimize();
                (key, canonical)
            }),
    );
    coverage_to_bytes(&Coverage::from_treemap(present))
}

fn take<'a>(remaining: &mut &'a [u8], len: usize) -> Result<&'a [u8], EntityCoverageSerdeError> {
    if remaining.len() < len {
        return Err(EntityCoverageSerdeError::Truncated);
    }
    let (value, rest) = remaining.split_at(len);
    *remaining = rest;
    Ok(value)
}

fn take_declared<'a>(
    remaining: &mut &'a [u8],
    len: u64,
    field: &'static str,
) -> Result<&'a [u8], EntityCoverageSerdeError> {
    let len =
        usize::try_from(len).map_err(|_| EntityCoverageSerdeError::InvalidLength { field })?;
    if len > remaining.len() {
        return Err(EntityCoverageSerdeError::InvalidLength { field });
    }
    take(remaining, len)
}

fn read_u32(remaining: &mut &[u8]) -> Result<u32, EntityCoverageSerdeError> {
    let mut encoded = [0; 4];
    encoded.copy_from_slice(take(remaining, 4)?);
    Ok(u32::from_be_bytes(encoded))
}

fn read_i32(remaining: &mut &[u8]) -> Result<i32, EntityCoverageSerdeError> {
    let mut encoded = [0; 4];
    encoded.copy_from_slice(take(remaining, 4)?);
    Ok(i32::from_be_bytes(encoded))
}

fn read_i64(remaining: &mut &[u8]) -> Result<i64, EntityCoverageSerdeError> {
    let mut encoded = [0; 8];
    encoded.copy_from_slice(take(remaining, 8)?);
    Ok(i64::from_be_bytes(encoded))
}

fn read_u64(remaining: &mut &[u8]) -> Result<u64, EntityCoverageSerdeError> {
    let mut encoded = [0; 8];
    encoded.copy_from_slice(take(remaining, 8)?);
    Ok(u64::from_be_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROARING_ZERO: &[u8] = &[
        1, 0, 0, 0, 0, 0, 0, 0, // Treemap entry count.
        0, 0, 0, 0, // Treemap key.
        0x3a, 0x30, 0, 0, // Bitmap cookie.
        1, 0, 0, 0, // Bitmap container count.
        0, 0, 0, 0, // Container key and cardinality minus one.
        16, 0, 0, 0, // Container offset.
        0, 0, // Array value.
    ];
    const ROARING_MAX: &[u8] = &[
        1, 0, 0, 0, 0, 0, 0, 0, // Treemap entry count.
        0xff, 0xff, 0xff, 0xff, // Treemap key.
        0x3a, 0x30, 0, 0, // Bitmap cookie.
        1, 0, 0, 0, // Bitmap container count.
        0xff, 0xff, 0, 0, // Container key and cardinality minus one.
        16, 0, 0, 0, // Container offset.
        0xff, 0xff, // Array value.
    ];

    fn identity(components: &[&str]) -> EntityIdentity {
        EntityIdentity::try_new(
            components
                .iter()
                .map(|component| EntityValue::from(*component))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn round_trip_empty_and_non_empty() {
        // Empty coverage
        let cov_empty = Coverage::empty();
        let bytes = coverage_to_bytes(&cov_empty).expect("serialize empty");
        let restored = coverage_from_bytes(&bytes).expect("deserialize empty");
        assert_eq!(cov_empty.cardinality(), restored.cardinality());

        // Non-empty coverage
        let cov = Coverage::from_iter(vec![1u64, 2, 3, u64::MAX]);
        let bytes = coverage_to_bytes(&cov).expect("serialize non-empty");
        let restored = coverage_from_bytes(&bytes).expect("deserialize non-empty");
        assert_eq!(cov.present(), restored.present());
    }

    #[test]
    fn deserialize_rejects_invalid_bytes() {
        let bad = b"not a roaring bitmap";
        let err = coverage_from_bytes(bad).unwrap_err();
        match err {
            CoverageSerdeError::Deserialize { .. } => {}
            _ => panic!("expected deserialize error"),
        }
    }

    #[test]
    fn deserialize_rejects_trailing_valid_payload() {
        let mut bytes = coverage_to_bytes(&Coverage::empty()).unwrap();
        bytes.extend_from_slice(&coverage_to_bytes(&Coverage::from_iter([1u64])).unwrap());

        let err = coverage_from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, CoverageSerdeError::Deserialize { .. }));
    }

    #[test]
    fn serialize_reports_io_error() {
        // Force an I/O error by using a writer that always errors.
        struct FailingWriter;
        impl std::io::Write for FailingWriter {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("fail"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let cov = Coverage::from_iter(vec![1u64]);

        // Reimplement minimal logic to inject failing writer
        let err = {
            let mut w = FailingWriter;
            cov.present()
                .serialize_into(&mut w)
                .map_err(|e| CoverageSerdeError::Serialize { source: e })
                .unwrap_err()
        };

        match err {
            CoverageSerdeError::Serialize { .. } => {}
            _ => panic!("expected serialize error"),
        }
    }

    #[test]
    fn entity_coverage_round_trips_empty_and_one_identity() {
        let empty_bytes = entity_coverage_to_bytes(&EntityCoverage::empty()).unwrap();
        assert_eq!(
            empty_bytes,
            [ENTITY_COVERAGE_MAGIC.as_slice(), &[0; 4]].concat()
        );
        assert_eq!(
            entity_coverage_from_bytes(&empty_bytes).unwrap(),
            EntityCoverage::empty()
        );

        let entity = identity(&["venue", "symbol"]);
        let mut coverage = EntityCoverage::empty();
        coverage.union_coverage(entity.clone(), Coverage::empty());
        let empty_identity_bytes = entity_coverage_to_bytes(&coverage).unwrap();
        assert_ne!(empty_identity_bytes, empty_bytes);
        assert_eq!(
            entity_coverage_from_bytes(&empty_identity_bytes).unwrap(),
            coverage
        );

        coverage.union_coverage(entity, [0, u64::MAX].into_iter().collect());
        let bytes = entity_coverage_to_bytes(&coverage).unwrap();
        assert_eq!(entity_coverage_from_bytes(&bytes).unwrap(), coverage);
    }

    #[test]
    fn entity_coverage_keeps_composite_identities_and_buckets_independent() {
        let first = identity(&["a", "b:c"]);
        let second = identity(&["a:b", "c"]);
        let mut coverage = EntityCoverage::empty();
        coverage.union_coverage(first.clone(), [7].into_iter().collect());
        coverage.union_coverage(second.clone(), [7].into_iter().collect());

        let restored =
            entity_coverage_from_bytes(&entity_coverage_to_bytes(&coverage).unwrap()).unwrap();
        assert_eq!(restored.get(&first).unwrap().cardinality(), 1);
        assert_eq!(restored.get(&second).unwrap().cardinality(), 1);
        assert_eq!(restored.cardinality(), 2);
    }

    #[test]
    fn entity_coverage_serialization_uses_canonical_identity_order() {
        let first = identity(&["A"]);
        let second = identity(&["B"]);
        let mut forward = EntityCoverage::empty();
        forward.union_coverage(first.clone(), [1].into_iter().collect());
        forward.union_coverage(second.clone(), [2].into_iter().collect());

        let mut reverse = EntityCoverage::empty();
        reverse.union_coverage(second, [2].into_iter().collect());
        reverse.union_coverage(first, [1].into_iter().collect());

        assert_eq!(
            entity_coverage_to_bytes(&forward).unwrap(),
            entity_coverage_to_bytes(&reverse).unwrap()
        );
    }

    #[test]
    fn entity_coverage_serialization_canonicalizes_roaring_storage() {
        let partition = 1u64 << 32;
        let inserted: Coverage = (1..=3).chain(partition + 1..=partition + 5_000).collect();
        let mut ranged = RoaringTreemap::new();
        ranged.insert_range(1..=3);
        ranged.insert_range(partition + 1..=partition + 5_000);
        let ranged = Coverage::from_treemap(ranged);
        assert_eq!(inserted, ranged);
        assert_ne!(
            coverage_to_bytes(&inserted).unwrap(),
            coverage_to_bytes(&ranged).unwrap()
        );

        let entity = identity(&["A"]);
        let mut left = EntityCoverage::empty();
        left.union_coverage(entity.clone(), inserted);
        let mut right = EntityCoverage::empty();
        right.union_coverage(entity, ranged);

        assert_eq!(
            entity_coverage_to_bytes(&left).unwrap(),
            entity_coverage_to_bytes(&right).unwrap()
        );

        let empty_partition = Coverage::from_treemap(RoaringTreemap::from_bitmaps([(
            7,
            roaring::RoaringBitmap::new(),
        )]));
        let mut logically_empty = EntityCoverage::empty();
        logically_empty.union_coverage(identity(&["empty"]), empty_partition);
        let mut canonical_empty = EntityCoverage::empty();
        canonical_empty.union_coverage(identity(&["empty"]), Coverage::empty());
        assert_eq!(
            entity_coverage_to_bytes(&logically_empty).unwrap(),
            entity_coverage_to_bytes(&canonical_empty).unwrap()
        );
    }

    #[test]
    fn entity_coverage_v2_golden_payload_is_stable() {
        let mut coverage = EntityCoverage::empty();
        coverage.union_coverage(
            EntityIdentity::try_new(vec![
                EntityValue::from("\u{6771}\u{4eac}"),
                EntityValue::Int32(-1),
                EntityValue::Int64(i64::MIN),
                EntityValue::UInt64(u64::MAX),
            ])
            .unwrap(),
            [0].into_iter().collect(),
        );

        let expected = [
            b"TSTECOV2".as_slice(),
            &[0, 0, 0, 1], // Entity count.
            &[0, 0, 0, 4], // Identity component count.
            &[ENTITY_VALUE_UTF8],
            &[0, 0, 0, 0, 0, 0, 0, 6],
            &[0xe6, 0x9d, 0xb1, 0xe4, 0xba, 0xac],
            &[ENTITY_VALUE_INT32],
            &(-1i32).to_be_bytes(),
            &[ENTITY_VALUE_INT64],
            &i64::MIN.to_be_bytes(),
            &[ENTITY_VALUE_UINT64],
            &u64::MAX.to_be_bytes(),
            &[0, 0, 0, 0, 0, 0, 0, 30],
            ROARING_ZERO,
        ]
        .concat();

        assert_eq!(entity_coverage_to_bytes(&coverage).unwrap(), expected);
        assert_eq!(entity_coverage_from_bytes(&expected).unwrap(), coverage);
    }

    #[test]
    fn entity_coverage_decoder_rejects_every_truncated_prefix() {
        let mut coverage = EntityCoverage::empty();
        coverage.union_coverage(identity(&["A"]), [1].into_iter().collect());
        let bytes = entity_coverage_to_bytes(&coverage).unwrap();

        for end in 0..bytes.len() {
            assert!(entity_coverage_from_bytes(&bytes[..end]).is_err());
        }
    }

    #[test]
    fn entity_coverage_decoder_rejects_invalid_magic_lengths_and_strings() {
        let mut coverage = EntityCoverage::empty();
        coverage.union_coverage(identity(&["A"]), [1].into_iter().collect());
        let bytes = entity_coverage_to_bytes(&coverage).unwrap();

        let mut invalid_magic = bytes.clone();
        invalid_magic[0] ^= 0xff;
        assert!(matches!(
            entity_coverage_from_bytes(&invalid_magic),
            Err(EntityCoverageSerdeError::InvalidMagic)
        ));

        let mut version_one = bytes.clone();
        version_one[..8].copy_from_slice(b"TSTECOV1");
        assert!(matches!(
            entity_coverage_from_bytes(&version_one),
            Err(EntityCoverageSerdeError::InvalidMagic)
        ));

        let mut invalid_count = bytes.clone();
        invalid_count[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(entity_coverage_from_bytes(&invalid_count).is_err());

        let mut empty_identity = bytes.clone();
        empty_identity[12..16].copy_from_slice(&0u32.to_be_bytes());
        assert!(matches!(
            entity_coverage_from_bytes(&empty_identity),
            Err(EntityCoverageSerdeError::InvalidIdentity { .. })
        ));

        let mut invalid_length = bytes.clone();
        invalid_length[17..25].copy_from_slice(&u64::MAX.to_be_bytes());
        assert!(matches!(
            entity_coverage_from_bytes(&invalid_length),
            Err(EntityCoverageSerdeError::InvalidLength { .. })
        ));

        let mut invalid_string = bytes.clone();
        invalid_string[25] = 0xff;
        assert!(matches!(
            entity_coverage_from_bytes(&invalid_string),
            Err(EntityCoverageSerdeError::InvalidString { .. })
        ));

        let mut unknown_type = bytes;
        unknown_type[16] = u8::MAX;
        assert!(matches!(
            entity_coverage_from_bytes(&unknown_type),
            Err(EntityCoverageSerdeError::UnknownValueType { tag: u8::MAX })
        ));
    }

    #[test]
    fn entity_coverage_decoder_rejects_duplicate_identities() {
        let mut coverage = EntityCoverage::empty();
        coverage.union_coverage(identity(&["A"]), Coverage::empty());
        let mut bytes = entity_coverage_to_bytes(&coverage).unwrap();
        let duplicate = bytes[12..].to_vec();
        bytes[8..12].copy_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&duplicate);

        assert!(matches!(
            entity_coverage_from_bytes(&bytes),
            Err(EntityCoverageSerdeError::DuplicateIdentity { .. })
        ));
    }

    #[test]
    fn entity_coverage_decoder_rejects_malformed_nested_and_trailing_bytes() {
        let mut coverage = EntityCoverage::empty();
        coverage.union_coverage(identity(&["A"]), [1].into_iter().collect());
        let bytes = entity_coverage_to_bytes(&coverage).unwrap();

        let mut malformed_nested = bytes.clone();
        malformed_nested[26..34].copy_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            entity_coverage_from_bytes(&malformed_nested),
            Err(EntityCoverageSerdeError::MalformedCoverage { .. })
        ));

        let mut trailing = bytes;
        trailing.push(0);
        assert!(matches!(
            entity_coverage_from_bytes(&trailing),
            Err(EntityCoverageSerdeError::TrailingBytes)
        ));
    }

    #[test]
    fn historical_global_coverage_codec_is_unchanged_and_distinct() {
        let global_empty = coverage_to_bytes(&Coverage::empty()).unwrap();
        assert_eq!(global_empty, vec![0; 8]);
        assert!(matches!(
            entity_coverage_from_bytes(&global_empty),
            Err(EntityCoverageSerdeError::InvalidMagic) | Err(EntityCoverageSerdeError::Truncated)
        ));

        let global_extremes: Coverage = [0, u64::MAX].into_iter().collect();
        let global_extremes_bytes = [
            &[2, 0, 0, 0, 0, 0, 0, 0],
            &ROARING_ZERO[8..],
            &ROARING_MAX[8..],
        ]
        .concat();
        assert_eq!(
            coverage_to_bytes(&global_extremes).unwrap(),
            global_extremes_bytes
        );
        assert_eq!(
            coverage_from_bytes(&global_extremes_bytes)
                .unwrap()
                .present(),
            global_extremes.present()
        );

        let entity_empty = entity_coverage_to_bytes(&EntityCoverage::empty()).unwrap();
        assert!(coverage_from_bytes(&entity_empty).is_err());
    }
}
