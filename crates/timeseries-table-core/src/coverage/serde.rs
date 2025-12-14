//! Serialization and deserialization of coverage bitmaps.
//!
//! This module provides helpers to convert [`Coverage`] instances to and from
//! byte buffers using the RoaringBitmap binary format. This is used for
//! persisting coverage snapshots to disk and loading them back.
//!
//! # Serialization Format
//!
//! Coverage data is serialized to bytes using the RoaringBitmap binary format
//! (portable across platforms). The byte format is opaque and should not be
//! interpreted directly; always use [`coverage_from_bytes`] to deserialize.
//!
//! # Example
//!
//! ```ignore
//! use timeseries_table_core::coverage::Coverage;
//! use timeseries_table_core::coverage::serde::{coverage_to_bytes, coverage_from_bytes};
//!
//! let cov = Coverage::from_iter(vec![1u32, 2, 3]);
//! let bytes = coverage_to_bytes(&cov)?;
//! let restored = coverage_from_bytes(&bytes)?;
//! assert_eq!(cov.cardinality(), restored.cardinality());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::io::Cursor;

use roaring::RoaringBitmap;
use snafu::{ResultExt, Snafu};

use crate::coverage::Coverage;

/// Errors that can occur during coverage serialization or deserialization.
///
/// These errors indicate I/O failures when reading or writing the RoaringBitmap
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

/// Serialize a coverage bitmap to a byte vector.
///
/// Converts the given [`Coverage`] instance to its RoaringBitmap binary representation,
/// which can be written to disk or transmitted over the network.
///
/// # Arguments
///
/// * `cov` - The coverage instance to serialize.
///
/// # Returns
///
/// A vector of bytes in RoaringBitmap binary format, or an error if serialization fails.
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
/// The byte format is the RoaringBitmap portable binary representation.
///
/// # Arguments
///
/// * `bytes` - A byte slice in RoaringBitmap binary format.
///
/// # Returns
///
/// A reconstructed [`Coverage`] instance, or an error if deserialization fails.
///
/// # Errors
///
/// Returns [`CoverageSerdeError::Deserialize`] if an I/O error occurs during deserialization
/// or if the byte sequence is not a valid RoaringBitmap.
pub fn coverage_from_bytes(bytes: &[u8]) -> Result<Coverage, CoverageSerdeError> {
    let mut r = Cursor::new(bytes);
    let bm = RoaringBitmap::deserialize_from(&mut r).context(DeserializeSnafu)?;
    Ok(Coverage::from_bitmap(bm))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_empty_and_non_empty() {
        // Empty coverage
        let cov_empty = Coverage::empty();
        let bytes = coverage_to_bytes(&cov_empty).expect("serialize empty");
        let restored = coverage_from_bytes(&bytes).expect("deserialize empty");
        assert_eq!(cov_empty.cardinality(), restored.cardinality());

        // Non-empty coverage
        let cov = Coverage::from_iter(vec![1u32, 2, 3, 100]);
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

        let cov = Coverage::from_iter(vec![1u32]);

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
}
