//! In-memory coverage and gap analysis over a discrete bucket domain.
//!
//! This module is intentionally small and generic:
//!
//! - It wraps `roaring::RoaringBitmap` in a `Coverage` struct.
//! - It does not know about timestamps, tables, or storage.
//! - Callers are expected to map their own domain (for example, time buckets)
//!   into `u32` bucket ids.
//!
//! Typical usage:
//!
//! ```
//! use roaring::RoaringBitmap;
//! use timeseries_table_core::coverage::Coverage;
//!
//! // "expected" domain: buckets 0..10
//! let expected: RoaringBitmap = (0u32..10).collect();
//!
//! // "present" coverage: everything except bucket 5
//! let mut present = RoaringBitmap::new();
//! for b in 0u32..10 {
//!     if b != 5 {
//!         present.insert(b);
//!     }
//! }
//!
//! let cov = Coverage::from_bitmap(present);
//!
//! let missing = cov.missing_points(&expected);
//! assert!(missing.contains(5));
//!
//! let ratio = cov.coverage_ratio(&expected);
//! assert!((ratio - 0.9).abs() < 1e-9);
//! ```

use std::ops::RangeInclusive;

use roaring::{RoaringBitmap, bitmap};

/// Type alias for bucket ids used by Coverage.
///
/// For v0.1 we use `u32`, which is enough for common time-bucket domains
/// (for example, minutes since epoch).
pub type Bucket = u32;

/// In-memory coverage over a discrete set of bucket ids.
///
/// This is a thin wrapper over `RoaringBitmap` that adds convenience
/// methods for gap analysis.
#[derive(Debug, Clone, Default)]
pub struct Coverage {
    bitmap: RoaringBitmap,
}

impl Coverage {
    /// Construct an empty coverage set (no bucket present).
    pub fn empty() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }

    /// Wrap an existing RoaringBitmap as Coverage.
    pub fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        Self { bitmap }
    }

    /// Borrow the underlying bitmap of present buckets.
    pub fn present(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    /// Consume the Coverage and return the underlying bitmap.
    pub fn into_bitmap(self) -> RoaringBitmap {
        self.bitmap
    }

    /// Return the union of `self` and `other`.
    pub fn union(&self, other: &Coverage) -> Coverage {
        let bitmap = &self.bitmap | &other.bitmap;
        Coverage { bitmap }
    }

    /// Return the intersection of `self` and `other`.
    pub fn intersect(&self, other: &Coverage) -> Coverage {
        let bitmap = &self.bitmap & &other.bitmap;
        Coverage { bitmap }
    }

    /// Number of buckets present in this coverage.
    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    /// Return bucket ids that are expected but not present in this coverage.
    ///
    /// This is `expected - present`.
    pub fn missing_points(&self, expected: &RoaringBitmap) -> RoaringBitmap {
        let mut missing = expected.clone();
        missing -= &self.bitmap;
        missing
    }

    pub fn missing_runs(
        &self,
        expected: &RoaringBitmap,
        max_run_len: Option<u64>,
    ) -> Vec<RangeInclusive<u64>> {
        let missing = self.missing_points(expected);
        let base_runs = runs_from_bitmap(&missing);
    }
}

impl FromIterator<Bucket> for Coverage {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Bucket>,
    {
        let bitmap: RoaringBitmap = iter.into_iter().collect();
        Self { bitmap }
    }
}

/// Convert a bitmap into contiguous runs of bucket ids.
///
/// Each run is returned as a `RangeInclusive<u64>`.
fn runs_from_bitmap(bitmap: &RoaringBitmap) -> Vec<RangeInclusive<u64>> {
    let mut out = Vec::new();
    let mut iter = bitmap.iter();

    let Some(mut start) = iter.next() else {
        return out;
    };
    let mut prev = start;

    for v in iter {
        if v == prev + 1 {
            // still in the same contiguous run
            prev = v;
        } else {
            // close previous run and start a new one
            out.push(start as u64..=prev as u64);
            start = v;
            prev = v;
        }
    }

    // finalize last run
    out.push(start as u64..=prev as u64);
    out
}
