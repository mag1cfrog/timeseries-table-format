//! Coverage and gap analysis using RoaringBitmap.
//!
//! This module provides small, composable helpers for reasoning about
//! which time buckets are present or missing in a time-series table.
//!
//! The core idea is to represent coverage over a discrete domain
//! (typically time buckets derived from a `TimeIndexSpec`) using a
//! `roaring::RoaringBitmap`. Each bit indicates whether a given bucket
//! is present.
//!
//! Typical use cases include:
//!
//! - Detecting missing buckets relative to an expected trading calendar
//!   or regular time grid.
//! - Grouping missing buckets into contiguous ranges ("gaps") for
//!   backfill jobs or data quality alerts.
//! - Computing simple health metrics such as coverage ratios and
//!   maximum gap length for a symbol/timeframe.
//! - Deriving "fully covered" windows (for example, the last N buckets
//!   where all required features exist) by intersecting coverage maps.
//!
//! This module is intentionally independent of IO and table creation;
//! it operates only on integer bucket identifiers and can be reused for
//! both raw price series and derived feature tables.
