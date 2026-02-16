//! Internal helper crate for release automation.
//!
//! This crate exists so release-plz can attribute commits under the `python/` directory to a
//! workspace package. The `timeseries-table-python` package can then include those commits in its
//! changelog (via `changelog_include`) without requiring Python sources to live under
//! `crates/timeseries-table-python/`.
