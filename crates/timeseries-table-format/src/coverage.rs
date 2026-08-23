//! In-memory coverage and gap analysis over the 64-bit index interval ID domain.

pub mod index_interval;
pub mod io;
pub mod layout;
pub mod serde;

use std::{
    collections::{BTreeMap, btree_map},
    ops::RangeInclusive,
};

use ::serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use snafu::Snafu;

pub use roaring::RoaringTreemap;

/// Ordered 64-bit index interval ID.
pub type IndexIntervalId = u64;

/// Exact scalar value in an entity identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "lowercase",
    deny_unknown_fields
)]
pub enum EntityValue {
    /// UTF-8 text from an Arrow `Utf8` or `LargeUtf8` column.
    Utf8(String),
    /// Signed 32-bit integer.
    Int32(i32),
    /// Signed 64-bit integer.
    Int64(i64),
    /// Unsigned 64-bit integer.
    UInt64(u64),
}

impl From<String> for EntityValue {
    fn from(value: String) -> Self {
        Self::Utf8(value)
    }
}

impl From<&str> for EntityValue {
    fn from(value: &str) -> Self {
        Self::Utf8(value.to_string())
    }
}

impl From<i32> for EntityValue {
    fn from(value: i32) -> Self {
        Self::Int32(value)
    }
}

impl From<i64> for EntityValue {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<u64> for EntityValue {
    fn from(value: u64) -> Self {
        Self::UInt64(value)
    }
}

/// Ordered composite entity identity.
///
/// Component positions correspond to the table's configured entity-column
/// order. Column names are deliberately not repeated in every identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityIdentity {
    components: Vec<EntityValue>,
}

impl Serialize for EntityIdentity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.components.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EntityIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let components = Vec::<EntityValue>::deserialize(deserializer)?;
        Self::try_new(components).map_err(D::Error::custom)
    }
}

impl EntityIdentity {
    /// Construct an identity from at least one ordered component.
    ///
    /// # Errors
    /// Returns [`EntityIdentityError::Empty`] when `components` is empty.
    pub fn try_new(components: Vec<EntityValue>) -> Result<Self, EntityIdentityError> {
        if components.is_empty() {
            return Err(EntityIdentityError::Empty);
        }
        Ok(Self { components })
    }

    /// Borrow components in configured entity-column order.
    pub fn components(&self) -> &[EntityValue] {
        &self.components
    }
}

/// Invalid entity identity construction.
#[derive(Debug, Clone, PartialEq, Eq, Snafu)]
pub enum EntityIdentityError {
    /// An entity-aware table requires at least one identity component.
    #[snafu(display("entity identity must contain at least one component"))]
    Empty,
}

/// In-memory coverage over a discrete set of index interval IDs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    present: RoaringTreemap,
}

impl Coverage {
    /// Construct an empty coverage set.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Wrap an existing treemap.
    pub fn from_treemap(present: RoaringTreemap) -> Self {
        Self { present }
    }

    /// Borrow the present index interval IDs.
    pub fn present(&self) -> &RoaringTreemap {
        &self.present
    }

    /// Consume the coverage and return its treemap.
    pub fn into_treemap(self) -> RoaringTreemap {
        self.present
    }

    /// Return the union of two coverage sets.
    pub fn union(&self, other: &Self) -> Self {
        Self::from_treemap(&self.present | &other.present)
    }

    /// Merge another coverage set into this one.
    pub fn union_inplace(&mut self, other: &Self) {
        self.present |= other.present();
    }

    /// Return the intersection of two coverage sets.
    pub fn intersect(&self, other: &Self) -> Self {
        Self::from_treemap(&self.present & &other.present)
    }

    /// Count index interval IDs present in both sets without materializing them.
    pub fn intersection_cardinality(&self, other: &Self) -> u64 {
        self.present.intersection_len(&other.present)
    }

    /// Number of present index interval IDs.
    pub fn cardinality(&self) -> u64 {
        self.present.len()
    }

    /// Whether no index interval IDs are present.
    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    /// Number of index interval IDs in an inclusive range.
    pub fn range_cardinality(range: &RangeInclusive<IndexIntervalId>) -> u128 {
        if range.is_empty() {
            return 0;
        }
        u128::from(*range.end()) - u128::from(*range.start()) + 1
    }

    /// Count present index interval IDs in a range without materializing it.
    pub fn covered_cardinality(&self, range: &RangeInclusive<IndexIntervalId>) -> u64 {
        if range.is_empty() {
            0
        } else {
            self.present.range_cardinality(range.clone())
        }
    }

    /// Return missing contiguous runs in an inclusive requested range.
    ///
    /// Long runs are optionally split into chunks of at most `max_run_len`.
    /// Work is proportional to present index interval IDs and returned runs, not the size
    /// of the requested range.
    pub fn missing_runs(
        &self,
        range: &RangeInclusive<IndexIntervalId>,
        max_run_len: Option<u64>,
    ) -> Vec<RangeInclusive<IndexIntervalId>> {
        if range.is_empty() || max_run_len == Some(0) {
            return Vec::new();
        }

        let start = *range.start();
        let end = *range.end();
        let mut cursor = Some(start);
        let mut runs = Vec::new();
        let mut present = self.present.iter();
        present.advance_to(start);

        for index_interval_id in present {
            if index_interval_id > end {
                break;
            }
            let Some(missing_start) = cursor else {
                break;
            };
            if missing_start < index_interval_id {
                runs.push(missing_start..=index_interval_id - 1);
            }
            cursor = index_interval_id.checked_add(1);
        }

        if let Some(missing_start) = cursor.filter(|value| *value <= end) {
            runs.push(missing_start..=end);
        }

        match max_run_len {
            Some(max_len) => split_runs_by_len(runs, max_len),
            None => runs,
        }
    }

    /// Return the last covered contiguous run of at least `min_len` IDs.
    pub fn last_run_with_min_len(
        &self,
        range: &RangeInclusive<IndexIntervalId>,
        min_len: u64,
    ) -> Option<RangeInclusive<IndexIntervalId>> {
        if range.is_empty() || min_len == 0 {
            return None;
        }

        let mut iter = self.present.iter();
        iter.advance_to(*range.start());
        iter.advance_back_to(*range.end());

        let mut current: Option<(IndexIntervalId, IndexIntervalId)> = None;
        let mut last = None;
        for index_interval_id in iter {
            match current {
                Some((start, end)) if end.checked_add(1) == Some(index_interval_id) => {
                    current = Some((start, index_interval_id));
                }
                Some((start, end)) => {
                    if inclusive_len(start, end) >= u128::from(min_len) {
                        last = Some(start..=end);
                    }
                    current = Some((index_interval_id, index_interval_id));
                }
                None => current = Some((index_interval_id, index_interval_id)),
            }
        }

        if let Some((start, end)) = current
            && inclusive_len(start, end) >= u128::from(min_len)
        {
            last = Some(start..=end);
        }
        last
    }

    /// Coverage ratio in `[0.0, 1.0]` for an inclusive range.
    pub fn coverage_ratio(&self, range: &RangeInclusive<IndexIntervalId>) -> f64 {
        let expected = Self::range_cardinality(range);
        if expected == 0 {
            return 1.0;
        }
        self.covered_cardinality(range) as f64 / expected as f64
    }

    /// Length of the largest missing run in an inclusive range.
    pub fn max_gap_len(&self, range: &RangeInclusive<IndexIntervalId>) -> u128 {
        self.missing_runs(range, None)
            .into_iter()
            .map(|run| inclusive_len(*run.start(), *run.end()))
            .max()
            .unwrap_or(0)
    }

    /// Return the last fully-covered window ending at or before an index interval ID.
    pub fn last_window_at_or_before(
        &self,
        end_index_interval_id: IndexIntervalId,
        len: u64,
    ) -> Option<RangeInclusive<IndexIntervalId>> {
        if len == 0 {
            return None;
        }

        let mut iter = self.present.iter();
        iter.advance_back_to(end_index_interval_id);
        let mut run_end = None;
        let mut previous: Option<IndexIntervalId> = None;
        let mut run_len = 0u64;

        for index_interval_id in iter.rev() {
            if previous.and_then(|value| value.checked_sub(1)) == Some(index_interval_id) {
                run_len += 1;
            } else {
                run_end = Some(index_interval_id);
                run_len = 1;
            }
            previous = Some(index_interval_id);

            if run_len >= len {
                return run_end.map(|end| index_interval_id..=end);
            }
        }
        None
    }
}

/// Independent index interval IDs for each ordered entity identity.
///
/// Explicit identities with empty coverage are preserved. An absent identity
/// is still treated as empty and returned as `None` by [`EntityCoverage::get`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EntityCoverage {
    by_identity: BTreeMap<EntityIdentity, Coverage>,
}

impl EntityCoverage {
    /// Construct empty entity-scoped coverage.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Borrow one identity's coverage.
    ///
    /// `None` means the identity is absent. Set operations treat absence as
    /// empty coverage.
    pub fn get(&self, identity: &EntityIdentity) -> Option<&Coverage> {
        self.by_identity.get(identity)
    }

    /// Iterate identities and their coverage in canonical order.
    pub fn iter(&self) -> btree_map::Iter<'_, EntityIdentity, Coverage> {
        self.by_identity.iter()
    }

    /// Number of stored identities.
    pub fn identity_count(&self) -> usize {
        self.by_identity.len()
    }

    /// Whether no identities are stored.
    pub fn is_empty(&self) -> bool {
        self.by_identity.is_empty()
    }

    /// Merge coverage for one identity.
    pub fn union_coverage(&mut self, identity: EntityIdentity, coverage: Coverage) {
        self.by_identity
            .entry(identity)
            .and_modify(|current| current.union_inplace(&coverage))
            .or_insert(coverage);
    }

    /// Return the union of two entity-scoped coverage values.
    pub fn union(&self, other: &Self) -> Self {
        let mut union = self.clone();
        union.union_inplace(other);
        union
    }

    /// Merge another entity-scoped coverage value into this one.
    pub fn union_inplace(&mut self, other: &Self) {
        for (identity, coverage) in other.iter() {
            if let Some(current) = self.by_identity.get_mut(identity) {
                current.union_inplace(coverage);
            } else {
                self.by_identity.insert(identity.clone(), coverage.clone());
            }
        }
    }

    /// Return overlap only where both identity and index interval ID match.
    pub fn intersect(&self, other: &Self) -> Self {
        let mut intersection = Self::empty();
        for (identity, coverage) in self.iter() {
            if let Some(other_coverage) = other.get(identity) {
                intersection.union_coverage(identity.clone(), coverage.intersect(other_coverage));
            }
        }
        intersection
    }

    /// Count covered `(entity identity, index interval ID)` pairs.
    pub fn cardinality(&self) -> u128 {
        self.by_identity
            .values()
            .map(|coverage| u128::from(coverage.cardinality()))
            .sum()
    }

    /// Count overlapping `(entity identity, index interval ID)` pairs.
    pub fn intersection_cardinality(&self, other: &Self) -> u128 {
        self.iter()
            .filter_map(|(identity, coverage)| {
                other.get(identity).map(|other_coverage| {
                    u128::from(coverage.intersection_cardinality(other_coverage))
                })
            })
            .sum()
    }

    /// Return the first canonical identity and smallest overlapping index interval ID.
    pub fn overlap_example<'a>(
        &'a self,
        other: &Self,
    ) -> Option<(&'a EntityIdentity, IndexIntervalId)> {
        self.iter().find_map(|(identity, coverage)| {
            let other_coverage = other.get(identity)?;
            if coverage.present().is_disjoint(other_coverage.present()) {
                return None;
            }
            coverage
                .intersect(other_coverage)
                .present()
                .min()
                .map(|index_interval_id| (identity, index_interval_id))
        })
    }
}

impl FromIterator<IndexIntervalId> for Coverage {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = IndexIntervalId>,
    {
        Self::from_treemap(iter.into_iter().collect())
    }
}

fn inclusive_len(start: IndexIntervalId, end: IndexIntervalId) -> u128 {
    u128::from(end) - u128::from(start) + 1
}

fn split_runs_by_len(
    runs: Vec<RangeInclusive<IndexIntervalId>>,
    max_len: u64,
) -> Vec<RangeInclusive<IndexIntervalId>> {
    if max_len == 0 {
        return Vec::new();
    }

    let mut out = Vec::new();
    for range in runs {
        let mut start = *range.start();
        let end = *range.end();
        loop {
            let chunk_end = start.saturating_add(max_len - 1).min(end);
            out.push(start..=chunk_end);
            if chunk_end == end {
                break;
            }
            start = chunk_end + 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_set_operations_use_u64_domain() {
        let a: Coverage = [0, u64::from(u32::MAX) + 1, u64::MAX].into_iter().collect();
        let b: Coverage = [u64::from(u32::MAX) + 1, 7].into_iter().collect();

        assert_eq!(a.cardinality(), 3);
        assert_eq!(a.intersection_cardinality(&b), 1);
        assert_eq!(a.intersect(&b).cardinality(), 1);
        assert_eq!(a.union(&b).cardinality(), 4);
    }

    #[test]
    fn sparse_huge_ranges_do_not_require_expected_bitmap() {
        let coverage: Coverage = [0, 2, u64::MAX].into_iter().collect();
        let range = 0..=u64::MAX;

        assert_eq!(Coverage::range_cardinality(&range), 1u128 << 64);
        assert_eq!(coverage.covered_cardinality(&range), 3);
        assert_eq!(
            coverage.missing_runs(&range, None),
            vec![1..=1, 3..=u64::MAX - 1]
        );
        assert_eq!(coverage.max_gap_len(&range), u128::from(u64::MAX) - 3);
    }

    #[test]
    fn missing_runs_split_without_enumerating_missing_points() {
        let coverage: Coverage = [2, 7].into_iter().collect();
        assert_eq!(
            coverage.missing_runs(&(0..=9), Some(2)),
            vec![0..=1, 3..=4, 5..=6, 8..=9]
        );
    }

    #[test]
    fn range_queries_respect_requested_bounds() {
        let coverage: Coverage = [2, 3, 4, 7, 8, 9].into_iter().collect();
        let range = 1..=8;

        assert_eq!(coverage.covered_cardinality(&range), 5);
        assert_eq!(coverage.missing_runs(&range, None), vec![1..=1, 5..=6]);
        assert_eq!(coverage.max_gap_len(&range), 2);
        assert_eq!(coverage.last_run_with_min_len(&range, 2), Some(7..=8));
        assert_eq!(coverage.coverage_ratio(&range), 5.0 / 8.0);
    }

    #[test]
    fn last_window_handles_zero_and_u64_max() {
        let coverage: Coverage = [0, 1, u64::MAX - 2, u64::MAX - 1, u64::MAX]
            .into_iter()
            .collect();

        assert_eq!(
            coverage.last_window_at_or_before(u64::MAX, 3),
            Some(u64::MAX - 2..=u64::MAX)
        );
        assert_eq!(coverage.last_window_at_or_before(1, 2), Some(0..=1));
        assert_eq!(coverage.last_window_at_or_before(u64::MAX, 0), None);
    }

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
    fn entity_identity_preserves_component_order() {
        assert_eq!(
            identity(&["venue", "symbol"]).components(),
            &[EntityValue::from("venue"), EntityValue::from("symbol")]
        );
        assert!(identity(&["A", "Z"]) < identity(&["B", "A"]));
        assert!(identity(&["A", "A"]) < identity(&["A", "Z"]));
        assert_ne!(identity(&["A", "A"]), identity(&["A", "Z"]));
        assert_eq!(
            EntityIdentity::try_new(Vec::new()),
            Err(EntityIdentityError::Empty)
        );
    }

    #[test]
    fn entity_identity_preserves_scalar_types_in_json() {
        let identity = EntityIdentity::try_new(vec![
            EntityValue::from("sensor"),
            EntityValue::Int32(-1),
            EntityValue::Int64(i64::MIN),
            EntityValue::UInt64(u64::MAX),
        ])
        .unwrap();

        let json = serde_json::to_value(&identity).unwrap();
        assert_eq!(
            json,
            serde_json::json!([
                { "type": "utf8", "value": "sensor" },
                { "type": "int32", "value": -1 },
                { "type": "int64", "value": i64::MIN },
                { "type": "uint64", "value": u64::MAX },
            ])
        );
        assert_eq!(
            serde_json::from_value::<EntityIdentity>(json).unwrap(),
            identity
        );
        assert_ne!(EntityValue::Int32(1), EntityValue::Int64(1));
        assert_ne!(EntityValue::Int64(1), EntityValue::UInt64(1));
    }

    #[test]
    fn entity_coverage_unions_only_matching_identities() {
        let a = identity(&["A"]);
        let b = identity(&["B"]);
        let c = identity(&["C"]);
        let mut left = EntityCoverage::empty();
        left.union_coverage(a.clone(), [1, 2].into_iter().collect());
        left.union_coverage(b.clone(), [1].into_iter().collect());

        let mut right = EntityCoverage::empty();
        right.union_coverage(a.clone(), [2, 3].into_iter().collect());
        right.union_coverage(c.clone(), [4].into_iter().collect());

        let union = left.union(&right);
        assert_eq!(union.identity_count(), 3);
        assert_eq!(union.cardinality(), 5);
        assert_eq!(
            union.get(&a).unwrap().present().iter().collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            union.get(&b).unwrap().present().iter().collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            union.get(&c).unwrap().present().iter().collect::<Vec<_>>(),
            vec![4]
        );
    }

    #[test]
    fn entity_coverage_intersection_requires_identity_and_interval() {
        let a = identity(&["A"]);
        let b = identity(&["B"]);
        let mut left = EntityCoverage::empty();
        left.union_coverage(a.clone(), [1, 2].into_iter().collect());
        left.union_coverage(b.clone(), [7].into_iter().collect());

        let mut right = EntityCoverage::empty();
        right.union_coverage(a.clone(), [2, 7].into_iter().collect());
        right.union_coverage(b.clone(), [1].into_iter().collect());

        let intersection = left.intersect(&right);
        assert_eq!(intersection.identity_count(), 2);
        assert_eq!(intersection.cardinality(), 1);
        assert_eq!(left.intersection_cardinality(&right), 1);
        assert_eq!(
            intersection
                .get(&a)
                .unwrap()
                .present()
                .iter()
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert!(intersection.get(&b).unwrap().is_empty());
    }

    #[test]
    fn entity_coverage_counts_same_interval_once_per_identity() {
        let mut coverage = EntityCoverage::empty();
        coverage.union_coverage(identity(&["A"]), [u64::MAX].into_iter().collect());
        coverage.union_coverage(identity(&["B"]), [u64::MAX].into_iter().collect());

        assert_eq!(coverage.cardinality(), 2);
    }

    #[test]
    fn entity_coverage_overlap_example_is_deterministic() {
        let first = identity(&["A", "one"]);
        let later = identity(&["B", "one"]);
        let mut left = EntityCoverage::empty();
        left.union_coverage(later.clone(), [1].into_iter().collect());
        left.union_coverage(first.clone(), [9, 3].into_iter().collect());

        let mut right = EntityCoverage::empty();
        right.union_coverage(later, [1].into_iter().collect());
        right.union_coverage(first.clone(), [3, 9].into_iter().collect());

        assert_eq!(left.overlap_example(&right), Some((&first, 3)));
    }

    #[test]
    fn entity_coverage_overlap_example_does_not_enumerate_dense_intervals() {
        let entity = identity(&["dense"]);
        let last = u64::from(u32::MAX);
        let mut dense = RoaringTreemap::new();
        dense.insert_range(0..=last);

        let mut left = EntityCoverage::empty();
        left.union_coverage(entity.clone(), Coverage::from_treemap(dense));
        let mut right = EntityCoverage::empty();
        right.union_coverage(entity.clone(), [last].into_iter().collect());

        assert_eq!(left.overlap_example(&right), Some((&entity, last)));
    }

    #[test]
    fn explicit_empty_entity_coverage_is_preserved() {
        let entity = identity(&["empty"]);
        let mut coverage = EntityCoverage::empty();
        coverage.union_coverage(entity.clone(), Coverage::empty());

        assert!(!coverage.is_empty());
        assert!(coverage.get(&entity).unwrap().is_empty());
        assert!(coverage.get(&identity(&["absent"])).is_none());
        assert_eq!(coverage.identity_count(), 1);
    }
}
