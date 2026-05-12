use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    num::NonZeroU16,
    ops::Deref,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::mass_counter::MassCounter;

pub static PROGRESS_METRIC: &str = "mass_partitioner::progress";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Bins cannot be empty")]
    NoBins,
    #[error("Too many bins for partitioning: {0} (max 2^32)")]
    TooManyBins(usize),
}

/// A bin holding a subset of [`Entry`] items whose `count` values sum to
/// [`Bin::total_count`].
#[derive(Debug, Clone)]
pub struct Bin {
    entries: Vec<(i64, usize)>,
    total_count: usize,
}

impl Bin {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            total_count: 0,
        }
    }

    /// The entries assigned to this bin.
    pub fn entries(&self) -> &[(i64, usize)] {
        &self.entries
    }

    /// The sum of [`Entry::count`] for all entries in this bin.
    pub fn total_count(&self) -> usize {
        self.total_count
    }

    /// Consume the bin and return its entries.
    pub fn into_entries(self) -> Vec<(i64, usize)> {
        self.entries
    }

    fn push(&mut self, entry: (i64, usize)) {
        self.total_count += entry.1;
        self.entries.push(entry);
    }

    pub fn min_mass(&self) -> Option<i64> {
        self.entries.iter().map(|e| e.0).min()
    }

    pub fn max_mass(&self) -> Option<i64> {
        self.entries.iter().map(|e| e.0).max()
    }
}

impl Default for Bin {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct MassPartitioning(HashMap<i64, i16>);

impl MassPartitioning {
    // TODO: Remove async
    pub async fn build(mass_counter: MassCounter, num_bins: NonZeroU16) -> Result<Self, Error> {
        let mass_count_entries = mass_counter.into_iter().collect::<Vec<_>>();

        let bins = Self::distribute_into_bins(mass_count_entries, num_bins);
        Ok(Self(Self::bins_to_lookup_table(&bins)?))
    }

    /// Distributes `entries` into exactly `num_bins` bins so that each bin's
    /// total record count is as equal as possible.
    ///
    /// # Algorithm — LPT (Longest Processing Time first)
    ///
    /// This is a well-known greedy heuristic for the *multiprocessor scheduling*
    /// problem (minimise makespan):
    ///
    /// 1. **Sort** entries by `count` in *descending* order.  Placing the largest
    ///    items first leaves less room for imbalance when smaller items are
    ///    assigned later.
    /// 2. **Iterate** over the sorted entries.  For each entry, assign it to the
    ///    bin that currently has the *smallest* total count.  A binary min-heap
    ///    makes this O(log k) per assignment, giving an overall complexity of
    ///    **O(n log k)** where *n* is the number of entries and *k* is
    ///    `num_bins`.
    ///
    /// The LPT heuristic guarantees that the heaviest bin is at most
    /// **4/3 − 1/(3k)** times the optimal makespan, and in practice the result
    /// is very close to optimal.
    ///
    /// # Edge cases
    ///
    /// * If `entries` is empty, every bin is returned empty.
    /// * If `num_bins` is greater than the number of entries, the surplus bins
    ///   are returned empty (zero total count, no entries).
    /// ```
    fn distribute_into_bins(mut entries: Vec<(i64, usize)>, num_bins: NonZeroU16) -> Vec<Bin> {
        let k = num_bins.get() as usize;

        // Sort descending by count so large entries are placed first.
        entries.sort_by_key(|entry| entry.0);

        // Min-heap keyed by (current_total, bin_index).
        // `Reverse` turns Rust's max-heap into a min-heap.
        let mut heap: BinaryHeap<Reverse<(usize, usize)>> =
            (0..k).map(|i| Reverse((0_usize, i))).collect();

        let mut bins: Vec<Bin> = (0..k).map(|_| Bin::new()).collect();

        for entry in entries {
            // The bin with the smallest running total.
            let Reverse((_, idx)) = heap.pop().expect("heap always has k elements");

            bins[idx].push(entry);

            // Re-insert with the updated total.
            heap.push(Reverse((bins[idx].total_count, idx)));
        }

        bins
    }

    fn bins_to_lookup_table(bins: &[Bin]) -> Result<HashMap<i64, i16>, Error> {
        if bins.len() > 2_usize.pow(32) {
            return Err(Error::TooManyBins(bins.len()));
        }
        if bins.is_empty() {
            return Err(Error::NoBins);
        }

        Ok(bins
            .iter()
            .enumerate()
            .flat_map(|(bin_idx, bin)| {
                bin.entries()
                    .iter()
                    .map(|entry| (entry.0, bin_idx as i16))
                    .collect::<Vec<_>>()
            })
            .collect())
    }

    /// Just for internal use int test
    #[cfg(test)]
    pub(crate) fn from_iter(iter: impl Iterator<Item = (i64, i16)>) -> Self {
        Self(iter.collect())
    }
}

impl Deref for MassPartitioning {
    type Target = HashMap<i64, i16>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn total_records(bins: &[Bin]) -> usize {
        bins.iter().map(|b| b.total_count()).sum()
    }

    /// Every record must end up in exactly one bin.
    #[test]
    fn test_total_count_is_preserved() {
        let entries: Vec<(i64, usize)> = vec![(100, 10), (200, 20), (300, 5), (400, 15), (500, 8)];
        let expected: usize = entries.iter().map(|e| e.1).sum();

        let bins = MassPartitioning::distribute_into_bins(entries, NonZeroU16::new(3).unwrap());

        assert_eq!(total_records(&bins), expected);
    }

    /// With a single bin all entries must land in it.
    #[test]
    fn test_single_bin() {
        let entries: Vec<(i64, usize)> = vec![(1, 7), (2, 3), (3, 11)];
        let bins = MassPartitioning::distribute_into_bins(entries, NonZeroU16::new(1).unwrap());

        assert_eq!(bins.len(), 1);
        assert_eq!(bins[0].entries().len(), 3);
        assert_eq!(bins[0].total_count(), 21);
    }

    /// When there are more bins than entries the surplus bins must be empty.
    #[test]
    fn test_more_bins_than_entries() {
        let entries: Vec<(i64, usize)> = vec![(1, 5), (2, 5)];
        let bins = MassPartitioning::distribute_into_bins(entries, NonZeroU16::new(5).unwrap());

        assert_eq!(bins.len(), 5);

        let non_empty: Vec<_> = bins.iter().filter(|b| !b.entries().is_empty()).collect();
        assert_eq!(non_empty.len(), 2);

        let empty: Vec<_> = bins.iter().filter(|b| b.entries().is_empty()).collect();
        assert_eq!(empty.len(), 3);
        for b in empty {
            assert_eq!(b.total_count(), 0);
        }
    }

    /// An empty input must produce the requested number of empty bins.
    #[test]
    fn test_empty_entries() {
        let bins = MassPartitioning::distribute_into_bins(vec![], NonZeroU16::new(4).unwrap());

        assert_eq!(bins.len(), 4);
        assert!(bins.iter().all(|b| b.entries().is_empty()));
        assert!(bins.iter().all(|b| b.total_count() == 0));
    }

    /// For perfectly uniform entries every bin should have the same total.
    #[test]
    fn test_uniform_entries_are_perfectly_balanced() {
        // 6 entries each with count=10, split into 3 bins → each bin gets 20.
        let entries: Vec<(i64, usize)> = (0..6).map(|i| (i, 10)).collect();
        let bins = MassPartitioning::distribute_into_bins(entries, NonZeroU16::new(3).unwrap());

        for bin in &bins {
            assert_eq!(bin.total_count(), 20);
            assert_eq!(bin.entries().len(), 2);
        }
    }

    /// Entries within every bin must be ordered by ascending mass.
    #[test]
    fn test_entries_are_sorted_by_ascending_mass_within_bins() {
        let entries: Vec<(i64, usize)> =
            vec![(500, 10), (100, 5), (300, 20), (200, 8), (400, 15), (50, 3)];

        let bins = MassPartitioning::distribute_into_bins(entries, NonZeroU16::new(3).unwrap());

        for bin in &bins {
            let masses: Vec<i64> = bin.entries().iter().map(|e| e.0).collect();
            let mut sorted = masses.clone();
            sorted.sort_unstable();
            assert_eq!(
                masses, sorted,
                "entries in bin are not sorted by ascending mass"
            );
        }
    }

    /// The maximum bin total must not exceed the minimum by more than the
    /// largest single entry count (a simple sanity bound for balance).
    #[test]
    fn test_reasonable_balance() {
        let entries = vec![
            (1, 100),
            (2, 90),
            (3, 80),
            (4, 70),
            (5, 60),
            (6, 50),
            (7, 40),
            (8, 30),
        ];
        let max_single = entries.iter().map(|e| e.1).max().unwrap();

        let bins = MassPartitioning::distribute_into_bins(entries, NonZeroU16::new(3).unwrap());

        let max_total = bins.iter().map(|b| b.total_count()).max().unwrap();
        let min_total = bins.iter().map(|b| b.total_count()).min().unwrap();

        assert!(
            max_total - min_total <= max_single,
            "imbalance ({}) exceeded largest entry ({})",
            max_total - min_total,
            max_single
        );
    }
}
