//! Resolves `peptides.partition` values to the Citus shard that stores them.
//!
//! A mass search fans out into one condition per `(partition, mass window)` pair — thousands
//! of them for a PTM-heavy query. Each condition used to become its own statement, and each
//! statement became one Citus task that scans *all* of its shard's columnar chunk-group
//! metadata (~12.5k entries, ~95k buffer accesses) just to read the handful of chunk groups
//! its mass window covers. That metadata scan is the dominant per-statement cost, and it is
//! paid **per task**, not per condition.
//!
//! Since there are far fewer shards than partitions (1024 vs ~800k in production), most of
//! that work is redundant: conditions whose partitions live on the same shard can be OR-ed
//! into one statement, which Citus plans as a single task and which still prunes chunk
//! groups per disjunct. Collapsing ~3.6 conditions per shard into one statement removes
//! ~72% of the metadata scans, plus the same share of round trips and coordinator planning.
//!
//! Grouping is a pure optimisation: if the shard mapping cannot be obtained (plain
//! PostgreSQL without Citus, a renamed UDF, missing privileges) the caller falls back to one
//! statement per condition, which is what the code did before.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, RwLock};

use crate::client::Client;
use crate::peptide_table::TABLE_NAME;

/// Resolves a batch of partition values to shard IDs in one round trip. `unnest` keeps this
/// a single statement regardless of how many partitions a search touches, and the table name
/// is inlined because it is a compile-time constant.
static LOOKUP_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT p, get_shard_id_for_distribution_column('{TABLE_NAME}'::regclass, p) \
         FROM unnest($1::bigint[]) AS p"
    )
});

/// One [`ShardMap`] per database URL. The partition → shard assignment is a pure function of
/// the distribution column and the table's shard ranges, so it never changes for a given
/// database and is worth caching for the process lifetime. Keying by URL keeps the cache
/// correct when the `admin-api` feature repoints the server at a different database.
static REGISTRY: LazyLock<RwLock<HashMap<String, Arc<ShardMap>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Returns the shared [`ShardMap`] for `client`'s database, creating it on first use.
pub fn for_client(client: &Client) -> Arc<ShardMap> {
    if let Some(map) = REGISTRY.read().unwrap().get(client.url()) {
        return map.clone();
    }
    REGISTRY
        .write()
        .unwrap()
        .entry(client.url().to_string())
        .or_default()
        .clone()
}

/// Lazily populated partition → shard cache for one database.
#[derive(Default)]
pub struct ShardMap {
    cache: RwLock<HashMap<i64, i64>>,
    /// Set once the lookup has failed, so a database that cannot answer it is asked only
    /// once instead of on every search.
    unavailable: AtomicBool,
}

impl ShardMap {
    /// Maps each of `partitions` to its shard, querying the database only for the ones not
    /// already cached.
    ///
    /// Returns `None` when the mapping is unavailable — the caller must then treat every
    /// condition as its own group. A partition missing from the returned map (the lookup
    /// yielded no row, or a `NULL` shard) is likewise the caller's cue to leave that
    /// condition ungrouped.
    ///
    /// # Arguments
    /// * `client` - Client to run the lookup through
    /// * `partitions` - Partition values to resolve; duplicates are fine
    pub async fn shards_for(
        &self,
        client: &Client,
        partitions: &[i64],
    ) -> Option<HashMap<i64, i64>> {
        if self.unavailable.load(Ordering::Relaxed) {
            return None;
        }

        let mut resolved: HashMap<i64, i64> = HashMap::with_capacity(partitions.len());
        let mut missing: Vec<i64> = Vec::new();
        {
            let cache = self.cache.read().unwrap();
            for partition in partitions {
                match cache.get(partition) {
                    Some(shard) => {
                        resolved.insert(*partition, *shard);
                    }
                    None => missing.push(*partition),
                }
            }
        }
        if missing.is_empty() {
            return Some(resolved);
        }
        missing.sort_unstable();
        missing.dedup();

        let rows = match client.query(&LOOKUP_SQL, &[&missing]).await {
            Ok(rows) => rows,
            Err(err) => {
                if !self.unavailable.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        "shard lookup failed, peptide search falls back to one statement per \
                         condition: {err}"
                    );
                }
                return None;
            }
        };

        let mut cache = self.cache.write().unwrap();
        for row in rows.iter() {
            let (Ok(partition), Ok(shard)) = (row.try_get::<_, i64>(0), row.try_get::<_, i64>(1))
            else {
                // No shard for this partition — leave it out so it stays ungrouped.
                continue;
            };
            cache.insert(partition, shard);
            resolved.insert(partition, shard);
        }

        Some(resolved)
    }
}
