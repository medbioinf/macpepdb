//! Integration tests for the search path, run against a live PostgreSQL/Citus database.
//!
//! Self-contained: each run resets the schema (`db.sql`) and builds the DB from the
//! checked-in E. coli K12 proteome fixture (`test_data/uniprot_2026_02_up000000625.txt.gz`,
//! UniProt proteome UP000000625), so the test doesn't depend on whatever was previously
//! loaded into the DB.
//!
//! These are `#[ignore]`d so plain `cargo test` stays DB-free (per CLAUDE.md). Run explicitly:
//!
//! ```bash
//! cargo test -p macpepdb --test live_db_peptide_search -- --ignored --nocapture
//! ```
//!
//! Requires a running Citus cluster (`docker compose up -d --scale worker=2`) and `psql` on
//! PATH. The DB URL defaults to `postgresql://postgres@127.0.0.1:5432/postgres` and can be
//! overridden with `MACPEPDB_TEST_DATABASE_URL`; `psql` connects to the same host/user,
//! reading the database name off that URL.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::{Mutex, OnceCell};

use macpepdb::amino_acid::{AminoAcid, GLYCINE};
use macpepdb::client::Client;
use macpepdb::configuration::RuntimeConfiguration;
use macpepdb::database_build::DatabaseBuild;
use macpepdb::peptide::{IsPeptide, Peptide};
use macpepdb::peptide_search::{PeptideCondition, PeptideConditionBuilder, PeptideSearch};
use macpepdb::peptide_table::{FULL_PEPTIDE_COLUMN_SELECTION, PeptideTable};
use macpepdb::post_translational_modification::{PTMCollection, PostTranslationalModification};
use macpepdb::protease::Protease;
use macpepdb::sequence::ModifiedSequencePart;

fn database_url() -> String {
    std::env::var("MACPEPDB_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres@127.0.0.1:5432/postgres".to_string())
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Applies `db.sql` (drops + recreates the schema — this is a rebuildable-from-source DB,
/// see CLAUDE.md) against the target database via `psql`, so every run starts from a known,
/// empty state.
fn reset_schema(database: &str) {
    let db_sql_path = repo_root().join("db.sql");
    let status = std::process::Command::new("psql")
        .args(["-h", "127.0.0.1", "-U", "postgres", "-d", database])
        .args(["-v", "ON_ERROR_STOP=1", "-f"])
        .arg(&db_sql_path)
        .status()
        .expect("run `psql` to reset the schema — is it on PATH and is the DB reachable?");
    assert!(
        status.success(),
        "`psql -f {}` failed",
        db_sql_path.display()
    );
}

/// Digests the checked-in E. coli K12 fixture (trypsin, length 6-50, up to 2 missed
/// cleavages) and persists the resulting `RuntimeConfiguration` — mirrors the `build` CLI
/// subcommand (`main.rs`), skipping taxonomy collection since these tests don't need it.
async fn build_from_fixture(client: Arc<Client>) -> RuntimeConfiguration {
    let protein_files = vec![
        repo_root()
            .join("test_data")
            .join("uniprot_2026_02_up000000625.txt.gz"),
    ];

    let protease = Protease::by_name(
        "trypsin",
        NonZeroUsize::new(6),
        NonZeroUsize::new(50),
        Some(2),
        false,
    )
    .unwrap();

    DatabaseBuild::new(
        client,
        Some("live_db_peptide_search integration test".to_string()),
        &protein_files,
        protease,
        NonZeroUsize::new(128).unwrap(),
        NonZeroUsize::new(5).unwrap(),
        0.8,
        false,
        false,
        true,
        NonZeroUsize::new(4).unwrap(),
        None,
        false,
        std::env::temp_dir(),
        None,
    )
    .start()
    .await
    .expect("build database from the E. coli fixture proteome")
}

/// Serializes the tests below: they hit the same DB, so running them concurrently — the
/// default `cargo test` behavior — races their queries against each other and can exhaust
/// the DB's connection limit. Held for a whole test's body, not just the build.
static TEST_LOCK: Mutex<()> = Mutex::const_new(());

/// Resets the schema and rebuilds the DB from the fixture exactly once per test binary run,
/// however many of the `#[ignore]`d tests below actually execute. The build's `Client` is
/// scoped to this function and dropped before returning — `deadpool_postgres` spawns each
/// pooled connection's driver task onto the runtime that's live when the connection is
/// created, and `#[tokio::test]` tears its runtime down when the test function returns, which
/// would kill those driver tasks out from under a `Client` shared across tests. So every test
/// below gets its own fresh `Client` on its own runtime; only the `RuntimeConfiguration` data
/// (no runtime-bound resources) is cached and shared via `OnceCell`.
async fn shared_configuration() -> Arc<RuntimeConfiguration> {
    static CONFIGURATION: OnceCell<Arc<RuntimeConfiguration>> = OnceCell::const_new();
    CONFIGURATION
        .get_or_init(|| async {
            let client = Arc::new(
                Client::new(&database_url())
                    .await
                    .expect("connect to live DB"),
            );
            reset_schema(client.database());
            Arc::new(build_from_fixture(client).await)
        })
        .await
        .clone()
}

/// Fresh `Client` for the calling test's own runtime, plus the shared (built-once)
/// configuration. See [`shared_configuration`] for why the `Client` isn't cached/shared.
async fn setup() -> (Arc<Client>, Arc<RuntimeConfiguration>) {
    let configuration = shared_configuration().await;
    let client = Arc::new(
        Client::new(&database_url())
            .await
            .expect("connect to live DB"),
    );
    (client, configuration)
}

/// Grabs one real peptide's mass from the DB so tests target a mass that's guaranteed to
/// have at least one match.
async fn sample_peptide_mass(client: &Arc<Client>, order_and_limit: &str) -> i64 {
    let table = PeptideTable::new(client.clone());
    let mut stream = table
        .select(&FULL_PEPTIDE_COLUMN_SELECTION, order_and_limit, Vec::new())
        .await
        .unwrap();
    let peptide: Peptide = stream
        .next()
        .await
        .expect("peptides table is empty — build the DB first")
        .unwrap();
    peptide.mass()
}

/// Same integer-truncating ppm window formula as `PeptideConditionBuilder::finalize`
/// (packages/macpepdb/src/peptide_search.rs).
fn ppm_window(mass: i64, lower_ppm: i64, upper_ppm: i64) -> (i64, i64) {
    let lower = mass - (mass / 1_000_000 * lower_ppm);
    let upper = mass + (mass / 1_000_000 * upper_ppm);
    (lower, upper)
}

fn ptm_collection_from_fixture() -> Arc<PTMCollection<Arc<PostTranslationalModification>>> {
    let path = repo_root().join("test_data").join("ptms.tsv");

    let ptms: Vec<Arc<PostTranslationalModification>> = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_path(path)
        .unwrap()
        .deserialize()
        .map(|result: Result<PostTranslationalModification, csv::Error>| result.map(Arc::new))
        .collect::<Result<Vec<_>, csv::Error>>()
        .unwrap();

    Arc::new(PTMCollection::new(ptms).unwrap())
}

/// Reads the independently-generated reference peptide list (one sequence per line,
/// gzip-compressed) — every peptide trypsin (length 6-50, ≤2 missed cleavages) should produce
/// from the E. coli K12 fixture proteome. Used as ground truth independent of this codebase's
/// own digestion/build logic.
fn expected_peptide_fixture() -> HashSet<String> {
    use std::io::{BufRead, BufReader};
    let path = repo_root()
        .join("test_data")
        .join("uniprot_2026_02_up000000625_peptides.txt.gz");
    let file =
        std::fs::File::open(&path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let reader = BufReader::new(flate2::read::GzDecoder::new(file));
    reader
        .lines()
        .map(|line| line.expect("read line from peptide fixture"))
        .filter(|line| !line.is_empty())
        .collect()
}

/// Every `Peptidoform` a search returns must have a mass inside the ppm window it was
/// searched with — regardless of which/how many PTMs were resolved onto it. Additionally, no
/// peptide left out of the results may actually satisfy the query mass + PTM collection: this
/// is checked independently of `MultiTaskSearch`'s partition-routing/streaming machinery by
/// rebuilding the same `PeptideCondition`s and running every un-returned DB peptide through
/// them directly.
#[tokio::test]
#[ignore]
async fn test_peptidoforms_match_queried_mass() {
    let _guard = TEST_LOCK.lock().await;
    let (client, configuration) = setup().await;
    let target_mass = sample_peptide_mass(&client, "ORDER BY mass LIMIT 1").await;

    let lower_ppm = 20;
    let upper_ppm = 20;
    let max_variable_modifications = 2;
    let (lower_mass, upper_mass) = ppm_window(target_mass, lower_ppm, upper_ppm);
    let ptm_collection = ptm_collection_from_fixture();

    let mut stream = PeptideSearch::search(
        client.clone(),
        &FULL_PEPTIDE_COLUMN_SELECTION,
        configuration.clone(),
        target_mass,
        lower_ppm,
        upper_ppm,
        max_variable_modifications,
        true,
        None,
        None,
        None,
        ptm_collection.clone(),
        true,
        NonZeroUsize::new(4).unwrap(),
    )
    .await
    .unwrap();

    let mut peptidoforms = Vec::new();
    while let Some(batch) = stream.next().await {
        peptidoforms.extend(batch.unwrap());
    }

    assert!(
        !peptidoforms.is_empty(),
        "expected at least one match for a mass sampled directly from the DB"
    );

    for peptidoform in &peptidoforms {
        let mass = peptidoform.mass();
        assert!(
            mass >= lower_mass && mass <= upper_mass,
            "peptidoform {} has mass {mass}, outside queried window [{lower_mass}, {upper_mass}]",
            peptidoform.sequence(),
        );
    }

    // Recall check: rebuild the same PTM conditions `MultiTaskSearch` would have used, and
    // confirm no DB peptide left out of `peptidoforms` actually matches one of them.
    let returned_sequences: HashSet<String> = peptidoforms
        .iter()
        .map(|peptidoform| {
            peptidoform
                .sequence()
                .iter()
                .filter_map(|part| match part {
                    ModifiedSequencePart::AminoAcid(aa) => Some(AminoAcid::by_bit_code(aa).code()),
                    _ => None,
                })
                .collect::<String>()
        })
        .collect();

    // Same min/max mass bound formula as `MultiTaskSearch::search` (peptide_search.rs:1368-1395).
    let min_mass = configuration.protease().min_length().get() as i64 * GLYCINE.mono_mass();
    let largest_negative_static_ptm = ptm_collection
        .get_static_ptms()
        .iter()
        .filter(|ptm| ptm.mass_delta().is_negative())
        .fold(0_i64, |acc, ptm| acc.min(ptm.mass_delta()))
        .abs();
    let largest_negative_variable_ptm = ptm_collection
        .get_variable_ptms()
        .iter()
        .filter(|ptm| ptm.mass_delta().is_negative())
        .fold(0_i64, |acc, ptm| acc.min(ptm.mass_delta()))
        .abs();
    let amino_acid_average = AminoAcid::canonical()
        .iter()
        .map(|aa| aa.mono_mass())
        .sum::<i64>()
        / AminoAcid::canonical().len() as i64;
    let possible_peptide_length = ((target_mass / amino_acid_average) as f64 * 1.3) as i64;
    let max_mass = target_mass
        + (largest_negative_static_ptm * possible_peptide_length)
        + (largest_negative_variable_ptm * possible_peptide_length);

    let mut conditions: Vec<PeptideCondition> = PeptideConditionBuilder::from_ptm_collection(
        &ptm_collection,
        target_mass,
        min_mass,
        max_mass,
        max_variable_modifications,
    )
    .into_iter()
    .flat_map(|builder| builder.finalize(configuration.mass_partitioning(), lower_ppm, upper_ppm))
    .collect();

    let table = PeptideTable::new(client.clone());
    let mut all_peptides = table
        .select(&FULL_PEPTIDE_COLUMN_SELECTION, "", Vec::new())
        .await
        .unwrap();
    while let Some(peptide) = all_peptides.next().await {
        let peptide = peptide.unwrap();
        if returned_sequences.contains(&peptide.sequence().to_string()) {
            continue;
        }
        let mass = peptide.mass();
        for condition in conditions.iter_mut() {
            assert!(
                !(mass >= condition.lower_mass()
                    && mass <= condition.upper_mass()
                    && condition.is_match(&peptide)),
                "peptide {} (mass {mass}) was not returned by the search but matches a PTM \
                 condition in window [{}, {}]",
                peptide.sequence(),
                condition.lower_mass(),
                condition.upper_mass(),
            );
        }
    }
}

/// Every peptide in the independently-generated reference list must exist in the DB after the
/// build — an end-to-end check on the whole digestion/build pipeline, not just search recall
/// within one mass window.
#[tokio::test]
#[ignore]
async fn test_every_fixture_peptide_exists_in_database() {
    let _guard = TEST_LOCK.lock().await;
    let (client, _configuration) = setup().await;

    let expected = expected_peptide_fixture();

    let table = PeptideTable::new(client.clone());
    let mut stream = table
        .select(&FULL_PEPTIDE_COLUMN_SELECTION, "", Vec::new())
        .await
        .unwrap();
    let mut actual = HashSet::new();
    while let Some(peptide) = stream.next().await {
        actual.insert(peptide.unwrap().sequence().to_string());
    }

    let missing: Vec<&String> = expected.difference(&actual).collect();
    assert!(
        missing.is_empty(),
        "{} of {} fixture peptides missing from DB, e.g. {:?}",
        missing.len(),
        expected.len(),
        missing.iter().take(20).collect::<Vec<_>>()
    );
}
