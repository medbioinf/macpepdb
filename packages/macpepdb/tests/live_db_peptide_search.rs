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
use postgres_types::ToSql;
use tokio::sync::{Mutex, OnceCell};

use macpepdb::client::Client;
use macpepdb::configuration::RuntimeConfiguration;
use macpepdb::database_build::DatabaseBuild;
use macpepdb::peptide::{IsPeptide, Peptide};
use macpepdb::peptide_search::{MultiTaskSearch, Search, UnionAllSearch};
use macpepdb::peptide_table::PeptideTable;
use macpepdb::post_translational_modification::{PTMCollection, PostTranslationalModification};
use macpepdb::protease::Protease;

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
    assert!(status.success(), "`psql -f {}` failed", db_sql_path.display());
}

/// Digests the checked-in E. coli K12 fixture (trypsin, length 6-50, up to 2 missed
/// cleavages) and persists the resulting `RuntimeConfiguration` — mirrors the `build` CLI
/// subcommand (`main.rs`), skipping taxonomy collection since these tests don't need it.
async fn build_from_fixture(client: Arc<Client>) -> RuntimeConfiguration {
    let protein_files = vec![repo_root()
        .join("test_data")
        .join("uniprot_2026_02_up000000625.txt.gz")];

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
        NonZeroUsize::new(512).unwrap(),
        NonZeroUsize::new(100).unwrap(),
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
    let mut stream = table.select(order_and_limit, Vec::new()).await.unwrap();
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

fn empty_ptm_collection() -> Arc<PTMCollection<Arc<PostTranslationalModification>>> {
    Arc::new(PTMCollection::new(Vec::<Arc<PostTranslationalModification>>::new()).unwrap())
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

/// Every `Peptidoform` a search returns must have a mass inside the ppm window it was
/// searched with — regardless of which/how many PTMs were resolved onto it.
#[tokio::test]
#[ignore]
async fn test_peptidoforms_match_queried_mass() {
    let _guard = TEST_LOCK.lock().await;
    let (client, configuration) = setup().await;
    let target_mass = sample_peptide_mass(&client, "ORDER BY mass LIMIT 1").await;

    let lower_ppm = 20;
    let upper_ppm = 20;
    let (lower_mass, upper_mass) = ppm_window(target_mass, lower_ppm, upper_ppm);

    let mut stream = MultiTaskSearch::search(
        client,
        configuration,
        target_mass,
        lower_ppm,
        upper_ppm,
        2,
        true,
        None,
        None,
        None,
        ptm_collection_from_fixture(),
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
}

/// The search must return every peptide in the DB whose mass falls in the queried window —
/// checked against an independent direct table scan, not against the search's own logic.
#[tokio::test]
#[ignore]
async fn test_search_recall_matches_every_peptide_in_mass_window() {
    let _guard = TEST_LOCK.lock().await;
    let (client, configuration) = setup().await;
    let target_mass = sample_peptide_mass(&client, "ORDER BY mass LIMIT 1 OFFSET 5").await;

    let lower_ppm = 50;
    let upper_ppm = 50;
    let (lower_mass, upper_mass) = ppm_window(target_mass, lower_ppm, upper_ppm);

    // Ground truth: direct table scan, bypassing PeptideConditionBuilder/partition pruning.
    let table = PeptideTable::new(client.clone());
    let params: Vec<Box<dyn ToSql + Sync + Send>> =
        vec![Box::new(lower_mass), Box::new(upper_mass)];
    let mut ground_truth_stream = table
        .select("WHERE mass BETWEEN $1 AND $2", params)
        .await
        .unwrap();
    let mut expected_sequences = HashSet::new();
    while let Some(peptide) = ground_truth_stream.next().await {
        expected_sequences.insert(peptide.unwrap().sequence().to_string());
    }
    assert!(
        !expected_sequences.is_empty(),
        "expected at least one peptide in the sampled mass window"
    );

    let multi_task_sequences = collect_sequences(
        MultiTaskSearch::search(
            client.clone(),
            configuration.clone(),
            target_mass,
            lower_ppm,
            upper_ppm,
            0,
            true,
            None,
            None,
            None,
            empty_ptm_collection(),
            false,
            NonZeroUsize::new(4).unwrap(),
        )
        .await
        .unwrap(),
    )
    .await;

    assert_eq!(
        multi_task_sequences, expected_sequences,
        "MultiTaskSearch missed or over-returned peptides vs. a direct table scan"
    );

    let union_all_sequences = collect_sequences(
        UnionAllSearch::search(
            client,
            configuration,
            target_mass,
            lower_ppm,
            upper_ppm,
            0,
            true,
            None,
            None,
            None,
            empty_ptm_collection(),
            false,
            NonZeroUsize::new(4).unwrap(),
        )
        .await
        .unwrap(),
    )
    .await;

    assert_eq!(
        union_all_sequences, expected_sequences,
        "UnionAllSearch missed or over-returned peptides vs. a direct table scan"
    );
}

async fn collect_sequences(
    mut stream: std::pin::Pin<Box<dyn macpepdb::peptide_search::IsFallibleMatchingPeptideStream>>,
) -> HashSet<String> {
    let mut sequences = HashSet::new();
    while let Some(batch) = stream.next().await {
        for peptidoform in batch.unwrap() {
            sequences.insert(peptidoform.sequence().to_string());
        }
    }
    sequences
}
