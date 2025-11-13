use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use axum::routing::{get, post};
use axum::{middleware, Router};
use dihardts_omicstools::biology::taxonomy::TaxonomyTree;
use http::Method;
use indicium::simple::SearchIndex;
use tower_http::cors::{Any, CorsLayer};

use crate::database::generic_client::GenericClient;
use crate::database::scylla::client::Client;
use crate::database::scylla::configuration_table::ConfigurationTable;
use crate::database::scylla::taxonomy_tree_table::TaxonomyTreeTable;
use crate::entities::configuration::Configuration;
use crate::web::app_state::{AppState, MatomoInfo};
use crate::web::chemistry_controller::{get_all_amino_acids, get_amino_acid};
use crate::web::configuration_controller::get_configuration;
use crate::web::error_controller::page_not_found;
use crate::web::middleware::tracking_middleware;
use crate::web::peptide_controller::{
    get_peptide, get_peptide_existence, get_search as get_peptide_search,
    post_search as post_peptide_search,
};
use crate::web::protein_controller::{get_protein, search_protein};
use crate::web::taxonomy_controller::{get_sub_taxonomies, get_taxonomy, search_taxonomies};
use crate::web::tools_controller::{digest, get_mass, get_partition, get_proteases};

/// Default shutdown signal handler for ctrl-c and terminate signals
///
async fn default_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Starts the MaCPepDB web server on the given interface and port.
///
/// # Arguments
/// * `database_nodes` - List of database nodes
/// * `interface` - Interface to listen on
/// * `port` - Port to listen on
/// * `with_taxonomy_search` - If taxonomy search index should be built
/// * `num_search_threads` - Number of concurrent search threads (and connections)
/// * `matomo_info` - Optional Matomo tracking information
///
pub async fn start(
    database_url: &str,
    interface: String,
    port: u16,
    with_taxonomy_search: bool,
    num_search_threads: usize,
    matomo_info: Option<MatomoInfo>,
    shutdown_signal: Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
) -> Result<()> {
    tracing::info!("Start MaCPepDB web server");
    let shutdown_signal = match shutdown_signal {
        Some(signal) => signal,
        None => Box::pin(default_shutdown_signal()),
    };

    // Create a database client
    // Session maintains it own connection pool internally: https://github.com/scylladb/scylla-rust-driver/issues/724
    // A single client with a session should be sufficient for the entire application
    let db_client = Client::new(database_url).await?;

    // Load configuration
    tracing::debug!("Loading configuration...");
    let configuration: Configuration = ConfigurationTable::select(&db_client).await?;

    // Load taxonomy tree
    tracing::debug!("Taxonomy tree...");
    let taxonomy_tree: TaxonomyTree = TaxonomyTreeTable::select(&db_client).await?;

    // Build search index for taxonomy scientific name
    let mut taxonomy_search: Option<SearchIndex<u64>> = None;

    if with_taxonomy_search {
        tracing::debug!("Build taxonomy search index...");
        let mut index = SearchIndex::default();
        for tax in taxonomy_tree.get_taxonomies() {
            index.insert(&tax.get_id(), &tax.get_scientific_name());
        }
        taxonomy_search = Some(index);
    } else {
        tracing::info!("No taxonomy search...");
    }

    tracing::debug!("Build app state...");
    let app_state = Arc::new(AppState::new(
        db_client,
        configuration,
        taxonomy_tree,
        taxonomy_search,
        num_search_threads,
        matomo_info,
    ));

    // Add CORS layer
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(vec![http::header::ACCEPT, http::header::CONTENT_TYPE])
        .allow_origin(Any);

    tracing::debug!("Create router...");
    // Build our application with route
    let mut app = Router::new()
        // Peptide routes
        .route(
            "/api/peptides/search/{payload}/{accept}",
            get(get_peptide_search),
        )
        .route("/api/peptides/search", post(post_peptide_search))
        .route(
            "/api/peptides/{sequence}/exists",
            get(get_peptide_existence),
        )
        .route("/api/peptides/{sequence}", get(get_peptide))
        // Protein routes
        .route("/api/proteins/search/{attribute}", get(search_protein))
        .route("/api/proteins/{accession}", get(get_protein))
        // Configuration routes
        .route("/api/configuration", get(get_configuration))
        // tools
        .route("/api/tools/digest", post(digest))
        .route("/api/tools/mass/{sequence}", get(get_mass))
        .route("/api/tools/proteases", get(get_proteases))
        .route("/api/tools/partition", get(get_partition))
        // taxonomy
        .route("/api/taxonomies/search", post(search_taxonomies))
        .route("/api/taxonomies/{id}/sub", get(get_sub_taxonomies))
        .route("/api/taxonomies/{id}", get(get_taxonomy))
        // chemistry
        .route("/api/chemistry/amino_acids", get(get_all_amino_acids))
        .route("/api/chemistry/amino_acids/{code}", get(get_amino_acid))
        .with_state(app_state.clone())
        .fallback(page_not_found)
        .layer(cors);

    if app_state.get_matomo_info().is_some() {
        tracing::info!("Add tracking middleware...");
        app = app.layer(middleware::from_fn_with_state(
            app_state.clone(),
            tracking_middleware::track_middleware,
        ));
    }

    tracing::debug!("Bind listener...");
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", interface, port)).await?;
    tracing::info!("Ready for connections, listening on {}:{}", interface, port);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal)
    .await
    .unwrap();

    Ok(())
}
