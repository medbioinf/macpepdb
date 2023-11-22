// std imports
use std::net::SocketAddr;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use axum::routing::{get, post};
use axum::Router;
use dihardts_omicstools::biology::taxonomy::TaxonomyTree;
use indicium::simple::SearchIndex;

// internal imports
use crate::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use crate::database::scylla::client::{Client, GenericClient};
use crate::database::scylla::configuration_table::ConfigurationTable;
use crate::database::scylla::taxonomy_tree_table::TaxonomyTreeTable;
use crate::entities::configuration::Configuration;
use crate::web::configuration_controller::get_configuration;
use crate::web::peptide_controller::{
    get_peptide, get_peptide_existence, search as peptide_search,
};
use crate::web::protein_controller::get_protein;
use crate::web::taxonomy_controller::{get_sub_taxonomies, get_taxonomy, search_taxonomies};
use crate::web::tools_controller::{digest, get_mass};

/// Starts the MaCPepDB web server on the given interface and port.
///
/// # Arguments
/// * `database_nodes` - List of database nodes
/// * `interface` - Interface to listen on
/// * `port` - Port to listen on
///
pub async fn start(database_url: &str, interface: String, port: u16) -> Result<()> {
    // Create a database client
    // Session maintains it own connection pool internally: https://github.com/scylladb/scylla-rust-driver/issues/724
    // A single client with a session should be sufficient for the entire application
    let db_client = Client::new(database_url).await?;
    let db_client = Arc::new(db_client);

    // Load configuration
    let configuration: Configuration = ConfigurationTable::select(db_client.as_ref()).await?;
    let configuration = Arc::new(configuration);

    // Load taxonomy tree
    let taxonomy_tree: TaxonomyTree = TaxonomyTreeTable::select(db_client.as_ref()).await?;
    let taxonomy_tree = Arc::new(taxonomy_tree);

    // Build search index for taxonomy scientific name
    let mut taxonomy_search: SearchIndex<u64> = SearchIndex::default();
    for tax in taxonomy_tree.get_taxonomies() {
        taxonomy_search.insert(&tax.get_id(), &tax.get_scientific_name());
    }
    let taxonomy_search = Arc::new(taxonomy_search);

    tracing::info!("Start MaCPepDB web server");

    // Build our application with route
    let app = Router::new()
        // Peptide routes
        .route("/api/peptides/search", post(peptide_search))
        .with_state((
            db_client.clone(),
            configuration.clone(),
            taxonomy_tree.clone(),
        ))
        .route("/api/peptides/:sequence/exists", get(get_peptide_existence))
        .with_state((db_client.clone(), configuration.clone()))
        .route("/api/peptides/:sequence", get(get_peptide))
        .with_state((db_client.clone(), configuration.clone()))
        // Protein routes
        .route("/api/proteins/:accession", get(get_protein))
        .with_state((db_client.clone(), configuration.clone()))
        // Configuration routes
        .route("/api/configuration", get(get_configuration))
        .with_state(configuration.clone())
        // tools
        .route("/api/tools/digest", post(digest))
        .with_state((db_client.clone(), configuration.clone()))
        .route("/api/tools/mass/:sequence", post(get_mass))
        // taxonomy
        .route("/api/taxonomies/search", post(search_taxonomies))
        .with_state((taxonomy_tree.clone(), taxonomy_search.clone()))
        .route("/api/taxonomies/:id/sub", get(get_sub_taxonomies))
        .with_state(taxonomy_tree.clone())
        .route("/api/taxonomies/:id", get(get_taxonomy))
        .with_state(taxonomy_tree.clone());

    // Run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr: SocketAddr = format!("{}:{}", interface, port).parse()?;
    tracing::info!("ready for connections, listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}
