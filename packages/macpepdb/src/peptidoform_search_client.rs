use std::time::Duration;
use std::{num::NonZeroUsize, pin::Pin, sync::Arc};

use dihardts_omicstools::proteomics::peptide::Terminus;
use dihardts_omicstools::proteomics::post_translational_modifications::{
    ModificationType as PtmType, Position as PtmPosition,
};
use futures::{AsyncBufReadExt, Stream, StreamExt, TryStreamExt};
use http::StatusCode;
use macpepdb_web_common::requests::{
    peptide::{SearchRequestBody, SearchRequestMass},
    ptm::{
        PostTranslationalModificationRequest, PtmPosition as RequestPtmPosition,
        PtmType as RequestPtmType,
    },
};
use reqwest::Client as WebClient;
use thiserror::Error;

use crate::peptide_search::PeptidoformPassthroughTransformation;
use crate::peptide_table::FULL_PEPTIDE_COLUMN_SELECTION;
use crate::taxonomy_table::TaxonomyTable;
use crate::{
    blob_table::BlobTable,
    client::Client as DbClient,
    configuration::RuntimeConfiguration,
    mass::to_float as mass_to_float,
    peptide_search::PeptideSearch,
    post_translational_modification::{PTMCollection, PostTranslationalModification},
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Blob table error in performance test: {0}")]
    BlobTable(Box<crate::blob_table::Error>),
    #[error("Database client error in performance test: {0}")]
    DbClient(Box<crate::client::Error>),
    #[error(
        "Client url can either start with http:// or https:// for web API or postgresql:// for database"
    )]
    InvalidClientUrl,
    #[error(
        "Missing runtime configuration in database. A database built before the mass partitioning \
         switched to per-partition ranges needs `config migrate`."
    )]
    MissingRuntimeConfig,
    #[error("Unable to get next chunk from HTTP response stream: {0}")]
    NextHttpPeptidoform(Box<std::io::Error>),
    #[error("Peptide search error in performance test: {0}")]
    PeptideSearch(Box<crate::peptide_search::Error>),
    #[error("Error building web client: {0}")]
    WebClientBuild(Box<reqwest::Error>),
    #[error("HTTP request error while performing peptide search: {0}")]
    Request(Box<reqwest::Error>),
    #[error("Taxonomy table error in peptidoform search client: {0}")]
    TaxonomyTable(Box<crate::taxonomy_table::Error>),
    #[error("Unsuccessful search request: status code {0}, content type {1}, response text {2}")]
    UnsuccessfullSearchRequest(StatusCode, String, String),
}

into_thiserror_boxed!(crate::blob_table::Error, Error, BlobTable);
into_thiserror_boxed!(crate::client::Error, Error, DbClient);
into_thiserror_boxed!(std::io::Error, Error, NextHttpPeptidoform);
into_thiserror_boxed!(crate::peptide_search::Error, Error, PeptideSearch);
into_thiserror_boxed!(crate::taxonomy_table::Error, Error, TaxonomyTable);

/// Client which searches peptidoforms matching the given conditions
/// reuturning them in as ProForma compliant strings
pub enum PeptidoformSearchClient {
    WebApi(WebClient, String),
    Database(Arc<DbClient>, Arc<RuntimeConfiguration>),
}

impl PeptidoformSearchClient {
    pub async fn try_from_url(url: &str) -> Result<Self, Error> {
        if url.starts_with("http://") || url.starts_with("https://") {
            let mut web_client_builder = WebClient::builder().timeout(Duration::from_secs(60));
            if url.starts_with("http://") {
                web_client_builder = web_client_builder
                    .http2_prior_knowledge() // force h2 without TLS ALPN (plaintext h2c) — rare, needs server support
                    .http2_keep_alive_interval(Duration::from_secs(20))
                    .http2_keep_alive_timeout(Duration::from_secs(10))
                    .http2_keep_alive_while_idle(true)
            }
            let web_client = web_client_builder
                .build()
                .map_err(|e| Error::WebClientBuild(Box::new(e)))?;
            let search_url = format!(
                "{}{}{}",
                url.trim_end_matches('/'),
                crate::web::peptide_controller::CONTROLLER_PATH,
                crate::web::peptide_controller::SEARCH_POST_PATH
            );
            Ok(PeptidoformSearchClient::WebApi(web_client, search_url))
        } else if url.starts_with("postgresql://") {
            let db_client = DbClient::new(url).await.map(Arc::new)?;
            let config = BlobTable::select::<RuntimeConfiguration>(
                db_client.as_ref(),
                RuntimeConfiguration::BLOB_KEY,
            )
            .await?
            .ok_or(Error::MissingRuntimeConfig)
            .map(Arc::new)?;

            Ok(PeptidoformSearchClient::Database(db_client, config))
        } else {
            Err(Error::InvalidClientUrl)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search(
        &self,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        taxonomy_id: Option<i32>,
        is_reviewed: Option<bool>,
        ptms: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
        resolve_modifications: bool,
        concurrent_searches: NonZeroUsize,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<String, Error>>>>, Error> {
        match self {
            PeptidoformSearchClient::WebApi(web_client, search_url) => {
                let mass = mass_to_float(mass);
                let modifications = ptms
                    .all()
                    .iter()
                    .map(|ptm| PostTranslationalModificationRequest {
                        name: ptm.name().to_string(),
                        amino_acid: ptm.amino_acid().code(),
                        mass_delta: mass_to_float(ptm.mass_delta()),
                        mod_type: match ptm.mod_type() {
                            PtmType::Static => RequestPtmType::Static,
                            PtmType::Variable => RequestPtmType::Variable,
                        },
                        position: match ptm.position() {
                            PtmPosition::Anywhere => RequestPtmPosition::Anywhere,
                            PtmPosition::Terminus(Terminus::N) => RequestPtmPosition::NTerminus,
                            PtmPosition::Terminus(Terminus::C) => RequestPtmPosition::CTerminus,
                            PtmPosition::Bond(Terminus::N) => RequestPtmPosition::NBond,
                            PtmPosition::Bond(Terminus::C) => RequestPtmPosition::CBond,
                        },
                    })
                    .collect::<Vec<_>>();

                let request = SearchRequestBody {
                    mass: SearchRequestMass::Dalton(mass),
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
                    max_variable_modifications,
                    modifications,
                    taxonomy_id,
                    is_reviewed,
                    resolve_modifications: Some(resolve_modifications),
                };

                let response = web_client
                    .post(search_url)
                    .header("Accept", "text/plain")
                    .json(&request)
                    .send()
                    .await
                    .map_err(|e| Error::Request(Box::new(e)))?;

                if !response.status().is_success() {
                    let status_code = response.status();
                    let content_type = response
                        .headers()
                        .get("content-type")
                        .and_then(|ct| ct.to_str().ok())
                        .unwrap_or("unknown content type")
                        .to_string();
                    let response_text = response
                        .text()
                        .await
                        .unwrap_or("text is not decodable".to_string());
                    return Err(Error::UnsuccessfullSearchRequest(
                        status_code,
                        content_type,
                        response_text,
                    ));
                }

                let peptidoform_stream = response
                    .bytes_stream()
                    .map_err(std::io::Error::other)
                    .into_async_read()
                    .lines()
                    .map(|line| {
                        line.map_err(std::io::Error::other).and_then(|line| {
                            let line = line.trim();
                            if !line.starts_with("!!!") {
                                Ok(line.to_string())
                            } else {
                                Err(std::io::Error::other(line.to_string()))
                            }
                        })
                    })
                    // drop the server's guaranteed leading blank line (sent so a zero-hit
                    // search never streams a fully empty body) so it isn't mistaken for a
                    // real (empty-sequence) result
                    .filter(|line| futures::future::ready(!matches!(line, Ok(s) if s.is_empty())))
                    .map_err(Error::from);

                Ok(Box::pin(peptidoform_stream))
            }
            PeptidoformSearchClient::Database(db_client, configuration) => {
                let taxonomy_ids = match taxonomy_id {
                    Some(taxonomy_id) => Some(
                        TaxonomyTable::new(db_client.clone())
                            .select_sub_species(taxonomy_id)
                            .await?
                            .map(|taxonomy_res| taxonomy_res.map(|taxonomy| taxonomy.id()))
                            .try_collect::<Vec<_>>()
                            .await?,
                    ),
                    None => None,
                };

                let db_client = db_client.clone();
                let configuration = configuration.clone();
                let ptms = ptms.clone();
                let peptide_stream = PeptideSearch::new(
                    db_client,
                    &FULL_PEPTIDE_COLUMN_SELECTION,
                    configuration,
                    mass,
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
                    max_variable_modifications,
                    true,
                    taxonomy_ids,
                    None,
                    ptms,
                    true,
                    concurrent_searches,
                )
                .search::<PeptidoformPassthroughTransformation>()
                .await?;

                let peptide_stream =
                    peptide_stream.flat_map(|peptidoform_result| match peptidoform_result {
                        Ok(peptidoforms) => Box::pin(futures::stream::iter(
                            peptidoforms
                                .into_iter()
                                .map(|peptidoform| Ok(peptidoform.sequence().to_string())),
                        ))
                            as Pin<Box<dyn Stream<Item = Result<String, Error>>>>,
                        Err(e) => Box::pin(futures::stream::once(async move {
                            Err(Error::PeptideSearch(Box::new(e)))
                        }))
                            as Pin<Box<dyn Stream<Item = Result<String, Error>>>>,
                    });

                Ok(Box::pin(peptide_stream))
            }
        }
    }
}
