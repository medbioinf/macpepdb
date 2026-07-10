use base64::{prelude::BASE64_STANDARD, Engine};
use macpepdb_web_common::{
    requests::{
        peptide::{SearchRequestBody, SearchRequestMass},
        ptm::PostTranslationalModificationRequest,
        taxonomy::SearchRequestBody as TaxonomySearchRequestBody,
    },
    responses::{
        amino_acid::AminoAcidResponse, configuration::RuntimeConfigurationResponse,
        peptide::PeptideResponse, protein::ProteinResponse, taxonomy::TaxonomyResponse,
    },
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{de::DeserializeOwned, Serialize};
use urlencoding::encode as urlencode;

use crate::{entities::mass_unit::MassUnit, errors::api_client_error::ApiClientError};

const X_DO_NOT_TRACK: HeaderName = HeaderName::from_static("x-do-not-track");

pub struct Client<'a> {
    base_url: &'a str,
    inner_client: reqwest::Client,
}

impl<'a> Client<'a> {
    pub fn new(base_url: &'a str) -> Result<Self, ApiClientError> {
        let inner_client = reqwest::Client::builder()
            .default_headers(HeaderMap::from_iter([
                (reqwest::header::DNT, HeaderValue::from_static("1")), // Set Do Not Track header to prevent API from tracking twice
                (X_DO_NOT_TRACK, HeaderValue::from_static("1")), // Set Do Not Track header to prevent API from tracking twice
            ]))
            .build()
            .map_err(ApiClientError::ClientCreationError)?;

        Ok(Self {
            base_url,
            inner_client,
        })
    }

    pub async fn get<T>(&self, endpoint: &str) -> Result<T, ApiClientError>
    where
        T: DeserializeOwned,
    {
        let url = format!("{}{endpoint}", self.base_url);

        let response = self
            .inner_client
            .get(&url)
            .send()
            .await
            .map_err(ApiClientError::NetworkError)?;

        if !response.status().is_success() {
            let status_code = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(ApiClientError::UnsuccessfulResponse(
                error_text,
                status_code,
            ));
        }

        if !response
            .headers()
            .get("Content-Type")
            .is_some_and(|content_type| {
                content_type
                    .to_str()
                    .unwrap_or("")
                    .starts_with("application/json")
            })
        {
            return Err(ApiClientError::UnexpectedResponseFormat);
        }

        response
            .json::<T>()
            .await
            .map_err(ApiClientError::JsonParsingError)
    }

    pub async fn post<T, B>(
        &self,
        endpoint: &str,
        body: B,
        headers: Option<&[(HeaderName, HeaderValue)]>,
    ) -> Result<T, ApiClientError>
    where
        T: DeserializeOwned,
        B: Serialize,
    {
        let url = format!("{}{endpoint}", self.base_url);

        let mut request_builder = self.inner_client.post(&url);
        if let Some(headers) = headers {
            for (name, value) in headers {
                request_builder = request_builder.header(name, value);
            }
        }
        request_builder = request_builder.json(&body);

        let response = request_builder
            .send()
            .await
            .map_err(ApiClientError::NetworkError)?;

        if !response.status().is_success() {
            let status_code = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(ApiClientError::UnsuccessfulResponse(
                error_text,
                status_code,
            ));
        }

        if !response
            .headers()
            .get("Content-Type")
            .is_some_and(|content_type| {
                content_type
                    .to_str()
                    .unwrap_or("")
                    .starts_with("application/json")
            })
        {
            return Err(ApiClientError::UnexpectedResponseFormat);
        }

        response
            .json::<T>()
            .await
            .map_err(ApiClientError::JsonParsingError)
    }

    /// Fetches the MaCPepDB configuration from the server
    ///
    pub async fn get_configuration(&self) -> Result<RuntimeConfigurationResponse, ApiClientError> {
        self.get("/api/configuration").await
    }

    /// Get peptide by sequence
    ///
    /// # Arguments
    /// * `sequence` - Peptide sequence
    ///
    pub async fn get_peptide(&self, sequence: &str) -> Result<PeptideResponse, ApiClientError> {
        let endpoint = format!("/api/peptides/{sequence}");
        self.get(&endpoint).await
    }

    #[allow(clippy::too_many_arguments)]
    fn build_search_peptide_body(
        selected_mass_unit: MassUnit,
        thompson: f64,
        charge: u8,
        dalton: f64,
        lower_mass_tolerance: i64,
        upper_mass_tolerance: i64,
        taxonomy: &Option<TaxonomyResponse>,
        max_variable_modifications: i16,
        ptms: &[PostTranslationalModificationRequest],
        is_reviewed: Option<bool>,
    ) -> SearchRequestBody {
        let mass = match selected_mass_unit {
            MassUnit::Thompson => SearchRequestMass::ThompsonCharge(thompson, charge),
            MassUnit::Dalton => SearchRequestMass::Dalton(dalton),
        };

        SearchRequestBody {
            mass,
            lower_mass_tolerance_ppm: lower_mass_tolerance,
            upper_mass_tolerance_ppm: upper_mass_tolerance,
            max_variable_modifications: max_variable_modifications.max(0) as usize,
            modifications: ptms.to_vec(),
            taxonomy_id: taxonomy.as_ref().map(|taxonomy| taxonomy.id),
            proteome_id: None,
            is_reviewed,
            resolve_modifications: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search_peptides(
        &self,
        selected_mass_unit: MassUnit,
        thompson: f64,
        charge: u8,
        dalton: f64,
        lower_mass_tolerance: i64,
        upper_mass_tolerance: i64,
        taxonomy: &Option<TaxonomyResponse>,
        max_variable_modifications: i16,
        ptms: &[PostTranslationalModificationRequest],
        is_reviewed: Option<bool>,
    ) -> Result<Vec<PeptideResponse>, ApiClientError> {
        let body = Self::build_search_peptide_body(
            selected_mass_unit,
            thompson,
            charge,
            dalton,
            lower_mass_tolerance,
            upper_mass_tolerance,
            taxonomy,
            max_variable_modifications,
            ptms,
            is_reviewed,
        );
        self.post(
            "/api/peptides/search",
            body,
            Some(&[(
                reqwest::header::ACCEPT,
                HeaderValue::from_static("application/json"),
            )]),
        )
        .await
    }

    /// Encode input string to base64 URL-safe format
    ///
    /// # Arguments
    /// * `input` - Input string to encode
    ///
    fn base64_urlsafe_encode(input: &str) -> String {
        urlencode(&BASE64_STANDARD.encode(input.as_bytes())).into_owned()
    }

    /// Returns a URL to download the peptides matching the search criteria as CSV.
    /// The URL can be used using a GET request to download the search results directly using
    /// window() in a browser environment or any HTTP client
    ///
    /// # Arguments
    /// * `selected_mass_unit` - Selected mass unit
    /// * `thompson` - Mass in Thompson
    /// * `charge` - Charge state
    /// * `dalton` - Mass in Dalton
    /// * `lower_mass_tolerance` - Lower mass tolerance in ppm
    /// * `upper_mass_tolerance` - Upper mass tolerance in ppm
    /// * `taxonomy` - Optional taxonomy filter
    /// * `max_variable_modifications` - Maximum number of variable modifications
    /// * `ptms` - List of post-translational modifications
    /// * `is_reviewed` - Optional filter for reviewed peptides
    ///
    #[allow(clippy::too_many_arguments)]
    pub fn peptide_search_download_url(
        &self,
        selected_mass_unit: MassUnit,
        thompson: f64,
        charge: u8,
        dalton: f64,
        lower_mass_tolerance: i64,
        upper_mass_tolerance: i64,
        taxonomy: &Option<TaxonomyResponse>,
        max_variable_modifications: i16,
        ptms: &[PostTranslationalModificationRequest],
        is_reviewed: Option<bool>,
    ) -> String {
        let body = Self::build_search_peptide_body(
            selected_mass_unit,
            thompson,
            charge,
            dalton,
            lower_mass_tolerance,
            upper_mass_tolerance,
            taxonomy,
            max_variable_modifications,
            ptms,
            is_reviewed,
        );

        format!(
            "{}/api/peptides/search/{}/{}?is_download=true",
            self.base_url,
            Self::base64_urlsafe_encode(serde_json::to_string(&body).unwrap().as_str()),
            urlencode("text/tab-separated-values")
        )
    }

    /// Search taxonomies by name or ID.
    ///
    /// # Arguments
    /// * `taxonomy_search_term` - Taxonomy name (substring, matched case-sensitively by the
    ///   backend, which wraps it in SQL `%...%` itself) or taxonomy ID search term.
    ///
    pub async fn search_taxonomies(
        &self,
        taxonomy_search_term: &str,
    ) -> Result<Vec<TaxonomyResponse>, ApiClientError> {
        let body = TaxonomySearchRequestBody {
            search_query: taxonomy_search_term.to_string(),
        };

        self.post("/api/taxonomies/search", body, None).await
    }

    /// Get taxonomy by ID
    ///
    /// # Arguments
    /// * `taxonomy_id` - Taxonomy ID
    pub async fn get_taxonomy(&self, taxonomy_id: u64) -> Result<TaxonomyResponse, ApiClientError> {
        let endpoint = format!("/api/taxonomies/{taxonomy_id}");
        self.get(&endpoint).await
    }

    /// Get protein
    ///
    /// # Arguments
    /// * `search_term` - Protein accession or gene name
    ///
    pub async fn search_protein(
        &self,
        search_term: &str,
    ) -> Result<Vec<ProteinResponse<String>>, ApiClientError> {
        let endpoint = format!("/api/proteins/search/{search_term}");

        self.get(&endpoint).await
    }

    /// Fetches amino acid
    ///
    pub async fn get_amino_acid(&self) -> Result<Vec<AminoAcidResponse>, ApiClientError> {
        self.get("/api/chemistry/amino-acids").await
    }

    /// Fetches a protein by its accession
    ///
    /// # Arguments
    /// * `accession` - Protein accession
    ///
    pub async fn get_protein(
        &self,
        accession: &str,
    ) -> Result<ProteinResponse<PeptideResponse>, ApiClientError> {
        let endpoint = format!("/api/proteins/{accession}");
        self.get(&endpoint).await
    }

    pub async fn hydrophobicity_korkhin(&self, sequence: &str) -> Result<f64, ApiClientError> {
        self.get(&format!("/api/chemistry/hydrophobicity/krokhin/{sequence}"))
            .await
    }
}
