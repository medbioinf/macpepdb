use base64::{prelude::BASE64_STANDARD, Engine};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::de::DeserializeOwned;
use serde_json::json;
use urlencoding::encode as urlencode;

use crate::{
    entities::{
        amino_acid::AminoAcid, configuration::Configuration as MacPepDBConfiguration,
        mass_unit::MassUnit, peptide::Peptide,
        post_translational_modification::PostTranslationalModification, protein::Protein,
        taxonomy::Taxonomy,
    },
    errors::api_client_error::ApiClientError,
};

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

    pub async fn post<T>(
        &self,
        endpoint: &str,
        body: serde_json::Value,
        headers: Option<&[(HeaderName, HeaderValue)]>,
    ) -> Result<T, ApiClientError>
    where
        T: DeserializeOwned,
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
    pub async fn get_configuration(&self) -> Result<MacPepDBConfiguration, ApiClientError> {
        self.get("/api/configuration").await
    }

    /// Get peptide by sequence
    ///
    /// # Arguments
    /// * `sequence` - Peptide sequence
    ///
    pub async fn get_peptide<T>(&self, sequence: &str) -> Result<Peptide<T>, ApiClientError>
    where
        T: 'static + PartialEq + DeserializeOwned,
    {
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
        taxonomy: &Option<Taxonomy>,
        max_variable_modifications: i16,
        ptms: &Vec<PostTranslationalModification>,
        is_reviewed: Option<bool>,
    ) -> serde_json::Value {
        let mut body = json!({
            "lower_mass_tolerance_ppm": lower_mass_tolerance,
            "upper_mass_tolerance_ppm": upper_mass_tolerance,
            "max_variable_modifications": max_variable_modifications,
            "modifications": *ptms,
        });

        match selected_mass_unit {
            MassUnit::Thompson => body["mass"] = json!((thompson, charge)),
            MassUnit::Dalton => body["mass"] = json!(dalton),
        };

        if let Some(taxonomy) = taxonomy {
            body["taxonomy_id"] = json!(taxonomy.id);
        }

        if let Some(is_reviewed) = is_reviewed {
            body["is_reviewed"] = json!(is_reviewed);
        }

        body
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search_peptides<T>(
        &self,
        selected_mass_unit: MassUnit,
        thompson: f64,
        charge: u8,
        dalton: f64,
        lower_mass_tolerance: i64,
        upper_mass_tolerance: i64,
        taxonomy: &Option<Taxonomy>,
        max_variable_modifications: i16,
        ptms: &Vec<PostTranslationalModification>,
        is_reviewed: Option<bool>,
    ) -> Result<Vec<Peptide<T>>, ApiClientError>
    where
        T: 'static + PartialEq + DeserializeOwned,
    {
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
        taxonomy: &Option<Taxonomy>,
        max_variable_modifications: i16,
        ptms: &Vec<PostTranslationalModification>,
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

    /// Search taxonomies by name
    ///
    /// # Arguments
    /// * `taxonomy_search_term` - Taxonomy name search term
    ///
    pub async fn search_taxonomies(
        &self,
        taxonomy_search_term: &str,
    ) -> Result<Vec<Taxonomy>, ApiClientError> {
        let body = json!({
            "name_query": format!("*{taxonomy_search_term}*"),
        });

        self.post("/api/taxonomies/search", body, None).await
    }

    /// Get taxonomy by ID
    ///
    /// # Arguments
    /// * `taxonomy_id` - Taxonomy ID
    pub async fn get_taxonomy(&self, taxonomy_id: u64) -> Result<Taxonomy, ApiClientError> {
        let endpoint = format!("/api/taxonomies/{taxonomy_id}");
        self.get(&endpoint).await
    }

    /// Get protein
    ///
    /// # Arguments
    /// * `search_term` - Protein accession or gene name
    ///
    pub async fn search_protein<T>(
        &self,
        search_term: &str,
    ) -> Result<Vec<Protein<T>>, ApiClientError>
    where
        T: 'static + PartialEq + DeserializeOwned,
    {
        let endpoint = format!("/api/proteins/search/{search_term}");

        self.get(&endpoint).await
    }

    /// Fetches amino acid
    ///
    pub async fn get_amino_acid(&self) -> Result<Vec<AminoAcid>, ApiClientError> {
        self.get("/api/chemistry/amino_acids").await
    }

    /// Fetches a protein by its accession
    ///
    /// # Arguments
    /// * `accession` - Protein accession
    ///
    pub async fn get_protein<T>(&self, accession: &str) -> Result<Protein<T>, ApiClientError>
    where
        T: 'static + PartialEq + DeserializeOwned,
    {
        let endpoint = format!("/api/proteins/{accession}");
        self.get(&endpoint).await
    }
}
