use std::time::Duration;

use http::StatusCode;
use macpepdb_web_common::responses::tools::SrmPrmTarget;
use serde::{Deserialize, Serialize};
use thiserror::Error;

static IM2DEEP_PATH: &str = "/v2/models/IM2Deep/infer";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Request error when trying to connect to Koina/IM2Deep: {0}")]
    Im2DeepConnection(Box<reqwest::Error>),
    #[error("Unsuccessful response from Koina/IM2Deep: {0}, status code: {1}")]
    Im2DeepUnsuccessfull(String, StatusCode),
    #[error("Error deserializing response from Koina/IM2Deep: {0}")]
    Im2DeepResponse(Box<serde_json::Error>),
}

into_thiserror_boxed!(reqwest::Error, Error, Im2DeepConnection);
into_thiserror_boxed!(serde_json::Error, Error, Im2DeepResponse);

#[derive(Serialize)]
struct Im2DeepInput<T: Serialize> {
    name: String,
    shape: (usize, usize),
    datatype: String,
    data: Vec<T>,
}

type Im2DeepPeptideSeqeunces = Im2DeepInput<String>;
type Im2DeepPrecursorCharges = Im2DeepInput<u8>;

impl Im2DeepPeptideSeqeunces {
    fn new(data: Vec<String>) -> Self {
        let shape = (data.len(), 1);
        Self {
            shape,
            data,
            name: "peptide_sequences".to_string(),
            datatype: "BYTES".to_string(),
        }
    }
}

impl Im2DeepPrecursorCharges {
    fn new(data: Vec<u8>) -> Self {
        let shape = (data.len(), 1);
        Self {
            shape,
            data,
            name: "precursor_charges".to_string(),
            datatype: "INT32".to_string(),
        }
    }
}

#[derive(Serialize)]
pub struct Im2DeepRequest {
    id: &'static str,
    inputs: (Im2DeepPeptideSeqeunces, Im2DeepPrecursorCharges),
}

impl Im2DeepRequest {
    pub fn new(peptide_sequences: Vec<String>, precursor_charges: Vec<u8>) -> Self {
        let inputs = (
            Im2DeepPeptideSeqeunces::new(peptide_sequences),
            Im2DeepPrecursorCharges::new(precursor_charges),
        );
        Self { id: "0", inputs }
    }
}

impl From<&Vec<SrmPrmTarget>> for Im2DeepRequest {
    fn from(targets: &Vec<SrmPrmTarget>) -> Self {
        let peptide_sequences = targets.iter().map(|t| t.sequence.clone()).collect();
        let precursor_charges = targets.iter().map(|t| t.charge).collect();
        Self::new(peptide_sequences, precursor_charges)
    }
}

#[derive(Deserialize)]
pub struct Im2DeepResponseOutput {
    data: Vec<f64>,
}

// {"id":"0","model_name":"IM2Deep","model_version":"1","parameters":{"sequence_id":0,"sequence_start":false,"sequence_end":false},"outputs":[{"name":"ccs","datatype":"FP32","shape":[2,1],"data":[317.7503356933594,317.7503356933594]}]}
#[derive(Deserialize)]
pub struct Im2DeepResponse {
    outputs: Vec<Im2DeepResponseOutput>,
}

impl From<Im2DeepResponse> for Vec<f64> {
    fn from(mut response: Im2DeepResponse) -> Self {
        match response.outputs.pop() {
            Some(output) => output.data,
            None => vec![],
        }
    }
}

pub struct KoinaClient {
    base_url: String,
}

impl KoinaClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
        }
    }

    pub async fn im2deep_prediction(
        &self,
        peptide_sequences: Vec<String>,
        precursor_charges: Vec<u8>,
    ) -> Result<Vec<f64>, Error> {
        let request_body = Im2DeepRequest::new(peptide_sequences, precursor_charges);
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap();

        let response = client
            .post(format!(
                "{}{IM2DEEP_PATH}",
                self.base_url.trim_end_matches('/')
            ))
            .json(&request_body)
            .send()
            .await
            .map_err(|e| Error::Im2DeepConnection(Box::new(e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Im2DeepUnsuccessfull(body, status));
        }

        let response = response.json::<Im2DeepResponse>().await?;

        Ok(response.into())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_im2deep_request_to_json() {
        let peptide_sequences = vec!["PEPTIDE1".to_string(), "PEPTIDE2".to_string()];
        let precursor_charges = vec![2, 3];
        let request = super::Im2DeepRequest::new(peptide_sequences, precursor_charges);
        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(
            json,
            r#"{"id":"0","inputs":[{"name":"peptide_sequences","shape":[2,1],"datatype":"BYTES","data":["PEPTIDE1","PEPTIDE2"]},{"name":"precursor_charges","shape":[2,1],"datatype":"INT32","data":[2,3]}]}"#
        )
    }

    #[test]
    fn test_im2deep_response_from_json() {
        let json = r#"{"id":"0","model_name":"IM2Deep","model_version":"1","parameters":{"sequence_id":0,"sequence_start":false,"sequence_end":false},"outputs":[{"name":"ccs","datatype":"FP32","shape":[2,1],"data":[317.7503356933594,319.8503356933594]}]}"#;
        let response: super::Im2DeepResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.outputs[0].data.len(), 2);
        assert_eq!(response.outputs[0].data[0], 317.7503356933594);
        assert_eq!(response.outputs[0].data[1], 319.8503356933594);
    }
}
