// 3rd party imports
use dioxus::prelude::*;
use reqwest;

// internal imports
use crate::components::configuration::*;
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::configuration::Configuration as MacPepDBConfiguration;

/// Fetch MaCPepDB configuration from the servers
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
///
pub async fn get_macpepdb_configuration(
    macpepdb_base_url: String,
) -> Result<MacPepDBConfiguration, reqwest::Error> {
    let url = format!("{}/api/configuration", macpepdb_base_url);
    reqwest::get(&url).await?.json().await
}

pub fn Status(cx: Scope) -> Element {
    let app_config = use_shared_state::<AppConfiguration>(cx).unwrap().read();
    let macpepdb_config = use_future(cx, (), |_| {
        get_macpepdb_configuration(app_config.get_macpepdb_base_url().to_owned())
    });

    render! {
        div {
            h1 { "Welcome to MaCPepDB - Mass Centric Peptide Database" }
            div {
                p {
                    "Quickly build and access a digest of the a large proteome."
                }
            }
        }
        match macpepdb_config.value() {
            Some(Ok(macpepdb_config)) => {
                render! {
                    Configuration {
                        macpepdb_configuration: macpepdb_config,

                    }
                }
            }
            Some(Err(e)) => {
                render! {
                    div { "Error loading the configuration {e}" }
                }
            }
            None => {
                render! {
                    div { "Loading ..." }
                }
            }
        }
    }
}
