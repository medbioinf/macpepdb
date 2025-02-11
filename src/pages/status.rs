use std::rc::Rc;

// 3rd party imports
use anyhow::Result;
use dioxus::prelude::*;

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
    macpepdb_base_url: Signal<String>,
) -> Result<Rc<MacPepDBConfiguration>> {
    let url = format!("{}/api/configuration", macpepdb_base_url);
    Ok(Rc::new(
        reqwest::get(&url)
            .await?
            .json::<MacPepDBConfiguration>()
            .await?,
    ))
}

pub fn Status() -> Element {
    let app_config = use_context::<AppConfiguration>();
    let macpepdb_base_url = use_signal(|| app_config.get_macpepdb_base_url().to_owned());
    let macpepdb_config = use_resource(move || get_macpepdb_configuration(macpepdb_base_url));

    rsx! {
        div {
            h1 { "Welcome to MaCPepDB - Mass Centric Peptide Database" }
            div {
                p { "Quickly build and access the digest of a large proteome." }
            }
        }
        match &*macpepdb_config.read_unchecked() {
            Some(Ok(macpepdb_config)) => {
                rsx! {
                    Configuration { macpepdb_configuration: macpepdb_config.clone() }
                }
            }
            Some(Err(e)) => {
                rsx! {
                    div { "Error loading the configuration {e}" }
                }
            }
            None => {
                rsx! {
                    div { "Loading ..." }
                }
            }
        }
    }
}
