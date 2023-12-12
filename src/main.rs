// Purpose: main entry point for the web app
#![allow(non_snake_case)]

/// Main entry point for the web app
mod app;
/// Components used by the web app
mod components;
/// Configuration for the web app
mod configuration;
/// Simplified copies of MaCPepDBs entities for easy deserialization
mod entities;
/// Pages (render multiple compontents) used by the web app
mod pages;
/// Routes used by the web app
mod routes;

// 3rd party imports
use dioxus_web::Config;

// internal import
use crate::app::{App, RootProps};
use crate::configuration::Configuration;

fn main() {
    // Get app configuration
    let app_config = Configuration::new();

    // launch the web app
    dioxus_web::launch_with_props(
        App,
        RootProps {
            configuration: app_config,
        },
        Config::new(),
    );
}
