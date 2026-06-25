// Purpose: main entry point for the web app
#![allow(non_snake_case)]

/// Client to query MaCPepDB API
mod api_client;

/// Main entry point for the web app
mod app;

/// Components used by the web app
mod components;
/// Configuration for the web app
mod configuration;
/// Simplified copies of MaCPepDBs entities for easy deserialization
mod entities;
/// Error types used in the web app
mod errors;
/// Layouts for different pages
mod layouts;
/// Pages (render multiple compontents) used by the web app
mod pages;
/// Routes used by the web app
mod routes;
/// Tracking of page visits
mod tracking;

use dioxus::{logger::tracing::Level, prelude::*};

// internal import
use crate::app::App;

fn main() {
    // Init debug
    dioxus_logger::init(Level::INFO).expect("failed to init logger");
    launch(App);
}
