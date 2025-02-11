// Purpose: main entry point for the web app
#![allow(non_snake_case)]

/// Helpers for dealing with API access
mod api_helpers;
/// Main entry point for the web app
mod app;
/// Components used by the web app
mod components;
/// Configuration for the web app
mod configuration;
/// Simplified copies of MaCPepDBs entities for easy deserialization
mod entities;
/// Layouts for different pages
mod layouts;
/// Pages (render multiple compontents) used by the web app
mod pages;
/// Routes used by the web app
mod routes;

use dioxus::{logger::tracing::Level, prelude::*};

// internal import
use crate::app::App;

fn main() {
    // Init debug
    dioxus_logger::init(Level::INFO).expect("failed to init logger");
    launch(App);
}
