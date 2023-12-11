// 3rd party imports
use dioxus::prelude::*;

// internal imports
use crate::components::*;
use crate::configuration::Configuration as AppConfiguration;

/// The root component of the web app
#[derive(PartialEq, Props)]
pub struct RootProps {
    pub configuration: AppConfiguration,
}

/// Root component for the entire frontend
/// Makes the frontend configuration available to all components
///
pub fn App(cx: Scope<'_, RootProps>) -> Element {
    use_shared_state_provider(cx, || cx.props.configuration.clone());

    render! {
        div {
            h1 { "Welcome to MaCPepDB - Mass Centric Peptide Database" }
            div {
                p {
                    "Quickly build and access a digest of the a large proteome."
                }
            }
        }
        configuration::Configuration {}
    }
}
