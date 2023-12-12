// 3rd party imports
use dioxus::prelude::*;
use dioxus_router::prelude::*;

// internal imports
use crate::configuration::Configuration as AppConfiguration;
use crate::routes::Routes;

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
        Router::<Routes> {}
    }
}
