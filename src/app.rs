// 3rd party imports
use dioxus::{
    document::{Script, Stylesheet},
    prelude::*,
};
use dioxus_router::prelude::Router;

// internal imports
use crate::{configuration::Configuration, routes::Routes};

/// Root component for the entire frontend
///
pub fn App() -> Element {
    #[allow(clippy::redundant_closure)]
    use_context_provider(|| Configuration::new());

    rsx! {
        Stylesheet { href: "https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" }
        Stylesheet { href: "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css" }
        Stylesheet { href: asset!("./public/index.css") }
        Script { src: "https://cdn.jsdelivr.net/npm/@popperjs/core@2.11.8/dist/umd/popper.min.js" }
        Script { src: "https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js" }
        Router::<Routes> {}
    }
}
