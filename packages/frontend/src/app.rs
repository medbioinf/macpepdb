// 3rd party imports
use dioxus::router::Router;
use dioxus::{
    document::{Script, Stylesheet},
    prelude::*,
};

use crate::components::spinner::Spinner;
// internal imports
use crate::{configuration::Configuration, routes::Routes, tracking::create_tracking_id};

/// Root component for the entire frontend
///
pub fn App() -> Element {
    let config = use_resource(Configuration::new);
    use_context_provider(|| config);
    use_context_provider(create_tracking_id);

    rsx! {
        Stylesheet { href: "https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" }
        Stylesheet { href: "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css" }
        Stylesheet { href: asset!("/assets/sass/index.sass") }
        Script { src: "https://cdn.jsdelivr.net/npm/@popperjs/core@2.11.8/dist/umd/popper.min.js" }
        Script { src: "https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js" }
        Script { src: "https://cdn.plot.ly/plotly-2.14.0.min.js" }

        match &*config.read_unchecked() {
            Some(_) => rsx! {
                Router::<Routes> {}
            },
            None => rsx! {
                Spinner {}
            },
        }
    }
}
