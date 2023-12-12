// 3rd party import
use dioxus::prelude::*;
use dioxus_router::prelude::*;

// internal imports
use crate::pages::*;

#[rustfmt::skip]
#[derive(Routable, Clone)]
pub enum Routes {
    #[route("/")]
    Status {},
    #[route("/proteins")]
    ProteinSearch {},
    #[route("/:..segments")]
    NotFound { segments: Vec<String> },
}
