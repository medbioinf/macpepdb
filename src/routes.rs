// 3rd party import
use dioxus::prelude::*;
use dioxus_router::prelude::*;

// internal imports
use crate::layouts::two_panes::TwoPanes;
use crate::pages::*;

#[derive(Routable, Clone)]
#[rustfmt::skip]
pub enum Routes {
    #[layout(TwoPanes)]
        #[route("/")]
        Status {},
        #[route("/proteins")]
        ProteinSearch {},
    #[end_layout]
    #[route("/:..segments")]
    NotFound { segments: Vec<String> },
}
