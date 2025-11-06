use dioxus::prelude::*;

use crate::{components::configuration::*, tracking::track_page_visit};

pub fn Status() -> Element {
    use_future(move || async move { track_page_visit(vec![]).await });

    rsx! {
        div {
            h1 { "Welcome to MaCPepDB - Mass Centric Peptide Database" }
            div {
                p { "Quickly build and access the digest of a large proteome." }
            }
        }
        Configuration {}
    }
}
