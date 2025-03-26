use dioxus::prelude::*;

use crate::components::configuration::*;

pub fn Status() -> Element {
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
