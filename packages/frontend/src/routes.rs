// The `Routable` derive (dioxus-router-macro 0.7.x) emits code that trips `unreachable_code`
// on recent rustc; the generated code is not ours to change.
#![allow(unreachable_code)]

use dioxus::prelude::*;

use crate::layouts::two_panes::TwoPanes;
use crate::pages::*;

#[derive(Routable, Clone)]
#[rustfmt::skip]
pub enum Routes {
    #[layout(TwoPanes)]
        #[route("/")]
        Status {},
        #[route("/proteins/:protein_id")]
        Protein { protein_id: String },
        #[route("/proteins")]
        ProteinSearch {},
        #[route("/peptides/:peptide_sequence")]
        Peptide { peptide_sequence: String },
        #[route("/peptides")]
        PeptideSearch { },
        #[route("/tools/target-finder")]
        TargetFinder {},
    #[end_layout]
    #[route("/:..segments")]
    NotFound { segments: Vec<String> },
}
