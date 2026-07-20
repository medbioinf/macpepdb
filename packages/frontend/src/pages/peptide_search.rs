use dioxus::prelude::*;

use crate::{
    components::peptide_search::{mass_search::MassSearch, sequence_search::SequenceSearch},
    tracking::track_page_visit,
};

const BY_MASS_TAB: &str = "by mass";
const BY_SEQUENCE_TAB: &str = "by sequence";

pub fn PeptideSearch() -> Element {
    let mut selected_tab = use_signal(|| BY_SEQUENCE_TAB);

    use_future(move || async move { track_page_visit(vec![]).await });

    rsx! {
        h3 { "Search for peptides" }
        ul { class: "nav nav-tabs mb-3",
            li { class: "nav-item",
                button {
                    class: if *selected_tab.read_unchecked() == BY_SEQUENCE_TAB { "nav-link active" } else { "nav-link" },
                    onclick: move |_| selected_tab.set(BY_SEQUENCE_TAB),
                    "{BY_SEQUENCE_TAB}"
                }
            }
            li { class: "nav-item",
                button {
                    class: if *selected_tab.read_unchecked() == BY_MASS_TAB { "nav-link active" } else { "nav-link" },
                    onclick: move |_| selected_tab.set(BY_MASS_TAB),
                    "{BY_MASS_TAB}"
                }
            }
        }
        div { class: "tab-content",
            div { class: if *selected_tab.read_unchecked() == BY_SEQUENCE_TAB { "tab-pane active" } else { "tab-pane" },
                SequenceSearch {}
            }
            div { class: if *selected_tab.read_unchecked() == BY_MASS_TAB { "tab-pane active" } else { "tab-pane" },
                MassSearch {}
            }
        }
    }
}
