// 3rd party imports
use dioxus::prelude::*;
use dioxus_router::prelude::*;

// internal imports
use crate::routes::Routes;

/// Layout with two panes. One for the menu and one for the main content
pub fn TwoPanes(cx: Scope) -> Element {
    render! {
        div {
            class: "layout-two-panes container-fluid",
            nav {
                class: "pane-menu",
                ul {
                    li {
                        Link {
                            to: Routes::Status {},
                            "Status"
                        }
                    }
                    li{
                        Link {
                            to: Routes::ProteinSearch {},
                            "Proteins"
                        }
                    }
                }
            }
            div {
                class: "pane-content container-fluid",
                Outlet::<Routes> {}
            }
        }

    }
}
