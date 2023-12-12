// 3rd party imports
use dioxus::prelude::*;
use dioxus_router::prelude::*;

// internal imports
use crate::routes::Routes;

/// Layout with two panes. One for the menu and one for the main content
pub fn TwoPanes(cx: Scope) -> Element {
    render! {
        nav {
            Link {
                to: Routes::Status {},
                "Status"
            }
            Link {
                to: Routes::ProteinSearch {},
                "Proteins"
            }
        }
        // The index route will be rendered here
        Outlet::<Routes> { }
    }
}
