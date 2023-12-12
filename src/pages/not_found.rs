// 3rd party imports
use dioxus::prelude::*;

#[derive(PartialEq, Props)]
pub struct NotFoundProps {
    pub segments: Vec<String>,
}

pub fn NotFound(cx: Scope<NotFoundProps>) -> Element {
    render! {
        div {
            h1 { "404" }
            p { "Sorry, this page does not exists." }
        }
    }
}
