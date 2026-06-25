// 3rd party imports
use dioxus::prelude::*;

#[derive(Clone, PartialEq, Props)]
pub struct NotFoundProps {
    pub segments: Vec<String>,
}

pub fn NotFound(props: NotFoundProps) -> Element {
    let segments = props.segments.join("/");
    rsx! {
        div {
            h1 { "404" }
            p { "Sorry, the page '{segments}' does not exists." }
        }
    }
}
