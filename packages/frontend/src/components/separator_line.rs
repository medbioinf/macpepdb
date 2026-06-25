use dioxus::prelude::*;

#[derive(Clone, PartialEq, Props)]
pub struct SeparatorLineProps {
    pub label: String,
}

pub fn SeparatorLine(props: SeparatorLineProps) -> Element {
    rsx! {
        div { class: "separator-line", "{props.label}" }
    }
}
