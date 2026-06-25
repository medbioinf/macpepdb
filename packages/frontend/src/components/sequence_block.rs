use dioxus::prelude::*;
use futures::StreamExt;

/// Max length per line
///
const MAX_LINE_LENGTH: usize = 60;

#[derive(Clone, PartialEq, Props)]
pub struct SequenceBlockProps {
    pub sequence: String,
}

pub fn SequenceBlock(props: SequenceBlockProps) -> Element {
    let lines = props
        .sequence
        .chars()
        .collect::<Vec<char>>()
        .chunks(MAX_LINE_LENGTH)
        .map(|line| line.iter().collect::<String>())
        .collect::<Vec<String>>();

    let button_class = use_signal(|| {
        let mut class = "btn btn-primary btn-sm".to_string();
        if lines.len() > 1 {
            class = format!("{} d-block", class);
        } else {
            class = format!("{} d-inline-block", class);
        }
        class
    });

    let copy_coroutine = use_coroutine(|mut rx: UnboundedReceiver<String>| async move {
        while let Some(sequence) = rx.next().await {
            let _ = web_sys::window()
                .unwrap()
                .navigator()
                .clipboard()
                .write_text(&sequence);
        }
    });

    rsx! {
        div { class: "sequence-block mb-1 me-1",
            div { class: "sequence-block-src",
                for (idx , line) in lines.iter().enumerate() {
                    span { class: "sequence-block-src-row",
                        if lines.len() > 1 {
                            span { class: "text-right pl-2 pr-1 font-monospace border-right border-dark sequence-block-src-row-number",
                                "{idx + 1}"
                            }
                        }
                        span { class: "px-2 text-dark font-monospace", "{line}" }
                    }
                }
            }
            button {
                class: button_class,
                onclick: move |_| copy_coroutine.send(props.sequence.clone()),
                i { class: "fas fa-copy" }
            }
        }
    }
}
