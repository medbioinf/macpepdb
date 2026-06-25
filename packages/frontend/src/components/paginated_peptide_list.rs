use std::{cmp::min, rc::Rc};

use dioxus::prelude::*;
use futures::StreamExt;

use crate::{
    components::rounded_mass::RoundedMass, entities::peptide::Peptide as MaCPepDBPeptide,
    routes::Routes,
};

/// Properties for protein list
///
#[derive(Clone, PartialEq, Props)]
pub struct PaginatedPeptideListProps<T>
where
    T: 'static + PartialEq,
{
    pub peptides_per_page: usize,

    /// List of elements to render
    pub peptides: Rc<Vec<MaCPepDBPeptide<T>>>,
}

pub fn PaginatedPeptideList<T>(props: PaginatedPeptideListProps<T>) -> Element
where
    T: 'static + PartialEq,
{
    let number_of_pages =
        (props.peptides.len() as f64 / props.peptides_per_page as f64).ceil() as usize;
    let number_of_peptides = props.peptides.len();
    let mut current_page = use_signal(|| 0);
    let current_element_range = use_resource(move || async move {
        let start = *current_page.read_unchecked() * props.peptides_per_page;
        let end = min(start + props.peptides_per_page, number_of_peptides - 1);
        start..end
    });
    let previous_coroutine = use_coroutine(move |mut rx: UnboundedReceiver<()>| async move {
        while rx.next().await.is_some() {
            let current_page_value = *current_page.read_unchecked();

            if current_page_value > 0 {
                current_page.set(current_page_value - 1);
            }
        }
    });
    let next_coroutine = use_coroutine(move |mut rx: UnboundedReceiver<()>| async move {
        while rx.next().await.is_some() {
            let current_page_value = *current_page.read_unchecked();

            if current_page_value < number_of_pages {
                current_page.set(current_page_value + 1);
            }
        }
    });

    rsx! {
        p { class: "mt-2 mb-1", "{props.peptides.len()} peptides in total" }
        table { class: "table table-striped table-hover table-sm table-responsive",
            thead {
                tr {
                    th { "Mass (Da)" }
                    th { "Sequence" }
                }
            }
            tbody {
                if let Some(range) = &*current_element_range.read_unchecked() {
                    for peptide in props.peptides[range.clone()].iter() {
                        tr {
                            td {
                                RoundedMass { mass: peptide.get_mass() }
                            }
                            td { class: "text-break",
                                Link {
                                    to: Routes::Peptide {
                                        peptide_sequence: peptide.get_sequence().to_owned(),
                                    },
                                    "{peptide.get_sequence()}"
                                }
                            }
                        }
                    }
                }
            }
        }

        if number_of_pages > 1 {
            div { class: "row",
                div { class: "col-12 col-md-6 col-lg-2",
                    div { class: "input-group mb-3",
                        button {
                            onclick: move |_| current_page.set(0),
                            class: "btn btn-primary",
                            i { class: "fa fa-chevron-left" }
                            i { class: "fa fa-chevron-left" }
                        }

                        button {
                            onclick: move |_| previous_coroutine.send(()),
                            class: "btn btn-primary",
                            i { class: "fa fa-chevron-left" }
                        }

                        input {
                            class: "form-control",
                            r#type: "number",
                            step: 1,
                            min: 1,
                            max: number_of_pages + 1,
                            value: current_page + 1,
                            oninput: move |evt| {
                                let value = evt.value().parse::<usize>().unwrap_or(0);
                                if value > number_of_pages {
                                    current_page.set(number_of_pages - 1);
                                } else {
                                    current_page.set(value - 1);
                                }
                            },
                        }

                        span { class: "input-group-text", "/ {number_of_pages}" }

                        button {
                            onclick: move |_| next_coroutine.send(()),
                            class: "btn btn-primary",
                            i { class: "fa fa-chevron-right" }
                        }


                        button {
                            onclick: move |_| current_page.set(number_of_pages - 1),
                            class: "btn btn-primary",
                            i { class: "fa fa-chevron-right" }
                            i { class: "fa fa-chevron-right" }
                        }
                    }
                }
            }
        }
    }
}
