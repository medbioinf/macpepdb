use std::collections::HashMap;
use std::sync::Arc;

use dioxus::html::input_data::keyboard_types::Code;
use dioxus::prelude::*;
use macpepdb_web_common::responses::protein::ProteinResponse;

use crate::api_client::Client;
use crate::components::protein_list::{ProteinList, ProteinSort, ProteinSortColumn};
use crate::components::spinner::Spinner;
use crate::configuration::Configuration as AppConfiguration;
use crate::errors::api_client_error::ApiClientError;
use crate::errors::general_error::GeneralError;
use crate::errors::protein_search_page_error::ProteinSearchPageError;
use crate::tracking::track_page_visit;

/// Minimum length of search term
///
const MIN_SEARCH_TERM_LENGTH: usize = 3;

/// Number of proteins shown per page by default
///
const DEFAULT_PAGE_SIZE: usize = 50;

/// Selectable page sizes
///
const PAGE_SIZE_OPTIONS: [usize; 4] = [25, 50, 100, 200];

/// Search for proteins by accession or gene name
///
pub fn ProteinSearch() -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();
    let mut protein_id = use_signal(|| "".to_string());
    let mut page = use_signal(|| 0usize);
    let mut page_size = use_signal(|| DEFAULT_PAGE_SIZE);
    let mut sort = use_signal(|| None::<ProteinSort>);

    let mut proteins = use_action(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Err(GeneralError::ConfigurationNotLoaded),
        };

        if protein_id.read().len() < MIN_SEARCH_TERM_LENGTH {
            return Err(ProteinSearchPageError::SearchTermTooShort(MIN_SEARCH_TERM_LENGTH).into());
        }

        let client = Client::new(macpepdb_base_url)?;

        let fetched_proteins: Result<Vec<ProteinResponse<String>>, ApiClientError> =
            client.search_protein(&protein_id.read()).await;

        match fetched_proteins {
            Ok(fetched_proteins) => {
                page.set(0);
                sort.set(None);
                Ok(Arc::new(fetched_proteins))
            }
            Err(err) => Err(err.into()),
        }
    });

    let taxonomy_names: Resource<Result<HashMap<i32, String>, GeneralError>> =
        use_resource(move || async move {
            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Err(GeneralError::ConfigurationNotLoaded),
            };

            // Resolve all taxonomies (not only the current page) so results can be sorted by name
            let ids: Vec<i32> = match proteins.value() {
                Some(Ok(sig)) => {
                    let mut ids: Vec<i32> = sig.read().iter().map(|p| p.taxonomy_id).collect();
                    ids.sort_unstable();
                    ids.dedup();
                    ids
                }
                _ => return Ok(HashMap::new()),
            };

            if ids.is_empty() {
                return Ok(HashMap::new());
            }

            let client = Client::new(macpepdb_base_url)?;
            Ok(client.resolve_taxonomy_ids(ids).await?)
        });

    use_future(move || async move { track_page_visit(vec![]).await });

    rsx! {
        h3 { "Search for proteins" }
        div { class: "input-group mb-3",
            input {
                class: "form-control",
                r#type: "text",
                placeholder: "Protein accession or gene name",
                value: "{protein_id}",
                oninput: move |evt| protein_id.set(evt.value()),
                onkeyup: move |evt| {
                    if evt.code() == Code::Enter || evt.code() == Code::NumpadEnter {
                        proteins.call();
                    }
                },
            }
            button {
                class: "btn btn-primary",
                r#type: "button",
                onclick: move |_| proteins.call(),
                "Search"
            }
        }
        match proteins.value() {
            Some(Ok(proteins)) => {
                let mut proteins = proteins.read().to_vec();
                if let Some(sort) = sort() {
                    match &*taxonomy_names.read_unchecked() {
                        Some(Ok(names)) => sort.sort(&mut proteins, names),
                        _ => sort.sort(&mut proteins, &HashMap::new()),
                    }
                }
                let total = proteins.len();
                let size = page_size();
                let page_count = total.div_ceil(size).max(1);
                let current_page = page().min(page_count - 1);
                let page_proteins: Vec<ProteinResponse<String>> = proteins
                    .iter()
                    .skip(current_page * size)
                    .take(size)
                    .cloned()
                    .collect();
                rsx! {
                    if total > 0 {
                        div { class: "d-flex align-items-center justify-content-between mb-3",
                            span { "{total} proteins in total" }
                            div { class: "d-flex align-items-center gap-2",
                                label { r#for: "protein-page-size", class: "mb-0", "Proteins per page" }
                                select {
                                    id: "protein-page-size",
                                    class: "form-select w-auto",
                                    value: "{size}",
                                    onchange: move |evt| {
                                        if let Ok(new_size) = evt.value().parse::<usize>() {
                                            page_size.set(new_size);
                                            page.set(0);
                                        }
                                    },
                                    for option in PAGE_SIZE_OPTIONS {
                                        option { value: "{option}", selected: option == size, "{option}" }
                                    }
                                }
                            }
                        }
                    }
                    ProteinList {
                        proteins: Arc::new(page_proteins),
                        taxonomy_names,
                        sort: sort(),
                        on_sort: move |column: ProteinSortColumn| {
                            sort.set(Some(ProteinSort::toggled(sort(), column)));
                            page.set(0);
                        },
                    }
                    if page_count > 1 {
                        div { class: "row",
                            div { class: "col-12 col-md-8 col-lg-4",
                                div { class: "input-group mb-3",
                                    button {
                                        class: "btn btn-primary",
                                        r#type: "button",
                                        disabled: current_page == 0,
                                        onclick: move |_| page.set(0),
                                        i { class: "fa fa-chevron-left" }
                                        i { class: "fa fa-chevron-left" }
                                    }
                                    button {
                                        class: "btn btn-primary",
                                        r#type: "button",
                                        disabled: current_page == 0,
                                        onclick: move |_| page.set(current_page.saturating_sub(1)),
                                        i { class: "fa fa-chevron-left" }
                                    }
                                    input {
                                        class: "form-control",
                                        r#type: "number",
                                        step: 1,
                                        min: 1,
                                        max: page_count,
                                        value: current_page + 1,
                                        onchange: move |evt| {
                                            if let Ok(requested) = evt.value().parse::<usize>() {
                                                page.set(requested.clamp(1, page_count) - 1);
                                            }
                                        },
                                    }
                                    span { class: "input-group-text", "/ {page_count}" }
                                    button {
                                        class: "btn btn-primary",
                                        r#type: "button",
                                        disabled: current_page + 1 >= page_count,
                                        onclick: move |_| page.set(current_page + 1),
                                        i { class: "fa fa-chevron-right" }
                                    }
                                    button {
                                        class: "btn btn-primary",
                                        r#type: "button",
                                        disabled: current_page + 1 >= page_count,
                                        onclick: move |_| page.set(page_count - 1),
                                        i { class: "fa fa-chevron-right" }
                                        i { class: "fa fa-chevron-right" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Some(Err(err)) => rsx! {
                div { class: "alert alert-danger", "Error getting proteins: {err}" }
            },
            None => rsx! {
                if proteins.pending() {
                    Spinner {}
                }
            },
        }
    }
}
