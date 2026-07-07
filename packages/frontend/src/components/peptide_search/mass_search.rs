use std::{rc::Rc, time::Duration};

use ::web_sys::window;
use async_std::task::sleep;
use dioxus::prelude::*;
use dioxus_logger::tracing::info;

use crate::{
    api_client::Client,
    components::{
        paginated_peptide_list::PaginatedPeptideList, separator_line::SeparatorLine,
        spinner::Spinner,
    },
    configuration::Configuration as AppConfiguration,
    entities::mass_unit::MassUnit,
    errors::{api_client_error::ApiClientError, general_error::GeneralError},
};
use macpepdb_web_common::{
    requests::ptm::{PostTranslationalModificationRequest, PtmPosition, PtmType},
    responses::{
        amino_acid::AminoAcidResponse, peptide::PeptideResponse, taxonomy::TaxonomyResponse,
    },
};

/// Default upper and lower mass tolerance
///
const DEFAULT_UPPER_LOWER_MASS_TOLERANCE: i64 = 10;

/// Default charge
///
const DEFAULT_CHARGE: u8 = 2;

/// Default max variable modifications
///
const DEFAULT_MAX_VAR_MODIFICATIONS: i16 = 2;

// `macpepdb_web_common::requests::ptm::{PtmType, PtmPosition}` intentionally only implement
// `Serialize`/`Deserialize` (with the serde renames the backend's wire format needs) and not
// `Display`/`FromStr` - the old, now-deleted `entities::post_translational_modification` copies
// had those impls purely for this UI's `<select>` elements. Reproduce that mapping locally
// instead of adding the impls to the shared crate.

/// UI label for a [`PtmType`], used as both the `<option>` value and display text.
fn ptm_type_label(ptm_type: PtmType) -> &'static str {
    match ptm_type {
        PtmType::Static => "Static",
        PtmType::Variable => "Variable",
    }
}

/// Parses a [`PtmType`] back from a label produced by [`ptm_type_label`], defaulting to
/// `PtmType::Static` for unrecognized input.
fn parse_ptm_type(value: &str) -> PtmType {
    match value {
        "Variable" => PtmType::Variable,
        _ => PtmType::Static,
    }
}

/// UI label for a [`PtmPosition`], used as both the `<option>` value and display text. These
/// happen to match the backend wire format (`Terminus-N`, ...) for consistency, but are only
/// used for display/parsing in this component.
fn ptm_position_label(position: PtmPosition) -> &'static str {
    match position {
        PtmPosition::Anywhere => "Anywhere",
        PtmPosition::NTerminus => "Terminus-N",
        PtmPosition::CTerminus => "Terminus-C",
        PtmPosition::NBond => "Bond-N",
        PtmPosition::CBond => "Bond-C",
    }
}

/// Parses a [`PtmPosition`] back from a label produced by [`ptm_position_label`], defaulting to
/// `PtmPosition::Anywhere` for unrecognized input.
fn parse_ptm_position(value: &str) -> PtmPosition {
    match value {
        "Terminus-N" => PtmPosition::NTerminus,
        "Terminus-C" => PtmPosition::CTerminus,
        "Bond-N" => PtmPosition::NBond,
        "Bond-C" => PtmPosition::CBond,
        _ => PtmPosition::Anywhere,
    }
}

pub fn MassSearch() -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();

    // ui state
    let mut are_filters_visible = use_signal(|| false);

    // mass filter
    let mut selected_mass_unit = use_signal(|| MassUnit::Thompson);
    let mut thompson = use_signal(|| 0.0);
    let mut charge = use_signal(|| DEFAULT_CHARGE);
    let mut dalton = use_signal(|| 0.0);
    let mut lower_mass_tolerance = use_signal(|| DEFAULT_UPPER_LOWER_MASS_TOLERANCE);
    let mut upper_mass_tolerance = use_signal(|| DEFAULT_UPPER_LOWER_MASS_TOLERANCE);

    // taxonomy filter
    let mut taxonomy_search_term = use_signal(|| "".to_string());
    let mut selected_taxonomy_id: Signal<Option<u64>> = use_signal(|| None);

    let taxonomies: Resource<Result<Option<Vec<TaxonomyResponse>>, GeneralError>> =
        use_resource(move || async move {
            if taxonomy_search_term.read_unchecked().is_empty() {
                return Ok(None);
            }

            sleep(Duration::from_millis(300)).await; // debounce

            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => Err(GeneralError::ConfigurationNotLoaded)?,
            };

            let client = Client::new(macpepdb_base_url)?;

            Ok(Some(
                client
                    .search_taxonomies(&taxonomy_search_term.read_unchecked())
                    .await?,
            ))
        });

    let selected_taxonomy: Resource<Result<Option<TaxonomyResponse>, GeneralError>> =
        use_resource(move || async move {
            if selected_taxonomy_id.read_unchecked().is_none() {
                return Ok(None);
            }

            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Ok(None),
            };

            let client = Client::new(macpepdb_base_url)?;

            Ok(Some(
                client
                    .get_taxonomy(selected_taxonomy_id.read_unchecked().unwrap())
                    .await?,
            ))
        });

    // post translational modifications
    let mut max_var_modifications = use_signal(|| DEFAULT_MAX_VAR_MODIFICATIONS);
    let mut new_ptm_amino_acid = use_signal(|| ' ');
    let mut new_ptm_mass = use_signal(|| 0.0);
    let mut new_ptm_type = use_signal(|| PtmType::Static);
    let mut new_ptm_position = use_signal(|| PtmPosition::Anywhere);
    let mut ptm_index = use_signal(|| 0); // Just to have something to use as name
    let mut ptms: Signal<Vec<PostTranslationalModificationRequest>> = use_signal(Vec::new);
    let amino_acids: Resource<Result<Option<Vec<AminoAcidResponse>>>> =
        use_resource(move || async move {
            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Ok(None),
            };

            let client = Client::new(macpepdb_base_url)?;
            let mut amino_acids = client.get_amino_acid().await?;
            amino_acids.sort_by_key(|x| x.code);
            Ok(Some(amino_acids))
        });

    // review filter
    let mut is_reviewed: Signal<Option<bool>> = use_signal(|| None);

    // search peptides
    let mut peptides = use_action(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Err(GeneralError::ConfigurationNotLoaded),
        };

        let selected_taxonomy_bound = selected_taxonomy.read_unchecked();
        let selected_taxonomy = match selected_taxonomy_bound.as_ref() {
            Some(Ok(taxonomy)) => taxonomy,
            Some(Err(_)) => &None,
            None => &None,
        };

        let peptides_result: Result<Vec<PeptideResponse>, ApiClientError> =
            Client::new(macpepdb_base_url)?
                .search_peptides(
                    selected_mass_unit.read_unchecked().clone(),
                    *thompson.read_unchecked(),
                    *charge.read_unchecked(),
                    *dalton.read_unchecked(),
                    *lower_mass_tolerance.read_unchecked(),
                    *upper_mass_tolerance.read_unchecked(),
                    selected_taxonomy,
                    *max_var_modifications.read_unchecked(),
                    &ptms.read_unchecked(),
                    *is_reviewed.read_unchecked(),
                )
                .await;

        match peptides_result {
            Ok(peptides) => Ok(Rc::new(peptides)),
            Err(err) => Err(err.into()),
        }
    });

    // Coroutine to intiate download with the last search parameters
    //
    let mut download = use_action(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Err(GeneralError::ConfigurationNotLoaded),
        };

        let selected_taxonomy_bound = selected_taxonomy.read_unchecked();
        let selected_taxonomy = match selected_taxonomy_bound.as_ref() {
            Some(Ok(taxonomy)) => taxonomy,
            Some(Err(_)) => &None,
            None => &None,
        };

        let url = Client::new(macpepdb_base_url)?.peptide_search_download_url(
            selected_mass_unit.read().clone(),
            *thompson.read(),
            *charge.read(),
            *dalton.read(),
            *lower_mass_tolerance.read(),
            *upper_mass_tolerance.read(),
            selected_taxonomy,
            *max_var_modifications.read(),
            &ptms.read(),
            *is_reviewed.read(),
        );
        window().unwrap().location().assign(&url).unwrap();

        Ok(())
    });

    rsx! {
        SeparatorLine { label: "Mass" }

        select {
            class: "form-select mb-3",
            oninput: move |evt| {
                selected_mass_unit.set(evt.value().parse().unwrap_or(MassUnit::Thompson))
            },
            option {
                value: MassUnit::Thompson.to_string(),
                selected: *selected_mass_unit.read() == MassUnit::Thompson,
                "{MassUnit::Thompson.to_string()}"
            }
            option {
                value: MassUnit::Dalton.to_string(),
                selected: *selected_mass_unit.read() == MassUnit::Dalton,
                "{MassUnit::Dalton.to_string()}"
            }
        }

        div { class: if *selected_mass_unit.read() != MassUnit::Thompson { "d-none" } else { "" },

            div { class: "input-group mb-3",
                span { class: "input-group-text", "m/z" }
                input {
                    id: "thompson",
                    r#type: "number",
                    class: "form-control",
                    value: "{thompson}",
                    oninput: move |evt| thompson.set(evt.value().parse().unwrap_or(0.0)),
                }
            }
            div { class: "input-group mb-3",
                span { class: "input-group-text", "charge" }
                input {
                    id: "charge",
                    r#type: "number",
                    class: "form-control",
                    step: 1,
                    value: "{charge}",
                    oninput: move |evt| charge.set(evt.value().parse().unwrap_or(DEFAULT_CHARGE)),
                }
            }
        }
        div { class: if *selected_mass_unit.read() != MassUnit::Dalton { "input-group mb-3 d-none" } else { "input-group mb-3" },
            span { class: "input-group-text", "Da" }
            input {
                id: "dalton",
                r#type: "number",
                class: "form-control",
                value: "{dalton}",
                oninput: move |evt| dalton.set(evt.value().parse().unwrap_or(0.0)),
            }
        }

        button {
            class: "btn btn-primary btn-sm",
            r#type: "button",
            onclick: move |_| {
                let new_value = !*are_filters_visible.read();
                are_filters_visible.set(new_value);
            },
            "Filters"
            if *are_filters_visible.read() {
                i { class: "fa-solid fa-chevron-up ms-2" }
            } else {
                i { class: "fa-solid fa-chevron-down ms-2" }
            }
        }
        div { class: if *are_filters_visible.read() { "collapse show" } else { "collapse" },
            SeparatorLine { label: "Mass tolerance (mandatory, unit: ppm)" }
            div { class: "input-group mb-3",
                span { class: "input-group-text", "Lower" }
                input {
                    r#type: "number",
                    class: "form-control",
                    step: 1,
                    value: "{lower_mass_tolerance}",
                    oninput: move |evt| {
                        lower_mass_tolerance
                            .set(evt.value().parse().unwrap_or(DEFAULT_UPPER_LOWER_MASS_TOLERANCE))
                    },
                }
            }
            div { class: "input-group mb-3",
                span { class: "input-group-text", "Upper" }
                input {
                    r#type: "number",
                    class: "form-control",
                    step: 1,
                    value: "{upper_mass_tolerance}",
                    oninput: move |evt| {
                        upper_mass_tolerance
                            .set(evt.value().parse().unwrap_or(DEFAULT_UPPER_LOWER_MASS_TOLERANCE))
                    },
                }
            }

            SeparatorLine { label: "Taxonomy (optional)" }
            div { class: "input-group mb-3",
                span { class: "input-group-text", "Taxonomy search" }
                input {
                    r#type: "text",
                    class: "form-control",
                    value: "{taxonomy_search_term}",
                    oninput: move |evt| { taxonomy_search_term.set(evt.value()) },
                }
            }
            match &*selected_taxonomy.read_unchecked() {
                Some(Ok(Some(taxonomy))) => rsx! {
                    div { class: "list-group",
                        div { class: "list-group-item d-flex justify-content-between align-items-center",
                            "Selected taxonomy: {taxonomy.scientific_name} (ID: {taxonomy.id}, Rank: {taxonomy.rank_name.clone().unwrap_or_default()})"
                            button {
                                class: "btn btn-danger",
                                r#type: "button",
                                onclick: move |_| {
                                    selected_taxonomy_id.set(None);
                                },
                                i { class: "fa-solid fa-xmark" }
                            }
                        }
                    }
                },
                Some(Ok(None)) => rsx! {
                    div {}
                },
                Some(Err(err)) => rsx! {
                    div { "Error fetching selected taxonomy: {err}" }
                },
                None => rsx! {
                    div {}
                },
            }
            match &*taxonomies.read_unchecked() {
                Some(Ok(Some(taxonomies))) => rsx! {
                    table { class: "table table-striped table-hover",
                        thead {
                            tr {
                                th { "ID" }
                                th { "Scientific name" }
                                th { "Rank" }
                                th { "Select" }
                            }
                        }
                        tbody {
                            for taxonomy in taxonomies {
                                tr {
                                    td { "{taxonomy.id}" }
                                    td { "{taxonomy.scientific_name}" }
                                    td { "{taxonomy.rank_name.clone().unwrap_or_default()}" }
                                    td {
                                        input {
                                            r#type: "radio",
                                            value: "{taxonomy.id}",
                                            name: "taxonomy",
                                            oninput: move |evt| {
                                                selected_taxonomy_id.set(Some(evt.value().parse().unwrap()));
                                                taxonomy_search_term.set("".to_string());
                                            },
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                Some(Ok(None)) => rsx! {
                    div {}
                },
                Some(Err(err)) => rsx! {
                    div { "Error fetching taxonomies: {err}" }
                },
                None => rsx! {
                    div { "Loading..." }
                },
            }

            SeparatorLine { label: "Post translational modifications" }
            div { class: "input-group mb-3",
                span { class: "input-group-text", "Max variable modifications" }
                input {
                    r#type: "number",
                    class: "form-control",
                    step: 1,
                    value: "{max_var_modifications}",
                    oninput: move |evt| {
                        max_var_modifications
                            .set(evt.value().parse().unwrap_or(DEFAULT_MAX_VAR_MODIFICATIONS))
                    },
                }
            }

            div { class: "input-group mb-3",
                select {
                    class: "form-control",
                    oninput: move |evt| {
                        new_ptm_amino_acid.set(evt.value().parse().unwrap_or(' '));
                    },
                    option { value: " ", "Select amino acid" }
                    match &*amino_acids.read_unchecked() {
                        Some(Ok(Some(amino_acids))) => rsx! {
                            for aa in amino_acids {
                                option { value: "{aa.code}", "{aa.code} - {aa.name}" }
                            }
                        },
                        Some(Err(e)) => rsx! {
                            option { "Error loading amino acids: {e}" }
                        },
                        None | Some(Ok(None)) => rsx! {
                            option { "Loading ..." }
                        },
                    }
                }
                input {
                    r#type: "number",
                    class: "form-control",
                    value: "{new_ptm_mass}",
                    oninput: move |evt| {
                        new_ptm_mass.set(evt.value().parse().unwrap_or(0.0));
                    },
                }
                select {
                    class: "form-control",
                    oninput: move |evt| {
                        new_ptm_type.set(parse_ptm_type(&evt.value()));
                    },
                    option { value: ptm_type_label(PtmType::Static), "{ptm_type_label(PtmType::Static)}" }
                    option { value: ptm_type_label(PtmType::Variable), "{ptm_type_label(PtmType::Variable)}" }
                }
                select {
                    class: "form-control",
                    oninput: move |evt| {
                        new_ptm_position.set(parse_ptm_position(&evt.value()));
                    },
                    option { value: ptm_position_label(PtmPosition::Anywhere),
                        "{ptm_position_label(PtmPosition::Anywhere)}"
                    }
                    option { value: ptm_position_label(PtmPosition::NTerminus),
                        "{ptm_position_label(PtmPosition::NTerminus)}"
                    }
                    option { value: ptm_position_label(PtmPosition::CTerminus),
                        "{ptm_position_label(PtmPosition::CTerminus)}"
                    }
                    option { value: ptm_position_label(PtmPosition::NBond), "{ptm_position_label(PtmPosition::NBond)}" }
                    option { value: ptm_position_label(PtmPosition::CBond), "{ptm_position_label(PtmPosition::CBond)}" }
                }
                button {
                    class: "btn btn-primary",
                    r#type: "button",
                    onclick: move |_| {
                        ptm_index += 1;
                        let ptm = PostTranslationalModificationRequest {
                            name: format!("PTM {}", ptm_index),
                            amino_acid: *new_ptm_amino_acid.read(),
                            mass_delta: *new_ptm_mass.read(),
                            mod_type: *new_ptm_type.read(),
                            position: *new_ptm_position.read(),
                        };
                        ptms.push(ptm);
                        info!("Add PTM");
                    },
                    "Add PTM"
                }
            }
            div { class: "list-group",
                for (idx , ptm) in ptms.iter().enumerate() {
                    div { class: "input-group",
                        input {
                            r#type: "text",
                            class: "form-control",
                            value: "{ptm.amino_acid}",
                            disabled: true,
                        }
                        input {
                            r#type: "number",
                            class: "form-control",
                            value: "{ptm.mass_delta}",
                            disabled: true,
                        }
                        input {
                            r#type: "text",
                            class: "form-control",
                            value: "{ptm_type_label(ptm.mod_type)}",
                            disabled: true,
                        }
                        input {
                            r#type: "text",
                            class: "form-control",
                            value: "{ptm_position_label(ptm.position)}",
                            disabled: true,
                        }
                        button {
                            class: "btn btn-danger",
                            r#type: "button",
                            onclick: move |_| {
                                ptms.remove(idx);
                            },
                            i { class: "fa-solid fa-xmark" }
                        }
                    }
                }
            }
            SeparatorLine { label: "Review status" }
            div { class: "form-check form-check-inline",
                label { class: "form-check-label", "Don't care" }
                input {
                    r#type: "radio",
                    class: "form-check-input",
                    name: "review-filter",
                    checked: is_reviewed.read().is_none(),
                    oninput: move |_| {
                        is_reviewed.set(None);
                    },
                }
            }
            div { class: "form-check form-check-inline",
                label { class: "form-check-label", "Reviewed" }
                input {
                    r#type: "radio",
                    class: "form-check-input",
                    name: "review-filter",
                    checked: is_reviewed.read().is_some() && is_reviewed.read().unwrap(),
                    oninput: move |_| {
                        is_reviewed.set(Some(true));
                    },
                }
            }
            div { class: "form-check form-check-inline",
                label { class: "form-check-label", "Unreviewed" }
                input {
                    r#type: "radio",
                    class: "form-check-input",
                    name: "review-filter",
                    checked: is_reviewed.read().is_some() && !is_reviewed.read().unwrap(),
                    oninput: move |_| {
                        is_reviewed.set(Some(false));
                    },
                }
            }
        }
        div { class: "row mt-3",
            div { class: "col d-flex justify-content-between",
                button {
                    class: "btn btn-primary",
                    r#type: "button",
                    onclick: move |_| { peptides.call() },
                    i { class: "fa-solid fa-search me-2" }
                    "Search"
                }
                if let Some(Ok(_)) = peptides.value() {
                    button {
                        class: "btn btn-primary",
                        r#type: "button",
                        onclick: move |_| {
                            download.call();
                        },
                        i { class: "fa-solid fa-download me-2" }
                        "Download"
                    }
                }
            }
        }
        match peptides.value() {
            Some(Ok(peptides)) => rsx! {
                PaginatedPeptideList { peptides_per_page: 100, peptides: peptides.read_unchecked().clone() }
            },
            Some(Err(err)) => rsx! {
                div { class: "alert alert-danger", "Error getting peptides: {err}" }
            },
            None => rsx! {
                if peptides.pending() {
                    Spinner {}
                }
            },
        }
    }
}
