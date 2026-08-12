use std::{collections::HashMap, fmt::Write, rc::Rc, time::Duration};

use ::web_sys::window;
use async_std::task::sleep;
use dioxus::prelude::*;
use wasm_bindgen::{JsCast, JsValue};

use crate::{
    api_client::Client,
    components::{separator_line::SeparatorLine, spinner::Spinner},
    configuration::Configuration as AppConfiguration,
    errors::general_error::GeneralError,
    tracking::track_page_visit,
};
use macpepdb_web_common::{
    requests::{
        ptm::{PostTranslationalModificationRequest, PtmPosition, PtmType},
        tools::SrmPrmRequest,
    },
    responses::taxonomy::TaxonomyResponse,
};

/// Default upper and lower mass tolerance
const DEFAULT_UPPER_LOWER_MASS_TOLERANCE: i64 = 10;

/// Default charge
const DEFAULT_CHARGE: u8 = 2;

/// Default max variable modifications
const DEFAULT_MAX_VAR_MODIFICATIONS: i16 = 2;

// See `components::peptide_search::mass_search` for why these labels/parsers are
// reproduced locally instead of living on `PtmType`/`PtmPosition` themselves.

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

/// UI label for a [`PtmPosition`], used as both the `<option>` value and display text.
fn ptm_position_label(position: PtmPosition) -> &'static str {
    match position {
        PtmPosition::Anywhere => "Anywhere",
        PtmPosition::NTerminus => "Terminus-N",
        PtmPosition::CTerminus => "Terminus-C",
        PtmPosition::NBond => "Bond-N",
        PtmPosition::CBond => "Bond-C",
    }
}

/// Parses a [`PtmPosition`] back from a label produced by [`ptm_position_label`], defaulting
/// to `PtmPosition::Anywhere` for unrecognized input.
fn parse_ptm_position(value: &str) -> PtmPosition {
    match value {
        "Terminus-N" => PtmPosition::NTerminus,
        "Terminus-C" => PtmPosition::CTerminus,
        "Bond-N" => PtmPosition::NBond,
        "Bond-C" => PtmPosition::CBond,
        _ => PtmPosition::Anywhere,
    }
}

/// Saves `contents` as a file download in the browser via a `Blob` + temporary `<a download>`
/// element, since there is no server response to attach a `Content-Disposition` header to.
fn trigger_tsv_download(filename: &str, contents: &str) {
    let blob_parts = js_sys::Array::of1(&JsValue::from_str(contents));
    let blob_options = web_sys::BlobPropertyBag::new();
    blob_options.set_type("text/tab-separated-values;charset=utf-8");
    let blob =
        web_sys::Blob::new_with_str_sequence_and_options(&blob_parts, &blob_options).unwrap();
    let url = web_sys::Url::create_object_url_with_blob(&blob).unwrap();

    let document = window().unwrap().document().unwrap();
    let anchor = document
        .create_element("a")
        .unwrap()
        .dyn_into::<web_sys::HtmlAnchorElement>()
        .unwrap();
    anchor.set_href(&url);
    anchor.set_download(filename);
    anchor.click();

    web_sys::Url::revoke_object_url(&url).unwrap();
}

enum MsVendor {
    ThermoFisher,
    Bruker,
}

pub fn SrmPrmTargetFinder() -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();

    use_future(move || async move { track_page_visit(vec![]).await });

    // target masses
    let mut new_target_mz = use_signal(|| 0.0);
    let mut new_target_charge = use_signal(|| DEFAULT_CHARGE);
    let mut targets: Signal<Vec<(f64, u8)>> = use_signal(Vec::new);

    // mass tolerance
    let mut lower_mass_tolerance = use_signal(|| DEFAULT_UPPER_LOWER_MASS_TOLERANCE);
    let mut upper_mass_tolerance = use_signal(|| DEFAULT_UPPER_LOWER_MASS_TOLERANCE);

    // taxonomy filter (multi-select)
    let mut taxonomy_search_term = use_signal(|| "".to_string());
    let mut selected_taxonomies: Signal<Vec<TaxonomyResponse>> = use_signal(Vec::new);

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
            let search_term = taxonomy_search_term.read_unchecked().clone();

            Ok(Some(client.search_taxonomies(&search_term).await?))
        });

    // post translational modifications
    let mut max_var_modifications = use_signal(|| DEFAULT_MAX_VAR_MODIFICATIONS);
    let mut new_ptm_amino_acid = use_signal(|| ' ');
    let mut new_ptm_mass = use_signal(|| 0.0);
    let mut new_ptm_type = use_signal(|| PtmType::Static);
    let mut new_ptm_position = use_signal(|| PtmPosition::Anywhere);
    let mut ptm_index = use_signal(|| 0); // Just to have something to use as name
    let mut ptms: Signal<Vec<PostTranslationalModificationRequest>> = use_signal(Vec::new);
    let amino_acids = use_resource(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Ok(None),
        };

        let client = Client::new(macpepdb_base_url)?;
        let mut amino_acids = client.get_amino_acid().await?;
        amino_acids.sort_by_key(|x| x.code);
        Ok::<_, GeneralError>(Some(amino_acids))
    });

    // normalized collision energy
    let mut normalized_collision_energy = use_signal(|| 0.0);

    // resolved taxonomy names for the results table (id -> scientific name)
    let mut taxonomy_names: Signal<HashMap<i32, String>> = use_signal(HashMap::new);

    // search for SRM/PRM targets
    let mut search = use_action(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Err(GeneralError::ConfigurationNotLoaded),
        };
        let client = Client::new(macpepdb_base_url)?;

        let request = SrmPrmRequest {
            thompson: targets.read_unchecked().clone(),
            lower_tolerance_ppm: *lower_mass_tolerance.read_unchecked(),
            upper_tolerance_ppm: *upper_mass_tolerance.read_unchecked(),
            max_variable_modifications: max_var_modifications.read_unchecked().max(0) as usize,
            ptms: ptms.read_unchecked().clone(),
            taxonomies: selected_taxonomies
                .read_unchecked()
                .iter()
                .map(|taxonomy| taxonomy.id)
                .collect(),
        };

        let response = client.search_srm_prm_targets(&request).await?;

        let mut distinct_taxonomy_ids: Vec<i32> = response
            .targets
            .iter()
            .map(|target| target.taxonomy_id)
            .collect();
        distinct_taxonomy_ids.sort_unstable();
        distinct_taxonomy_ids.dedup();
        taxonomy_names.set(if distinct_taxonomy_ids.is_empty() {
            HashMap::new()
        } else {
            client.resolve_taxonomy_ids(distinct_taxonomy_ids).await?
        });

        Ok::<_, GeneralError>(Rc::new(response.targets))
    });

    let download = move |vendor: MsVendor| {
        let Some(Ok(results)) = search.value() else {
            return;
        };

        let normalized_collision_energy = *normalized_collision_energy.read_unchecked();
        let taxonomy_names = taxonomy_names.read_unchecked().clone();

        let tsv = match vendor {
            MsVendor::ThermoFisher => {
                let mut tsv = String::from(
                    "Compound\tMass [m/z]\tFormula [M]\tSpecies\tCS [z]\tStart [min]\tEnd [min]\tNCE\n",
                );
                for target in results.read_unchecked().iter() {
                    writeln!(
                        tsv,
                        "{}\t{}\t\t{} ({})\t{}\t\t\t{}",
                        target.sequence,
                        target.mz,
                        taxonomy_names
                            .get(&target.taxonomy_id)
                            .map(|id| id.to_string())
                            .unwrap_or_default(),
                        target.taxonomy_id,
                        target.charge,
                        normalized_collision_energy,
                    )
                    .unwrap();
                }
                tsv
            }
            MsVendor::Bruker => {
                let mut tsv = String::from(
                    "Mass [m/z]\tCharge\tIsolation Width [m/z]\tRT [s]\tRT Range [s]\tStart IM [1/K0]\tEnd IM [1/K0]\tCE [eV]\tExternal ID\tDescription\n"
                );
                for target in results.read_unchecked().iter() {
                    writeln!(
                        tsv,
                        "{}\t{}\t\t\t\t\t\t{}\t{}\tSpecies: {} ({})",
                        target.mz,
                        target.charge,
                        normalized_collision_energy,
                        target.sequence,
                        taxonomy_names
                            .get(&target.taxonomy_id)
                            .map(|id| id.to_string())
                            .unwrap_or_default(),
                        target.taxonomy_id
                    )
                    .unwrap();
                }
                tsv
            }
        };

        trigger_tsv_download("macpepdb_srm_prm_targets.tsv", &tsv);
    };

    rsx! {
        h1 { "SRM / PRM target finder" }
        p {
            """"
            "This tool is a specialiced version of the mass search. Using the provided information, MaCPepDB searchs for all peptides matching the targetes masses which are unique for the in the given species. \
            If multiple species were selected, MaCPepDB will also remove all peptides which might be unique in each species but occure on more then one of the selected. \
            Same is true if a higher taxonomy is selected like genus. In this case genus i resolved to all contained species."
        }

        SeparatorLine { label: "Target masses" }
        div { class: "input-group mb-3",
            span { class: "input-group-text", "m/z" }
            input {
                r#type: "number",
                class: "form-control",
                value: "{new_target_mz}",
                oninput: move |evt| new_target_mz.set(evt.value().parse().unwrap_or(0.0)),
            }
            span { class: "input-group-text", "charge" }
            input {
                r#type: "number",
                class: "form-control",
                step: 1,
                value: "{new_target_charge}",
                oninput: move |evt| new_target_charge.set(evt.value().parse().unwrap_or(DEFAULT_CHARGE)),
            }
            button {
                class: "btn btn-primary",
                r#type: "button",
                onclick: move |_| {
                    targets.push((*new_target_mz.read(), *new_target_charge.read()));
                    new_target_mz.set(0.0);
                    new_target_charge.set(DEFAULT_CHARGE);
                },
                "Add target"
            }
        }
        div { class: "list-group mb-3",
            if targets.is_empty() {
                div { class: "list-group-item list-group-item-warning", "No targets added yet." }
            }
            for (idx , target) in targets.iter().enumerate() {
                div { class: "list-group-item d-flex justify-content-between align-items-center",
                    "m/z {target.0}, charge {target.1}"
                    button {
                        class: "btn btn-danger",
                        r#type: "button",
                        onclick: move |_| {
                            targets.remove(idx);
                        },
                        i { class: "fa-solid fa-xmark" }
                    }
                }
            }
        }

        div {
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

            SeparatorLine { label: "Taxonomies" }
            div { class: "input-group mb-3",
                span { class: "input-group-text", "Taxonomy search *" }
                input {
                    r#type: "text",
                    class: "form-control",
                    value: "{taxonomy_search_term}",
                    oninput: move |evt| { taxonomy_search_term.set(evt.value()) },
                }
            }
            div { class: "list-group mb-3",
                for taxonomy in selected_taxonomies.iter().map(|t| t.clone()) {
                    div { class: "list-group-item d-flex justify-content-between align-items-center",
                        "{taxonomy.scientific_name} (ID: {taxonomy.id}, Rank: {taxonomy.rank_name.clone().unwrap_or_default()})"
                        button {
                            class: "btn btn-danger",
                            r#type: "button",
                            onclick: move |_| {
                                selected_taxonomies.write().retain(|t| t.id != taxonomy.id);
                            },
                            i { class: "fa-solid fa-xmark" }
                        }
                    }
                }
            }
            match &*taxonomies.read_unchecked() {
                Some(Ok(Some(found_taxonomies))) => rsx! {
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
                            for taxonomy in found_taxonomies.iter().cloned() {
                                tr {
                                    td { "{taxonomy.id}" }
                                    td { "{taxonomy.scientific_name}" }
                                    td { "{taxonomy.rank_name.clone().unwrap_or_default()}" }
                                    td {
                                        button {
                                            class: "btn btn-sm btn-primary",
                                            r#type: "button",
                                            disabled: selected_taxonomies.read().iter().any(|t| t.id == taxonomy.id),
                                            onclick: move |_| {
                                                if !selected_taxonomies.read().iter().any(|t| t.id == taxonomy.id) {
                                                    selected_taxonomies.push(taxonomy.clone());
                                                }
                                            },
                                            "Add"
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
                    },
                    "Add PTM"
                }
            }
            div { class: "list-input-group",
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

            SeparatorLine { label: "Normalized collision energy" }
            div { class: "input-group mb-3",
                span { class: "input-group-text", "NCE" }
                input {
                    r#type: "number",
                    class: "form-control",
                    value: "{normalized_collision_energy}",
                    oninput: move |evt| {
                        normalized_collision_energy.set(evt.value().parse().unwrap_or(0.0))
                    },
                }
            }
        }

        div { class: "row mt-3",
            div { class: "col d-flex justify-content-between",
                button {
                    class: "btn btn-primary",
                    r#type: "button",
                    disabled: search.pending() || targets.is_empty() || selected_taxonomies.is_empty(),
                    onclick: move |_| { search.call() },
                    i { class: "fa-solid fa-search me-2" }
                    "Search"
                }
                if let Some(Ok(_)) = search.value() {
                    div {
                        class: "dropdown",
                        button {
                            aria_expanded: "false",
                            class: "btn btn-primary dropdown-toggle",
                            r#type: "button",
                            "data-bs-toggle": "dropdown",
                            i { class: "fa-solid fa-download me-2" }
                            "Download"
                        }
                        ul {
                            class: "dropdown-menu",
                            li {
                                button {
                                    class: "dropdown-item",
                                    r#type: "button",
                                    onclick: move |_| download(MsVendor::ThermoFisher),
                                    "For Thermo Fisher"
                                }
                            }
                            li {
                                button {
                                    class: "dropdown-item",
                                    r#type: "button",
                                    onclick: move |_| download(MsVendor::Bruker),
                                    "For Bruker"
                                }
                            }
                        }
                    }
                }
            }
        }

        match search.value() {
            Some(Ok(results)) => rsx! {
                table { class: "table table-striped table-hover",
                    thead {
                        tr {
                            th { "Sequence" }
                            th { "m/z" }
                            th { "Charge" }
                            th { "Taxonomy" }
                        }
                    }
                    tbody {
                        for target in results.read_unchecked().iter().cloned() {
                            tr {
                                td { "{target.sequence}" }
                                td { "{target.mz}" }
                                td { "{target.charge}" }
                                td {
                                    "{taxonomy_names.read().get(&target.taxonomy_id).cloned().unwrap_or_else(|| target.taxonomy_id.to_string())}"
                                }
                            }
                        }
                    }
                }
                if results.read_unchecked().is_empty() {
                    div { class: "alert alert-info", "No unique targets found for the given input." }
                }
            },
            Some(Err(err)) => rsx! {
                div { class: "alert alert-danger", "Error searching for SRM/PRM targets: {err}" }
            },
            None => rsx! {
                if search.pending() {
                    Spinner {}
                }
            },
        }
    }
}
