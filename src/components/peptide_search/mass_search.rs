use std::{fmt::Display, rc::Rc, str::FromStr};

use ::web_sys::window;
use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as Base64Standard, Engine as _};
use dioxus::prelude::*;
use dioxus_logger::tracing::info;
use futures_util::StreamExt;
use serde_json::json;
use urlencoding::encode as urlencode;

use crate::{
    api_helpers::fetch_status::FetchStatus,
    components::{
        paginated_peptide_list::PaginatedPeptideList, separator_line::SeparatorLine,
        spinner::Spinner,
    },
    configuration::Configuration as AppConfiguration,
    entities::{
        amino_acid::AminoAcid,
        peptide::Peptide as MaCPepDBPeptide,
        post_translational_modification::{PostTranslationalModification, PtmPosition, PtmType},
        taxonomy::Taxonomy,
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

/// As proteins contain their peptides and peptides contain their protein of origin, MaCPepDB
/// stops the recursion on second level by only adding the Protein accession to the peptide
/// instead of the whole protein.
type PeptideEntity = MaCPepDBPeptide<String>;

/// Supported mass units for the search
///
#[derive(PartialEq)]
enum MassUnit {
    Thompson,
    Dalton,
}

impl Display for MassUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MassUnit::Thompson => write!(f, "Thompson"),
            MassUnit::Dalton => write!(f, "Dalton"),
        }
    }
}

impl FromStr for MassUnit {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "thompson" => Ok(MassUnit::Thompson),
            "dalton" => Ok(MassUnit::Dalton),
            _ => Err(()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn create_search_body(
    selected_mass_unit: Signal<MassUnit>,
    thompson: Signal<f64>,
    charge: Signal<u8>,
    dalton: Signal<f64>,
    lower_mass_tolerance: Signal<i64>,
    upper_mass_tolerance: Signal<i64>,
    taxonomy: Resource<Result<Option<Taxonomy>>>,
    max_variable_modifications: Signal<i16>,
    ptms: Signal<Vec<PostTranslationalModification>>,
    is_reviewed: Signal<Option<bool>>,
) -> serde_json::Value {
    let mut body = json!({
        "lower_mass_tolerance_ppm": *lower_mass_tolerance.read(),
        "upper_mass_tolerance_ppm": *upper_mass_tolerance.read(),
        "max_variable_modifications": *max_variable_modifications.read(),
        "modifications": *ptms.read(),
    });

    match *selected_mass_unit.read() {
        MassUnit::Thompson => body["mass"] = json!((*thompson.read(), *charge.read())),
        MassUnit::Dalton => body["mass"] = json!(*dalton.read()),
    };

    if let Some(Ok(Some(taxonomy))) = &*taxonomy.read_unchecked() {
        body["taxonomy_id"] = json!(taxonomy.id);
    }

    if let Some(is_reviewed) = &*is_reviewed.read() {
        body["is_reviewed"] = json!(is_reviewed);
    }

    body
}

async fn search_peptides(
    macpepdb_base_url: &str,
    search_body: serde_json::Value,
) -> Result<Vec<PeptideEntity>> {
    let url = format!("{}/api/peptides/search", macpepdb_base_url);

    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .header("Accept", "application/json")
        .json(&search_body)
        .send()
        .await?;
    if !response.status().is_success() {
        bail!(response.text().await?);
    }

    Ok(response.json().await?)
}

fn base64_urlsafe_encode(input: &str) -> String {
    urlencode(&Base64Standard.encode(input.as_bytes())).into_owned()
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
    let taxonomies: Resource<Result<Option<Vec<Taxonomy>>>> = use_resource(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Ok(None),
        };

        if taxonomy_search_term.read_unchecked().is_empty() {
            return Ok(None);
        }

        let url = format!("{macpepdb_base_url}/api/taxonomies/search");
        let client = reqwest::Client::new();
        let response = client
            .post(&url)
            .json(&json!({
                "name_query": format!("*{taxonomy_search_term}*"),
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            bail!(
                "{}, status code: {}",
                response
                    .text()
                    .await
                    .unwrap_or("Could not decode body of error response".to_string()),
                status
            );
        }

        Ok(Some(response.json().await?))
    });

    let selected_taxonomy: Resource<Result<Option<Taxonomy>>> = use_resource(move || async move {
        if selected_taxonomy_id.read_unchecked().is_none() {
            return Ok(None);
        }

        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Ok(None),
        };

        let url = format!(
            "{macpepdb_base_url}/api/taxonomies/{}",
            selected_taxonomy_id.unwrap()
        );

        Ok(Some(reqwest::get(url).await?.json().await?))
    });

    // post translational modifications
    let mut max_var_modifications = use_signal(|| DEFAULT_MAX_VAR_MODIFICATIONS);
    let mut new_ptm_amino_acid = use_signal(|| ' ');
    let mut new_ptm_mass = use_signal(|| 0.0);
    let mut new_ptm_type = use_signal(|| PtmType::Static);
    let mut new_ptm_position = use_signal(|| PtmPosition::Anywhere);
    let mut ptm_index = use_signal(|| 0); // Just to have something to use as name
    let mut ptms: Signal<Vec<PostTranslationalModification>> = use_signal(Vec::new);
    let amino_acids: Resource<Result<Option<Vec<AminoAcid>>>> = use_resource(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Ok(None),
        };

        let url = format!("{macpepdb_base_url}/api/chemistry/amino_acids");
        let mut amino_acids = reqwest::get(&url).await?.json::<Vec<AminoAcid>>().await?;
        amino_acids.sort_by(|x, y| x.get_code().cmp(y.get_code()));
        Ok(Some(amino_acids))
    });

    // review filter
    let mut is_reviewed: Signal<Option<bool>> = use_signal(|| None);

    // search peptides
    let mut peptides: Signal<FetchStatus<Rc<Vec<PeptideEntity>>>> =
        use_signal(|| FetchStatus::None);
    let search_coroutine = use_coroutine(move |mut rx: UnboundedReceiver<()>| async move {
        while rx.next().await.is_some() {
            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => {
                    peptides.set(FetchStatus::Error(anyhow!(
                        "App configuration not loaded yet"
                    )));
                    continue;
                }
            };

            peptides.set(FetchStatus::Loading);
            let search_body = create_search_body(
                selected_mass_unit,
                thompson,
                charge,
                dalton,
                lower_mass_tolerance,
                upper_mass_tolerance,
                selected_taxonomy,
                max_var_modifications,
                ptms,
                is_reviewed,
            );
            let peptides_result = search_peptides(macpepdb_base_url, search_body).await;
            match peptides_result {
                Ok(new_peptides) => peptides.set(FetchStatus::Finished(Rc::new(new_peptides))),
                Err(e) => peptides.set(FetchStatus::Error(e)),
            }
        }
    });

    // Coroutine to intiate download with the last search parameters
    //
    let download_coroutine = use_coroutine(move |mut rx: UnboundedReceiver<()>| async move {
        while rx.next().await.is_some() {
            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => {
                    peptides.set(FetchStatus::Error(anyhow!(
                        "App configuration not loaded yet"
                    )));
                    continue;
                }
            };

            let search_body = create_search_body(
                selected_mass_unit,
                thompson,
                charge,
                dalton,
                lower_mass_tolerance,
                upper_mass_tolerance,
                selected_taxonomy,
                max_var_modifications,
                ptms,
                is_reviewed,
            );
            let url = format!(
                "{}/api/peptides/search/{}/{}?is_download=true",
                macpepdb_base_url,
                base64_urlsafe_encode(serde_json::to_string(&search_body).unwrap().as_str()),
                urlencode("text/tab-separated-values")
            );
            window().unwrap().location().assign(&url).unwrap();
        }
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
                            "Selected taxonomy: {taxonomy.scientific_name} (ID: {taxonomy.id}, Rank: {taxonomy.rank})"
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
                                    td { "{taxonomy.rank}" }
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
                                option { value: "{aa.get_code()}", "{aa.get_code()} - {aa.get_name()}" }
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
                        new_ptm_type.set(evt.value().parse().unwrap_or(PtmType::Static));
                    },
                    option { value: PtmType::Static.to_string(), "{PtmType::Static.to_string()}" }
                    option { value: PtmType::Variable.to_string(), "{PtmType::Variable.to_string()}" }
                }
                select {
                    class: "form-control",
                    oninput: move |evt| {
                        new_ptm_position.set(evt.value().parse().unwrap_or(PtmPosition::Anywhere));
                    },
                    option { value: PtmPosition::Anywhere.to_string(),
                        "{PtmPosition::Anywhere.to_string()}"
                    }
                    option { value: PtmPosition::NTerminus.to_string(),
                        "{PtmPosition::NTerminus.to_string()}"
                    }
                    option { value: PtmPosition::CTerminus.to_string(),
                        "{PtmPosition::CTerminus.to_string()}"
                    }
                    option { value: PtmPosition::NBond.to_string(), "{PtmPosition::NBond.to_string()}" }
                    option { value: PtmPosition::CBond.to_string(), "{PtmPosition::CBond.to_string()}" }
                }
                button {
                    class: "btn btn-primary",
                    r#type: "button",
                    onclick: move |_| {
                        ptm_index += 1;
                        let ptm = PostTranslationalModification {
                            name: format!("PTM {}", ptm_index),
                            amino_acid: *new_ptm_amino_acid.read(),
                            mass_delta: *new_ptm_mass.read(),
                            mod_type: new_ptm_type.read().clone(),
                            position: new_ptm_position.read().clone(),
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
                            value: "{ptm.mod_type}",
                            disabled: true,
                        }
                        input {
                            r#type: "text",
                            class: "form-control",
                            value: "{ptm.position}",
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
                    onclick: move |_| { search_coroutine.send(()) },
                    i { class: "fa-solid fa-search me-2" }
                    "Search"
                }
                if let FetchStatus::Finished(_) = &*peptides.read_unchecked() {
                    button {
                        class: "btn btn-primary",
                        r#type: "button",
                        onclick: move |_| {
                            download_coroutine.send(());
                        },
                        i { class: "fa-solid fa-download me-2" }
                        "Download"
                    }
                }
            }
        }
        match &*peptides.read_unchecked() {
            FetchStatus::None => rsx! { "" },
            FetchStatus::Loading => rsx! {
                Spinner {}
            },
            FetchStatus::Finished(peptides) => rsx! {
                PaginatedPeptideList { peptides_per_page: 100, peptides: peptides.clone() }
            },
            FetchStatus::Error(err) => rsx! {
                div { class: "alert alert-danger", "Error fetching peptides: {err}" }
            },
        }
    }
}
