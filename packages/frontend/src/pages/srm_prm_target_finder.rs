use std::{
    collections::{HashMap, HashSet},
    fmt::Write,
    rc::Rc,
};

use ::web_sys::window;
use dioxus::prelude::*;
use wasm_bindgen::{JsCast, JsValue};

use crate::{
    api_client::Client,
    components::{
        pagination::{sortable_th, PageSizeSelect, Pager},
        protein_list::{ProteinSort, ProteinSortColumn, SortDirection},
        separator_line::SeparatorLine,
        spinner::Spinner,
    },
    configuration::Configuration as AppConfiguration,
    errors::general_error::GeneralError,
    tracking::track_page_visit,
};
use macpepdb_web_common::{
    requests::{
        ptm::{PostTranslationalModificationRequest, PtmPosition, PtmType},
        tools::{ReviewStatus, SrmPrmRequest},
    },
    responses::{protein::ProteinResponse, taxonomy::TaxonomyResponse},
};

/// Default max variable modifications
const DEFAULT_MAX_VAR_MODIFICATIONS: i16 = 2;
const DEFAULT_MAX_MISSED_CLEAVAGES: i16 = 0;

/// Minimum length of the accession/gene search term before querying for suggestions.
const MIN_PROTEIN_SEARCH_TERM_LENGTH: usize = 3;

/// Rows per page by default in the protein/taxonomy selection tables.
const DEFAULT_PAGE_SIZE: usize = 10;

/// Selectable page sizes of the protein/taxonomy selection tables.
const PAGE_SIZE_OPTIONS: [usize; 4] = [10, 25, 50, 100];

/// Which proteins to consider, by review status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReviewFilter {
    SwissProt,
    TrEMBL,
    Both,
}

impl ReviewFilter {
    fn label(self) -> &'static str {
        match self {
            ReviewFilter::SwissProt => "SwissProt",
            ReviewFilter::TrEMBL => "TrEMBL",
            ReviewFilter::Both => "SwissProt + TrEMBL",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "TrEMBL" => ReviewFilter::TrEMBL,
            "SwissProt + TrEMBL" => ReviewFilter::Both,
            _ => ReviewFilter::SwissProt,
        }
    }

    fn status(self) -> ReviewStatus {
        match self {
            ReviewFilter::SwissProt => ReviewStatus::SwissProt,
            ReviewFilter::TrEMBL => ReviewStatus::TrEMBL,
            ReviewFilter::Both => ReviewStatus::Both,
        }
    }

    fn matches(self, protein: &ProteinResponse<String>) -> bool {
        match self {
            ReviewFilter::SwissProt => protein.is_reviewed,
            ReviewFilter::TrEMBL => !protein.is_reviewed,
            ReviewFilter::Both => true,
        }
    }
}

/// Columns the taxonomy selection table can be sorted by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TaxonomySortColumn {
    Id,
    ScientificName,
}

/// Returns the sorting after a click on `column`: toggles the direction if the column is
/// already active, otherwise sorts ascending by the new column.
fn toggle_taxonomy_sort(
    current: Option<(TaxonomySortColumn, SortDirection)>,
    column: TaxonomySortColumn,
) -> (TaxonomySortColumn, SortDirection) {
    match current {
        Some((active, SortDirection::Ascending)) if active == column => {
            (column, SortDirection::Descending)
        }
        _ => (column, SortDirection::Ascending),
    }
}

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

/// Mass difference between 13C and 12C in Dalton.
const C13_DELTA: f64 = 1.003_354_837_8;
/// Mass difference between 15N and 14N in Dalton.
const N15_DELTA: f64 = 0.997_034_9;

/// Heavy isotopes a residue can be labeled with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Isotope {
    CarbonNitrogen,
    Nitrogen,
    Carbon,
}

impl Isotope {
    /// UI label, used as both the `<option>` value and display text.
    fn label(self) -> &'static str {
        match self {
            Isotope::CarbonNitrogen => "13C/15N",
            Isotope::Nitrogen => "15N",
            Isotope::Carbon => "13C",
        }
    }

    /// Parses an [`Isotope`] from [`Isotope::label`], defaulting to `13C/15N`.
    fn parse(value: &str) -> Self {
        match value {
            "15N" => Isotope::Nitrogen,
            "13C" => Isotope::Carbon,
            _ => Isotope::CarbonNitrogen,
        }
    }
}

/// Where in the peptide an isotope label is applied. Unlike PTMs there are no bond positions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LabelPosition {
    Anywhere,
    NTerminus,
    CTerminus,
}

impl LabelPosition {
    /// UI label, used as both the `<option>` value and display text (same as PTM positions).
    fn label(self) -> &'static str {
        match self {
            LabelPosition::Anywhere => "Anywhere",
            LabelPosition::NTerminus => "Terminus-N",
            LabelPosition::CTerminus => "Terminus-C",
        }
    }

    /// Parses a [`LabelPosition`] from [`LabelPosition::label`], defaulting to `Anywhere`.
    fn parse(value: &str) -> Self {
        match value {
            "Terminus-N" => LabelPosition::NTerminus,
            "Terminus-C" => LabelPosition::CTerminus,
            _ => LabelPosition::Anywhere,
        }
    }
}

/// Isotope label of an amino acid. Applied to the results after the search, so it is not
/// part of the search request.
#[derive(Clone, Copy, Debug, PartialEq)]
struct IsotopeLabel {
    amino_acid: char,
    isotope: Isotope,
    position: LabelPosition,
}

/// Number of carbon and nitrogen atoms of the amino acid residue (as in a peptide chain).
fn residue_carbon_nitrogen(amino_acid: char) -> Option<(u32, u32)> {
    Some(match amino_acid {
        'G' => (2, 1),
        'A' => (3, 1),
        'S' => (3, 1),
        'P' => (5, 1),
        'V' => (5, 1),
        'T' => (4, 1),
        'C' => (3, 1),
        'L' => (6, 1),
        'I' => (6, 1),
        'N' => (4, 2),
        'D' => (4, 1),
        'Q' => (5, 2),
        'K' => (6, 2),
        'E' => (5, 1),
        'M' => (5, 1),
        'H' => (6, 3),
        'F' => (9, 1),
        'R' => (6, 4),
        'Y' => (9, 1),
        'W' => (11, 2),
        _ => return None,
    })
}

/// Mass shift in Dalton of `amino_acid` labeled with `isotope`; 0 for unknown amino acids.
fn label_mass_delta(amino_acid: char, isotope: Isotope) -> f64 {
    let Some((carbons, nitrogens)) = residue_carbon_nitrogen(amino_acid) else {
        return 0.0;
    };
    let carbon_delta = carbons as f64 * C13_DELTA;
    let nitrogen_delta = nitrogens as f64 * N15_DELTA;
    match isotope {
        Isotope::CarbonNitrogen => carbon_delta + nitrogen_delta,
        Isotope::Nitrogen => nitrogen_delta,
        Isotope::Carbon => carbon_delta,
    }
}

/// Part of a ProForma sequence: a residue with its trailing modification brackets, or leading
/// terminal modification text. `delta` is the label mass shift if the residue is labeled.
#[derive(Clone, Debug, PartialEq)]
struct SequenceSegment {
    text: String,
    delta: Option<f64>,
}

/// Splits the ProForma `sequence` into segments and resolves the label of each residue.
///
/// Every residue gets at most one label: one matching its terminal position (first residue
/// N-terminus, last residue C-terminus; N wins for single residue peptides) beats an
/// `Anywhere` label. Modifications in square brackets stay attached to the preceding residue
/// (or form an unlabeled segment of their own in front of the first residue).
fn label_segments(sequence: &str, labels: &[IsotopeLabel]) -> Vec<SequenceSegment> {
    let mut segments: Vec<SequenceSegment> = Vec::with_capacity(sequence.len());
    let mut bracket_depth = 0usize;
    for c in sequence.chars() {
        match c {
            '[' => {
                bracket_depth += 1;
                push_char(&mut segments, c, false);
            }
            ']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                push_char(&mut segments, c, false);
            }
            c if bracket_depth == 0 && c.is_ascii_uppercase() => push_char(&mut segments, c, true),
            c => push_char(&mut segments, c, false),
        }
    }

    let find = |amino_acid: char, position: LabelPosition| {
        labels
            .iter()
            .find(|label| label.amino_acid == amino_acid && label.position == position)
    };

    let residue_count = segments.iter().filter(|s| s.delta.is_some()).count();
    let mut residue_idx = 0usize;
    for segment in segments.iter_mut() {
        // residue segments are marked with `Some(0.0)` by `push_char`
        if segment.delta.is_none() {
            continue;
        }
        let amino_acid = segment.text.chars().next().unwrap_or(' ');
        let n_terminal = (residue_idx == 0)
            .then(|| find(amino_acid, LabelPosition::NTerminus))
            .flatten();
        let c_terminal = (residue_idx + 1 == residue_count)
            .then(|| find(amino_acid, LabelPosition::CTerminus))
            .flatten();
        segment.delta = n_terminal
            .or(c_terminal)
            .or_else(|| find(amino_acid, LabelPosition::Anywhere))
            .map(|label| label_mass_delta(amino_acid, label.isotope));
        residue_idx += 1;
    }
    segments
}

/// Appends `c` to the last segment, or starts a new residue segment (`residue` = true, marked
/// by `Some(0.0)`) / continues the previous one. Helper of [`label_segments`].
fn push_char(segments: &mut Vec<SequenceSegment>, c: char, residue: bool) {
    if residue {
        segments.push(SequenceSegment {
            text: c.to_string(),
            delta: Some(0.0),
        });
    } else if let Some(last) = segments.last_mut() {
        last.text.push(c);
    } else {
        segments.push(SequenceSegment {
            text: c.to_string(),
            delta: None,
        });
    }
}

/// Segments of `sequence` to display: labeled residues carry a delta (shown bold) in the
/// labeled form, the unlabeled form is one plain segment.
fn display_segments(
    sequence: &str,
    labels: &[IsotopeLabel],
    labeled: bool,
) -> Vec<SequenceSegment> {
    if labeled {
        label_segments(sequence, labels)
    } else {
        vec![SequenceSegment {
            text: sequence.to_string(),
            delta: None,
        }]
    }
}

/// Total mass shift in Dalton of the ProForma `sequence` through `labels`.
fn label_delta(sequence: &str, labels: &[IsotopeLabel]) -> f64 {
    if labels.is_empty() {
        return 0.0;
    }
    label_segments(sequence, labels)
        .iter()
        .filter_map(|segment| segment.delta)
        .sum()
}

/// One form (unlabeled or labeled) of a target, shown/exported as its own row.
#[derive(Clone, Debug, PartialEq)]
struct TargetForm {
    labeled: bool,
    mz: f64,
    /// Label mass shift in Dalton, 0 for the unlabeled form.
    delta: f64,
}

/// Forms of a target: always the unlabeled one, followed by the labeled one if any label
/// applies to the peptide.
fn target_forms(sequence: &str, mz: f64, charge: u8, labels: &[IsotopeLabel]) -> Vec<TargetForm> {
    let mut forms = vec![TargetForm {
        labeled: false,
        mz,
        delta: 0.0,
    }];
    let delta = label_delta(sequence, labels);
    if delta > 0.0 {
        forms.push(TargetForm {
            labeled: true,
            mz: mz + delta / charge.max(1) as f64,
            delta,
        });
    }
    forms
}

/// Compound name of a target form in exports: labeled forms are marked to tell them apart.
fn form_name(sequence: &str, form: &TargetForm) -> String {
    if form.labeled {
        format!("{sequence} (labeled)")
    } else {
        sequence.to_string()
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

    // targets: protein accession + charge spec (single int, comma list, or "N-M" range)
    let mut protein_search_term = use_signal(String::new);
    // term submitted by button/enter; the counter makes re-submitting the same term re-run the search
    let mut protein_search_submitted = use_signal(|| (0u32, String::new()));
    // charge spec typed into the suggestion row, keyed by accession
    let mut target_charge_specs: Signal<HashMap<String, String>> = use_signal(HashMap::new);
    let mut targets: Signal<Vec<(String, String)>> = use_signal(Vec::new);
    // species contained in each selected taxonomy (selected taxonomy ID -> species IDs); proteins are
    // matched by their species taxonomy
    let mut selected_species_ids: Signal<HashMap<i32, HashSet<i32>>> = use_signal(HashMap::new);
    let mut taxonomy_add_error = use_signal(|| None::<String>);
    // species taxonomy ID of each added target accession, to drop targets with their taxonomy
    let mut target_taxonomy_ids: Signal<HashMap<String, i32>> = use_signal(HashMap::new);

    // taxonomy filter (multi-select)
    let mut taxonomy_search_term = use_signal(|| "".to_string());
    // term submitted by button/enter; the counter makes re-submitting the same term re-run the search
    let mut taxonomy_search_submitted = use_signal(|| (0u32, String::new()));
    let mut selected_taxonomies: Signal<Vec<TaxonomyResponse>> = use_signal(Vec::new);
    let mut taxonomy_page = use_signal(|| 0usize);
    let taxonomy_page_size = use_signal(|| DEFAULT_PAGE_SIZE);
    let mut taxonomy_sort = use_signal(|| None::<(TaxonomySortColumn, SortDirection)>);

    // pagination/sorting of the protein suggestions
    let mut protein_page = use_signal(|| 0usize);
    let protein_page_size = use_signal(|| DEFAULT_PAGE_SIZE);
    let mut protein_sort = use_signal(|| None::<ProteinSort>);
    // review status of the proteins to consider: suggestions, targets and peptide uniqueness
    let mut review_filter = use_signal(|| ReviewFilter::SwissProt);

    let taxonomies: Resource<Result<Option<Vec<TaxonomyResponse>>, GeneralError>> =
        use_resource(move || async move {
            let search_term = taxonomy_search_submitted.read_unchecked().1.clone();
            if search_term.is_empty() {
                return Ok(None);
            }

            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => Err(GeneralError::ConfigurationNotLoaded)?,
            };

            let client = Client::new(macpepdb_base_url)?;

            let found = client.search_taxonomies(&search_term).await?;

            taxonomy_page.set(0);
            taxonomy_sort.set(None);

            Ok(Some(found))
        });

    let protein_suggestions: Resource<Result<Option<Vec<ProteinResponse<String>>>, GeneralError>> =
        use_resource(move || async move {
            let term = protein_search_submitted.read_unchecked().1.clone();
            if term.len() < MIN_PROTEIN_SEARCH_TERM_LENGTH {
                return Ok(None);
            }

            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => Err(GeneralError::ConfigurationNotLoaded)?,
            };

            let client = Client::new(macpepdb_base_url)?;

            let mut proteins = client.search_protein(&term).await?;

            proteins.sort_unstable_by_key(|prot| !prot.is_reviewed);

            protein_page.set(0);
            protein_sort.set(None);

            Ok(Some(proteins))
        });

    // proteins are only offered for the selected taxonomies
    let in_selected_taxonomy = move |protein: &ProteinResponse<String>| {
        selected_species_ids
            .read()
            .values()
            .any(|species_ids| species_ids.contains(&protein.taxonomy_id))
    };

    let mut submit_protein_search = move || {
        let term = protein_search_term.read().trim().to_string();
        if term.len() >= MIN_PROTEIN_SEARCH_TERM_LENGTH {
            let counter = protein_search_submitted.read().0.wrapping_add(1);
            protein_search_submitted.set((counter, term));
        }
    };
    let mut submit_taxonomy_search = move || {
        let term = taxonomy_search_term.read().trim().to_string();
        if !term.is_empty() {
            let counter = taxonomy_search_submitted.read().0.wrapping_add(1);
            taxonomy_search_submitted.set((counter, term));
        }
    };

    // post translational modifications
    let mut max_var_modifications = use_signal(|| DEFAULT_MAX_VAR_MODIFICATIONS);
    let mut max_missed_cleavages = use_signal(|| DEFAULT_MAX_MISSED_CLEAVAGES);
    let mut new_ptm_amino_acid = use_signal(|| ' ');
    let mut new_ptm_mass = use_signal(|| 0.0);
    let mut new_ptm_type = use_signal(|| PtmType::Static);
    let mut new_ptm_position = use_signal(|| PtmPosition::Anywhere);
    let mut ptm_index = use_signal(|| 0); // Just to have something to use as name
    let mut ptms: Signal<Vec<PostTranslationalModificationRequest>> = use_signal(Vec::new);
    // isotope labels, applied to the results after the search
    let mut new_label_amino_acid = use_signal(|| ' ');
    let mut new_label_isotope = use_signal(|| Isotope::CarbonNitrogen);
    let mut new_label_position = use_signal(|| LabelPosition::Anywhere);
    let mut isotope_labels: Signal<Vec<IsotopeLabel>> = use_signal(Vec::new);
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

    // indices of results rows the user removed from the table before export; reset on new search
    let mut removed_target_indices: Signal<HashSet<usize>> = use_signal(HashSet::new);

    // search for SRM/PRM targets
    let mut search = use_action(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Err(GeneralError::ConfigurationNotLoaded),
        };
        let client = Client::new(macpepdb_base_url)?;

        let request = SrmPrmRequest {
            targets: targets.read_unchecked().clone(),
            max_variable_modifications: max_var_modifications.read_unchecked().max(0) as usize,
            ptms: ptms.read_unchecked().clone(),
            taxonomies: selected_taxonomies
                .read_unchecked()
                .iter()
                .map(|taxonomy| taxonomy.id)
                .collect(),
            max_missed_cleavages: max_missed_cleavages.read_unchecked().max(0) as usize,
            review_status: review_filter.read_unchecked().status(),
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
        let labels = isotope_labels.read_unchecked().clone();

        let tsv = match vendor {
            MsVendor::ThermoFisher => {
                let mut tsv = String::from(
                    "Compound\tMass [m/z]\tFormula [M]\tSpecies\tCS [z]\tStart [min]\tEnd [min]\tNCE\tAccession\n",
                );
                for (idx, target) in results.read_unchecked().iter().enumerate() {
                    if removed_target_indices.read_unchecked().contains(&idx) {
                        continue;
                    }
                    for form in target_forms(&target.sequence, target.mz, target.charge, &labels) {
                        writeln!(
                            tsv,
                            "{}\t{}\t\t{} ({})\t{}\t\t\t{}\t{}",
                            form_name(&target.sequence, &form),
                            form.mz,
                            taxonomy_names
                                .get(&target.taxonomy_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            target.taxonomy_id,
                            target.charge,
                            normalized_collision_energy,
                            target.accession,
                        )
                        .unwrap();
                    }
                }
                tsv
            }
            MsVendor::Bruker => {
                let mut tsv = String::from(
                    "Mass [m/z]\tCharge\tIsolation Width [m/z]\tRT [s]\tRT Range [s]\tStart IM [1/K0]\tEnd IM [1/K0]\tCE [eV]\tExternal ID\tDescription\tAccession\n"
                );
                for (idx, target) in results.read_unchecked().iter().enumerate() {
                    if removed_target_indices.read_unchecked().contains(&idx) {
                        continue;
                    }
                    for form in target_forms(&target.sequence, target.mz, target.charge, &labels) {
                        writeln!(
                            tsv,
                            "{}\t{}\t\t\t\t\t\t{}\t{}\tSpecies: {} ({})\t{}",
                            form.mz,
                            target.charge,
                            normalized_collision_energy,
                            form_name(&target.sequence, &form),
                            taxonomy_names
                                .get(&target.taxonomy_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            target.taxonomy_id,
                            target.accession,
                        )
                        .unwrap();
                    }
                }
                tsv
            }
        };

        trigger_tsv_download("macpepdb_srm_prm_targets.tsv", &tsv);
    };

    rsx! {
        h1 { "SRM / PRM target finder" }
        p {
            "Finds peptides of the given proteins which are unique for the selected taxonomies. \
            First select the taxonomies (any rank; all contained taxonomies down to strains and isolates are included), then the target proteins and their charges. \
            Proteins can only be selected for the selected taxonomies; if a taxonomy is removed, its targets are removed as well. \
            Uniqueness is only judged within the selected taxonomies, sharing with organisms outside the selection is ignored."
        }
        h2 { class: "h5", "Uniqueness rules" }
        ul {
            li { "A peptide is a target only if it occurs in exactly one of the selected taxonomies; occurring in two or more selected taxonomies removes it." }
            li { "Strains, isolates and other sub-taxonomies of one species count as that species: a peptide shared by several strains of the same species stays a target, as long as each strain contributes only a single protein. Without a species in the selected subtree (e.g. a `no rank` node like SARS-CoV-2), the selected taxonomy itself counts as one." }
            li { "Within that taxonomy the peptide must stem from a single protein. A protein and its isoforms (accession suffix `-N`) count as one protein." }
            li { "A peptide shared with another protein of the same taxonomy is removed." }
            li { "Sharing with organisms outside the selected taxonomies is ignored." }
            li { "Only proteins with the selected review status (SwissProt, TrEMBL or both) are considered: targets must match it and sharing with proteins of another review status is ignored. A peptide can therefore be unique among SwissProt proteins but not among SwissProt + TrEMBL." }
            li { "Repeats of the peptide within one protein do not affect uniqueness." }
            li { "Uniqueness is defined by the initial input data of the database (loaded UniProt files, protease, missed cleavages (check start page))." }
            li { "Highlighted rows: another target has a similar m/z at the same charge, e.g. due to PTMs." }
        }

        SeparatorLine { label: "Taxonomies of interest" }
        div { class: "input-group mb-3",
            span { class: "input-group-text", "Taxonomy search *" }
            input {
                r#type: "text",
                class: "form-control",
                value: "{taxonomy_search_term}",
                oninput: move |evt| { taxonomy_search_term.set(evt.value()) },
                onkeydown: move |evt| {
                    if evt.key() == Key::Enter {
                        submit_taxonomy_search();
                    }
                },
            }
            button {
                class: "btn btn-primary",
                r#type: "button",
                disabled: taxonomy_search_term.read().trim().is_empty() || taxonomies.pending(),
                onclick: move |_| submit_taxonomy_search(),
                i { class: "fa-solid fa-search me-2" }
                "Search"
            }
        }
        if let Some(err) = taxonomy_add_error() {
            div { class: "alert alert-danger", "{err}" }
        }
        if taxonomies.pending() && !taxonomy_search_submitted.read().1.is_empty() {
            div { class: "mb-3",
                Spinner {}
            }
        } else {
            match &*taxonomies.read_unchecked() {
            Some(Ok(Some(found_taxonomies))) => {
                let mut sorted_taxonomies = found_taxonomies.clone();
                if let Some((column, direction)) = taxonomy_sort() {
                    sorted_taxonomies.sort_by(|a, b| {
                        let ordering = match column {
                            TaxonomySortColumn::Id => a.id.cmp(&b.id),
                            TaxonomySortColumn::ScientificName => a
                                .scientific_name
                                .to_lowercase()
                                .cmp(&b.scientific_name.to_lowercase()),
                        };
                        match direction {
                            SortDirection::Ascending => ordering,
                            SortDirection::Descending => ordering.reverse(),
                        }
                    });
                }
                let total = sorted_taxonomies.len();
                let size = taxonomy_page_size();
                let page_count = total.div_ceil(size).max(1);
                let current_page = taxonomy_page().min(page_count - 1);
                let page_taxonomies: Vec<TaxonomyResponse> = sorted_taxonomies
                    .into_iter()
                    .skip(current_page * size)
                    .take(size)
                    .collect();
                let direction_of = move |column: TaxonomySortColumn| {
                    taxonomy_sort().filter(|(c, _)| *c == column).map(|(_, d)| d)
                };
                rsx! {
                    if total > 0 {
                        div { class: "d-flex align-items-center justify-content-between mb-3",
                            span { "{total} taxonomies found" }
                            PageSizeSelect {
                                id: "taxonomy-page-size",
                                label: "Taxonomies per page",
                                page_size: taxonomy_page_size,
                                page: taxonomy_page,
                                options: PAGE_SIZE_OPTIONS.to_vec(),
                            }
                        }
                    }
                    table { class: "table table-striped table-hover",
                    thead {
                        tr {
                            {sortable_th("ID", direction_of(TaxonomySortColumn::Id), EventHandler::new(move |_| {
                                taxonomy_sort.set(Some(toggle_taxonomy_sort(taxonomy_sort(), TaxonomySortColumn::Id)));
                                taxonomy_page.set(0);
                            }))}
                            {sortable_th("Scientific name", direction_of(TaxonomySortColumn::ScientificName), EventHandler::new(move |_| {
                                taxonomy_sort.set(Some(toggle_taxonomy_sort(taxonomy_sort(), TaxonomySortColumn::ScientificName)));
                                taxonomy_page.set(0);
                            }))}
                            th { "Rank" }
                            th { "Select" }
                        }
                    }
                    tbody {
                        for taxonomy in page_taxonomies {
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
                                            if selected_taxonomies.read().iter().any(|t| t.id == taxonomy.id) {
                                                return;
                                            }
                                            let taxonomy = taxonomy.clone();
                                            spawn(async move {
                                                let base_url = app_config
                                                    .read_unchecked()
                                                    .as_ref()
                                                    .map(|config| config.get_macpepdb_base_url().to_string());
                                                let Some(base_url) = base_url else {
                                                    taxonomy_add_error.set(Some(GeneralError::ConfigurationNotLoaded.to_string()));
                                                    return;
                                                };
                                                let species = match Client::new(&base_url) {
                                                    Ok(client) => client.get_sub_species(taxonomy.id).await.map_err(GeneralError::from),
                                                    Err(err) => Err(GeneralError::from(err)),
                                                };
                                                match species {
                                                    Ok(species) => {
                                                        taxonomy_add_error.set(None);
                                                        selected_species_ids.write().insert(
                                                            taxonomy.id,
                                                            species.iter().map(|species| species.id).collect(),
                                                        );
                                                        selected_taxonomies.write().push(taxonomy);
                                                    }
                                                    Err(err) => taxonomy_add_error.set(Some(format!("Error resolving species of {}: {err}", taxonomy.scientific_name))),
                                                }
                                            });
                                        },
                                        "Add taxonomy"
                                    }
                                }
                            }
                        }
                    }
                    }
                    Pager { page: taxonomy_page, page_count }
                }
            }
            Some(Ok(None)) => rsx! {
                div {}
            },
            Some(Err(err)) => rsx! {
                div { "Error fetching taxonomies: {err}" }
            },
            None => rsx! {
                Spinner {}
            },
            }
        }
        div { class: "list-group",
            for taxonomy in selected_taxonomies.iter().map(|t| t.clone()) {
                div { class: "list-group-item d-flex justify-content-between align-items-center",
                    "{taxonomy.scientific_name} (ID: {taxonomy.id}, Rank: {taxonomy.rank_name.clone().unwrap_or_default()})"
                    button {
                        class: "btn btn-danger",
                        r#type: "button",
                        onclick: move |_| {
                            selected_taxonomies.write().retain(|t| t.id != taxonomy.id);
                            selected_species_ids.write().remove(&taxonomy.id);
                            // drop targets which are not covered by any remaining taxonomy
                            let selected_species_ids = selected_species_ids.read();
                            let target_taxonomy_ids = target_taxonomy_ids.read();
                            targets.write().retain(|(accession, _)| {
                                target_taxonomy_ids.get(accession).is_some_and(|species_id| {
                                    selected_species_ids.values().any(|ids| ids.contains(species_id))
                                })
                            });
                        },
                        i { class: "fa-solid fa-xmark" }
                    }
                }
            }
        }

        SeparatorLine { label: "Review status" }
        div { class: "input-group mb-1",
            span { class: "input-group-text", "Review status to consider" }
            select {
                class: "form-select",
                value: "{review_filter().label()}",
                onchange: move |evt| {
                    review_filter.set(ReviewFilter::parse(&evt.value()));
                },
                for filter in [ReviewFilter::SwissProt, ReviewFilter::TrEMBL, ReviewFilter::Both] {
                    option {
                        value: filter.label(),
                        selected: filter == review_filter(),
                        "{filter.label()}"
                    }
                }
            }
        }
        small { class: "text-muted px-2 mb-3", "Affects the protein search and the peptide uniqueness." }

        SeparatorLine { label: "Targets (protein accession + charge)" }
        if selected_taxonomies.is_empty() {
            div { class: "alert alert-warning", "Select at least one taxonomy first." }
        }
        div { class: "input-group mb-3",
            span { class: "input-group-text", "Protein search *" }
            input {
                r#type: "text",
                class: "form-control",
                placeholder: "Search by accession or gene name (at least 3 characters)",
                value: "{protein_search_term}",
                disabled: selected_taxonomies.is_empty(),
                oninput: move |evt| protein_search_term.set(evt.value()),
                onkeydown: move |evt| {
                    if evt.key() == Key::Enter {
                        submit_protein_search();
                    }
                },
            }
            button {
                class: "btn btn-primary",
                r#type: "button",
                disabled: selected_taxonomies.is_empty()
                    || protein_search_term.read().trim().len() < MIN_PROTEIN_SEARCH_TERM_LENGTH
                    || protein_suggestions.pending(),
                onclick: move |_| submit_protein_search(),
                i { class: "fa-solid fa-search me-2" }
                "Search"
            }
        }
        if protein_suggestions.pending()
            && protein_search_submitted.read().1.len() >= MIN_PROTEIN_SEARCH_TERM_LENGTH
        {
            div { class: "mb-3",
                Spinner {}
            }
        } else {
            match &*protein_suggestions.read_unchecked() {
            Some(Ok(Some(found_proteins)))
                if found_proteins.iter().any(|protein| review_filter().matches(protein) && in_selected_taxonomy(protein)) =>
            {
                let filter = review_filter();
                let mut sorted_proteins: Vec<ProteinResponse<String>> = found_proteins
                    .iter()
                    .filter(|protein| filter.matches(protein) && in_selected_taxonomy(protein))
                    .cloned()
                    .collect();
                if let Some(sort) = protein_sort() {
                    sort.sort(&mut sorted_proteins, &HashMap::new());
                }
                let total = sorted_proteins.len();
                let size = protein_page_size();
                let page_count = total.div_ceil(size).max(1);
                let current_page = protein_page().min(page_count - 1);
                let page_proteins: Vec<ProteinResponse<String>> = sorted_proteins
                    .into_iter()
                    .skip(current_page * size)
                    .take(size)
                    .collect();
                let direction_of = move |column: ProteinSortColumn| {
                    protein_sort()
                        .filter(|sort| sort.column == column)
                        .map(|sort| sort.direction)
                };
                rsx! {
                    div { class: "d-flex align-items-center justify-content-between mb-3",
                        span { "{total} proteins found" }
                        PageSizeSelect {
                            id: "protein-target-page-size",
                            label: "Proteins per page",
                            page_size: protein_page_size,
                            page: protein_page,
                            options: PAGE_SIZE_OPTIONS.to_vec(),
                        }
                    }
                    table { class: "table table-sm table-striped table-hover mb-3",
                    thead {
                        tr {
                            {sortable_th("Accession", direction_of(ProteinSortColumn::Accession), EventHandler::new(move |_| {
                                protein_sort.set(Some(ProteinSort::toggled(protein_sort(), ProteinSortColumn::Accession)));
                                protein_page.set(0);
                            }))}
                            {sortable_th("Genes", direction_of(ProteinSortColumn::Genes), EventHandler::new(move |_| {
                                protein_sort.set(Some(ProteinSort::toggled(protein_sort(), ProteinSortColumn::Genes)));
                                protein_page.set(0);
                            }))}
                            th { "Reviewed" }
                            th { "Charge state(s)" }
                        }
                    }
                    tbody {
                        for protein in page_proteins {
                            tr {
                                td { "{protein.accession}" }
                                td { "{protein.genes.join(\", \")}" }
                                td { if protein.is_reviewed { "SwissProt" } else { "TrEMBL" } }
                                td {
                                    {
                                        let accession = protein.accession.clone();
                                        let protein_taxonomy_id = protein.taxonomy_id;
                                        let charge_spec = target_charge_specs
                                            .read()
                                            .get(&accession)
                                            .cloned()
                                            .unwrap_or_default();
                                        let already_added = targets.read().iter().any(|(a, c)| a == &accession && c == charge_spec.trim());
                                        let input_accession = accession.clone();
                                        rsx! {
                                            div { class: "input-group input-group-sm",
                                                input {
                                                    r#type: "text",
                                                    class: "form-control",
                                                    style: "min-width: 7rem",
                                                    placeholder: "Charge, e.g. 2 or 2,3 or 2-4",
                                                    value: "{charge_spec}",
                                                    oninput: move |evt| {
                                                        target_charge_specs.write().insert(input_accession.clone(), evt.value());
                                                    },
                                                }
                                                button {
                                                    class: "btn btn-primary",
                                                    r#type: "button",
                                                    disabled: charge_spec.trim().is_empty() || already_added,
                                                    onclick: move |_| {
                                                        target_taxonomy_ids.write().insert(accession.clone(), protein_taxonomy_id);
                                                        targets.push((accession.clone(), charge_spec.trim().to_string()));
                                                    },
                                                    "Add target"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    }
                    Pager { page: protein_page, page_count }
                }
            }
            Some(Ok(Some(found_proteins))) if !found_proteins.is_empty() => rsx! {
                div { class: "alert alert-info mb-3",
                    "{found_proteins.len()} proteins found, but none are {review_filter().label()} and part of the selected taxonomies. Change the filter or taxonomies and search again."
                }
            },
            Some(Err(err)) => rsx! {
                div { class: "alert alert-danger mb-3", "Error searching for proteins: {err}" }
            },
            _ => rsx! {},
            }
        }

        div { class: "list-group",
            if targets.is_empty() {
                div { class: "list-group-item list-group-item-warning", "No targets added yet." }
            }
            for (idx , target) in targets.iter().enumerate() {
                div { class: "list-group-item d-flex justify-content-between align-items-center",
                    "{target.0}, charge {target.1}"
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
            SeparatorLine { label: "Digestion" }
            div { class: "input-group mb-3",
                span { class: "input-group-text", "Max missed cleavages" }
                input {
                    r#type: "number",
                    class: "form-control",
                    min: 0,
                    step: 1,
                    value: "{max_missed_cleavages}",
                    oninput: move |evt| {
                        max_missed_cleavages
                            .set(evt.value().parse().unwrap_or(DEFAULT_MAX_MISSED_CLEAVAGES))
                    },
                }
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

            SeparatorLine { label: "Isotope labeling" }
            p { class: "text-muted",
                "Labels are applied to the found targets afterwards; they are not part of the search. \
                Peptides with labels get an additional labeled row (labeled residues in bold) next to the unlabeled one; both are included in the downloads. \
                Per residue only one label is applied, a terminal label wins over `Anywhere`. \
                The highlighting of similar m/z does not consider labels."
            }
            div { class: "input-group mb-3",
                select {
                    class: "form-control",
                    oninput: move |evt| {
                        new_label_amino_acid.set(evt.value().parse().unwrap_or(' '));
                    },
                    option { value: " ", "Select amino acid" }
                    match &*amino_acids.read_unchecked() {
                        Some(Ok(Some(amino_acids))) => rsx! {
                            for aa in amino_acids.iter().filter(|aa| residue_carbon_nitrogen(aa.code).is_some()) {
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
                select {
                    class: "form-control",
                    oninput: move |evt| {
                        new_label_isotope.set(Isotope::parse(&evt.value()));
                    },
                    option { value: Isotope::CarbonNitrogen.label(), "{Isotope::CarbonNitrogen.label()}" }
                    option { value: Isotope::Nitrogen.label(), "{Isotope::Nitrogen.label()}" }
                    option { value: Isotope::Carbon.label(), "{Isotope::Carbon.label()}" }
                }
                select {
                    class: "form-control",
                    oninput: move |evt| {
                        new_label_position.set(LabelPosition::parse(&evt.value()));
                    },
                    option { value: LabelPosition::Anywhere.label(), "{LabelPosition::Anywhere.label()}" }
                    option { value: LabelPosition::NTerminus.label(), "{LabelPosition::NTerminus.label()}" }
                    option { value: LabelPosition::CTerminus.label(), "{LabelPosition::CTerminus.label()}" }
                }
                button {
                    class: "btn btn-primary",
                    r#type: "button",
                    disabled: *new_label_amino_acid.read() == ' ',
                    onclick: move |_| {
                        let label = IsotopeLabel {
                            amino_acid: *new_label_amino_acid.read(),
                            isotope: *new_label_isotope.read(),
                            position: *new_label_position.read(),
                        };
                        // one label per amino acid and position: replace instead of stacking
                        isotope_labels
                            .write()
                            .retain(|existing| {
                                existing.amino_acid != label.amino_acid
                                    || existing.position != label.position
                            });
                        isotope_labels.push(label);
                    },
                    "Add label"
                }
            }
            div { class: "list-input-group",
                for (idx , label) in isotope_labels.iter().enumerate() {
                    div { class: "input-group",
                        input {
                            r#type: "text",
                            class: "form-control",
                            value: "{label.amino_acid}",
                            disabled: true,
                        }
                        input {
                            r#type: "text",
                            class: "form-control",
                            value: "{label.isotope.label()}",
                            disabled: true,
                        }
                        input {
                            r#type: "text",
                            class: "form-control",
                            value: "{label.position.label()}",
                            disabled: true,
                        }
                        input {
                            r#type: "text",
                            class: "form-control",
                            value: "+{label_mass_delta(label.amino_acid, label.isotope):.5}",
                            disabled: true,
                        }
                        button {
                            class: "btn btn-danger",
                            r#type: "button",
                            onclick: move |_| {
                                isotope_labels.remove(idx);
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
                    onclick: move |_| {
                        removed_target_indices.write().clear();
                        search.call();
                    },
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
                if results.read_unchecked().iter().any(|target| target.similar_mz) {
                    div { class: "alert alert-warning mt-3",
                        "Highlighted rows: another target has a similar m/z at the same charge (e.g. due to PTMs)."
                    }
                }
                table { class: "table table-striped table-hover",
                    thead {
                        tr {
                            th { "Sequence" }
                            th { "Accession" }
                            th { "m/z" }
                            if !isotope_labels.read().is_empty() {
                                th { "Label Δ" }
                            }
                            th { "Charge" }
                            th { "Hydrophobicity" }
                            th { "Taxonomy" }
                            th { "" }
                        }
                    }
                    tbody {
                        for (idx , target) in results.read_unchecked().iter().cloned().enumerate() {
                            if !removed_target_indices.read().contains(&idx) {
                                for form in target_forms(&target.sequence, target.mz, target.charge, &isotope_labels.read()) {
                                    tr { class: if target.similar_mz { "table-warning" } else { "" },
                                        td {
                                            for segment in display_segments(&target.sequence, &isotope_labels.read(), form.labeled) {
                                                if segment.delta.is_some() {
                                                    b { "{segment.text}" }
                                                } else {
                                                    "{segment.text}"
                                                }
                                            }
                                        }
                                        td { "{target.accession}" }
                                        td { "{form.mz}" }
                                        if !isotope_labels.read().is_empty() {
                                            td {
                                                if form.labeled {
                                                    "+{form.delta:.5}"
                                                }
                                            }
                                        }
                                        td { "{target.charge}" }
                                        td { "{target.hydrophobicity:.3}" }
                                        td {
                                            "{taxonomy_names.read().get(&target.taxonomy_id).cloned().unwrap_or_else(|| target.taxonomy_id.to_string())}"
                                        }
                                        td {
                                            button {
                                                class: "btn btn-danger",
                                                r#type: "button",
                                                onclick: move |_| {
                                                    removed_target_indices.write().insert(idx);
                                                },
                                                i { class: "fa-solid fa-xmark" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                {
                    let total = results.read_unchecked().len();
                    let remaining = total - removed_target_indices.read().len();
                    rsx! {
                        if total == 0 {
                            div { class: "alert alert-info",
                                "No peptides match (or the database was built without peptide taxonomy metadata)."
                            }
                        } else if remaining == 0 {
                            div { class: "alert alert-info", "All targets removed." }
                        }
                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn label(amino_acid: char, isotope: Isotope, position: LabelPosition) -> IsotopeLabel {
        IsotopeLabel {
            amino_acid,
            isotope,
            position,
        }
    }

    #[test]
    fn test_label_mass_delta() {
        // K: 6 C, 2 N
        assert!(
            (label_mass_delta('K', Isotope::CarbonNitrogen) - (6.0 * C13_DELTA + 2.0 * N15_DELTA))
                .abs()
                < 1e-9
        );
        assert!((label_mass_delta('K', Isotope::Nitrogen) - 2.0 * N15_DELTA).abs() < 1e-9);
        assert!((label_mass_delta('K', Isotope::Carbon) - 6.0 * C13_DELTA).abs() < 1e-9);
        assert_eq!(label_mass_delta('X', Isotope::Carbon), 0.0);
    }

    #[test]
    fn test_label_delta_anywhere_ignores_modifications() {
        let labels = [label('K', Isotope::Carbon, LabelPosition::Anywhere)];
        let expected = 2.0 * 6.0 * C13_DELTA;
        assert!((label_delta("AKC[+57.021464]K", &labels) - expected).abs() < 1e-9);
        assert_eq!(label_delta("AKK", &[]), 0.0);
    }

    #[test]
    fn test_label_delta_terminal() {
        let labels = [label('K', Isotope::Nitrogen, LabelPosition::CTerminus)];
        assert!((label_delta("AKAK", &labels) - 2.0 * N15_DELTA).abs() < 1e-9);
        assert_eq!(label_delta("AKAR", &labels), 0.0);

        let labels = [label('A', Isotope::Nitrogen, LabelPosition::NTerminus)];
        assert!((label_delta("AAK", &labels) - N15_DELTA).abs() < 1e-9);
        assert_eq!(label_delta("KAA", &labels), 0.0);
    }

    #[test]
    fn test_label_delta_terminal_beats_anywhere() {
        let labels = [
            label('K', Isotope::Carbon, LabelPosition::Anywhere),
            label('K', Isotope::CarbonNitrogen, LabelPosition::CTerminus),
        ];
        let expected =
            label_mass_delta('K', Isotope::Carbon) + label_mass_delta('K', Isotope::CarbonNitrogen);
        assert!((label_delta("KAK", &labels) - (expected)).abs() < 1e-9);
    }

    #[test]
    fn test_label_segments_marks_labeled_residues() {
        let labels = [label('K', Isotope::Carbon, LabelPosition::Anywhere)];
        let segments = label_segments("[+42.0]-AC[+57.021464]K", &labels);
        let texts: Vec<_> = segments.iter().map(|s| s.text.as_str()).collect();
        assert_eq!(texts, ["[+42.0]-", "A", "C[+57.021464]", "K"]);
        let labeled: Vec<_> = segments.iter().map(|s| s.delta.is_some()).collect();
        assert_eq!(labeled, [false, false, false, true]);
    }

    #[test]
    fn test_target_forms() {
        let labels = [label('K', Isotope::Carbon, LabelPosition::Anywhere)];
        let forms = target_forms("AK", 500.0, 2, &labels);
        assert_eq!(forms.len(), 2);
        assert!(!forms[0].labeled);
        assert_eq!(forms[0].mz, 500.0);
        assert!(forms[1].labeled);
        assert!((forms[1].mz - (500.0 + 6.0 * C13_DELTA / 2.0)).abs() < 1e-9);
        // no label applies -> only unlabeled form
        assert_eq!(target_forms("AR", 500.0, 2, &labels).len(), 1);
        assert_eq!(target_forms("AK", 500.0, 2, &[]).len(), 1);
    }
}
