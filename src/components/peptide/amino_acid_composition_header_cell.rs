// std imports
use std::{collections::HashMap, rc::Rc};

// 3rd party imports
use dioxus::prelude::*;

// internal imports
use crate::entities::amino_acid::AminoAcid;

/// Properties for mass composition table thead component
///
#[derive(Clone, PartialEq, Props)]
pub struct AminoAcidCompositionHeaderCellProps {
    /// Index of amino acid composition
    pub index: usize,
    /// Peptide sequence to fetch
    pub amino_acid_map: Rc<HashMap<char, AminoAcid>>,
}

/// Renders the table header cell for an amino acid in the amino acid composition table
/// Shown is the one letter code of the amino acid to safe space but it is also rendered with a tooltip on hover
///
pub fn AminoAcidCompositionHeaderCell(props: AminoAcidCompositionHeaderCellProps) -> Element {
    let code = (props.index as u8 + 65) as char;
    let amino_acid = props.amino_acid_map.get(&code).unwrap();
    let rounded_amino_acid_mass = format!("{:.4}", amino_acid.get_mono_mass());

    rsx! {
        th {
            "dataToggle": "tooltip",
            "dataPlacement": "top",
            "title": "{amino_acid.get_name()} - {amino_acid.get_abbreviation()} - {rounded_amino_acid_mass} Da",
            "{code}"
        }
    }
}
