// 3rd party imports
use dioxus::prelude::*;

/// Properties for peptide page
#[derive(Clone, PartialEq, Props)]
pub struct RoundedMassProps {
    /// Mass
    pub mass: f64,
}

/// Many users requested rounded masses as 9 decimal places are too much. This component
/// renders the mass rounded to 4 places and shows the full mass on hover.
///
pub fn RoundedMass(props: RoundedMassProps) -> Element {
    let rounded_mass = format!("{:.4}", props.mass);

    rsx! {
        span {
            "data-toggle": "tooltip",
            "data-placement": "top",
            "title": "{props.mass}",
            "{rounded_mass}"
        }
    }
}
