// 3rd party imports
use dioxus::prelude::*;

/// Properties for peptide page
#[derive(Clone, PartialEq, Props)]
pub struct RoundedMassProps {
    /// Mass
    pub mass: f64,
}

/// Many users requested rounded masses as 9 decimal places are too much. This component
/// renders the mass rounded to 4 places and shows the full mass on hover. Double-clicking
/// toggles the displayed value to the full-precision mass.
///
pub fn RoundedMass(props: RoundedMassProps) -> Element {
    let mut show_full_mass = use_signal(|| false);

    let displayed_mass = if *show_full_mass.read() {
        format!("{}", props.mass)
    } else {
        format!("{:.4}", props.mass)
    };

    rsx! {
        span {
            "data-toggle": "tooltip",
            "data-placement": "top",
            "title": "Double click to display mass in full-precision.",
            ondoubleclick: move |_| show_full_mass.toggle(),
            "{displayed_mass}"
        }
    }
}
