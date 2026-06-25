use std::{fmt::Display, str::FromStr};

use serde::Serialize;

/// Supported post translational modification types
///
#[derive(Clone, Serialize)]
pub enum PtmType {
    Static,
    Variable,
}

impl Display for PtmType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PtmType::Static => write!(f, "Static"),
            PtmType::Variable => write!(f, "Variable"),
        }
    }
}

impl FromStr for PtmType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "static" => Ok(PtmType::Static),
            "variable" => Ok(PtmType::Variable),
            _ => Err(()),
        }
    }
}

/// Supported post translational modification positions
///
#[derive(Clone, Serialize)]
pub enum PtmPosition {
    Anywhere,
    NTerminus,
    CTerminus,
    NBond,
    CBond,
}

impl Display for PtmPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PtmPosition::Anywhere => write!(f, "Anywhere"),
            PtmPosition::NTerminus => write!(f, "Terminus-N"),
            PtmPosition::CTerminus => write!(f, "Terminus-C"),
            PtmPosition::NBond => write!(f, "Bond-N"),
            PtmPosition::CBond => write!(f, "Bond-C"),
        }
    }
}

impl FromStr for PtmPosition {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "anywhere" => Ok(PtmPosition::Anywhere),
            "terminus-n" => Ok(PtmPosition::NTerminus),
            "terminus-c" => Ok(PtmPosition::CTerminus),
            "bond-n" => Ok(PtmPosition::NBond),
            "bond-c" => Ok(PtmPosition::CBond),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Serialize)]
pub struct PostTranslationalModification {
    pub name: String,
    pub amino_acid: char,
    pub mass_delta: f64,
    pub mod_type: PtmType,
    pub position: PtmPosition,
}
