use std::{fmt::Display, str::FromStr};

/// Supported mass units for the search
///
#[derive(Clone, PartialEq)]
pub enum MassUnit {
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
