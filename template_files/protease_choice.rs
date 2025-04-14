use std::str::FromStr;

use anyhow::Error;
use clap::builder::PossibleValue;
use clap::ValueEnum;
use dihardts_omicstools::proteomics::proteases::functions::ALL as ALL_PROTEASES;

/// Wrapper for supported Omicstools proteases, to make them available as choices for the CLI
/// 
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProteaseChoice {
    <<VARIANTS>>
}

impl ProteaseChoice {
    pub fn to_str(&self) -> &'static str {
        match self {
            <<TO_STR>>
        }
    }
}

impl FromStr for ProteaseChoice {
    type Err = Error;

    // Required method
    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            <<FROM_STR>>
            _ => bail!("Unknown protease: {}", s),
        }
    }
}

pub const PROTEASE_VARIANTS: &[ProteaseChoice; <<VARIANTS_LEN>>] = &[
    <<VARIANTS_WITH_ENUM_PREFIX>>
];

impl std::fmt::Display for ProteaseChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

impl ValueEnum for ProteaseChoice {
    fn value_variants<'a>() -> &'a [Self] {
        PROTEASE_VARIANTS
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        Some(PossibleValue::new(self.to_str()))
    }
}
