// std imports
use std::{collections::HashSet, fmt::Display, hash::Hash};

// 3rd party imports
use dihardts_omicstools::proteomics::{
    peptide::Terminus,
    post_translational_modifications::{
        ModificationType, Position,
        PostTranslationalModification as OmicstoolsPostTranslationalModification,
    },
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    amino_acid::AminoAcid,
    mass::{to_float as mass_to_float, to_int as mass_to_int},
};

/// Errors which might occur during PTM collection validation
#[derive(Debug, Error)]
pub enum Error {
    #[error("Amino acid {0} is statically modified twice or more.")]
    StaticallyModifiedTwiceOrMore(String),
    #[error("Amino acid {0} is already statically modified.")]
    AlreadyStaticallyModified(String),
    #[error("Amino acid error in PTM: {0}")]
    AminoAcid(#[from] crate::amino_acid::Error),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostTranslationalModification {
    name: String,
    #[serde(with = "amino_acid_serde")]
    amino_acid: &'static AminoAcid,
    #[serde(with = "mass_delta_serde")]
    mass_delta: i64,
    #[serde(skip)]
    total_mono_mass: i64,
    mod_type: ModificationType,
    position: Position,
}

impl PostTranslationalModification {
    pub fn new(
        name: impl Into<String>,
        amino_acid: &'static AminoAcid,
        mass_delta: i64,
        mod_type: ModificationType,
        position: Position,
    ) -> Self {
        Self {
            amino_acid,
            mass_delta,
            mod_type,
            position,
            name: name.into(),
            total_mono_mass: mass_delta + amino_acid.mono_mass(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn amino_acid(&self) -> &'static AminoAcid {
        self.amino_acid
    }

    pub fn mass_delta(&self) -> i64 {
        self.mass_delta
    }

    pub fn total_mono_mass(&self) -> i64 {
        self.total_mono_mass
    }

    pub fn mod_type(&self) -> ModificationType {
        self.mod_type.clone()
    }

    pub fn position(&self) -> Position {
        self.position.clone()
    }

    /// Returns true if the modification is static
    ///
    pub fn is_static(&self) -> bool {
        self.mod_type == ModificationType::Static
    }

    /// Returns true if the modification is variable
    ///
    pub fn is_variable(&self) -> bool {
        self.mod_type == ModificationType::Variable
    }

    /// Returns true if the modification is a terminus modification
    ///
    pub fn is_terminus(&self) -> bool {
        matches!(self.position, Position::Terminus(_))
    }

    /// Returns true if the modification is a terminus modification
    ///
    pub fn is_n_terminus(&self) -> bool {
        self.position == Position::Terminus(Terminus::N)
    }

    /// Returns true if the modification is a terminus modification
    pub fn is_c_terminus(&self) -> bool {
        self.position == Position::Terminus(Terminus::C)
    }

    /// Returns true if the modification is a bond modification
    ///
    pub fn is_bond(&self) -> bool {
        matches!(self.position, Position::Bond(_))
    }

    /// Returns true if the modification is a N terminus bond modification
    ///
    pub fn is_n_bond(&self) -> bool {
        self.position == Position::Bond(Terminus::N)
    }

    /// Returns true if the modification is a C terminus bond modification
    pub fn is_c_bond(&self) -> bool {
        self.position == Position::Bond(Terminus::C)
    }

    /// Returns true if the modification is a terminus modification
    ///
    pub fn is_anywhere(&self) -> bool {
        self.position == Position::Anywhere
    }
}

impl AsRef<PostTranslationalModification> for PostTranslationalModification {
    fn as_ref(&self) -> &PostTranslationalModification {
        self
    }
}

impl TryFrom<OmicstoolsPostTranslationalModification> for PostTranslationalModification {
    type Error = Error;
    fn try_from(ptm: OmicstoolsPostTranslationalModification) -> Result<Self, Self::Error> {
        let amino_acid = AminoAcid::by_code(*ptm.get_amino_acid().get_code())?;
        let mass_delta = mass_to_int!(*ptm.get_mass_delta());
        Ok(Self {
            amino_acid,
            mass_delta,
            name: ptm.get_name().to_string(),
            total_mono_mass: mass_delta + amino_acid.mono_mass(),
            mod_type: ptm.get_mod_type().clone(),
            position: ptm.get_position().clone(),
        })
    }
}

impl TryFrom<macpepdb_web_common::requests::ptm::PostTranslationalModificationRequest>
    for PostTranslationalModification
{
    type Error = Error;

    fn try_from(
        ptm: macpepdb_web_common::requests::ptm::PostTranslationalModificationRequest,
    ) -> Result<Self, Self::Error> {
        use macpepdb_web_common::requests::ptm::{PtmPosition, PtmType};

        let amino_acid = AminoAcid::by_code(ptm.amino_acid)?;
        let mass_delta = mass_to_int(ptm.mass_delta);
        let mod_type = match ptm.mod_type {
            PtmType::Static => ModificationType::Static,
            PtmType::Variable => ModificationType::Variable,
        };
        let position = match ptm.position {
            PtmPosition::Anywhere => Position::Anywhere,
            PtmPosition::NTerminus => Position::Terminus(Terminus::N),
            PtmPosition::CTerminus => Position::Terminus(Terminus::C),
            PtmPosition::NBond => Position::Bond(Terminus::N),
            PtmPosition::CBond => Position::Bond(Terminus::C),
        };

        Ok(Self::new(
            ptm.name, amino_acid, mass_delta, mod_type, position,
        ))
    }
}

impl Eq for PostTranslationalModification {}

impl PartialEq for PostTranslationalModification {
    fn eq(&self, other: &Self) -> bool {
        self.amino_acid.code() == other.amino_acid.code()
            && self.mass_delta == other.mass_delta
            && self.mod_type == other.mod_type
            && self.position == other.position
    }
}

impl Hash for PostTranslationalModification {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.amino_acid.code().hash(state);
        self.mass_delta.hash(state);
        match self.mod_type {
            ModificationType::Static => 0.hash(state),
            ModificationType::Variable => 1.hash(state),
        }
        match self.position {
            Position::Anywhere => 0.hash(state),
            Position::Terminus(Terminus::N) => 1.hash(state),
            Position::Terminus(Terminus::C) => 2.hash(state),
            Position::Bond(Terminus::N) => 3.hash(state),
            Position::Bond(Terminus::C) => 4.hash(state),
        }
    }
}

/// Collection of post-translational modifications (PTMs).
/// Rules:
/// * Static PTMs are applied to every occurrence of the targeted amino acid
/// * Variable PTMs can be applied to any occurrence of the targeted amino acid (usually limited to a maximum number of variable modifications).
///   Amino acids target cannot be the same as the static PTM.
/// * N-/C-terminal PTMs are applied to the first/last amino acid of the peptide.
///   Treated as variable modifications, although they are not counted for a variable modification limit.
///   Cannot be applied to an amino acid which is statically PTM.
/// * N-/C-bond PTMs are applied to the bond between peptides. Treated as variable modifications but not counted for variable modification limits.
///   Amino acid target is not relevant.
///
pub struct PTMCollection<P>
where
    P: AsRef<PostTranslationalModification>,
{
    static_ptms: Vec<P>,
    variable_ptms: Vec<P>,
    n_terminal_ptms: Vec<P>,
    c_terminal_ptms: Vec<P>,
    n_bond_ptms: Vec<P>,
    c_bond_ptms: Vec<P>,
}

impl<P> PTMCollection<P>
where
    P: AsRef<PostTranslationalModification>,
{
    /// Creates a new PTMCollection from a slice of PTMs, by sorting them
    /// and checks the validity of the collection.
    ///
    /// # Arguments
    /// * `ptms` - A slice of PTMs to create the collection
    ///
    pub fn new(ptms: impl IntoIterator<Item = P>) -> Result<Self, Error> {
        let mut static_ptms: Vec<P> = Vec::new();
        let mut variable_ptms: Vec<P> = Vec::new();
        let mut n_terminal_ptms: Vec<P> = Vec::new();
        let mut c_terminal_ptms: Vec<P> = Vec::new();
        let mut n_bond_ptms: Vec<P> = Vec::new();
        let mut c_bond_ptms: Vec<P> = Vec::new();

        // Sort ptms
        for ptm in ptms {
            if ptm.as_ref().is_static() && ptm.as_ref().is_anywhere() {
                static_ptms.push(ptm);
            } else if ptm.as_ref().is_variable() && ptm.as_ref().is_anywhere() {
                variable_ptms.push(ptm);
            } else if ptm.as_ref().is_n_terminus() {
                n_terminal_ptms.push(ptm);
            } else if ptm.as_ref().is_c_terminus() {
                c_terminal_ptms.push(ptm);
            } else if ptm.as_ref().is_n_bond() {
                n_bond_ptms.push(ptm);
            } else if ptm.as_ref().is_c_bond() {
                c_bond_ptms.push(ptm);
            }
        }

        let mut static_modification_targets = HashSet::with_capacity(static_ptms.len());
        for ptm in static_ptms.iter() {
            if !static_modification_targets.insert(ptm.as_ref().amino_acid().code()) {
                return Err(Error::StaticallyModifiedTwiceOrMore(
                    ptm.as_ref().name().to_string(),
                ));
            }
        }

        for variable_ptm in variable_ptms.iter() {
            if static_modification_targets.contains(&variable_ptm.as_ref().amino_acid().code()) {
                return Err(Error::AlreadyStaticallyModified(
                    variable_ptm.as_ref().name().to_string(),
                ));
            }
        }

        for n_terminal_ptm in n_terminal_ptms.iter() {
            if static_modification_targets.contains(&n_terminal_ptm.as_ref().amino_acid().code()) {
                return Err(Error::AlreadyStaticallyModified(
                    n_terminal_ptm.as_ref().name().to_string(),
                ));
            }
        }

        for c_terminal_ptm in c_terminal_ptms.iter() {
            if static_modification_targets.contains(&c_terminal_ptm.as_ref().amino_acid().code()) {
                return Err(Error::AlreadyStaticallyModified(
                    c_terminal_ptm.as_ref().name().to_string(),
                ));
            }
        }

        Ok(PTMCollection {
            static_ptms,
            variable_ptms,
            n_terminal_ptms,
            c_terminal_ptms,
            n_bond_ptms,
            c_bond_ptms,
        })
    }

    pub fn get_static_ptms(&self) -> &Vec<P> {
        &self.static_ptms
    }

    pub fn get_variable_ptms(&self) -> &Vec<P> {
        &self.variable_ptms
    }

    pub fn get_n_terminal_ptms(&self) -> &Vec<P> {
        &self.n_terminal_ptms
    }

    pub fn get_c_terminal_ptms(&self) -> &Vec<P> {
        &self.c_terminal_ptms
    }

    pub fn get_n_bond_ptms(&self) -> &Vec<P> {
        &self.n_bond_ptms
    }

    pub fn get_c_bond_ptms(&self) -> &Vec<P> {
        &self.c_bond_ptms
    }

    pub fn len(&self) -> usize {
        self.static_ptms.len()
            + self.variable_ptms.len()
            + self.n_terminal_ptms.len()
            + self.c_terminal_ptms.len()
            + self.n_bond_ptms.len()
            + self.c_bond_ptms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn all(&self) -> Vec<&P> {
        let mut all_ptms: Vec<&P> = Vec::with_capacity(self.len());

        all_ptms.extend(self.static_ptms.iter());
        all_ptms.extend(self.variable_ptms.iter());
        all_ptms.extend(self.n_terminal_ptms.iter());
        all_ptms.extend(self.c_terminal_ptms.iter());
        all_ptms.extend(self.n_bond_ptms.iter());
        all_ptms.extend(self.c_bond_ptms.iter());

        all_ptms
    }
}

impl<P> Display for PTMCollection<P>
where
    P: AsRef<PostTranslationalModification>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ptms: Vec<String> = Vec::with_capacity(self.len());

        ptms.extend(
            self.static_ptms
                .iter()
                .map(|ptm| ptm_to_string(ptm.as_ref())),
        );

        ptms.extend(
            self.variable_ptms
                .iter()
                .map(|ptm| ptm_to_string(ptm.as_ref())),
        );

        ptms.extend(
            self.n_terminal_ptms
                .iter()
                .map(|ptm| ptm_to_string(ptm.as_ref())),
        );

        ptms.extend(
            self.c_terminal_ptms
                .iter()
                .map(|ptm| ptm_to_string(ptm.as_ref())),
        );

        ptms.extend(
            self.n_terminal_ptms
                .iter()
                .map(|ptm| ptm_to_string(ptm.as_ref())),
        );

        ptms.extend(
            self.c_terminal_ptms
                .iter()
                .map(|ptm| ptm_to_string(ptm.as_ref())),
        );

        write!(
            f,
            "PTMCollection (static: {}, variable: {}, n_terminal: {}, c_terminal: {}, n_bond: {}, c_bond: {}):\n\t{}",
            self.static_ptms.len(),
            self.variable_ptms.len(),
            self.n_terminal_ptms.len(),
            self.c_terminal_ptms.len(),
            self.n_bond_ptms.len(),
            self.c_bond_ptms.len(),
            ptms.join("\n\t")
        )
    }
}

/// Converts a PTM to a string representation.
///
fn ptm_to_string(ptm: &PostTranslationalModification) -> String {
    format!(
        "{}, {}, {}, {}, {}",
        ptm.name(),
        ptm.amino_acid().code(),
        ptm.mass_delta(),
        ptm.mod_type(),
        ptm.position()
    )
}

mod amino_acid_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(amino_acid: &&'static AminoAcid, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_char(amino_acid.code())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<&'static AminoAcid, D::Error>
    where
        D: Deserializer<'de>,
    {
        let code = String::deserialize(deserializer)?;
        AminoAcid::by_code(code.chars().next().unwrap()).map_err(serde::de::Error::custom)
    }
}

mod mass_delta_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(mass_delta: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(mass_to_float(*mass_delta))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(mass_to_int(f64::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod test {
    use dihardts_omicstools::chemistry::amino_acid::{CYSTEINE, GLYCINE, METHIONINE};

    // internal imports
    use super::*;

    #[test]
    fn test_validate_ptm_vec() {
        let valid_ptms = vec![
            PostTranslationalModification::try_from(OmicstoolsPostTranslationalModification::new(
                "Carbamidomethyl",
                &CYSTEINE,
                57.021464,
                ModificationType::Static,
                Position::Anywhere,
            ))
            .unwrap(),
            PostTranslationalModification::try_from(OmicstoolsPostTranslationalModification::new(
                "Oxidation",
                &METHIONINE,
                15.994915,
                ModificationType::Variable,
                Position::Anywhere,
            ))
            .unwrap(),
            PostTranslationalModification::try_from(OmicstoolsPostTranslationalModification::new(
                "Imaginary",
                &GLYCINE,
                5.6,
                ModificationType::Static,
                Position::Terminus(Terminus::N),
            ))
            .unwrap(),
        ];

        assert!(PTMCollection::new(valid_ptms).is_ok());

        let invalid_ptms = vec![
            PostTranslationalModification::try_from(OmicstoolsPostTranslationalModification::new(
                "Carbamidomethyl",
                &CYSTEINE,
                57.021464,
                ModificationType::Static,
                Position::Anywhere,
            ))
            .unwrap(),
            PostTranslationalModification::try_from(OmicstoolsPostTranslationalModification::new(
                "Oxidation",
                &METHIONINE,
                15.994915,
                ModificationType::Variable,
                Position::Anywhere,
            ))
            .unwrap(),
            PostTranslationalModification::try_from(OmicstoolsPostTranslationalModification::new(
                "Imaginary",
                &CYSTEINE,
                5.6,
                ModificationType::Variable,
                Position::Terminus(Terminus::N),
            ))
            .unwrap(),
        ];
        assert!(PTMCollection::new(invalid_ptms).is_err());
    }
}
