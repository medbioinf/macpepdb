// std imports
use std::{collections::HashSet, fmt::Display};

// 3rd party imports
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use thiserror::Error;

/// Errors which might occur during PTM collection validation
#[derive(Debug, Error)]
pub enum PTMCollectionValidationError {
    #[error("Amino acid {0} is statically modified twice or more.")]
    StaticallyModifiedTwiceOrMore(String),
    #[error("Amino acid {0} is already statically modified.")]
    AlreadyStaticallyModified(String),
}

/// Collection of post-translational modifications (PTMs).
/// Rules:
/// * Static PTMs are applied to every occurence of the targeted amino acid
/// * Variable PTMs can be applied to any occurence of the targeted amino acid (usually limited to a maximum number of variable modifications).
///   Amino acids target cannot be the same as the static PTM.
/// * N-/C-terminal PTMs are applied to the first/last amino acid of the peptide.
///   Treated as variable modifications, although they are not counted for a variable modification limit.
///   Cannot be applied to an amino acid which is statically PTM.
/// * N-/C-bond PTMs are applied to the bond between peptides. Treated as variable modifications but not counted for variable modification limits.
///   Amino acid target is not relevant.
///
pub struct PTMCollection<'a> {
    static_ptms: Vec<&'a PTM>,
    variable_ptms: Vec<&'a PTM>,
    n_terminal_ptms: Vec<&'a PTM>,
    c_terminal_ptms: Vec<&'a PTM>,
    n_bond_ptms: Vec<&'a PTM>,
    c_bond_ptms: Vec<&'a PTM>,
}

impl<'a> PTMCollection<'a> {
    /// Creates a new PTMCollection from a slice of PTMs, by sorting them
    /// and checks the validity of the collection.
    ///
    /// # Arguments
    /// * `ptms` - A slice of PTMs to create the collection
    ///
    pub fn new(ptms: &'a [PTM]) -> Result<Self, PTMCollectionValidationError> {
        let mut static_ptms: Vec<&'a PTM> = Vec::new();
        let mut variable_ptms: Vec<&'a PTM> = Vec::new();
        let mut n_terminal_ptms: Vec<&'a PTM> = Vec::new();
        let mut c_terminal_ptms: Vec<&'a PTM> = Vec::new();
        let mut n_bond_ptms: Vec<&'a PTM> = Vec::new();
        let mut c_bond_ptms: Vec<&'a PTM> = Vec::new();

        // Sort ptms
        for ptm in ptms {
            if ptm.is_static() && ptm.is_anywhere() {
                static_ptms.push(ptm);
            } else if ptm.is_variable() && ptm.is_anywhere() {
                variable_ptms.push(ptm);
            } else if ptm.is_n_terminus() {
                n_terminal_ptms.push(ptm);
            } else if ptm.is_c_terminus() {
                c_terminal_ptms.push(ptm);
            } else if ptm.is_n_bond() {
                n_bond_ptms.push(ptm);
            } else if ptm.is_c_bond() {
                c_bond_ptms.push(ptm);
            }
        }

        let mut static_modification_targets = HashSet::with_capacity(static_ptms.len());
        for ptm in static_ptms.iter() {
            if !static_modification_targets.insert(ptm.get_amino_acid().get_code()) {
                return Err(PTMCollectionValidationError::StaticallyModifiedTwiceOrMore(
                    ptm.get_name().to_string(),
                ));
            }
        }

        for variable_ptm in variable_ptms.iter() {
            if static_modification_targets.contains(variable_ptm.get_amino_acid().get_code()) {
                return Err(PTMCollectionValidationError::AlreadyStaticallyModified(
                    variable_ptm.get_name().to_string(),
                ));
            }
        }

        for n_terminal_ptm in n_terminal_ptms.iter() {
            if static_modification_targets.contains(n_terminal_ptm.get_amino_acid().get_code()) {
                return Err(PTMCollectionValidationError::AlreadyStaticallyModified(
                    n_terminal_ptm.get_name().to_string(),
                ));
            }
        }

        for c_terminal_ptm in c_terminal_ptms.iter() {
            if static_modification_targets.contains(c_terminal_ptm.get_amino_acid().get_code()) {
                return Err(PTMCollectionValidationError::AlreadyStaticallyModified(
                    c_terminal_ptm.get_name().to_string(),
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

    pub fn get_static_ptms(&self) -> &Vec<&'a PTM> {
        &self.static_ptms
    }

    pub fn get_variable_ptms(&self) -> &Vec<&'a PTM> {
        &self.variable_ptms
    }

    pub fn get_n_terminal_ptms(&self) -> &Vec<&'a PTM> {
        &self.n_terminal_ptms
    }

    pub fn get_c_terminal_ptms(&self) -> &Vec<&'a PTM> {
        &self.c_terminal_ptms
    }

    pub fn get_n_bond_ptms(&self) -> &Vec<&'a PTM> {
        &self.n_bond_ptms
    }

    pub fn get_c_bond_ptms(&self) -> &Vec<&'a PTM> {
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
}

impl Display for PTMCollection<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ptms: Vec<String> = Vec::with_capacity(self.len());

        ptms.extend(self.static_ptms.iter().map(|ptm| ptm_to_string(ptm)));

        ptms.extend(self.variable_ptms.iter().map(|ptm| ptm_to_string(ptm)));

        ptms.extend(self.n_terminal_ptms.iter().map(|ptm| ptm_to_string(ptm)));

        ptms.extend(self.c_terminal_ptms.iter().map(|ptm| ptm_to_string(ptm)));

        ptms.extend(self.n_terminal_ptms.iter().map(|ptm| ptm_to_string(ptm)));

        ptms.extend(self.c_terminal_ptms.iter().map(|ptm| ptm_to_string(ptm)));

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
fn ptm_to_string(ptm: &PTM) -> String {
    format!(
        "{}, {}, {}, {}, {}",
        ptm.get_name(),
        ptm.get_amino_acid().get_code(),
        ptm.get_mass_delta(),
        ptm.get_mod_type(),
        ptm.get_position()
    )
}

#[cfg(test)]
mod test {
    // 3rd party imports
    use dihardts_omicstools::{
        chemistry::amino_acid::{CYSTEINE, GLYCINE, METHIONINE},
        proteomics::{
            peptide::Terminus,
            post_translational_modifications::{
                ModificationType, Position, PostTranslationalModification as PTM,
            },
        },
    };

    // internal imports
    use super::*;

    #[test]
    fn test_validate_ptm_vec() {
        let valid_ptms = vec![
            PTM::new(
                "Carbamidomethyl",
                &CYSTEINE,
                57.021464,
                ModificationType::Static,
                Position::Anywhere,
            ),
            PTM::new(
                "Oxidation",
                &METHIONINE,
                15.994915,
                ModificationType::Variable,
                Position::Anywhere,
            ),
            PTM::new(
                "Imaginary",
                &GLYCINE,
                5.6,
                ModificationType::Static,
                Position::Terminus(Terminus::N),
            ),
        ];
        assert!(PTMCollection::new(&valid_ptms).is_ok());

        let invalid_ptms = vec![
            PTM::new(
                "Carbamidomethyl",
                &CYSTEINE,
                57.021464,
                ModificationType::Static,
                Position::Anywhere,
            ),
            PTM::new(
                "Oxidation",
                &METHIONINE,
                15.994915,
                ModificationType::Variable,
                Position::Anywhere,
            ),
            PTM::new(
                "Imaginary",
                &CYSTEINE,
                5.6,
                ModificationType::Variable,
                Position::Terminus(Terminus::N),
            ),
        ];
        assert!(PTMCollection::new(&invalid_ptms).is_err());
    }
}
