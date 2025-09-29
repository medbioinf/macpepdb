// std imports
use std::{collections::HashSet, fmt::Display, ops::Deref};

// 3rd party imports
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as OmicstoolsPostTranslationalModification;
use thiserror::Error;

/// Errors which might occur during PTM collection validation
#[derive(Debug, Error)]
pub enum PTMCollectionValidationError {
    #[error("Amino acid {0} is statically modified twice or more.")]
    StaticallyModifiedTwiceOrMore(String),
    #[error("Amino acid {0} is already statically modified.")]
    AlreadyStaticallyModified(String),
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct PostTranslationalModification {
    #[serde(flatten)]
    ptm: OmicstoolsPostTranslationalModification,
}

impl PostTranslationalModification {
    pub fn new(
        name: &str,
        amino_acid: &'static dyn dihardts_omicstools::chemistry::amino_acid::AminoAcid,
        mass_delta: f64,
        mod_type: dihardts_omicstools::proteomics::post_translational_modifications::ModificationType,
        position: dihardts_omicstools::proteomics::post_translational_modifications::Position,
    ) -> Self {
        Self {
            ptm: OmicstoolsPostTranslationalModification::new(
                name, amino_acid, mass_delta, mod_type, position,
            ),
        }
    }
}

impl Deref for PostTranslationalModification {
    type Target = OmicstoolsPostTranslationalModification;

    fn deref(&self) -> &Self::Target {
        &self.ptm
    }
}

impl AsRef<PostTranslationalModification> for PostTranslationalModification {
    fn as_ref(&self) -> &PostTranslationalModification {
        self
    }
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
    pub fn new(ptms: impl IntoIterator<Item = P>) -> Result<Self, PTMCollectionValidationError> {
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
            if !static_modification_targets.insert(ptm.as_ref().get_amino_acid().get_code()) {
                return Err(PTMCollectionValidationError::StaticallyModifiedTwiceOrMore(
                    ptm.as_ref().get_name().to_string(),
                ));
            }
        }

        for variable_ptm in variable_ptms.iter() {
            if static_modification_targets
                .contains(variable_ptm.as_ref().get_amino_acid().get_code())
            {
                return Err(PTMCollectionValidationError::AlreadyStaticallyModified(
                    variable_ptm.as_ref().get_name().to_string(),
                ));
            }
        }

        for n_terminal_ptm in n_terminal_ptms.iter() {
            if static_modification_targets
                .contains(n_terminal_ptm.as_ref().get_amino_acid().get_code())
            {
                return Err(PTMCollectionValidationError::AlreadyStaticallyModified(
                    n_terminal_ptm.as_ref().get_name().to_string(),
                ));
            }
        }

        for c_terminal_ptm in c_terminal_ptms.iter() {
            if static_modification_targets
                .contains(c_terminal_ptm.as_ref().get_amino_acid().get_code())
            {
                return Err(PTMCollectionValidationError::AlreadyStaticallyModified(
                    c_terminal_ptm.as_ref().get_name().to_string(),
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
            post_translational_modifications::{ModificationType, Position},
        },
    };

    // internal imports
    use super::*;

    #[test]
    fn test_validate_ptm_vec() {
        let valid_ptms = vec![
            PostTranslationalModification::new(
                "Carbamidomethyl",
                &CYSTEINE,
                57.021464,
                ModificationType::Static,
                Position::Anywhere,
            ),
            PostTranslationalModification::new(
                "Oxidation",
                &METHIONINE,
                15.994915,
                ModificationType::Variable,
                Position::Anywhere,
            ),
            PostTranslationalModification::new(
                "Imaginary",
                &GLYCINE,
                5.6,
                ModificationType::Static,
                Position::Terminus(Terminus::N),
            ),
        ];

        assert!(PTMCollection::new(valid_ptms).is_ok());

        let invalid_ptms = vec![
            PostTranslationalModification::new(
                "Carbamidomethyl",
                &CYSTEINE,
                57.021464,
                ModificationType::Static,
                Position::Anywhere,
            ),
            PostTranslationalModification::new(
                "Oxidation",
                &METHIONINE,
                15.994915,
                ModificationType::Variable,
                Position::Anywhere,
            ),
            PostTranslationalModification::new(
                "Imaginary",
                &CYSTEINE,
                5.6,
                ModificationType::Variable,
                Position::Terminus(Terminus::N),
            ),
        ];
        assert!(PTMCollection::new(invalid_ptms).is_err());
    }
}
