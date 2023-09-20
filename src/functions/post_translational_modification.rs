// std imports
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Display;

// 3rd party imports
use anyhow::{bail, Result};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;

// internal imports
use crate::chemistry::amino_acid::AminoAcid as InternalAminoAcid;
use crate::entities::peptide::Peptide;
use crate::mass::convert::{to_float as mass_to_float, to_int as mass_to_int};

/// Validates a list of PTMs:
/// Makes sure that there are no static and variable PTMs for the same amino acid
///
pub fn validate_ptm_vec(ptms: &Vec<PTM>) -> Result<()> {
    let static_ptms: Vec<&PTM> = ptms.iter().filter(|ptm| ptm.is_static()).collect();
    let variable_ptms: Vec<&PTM> = ptms.iter().filter(|ptm| ptm.is_variable()).collect();
    // Check if there are any static/variable PTMs for the same amino acid
    let mut errors = String::new();
    for static_ptm in &static_ptms {
        for variable_ptm in &variable_ptms {
            if static_ptm.get_amino_acid().get_code() == variable_ptm.get_amino_acid().get_code() {
                errors.push_str(&format!(
                    "Static PTM {} and variable PTM {} are supposed for the same amino acid {}.\n",
                    static_ptm.get_name(),
                    variable_ptm.get_name(),
                    static_ptm.get_amino_acid().get_code()
                ));
            }
        }
    }
    if errors.len() > 0 {
        bail!(errors);
    }
    Ok(())
}

/// Simple struct to store and check the occurrence of an amino acid
///
pub enum AminoAcidOccurrence {
    Equal(i16),
    GreaterOrEqual(i16),
}

impl AminoAcidOccurrence {
    /// Checks if the given count matches the occurrence
    ///
    /// # Arguments
    /// * `count` - The count to check
    ///
    pub fn check(&self, count: &i16) -> bool {
        match self {
            AminoAcidOccurrence::Equal(value) => count == value,
            AminoAcidOccurrence::GreaterOrEqual(value) => count >= value,
        }
    }

    /// Increments the occurrence by the given amount
    ///
    /// # Arguments
    /// * `amount` - The amount to increment by
    fn increment_by(&mut self, amount: i16) {
        match self {
            AminoAcidOccurrence::Equal(value) => *value += amount,
            AminoAcidOccurrence::GreaterOrEqual(value) => *value += amount,
        }
    }
}

impl Display for AminoAcidOccurrence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AminoAcidOccurrence::Equal(value) => write!(f, "== {}", value),
            AminoAcidOccurrence::GreaterOrEqual(value) => write!(f, ">= {}", value),
        }
    }
}

/// PTMCondition to check peptides against a given mass and PTMs.
/// Each PTM condition can be used to query a range of peptides.
///
pub struct PTMCondition {
    mass: i64,
    amino_acid_occurrences: HashMap<usize, AminoAcidOccurrence>,
    n_terminus_amino_acid: Option<&'static InternalAminoAcid>,
    c_terminus_amino_acid: Option<&'static InternalAminoAcid>,
}

impl PTMCondition {
    fn new(mass: f64, counters: &Vec<PTMCounter>) -> Result<PTMCondition> {
        let mut amino_acid_occurrences: HashMap<usize, AminoAcidOccurrence> = HashMap::new();
        let mut mass_delta: f64 = 0.0;

        let mut n_terminus_amino_acid: Option<&'static InternalAminoAcid> = None;
        let mut c_terminus_amino_acid: Option<&'static InternalAminoAcid> = None;

        for counter in counters.iter().filter(|ctr| !ctr.ptm.is_bond()) {
            if counter.count == 0 {
                continue;
            }
            amino_acid_occurrences
                .entry(*counter.ptm.get_amino_acid().get_code() as usize % 65)
                .or_insert(if counter.ptm.is_static() {
                    AminoAcidOccurrence::Equal(0)
                } else {
                    AminoAcidOccurrence::GreaterOrEqual(0)
                })
                .increment_by(counter.count);
            mass_delta += counter.count as f64 * counter.ptm.get_mass_delta();
            if counter.ptm.is_n_terminus() {
                n_terminus_amino_acid = Some(InternalAminoAcid::get_by_one_letter_code(
                    *counter.ptm.get_amino_acid().get_code(),
                )?);
            } else if counter.ptm.is_c_terminus() {
                c_terminus_amino_acid = Some(InternalAminoAcid::get_by_one_letter_code(
                    *counter.ptm.get_amino_acid().get_code(),
                )?);
            }
        }

        // Bond modification are currently not supposed to be amino acid specific. So we do not need to adjust a counter.
        for counter in counters.iter().filter(|ctr| ctr.ptm.is_bond()) {
            mass_delta += counter.count as f64 * counter.ptm.get_mass_delta();
        }

        // convert back into the internal integer representation
        let mass = mass_to_int(mass - mass_delta);
        Ok(PTMCondition {
            mass,
            amino_acid_occurrences,
            n_terminus_amino_acid,
            c_terminus_amino_acid,
        })
    }

    pub fn get_mass(&self) -> &i64 {
        return &self.mass;
    }

    pub fn get_amino_acid_occurrences(&self) -> &HashMap<usize, AminoAcidOccurrence> {
        return &self.amino_acid_occurrences;
    }

    pub fn get_n_terminus_amino_acid(&self) -> &Option<&'static InternalAminoAcid> {
        return &self.n_terminus_amino_acid;
    }

    pub fn get_c_terminus_amino_acid(&self) -> &Option<&'static InternalAminoAcid> {
        return &self.c_terminus_amino_acid;
    }

    pub fn check_peptide(&self, peptide: &Peptide) -> bool {
        return self
            .amino_acid_occurrences
            .iter()
            .all(|(amino_acid_idx, amino_acid_occurence)| {
                amino_acid_occurence.check(&peptide.get_aa_counts()[*amino_acid_idx])
            })
            && match &self.n_terminus_amino_acid {
                Some(amino_acid) => {
                    peptide.get_sequence().chars().next().unwrap()
                        == *amino_acid.get_one_letter_code()
                }
                None => true,
            }
            && match &self.c_terminus_amino_acid {
                Some(amino_acid) => {
                    peptide.get_sequence().chars().last().unwrap()
                        == *amino_acid.get_one_letter_code()
                }
                None => true,
            };
    }
}

impl Display for PTMCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut amino_acid_occurrences_str = String::new();
        for (amino_acid, count) in &self.amino_acid_occurrences {
            amino_acid_occurrences_str.push_str(&format!(
                "{}: {} ",
                char::from_u32(*amino_acid as u32 + 65).unwrap_or_default(),
                count
            ));
        }

        let n_terminus_amino_acid_str = match &self.n_terminus_amino_acid {
            Some(amino_acid) => format!("{}", amino_acid.get_one_letter_code()),
            None => String::from("None"),
        };

        let c_terminus_amino_acid_str = match &self.c_terminus_amino_acid {
            Some(amino_acid) => format!("{}", amino_acid.get_one_letter_code()),
            None => String::from("None"),
        };

        write!(
            f,
            "Mass: {}, Amino Acid Occurrences: {}, N-Terminus Amino Acid: {}, C-Terminus Amino Acid: {}",
            self.mass,
            amino_acid_occurrences_str,
            n_terminus_amino_acid_str,
            c_terminus_amino_acid_str
        )
    }
}

/// A struct to count the usage of a PTM
///
struct PTMCounter<'a> {
    ptm: &'a PTM,
    count: i16,
}

impl PTMCounter<'_> {
    pub fn new(ptm: &PTM) -> PTMCounter {
        PTMCounter { ptm: ptm, count: 0 }
    }
}

/// Calculates the combinations of PTMs which can be applied for a given mass
/// and returns a vector of PTMConditions check if queried peptides are matching
/// the applied PTMs.
///
/// # Arguments
/// * `mass` - The mass to calculate the combinations for
/// * `max_variable_modifications` - The maximum number of variable modifications
/// * `ptms` - The PTMs to use
///
pub fn get_ptm_conditions(
    mass: i64,
    max_variable_modifications: i16,
    ptms: &Vec<PTM>,
) -> Result<Vec<PTMCondition>> {
    validate_ptm_vec(ptms)?;
    // Create counter and a vector for the conditions
    let mut counters = ptms.iter().map(|ptm| PTMCounter::new(ptm)).collect();
    let mut ptm_conditions: Vec<PTMCondition> = Vec::new();
    // As the PTM combinations are calculated on the PTM struct of the external create di_hardts_omicstools which works with float mass,
    // we need to convert the mass from internal integer representation to float
    let mass = mass_to_float(mass);
    recursively_apply_ptms(
        mass,
        0,
        &mut counters,
        mass,
        max_variable_modifications,
        false,
        false,
        false,
        false,
        &mut ptm_conditions,
    )?;
    return Ok(ptm_conditions);
}

/// Recursively calculates the combinations of PTMs
/// which can be applied for a given mass and returns
/// a vector of PTMConditions to check on a peptide it
/// it match this combination.
///
/// # Arguments
/// * `mass` - The mass to calculate the combinations for
/// * `counter_idx` - The index of the current counter
/// * `counters` - The counters to use
/// * `remaining_mass` - The remaining mass to calculate the combinations for
/// * `free_variable_modifications` - The number of free variable modifications
/// * `is_n_terminus_used` - True if the n-terminus already has a PTM used
/// * `is_c_terminus_used` - True if the c-terminus already has a PTM used
/// * `is_n_bond_used` - True if the n-bond already has a PTM used
/// * `is_c_bond_used` - True if the c-bond already has a PTM used
/// * `ptm_conditions` - The vector to store the PTMConditions in
///
fn recursively_apply_ptms(
    mass: f64,
    counter_idx: usize,
    counters: &mut Vec<PTMCounter>,
    remaining_mass: f64,
    free_variable_modifications: i16,
    is_n_terminus_used: bool,
    is_c_terminus_used: bool,
    is_n_bond_used: bool,
    is_c_bond_used: bool,
    ptm_conditions: &mut Vec<PTMCondition>,
) -> Result<()> {
    // Exit method if index is greater than number of counters
    if counter_idx >= counters.len() {
        return Ok(());
    }

    let ptm = counters[counter_idx].ptm;

    let mut mod_max_count = 0;

    if ptm.is_anywhere() {
        // if PTM is anywhere
        if ptm.is_static() {
            // if PTM is static, it apply any time
            mod_max_count = (remaining_mass / ptm.get_total_mono_mass()).floor() as i16;
        } else {
            // if PTM is not static, it is vairable. So it apply only if there is "space" for more variable modifications
            mod_max_count = min(
                (remaining_mass / ptm.get_total_mono_mass()).floor() as i16,
                free_variable_modifications,
            );
        }
    } else if ptm.is_n_terminus() {
        if ptm.is_static() {
            // 0 if terminus is already in use, 1 if the terminus is free
            // and the modification has to fit the mass
            mod_max_count = min(
                if is_n_terminus_used { 0 } else { 1 },
                (remaining_mass / ptm.get_total_mono_mass()).floor() as i16,
            );
        } else {
            // 0 if terminus is already in use or there is no more space for a variable modification left
            // and the modification has to fit the remaining mass
            mod_max_count = min(
                if is_n_terminus_used || free_variable_modifications == 0 {
                    0
                } else {
                    1
                },
                (remaining_mass / ptm.get_total_mono_mass()).floor() as i16,
            );
        }
    } else if ptm.is_c_terminus() {
        if ptm.is_static() {
            // 0 if terminus is already in use, 1 if the terminus is free
            // and the modification has to fit the mass
            mod_max_count = min(
                if is_c_terminus_used { 0 } else { 1 },
                (remaining_mass / ptm.get_total_mono_mass()).floor() as i16,
            );
        } else {
            // 0 if terminus is already in use or there is no more space for a variable modification left
            // and the modification has to fit the remaining mass
            mod_max_count = min(
                if is_c_terminus_used || free_variable_modifications == 0 {
                    0
                } else {
                    1
                },
                (remaining_mass / ptm.get_total_mono_mass()).floor() as i16,
            );
        }
    } else if ptm.is_n_bond() {
        if ptm.is_static() {
            // 0 if bond is already in use, 1 if the bond is free
            // and the modification has to fit the mass
            mod_max_count = min(
                if is_n_bond_used { 0 } else { 1 },
                (remaining_mass / ptm.get_total_mono_mass()).floor() as i16,
            );
        } else {
            // 0 if bond is already in use or there is no more space for a variable modification left
            // and the modification has to fit the remaining mass
            mod_max_count = min(
                if is_n_bond_used || free_variable_modifications == 0 {
                    0
                } else {
                    1
                },
                (remaining_mass / ptm.get_total_mono_mass()).floor() as i16,
            );
        }
    } else if ptm.is_c_bond() {
        if ptm.is_static() {
            // 0 if bond is already in use, 1 if the bond is free
            // and the modification has to fit the mass
            mod_max_count = min(
                if is_c_bond_used { 0 } else { 1 },
                (remaining_mass / ptm.get_total_mono_mass()).floor() as i16,
            );
        } else {
            // 0 if bond is already in use or there is no more space for a variable modification left
            // and the modification has to fit the remaining mass
            mod_max_count = min(
                if is_c_bond_used || free_variable_modifications == 0 {
                    0
                } else {
                    1
                },
                (remaining_mass / ptm.get_total_mono_mass()).floor() as i16,
            );
        }
    }

    let mut is_mass_reached = false;

    // Increase the counter for the current modification until maximum is reached
    for count in 0..=mod_max_count {
        // Reset all following modification counts to zero
        for i in (counter_idx + 1)..counters.len() {
            counters[i].count = 0;
        }

        // Calculate the remaining precursor mass for the following modifications
        let next_remaining_mass = remaining_mass - (ptm.get_total_mono_mass() * count as f64);

        // Check if remaining precursor has space for more modifications
        if next_remaining_mass > 0.0 {
            // Assign the count for the current mod to the counter
            counters[counter_idx].count = count;

            // calculate free_variable_modifications for next iteration
            let next_free_variable_modifications = if ptm.is_static() {
                free_variable_modifications
            } else {
                free_variable_modifications - count
            };

            // set next terminus/terminal residue used to last value
            let mut next_is_n_terminus_used = is_n_terminus_used;
            let mut next_is_c_terminus_used = is_c_terminus_used;
            let mut next_is_n_bond_used = is_n_bond_used;
            let mut next_is_c_bond_used = is_c_bond_used;

            // check if n-terminal is used
            if ptm.is_n_terminus() && count > 0 {
                next_is_n_terminus_used = true;
            } else if ptm.is_c_terminus() && count > 0 {
                // check if c-terminal is used
                next_is_c_terminus_used = true;
            } else if ptm.is_n_bond() && count > 0 {
                // check if n-bond is used
                next_is_n_bond_used = true;
            } else if ptm.is_c_bond() && count > 0 {
                // check if c-bond is used
                next_is_c_bond_used = true;
            }

            // start the next iteration
            recursively_apply_ptms(
                mass,
                counter_idx + 1,
                counters,
                next_remaining_mass,
                next_free_variable_modifications,
                next_is_n_terminus_used,
                next_is_c_terminus_used,
                next_is_n_bond_used,
                next_is_c_bond_used,
                ptm_conditions,
            )?;
        } else {
            is_mass_reached = true;
        }

        // Add current counter state to matrix, if get current counter is last counter or precursor is reached
        if counter_idx == counters.len() - 1 || is_mass_reached {
            ptm_conditions.push(PTMCondition::new(mass, &counters)?);
        }

        // Stop iteration if precursor is reached
        if is_mass_reached {
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    // 3rd party imports
    use dihardts_omicstools::{
        chemistry::amino_acid::{CYSTEINE, METHIONINE, TRYPTOPHAN},
        proteomics::peptide::Terminus,
        proteomics::post_translational_modifications::{
            ModificationType, Position, PostTranslationalModification as PTM,
        },
    };

    // internal imports
    use super::*;
    use crate::mass::convert::to_int as mass_to_int;

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
                &CYSTEINE,
                5.6,
                ModificationType::Static,
                Position::Terminus(Terminus::N),
            ),
        ];
        assert!(validate_ptm_vec(&valid_ptms).is_ok());

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
        assert!(validate_ptm_vec(&invalid_ptms).is_err());
    }

    #[test]
    fn test_get_ptm_conditions() {
        let ptms = vec![
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
                ModificationType::Static,
                Position::Terminus(Terminus::N),
            ),
        ];

        let ptm_conditions = get_ptm_conditions(mass_to_int(1000.0), 3, &ptms).unwrap();
        assert!(ptm_conditions.len() == 40);
        // TODO: need to come up with serious tests
    }
}
