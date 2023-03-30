// std imports
use std::cmp;
use std::collections::{HashMap, HashSet};

// 3rd party imports
use fancy_regex::Regex;

// internal imports
use crate::chemistry::amino_acid::{UNKNOWN, REPLACEABLE_AMBIGIOUS_AMINO_ACID_LOOKUP};
use crate::tools::fancy_regex::split as fancy_regex_split_string;

/// Trait defining the behavior fo a digestion enzyme
pub trait Enzyme {
    /// Returns a new instance of the enzyme
    /// 
    /// # Arguments
    /// * `max_number_of_missed_cleavages` - Maximum number of missed cleavages
    /// * `min_peptide_length` - Minimum length of a peptide
    /// * `max_peptide_length` - Maximum length of a peptide
    /// 
    fn new(max_number_of_missed_cleavages: usize, min_peptide_length: usize, max_peptide_length: usize) -> Self where Self: Sized;
    /// Returns the regex for finding the cleavage sites
    fn get_cleavages_site_regex(&self) -> &Regex;
    /// Returns the maximum number of missed cleavages
    fn get_max_number_of_missed_cleavages(&self) -> usize;
    /// Returns the minimum peptide length
    fn get_min_peptide_length(&self) -> usize;
    /// Returns the maximum peptide length
    fn get_max_peptide_length(&self) -> usize;

    /// Digests a protein into peptides
    fn digest(&self, amino_acid_sequence: &str) -> HashMap<String, i16> {
        let cleavages_site_regex: &Regex = self.get_cleavages_site_regex();
        let mut peptides: HashMap<String, i16> = HashMap::new();
        // Split protein sequence on every cleavage position
        let protein_parts: Vec<&str> = fancy_regex_split_string(cleavages_site_regex, amino_acid_sequence);
        // Start with every part
        for part_index in 0..protein_parts.len() {
            // Check if end of protein_parts is reached before the last missed cleavage (prevent overflow)
            let last_part_to_add = cmp::min(
                part_index + self.get_max_number_of_missed_cleavages() + 1,
                protein_parts.len()
            );
            let mut peptide_sequence: String = String::new();
            for missed_cleavage in part_index..last_part_to_add {
                peptide_sequence.push_str(protein_parts[missed_cleavage]);
                if self.get_min_peptide_length() <= peptide_sequence.len() && peptide_sequence.len() <= self.get_max_peptide_length() && !peptide_sequence.contains(*UNKNOWN.get_one_letter_code()) {
                    peptides.insert(peptide_sequence.to_string(), (missed_cleavage - part_index) as i16);
                    if is_sequence_containing_replaceable_ambiguous_amino_acids(&peptide_sequence) {
                        /*
                        If there is a replaceable ambiguous amino acid within the sequence, calculate each sequence combination of the actual amino acids
                        Note: Some protein sequences in SwissProt and TrEMBL contain ambiguous amino acids (B, Z). In most cases B and Z are denoted with the average mass of their encoded amino acids (D, N and E, Q).
                        The average mass makes it difficult to create precise queries for these sequences in MaCPepDB. Therefor we calculates each differentiated version of the ambiguous sequence and store it with the differentiated masses.
                        This works only, when the actual amino acids have distinct masses like for B and Z, therefore we have to tolerate Js.
                        */
                        for sequence in differentiate_ambiguous_sequences(&peptide_sequence) {
                            peptides.insert(
                                sequence.to_string(), 
                                (missed_cleavage - part_index) as i16);
                        }
                    }
                }
            }
        }
        return peptides;
    }
}

/// Checks if the sequence contains a replaceable ambiguous amino acid
/// 
/// # Arguments
/// * `sequence` - Peptide sequence
/// 
fn is_sequence_containing_replaceable_ambiguous_amino_acids(sequence: &str) -> bool {
    for one_letter_code in REPLACEABLE_AMBIGIOUS_AMINO_ACID_LOOKUP.keys() {
        if sequence.contains(*one_letter_code) {
            return true
        }
    }
    return false;
}


/// Calculates all possible combinations of an ambiguous sequence.
/// 
/// # Arguments
/// * `ambiguous_sequence`
/// 
fn differentiate_ambiguous_sequences(ambiguous_sequence: &str) -> HashSet<String> {
    let mut differentiated_sequences: HashSet<String> = HashSet::new();
    recursively_differentiate_ambiguous_sequences(ambiguous_sequence, &mut differentiated_sequences, 0);
    return differentiated_sequences;
}

/// Recursively calculates all possible differentiate combinations of ambiguous sequence.
/// 
/// # Arguments
/// * `sequence` -  Current sequence
/// * `differentiated_sequences` -  A HashMap to store the differentiated sequences
/// * `position` -  Current position in the sequence
///
fn recursively_differentiate_ambiguous_sequences(sequence: &str, differentiated_sequences: &mut HashSet<String>, position: usize) {
    if position < sequence.len() {
        let current_one_letter_code: char = sequence.chars().nth(position).unwrap();
        if !REPLACEABLE_AMBIGIOUS_AMINO_ACID_LOOKUP.contains_key(&current_one_letter_code) {
            // If the amino acid on the current position is not a replaceable ambiguous amino acid, pass the unchanged sequence to to next recursion level
            recursively_differentiate_ambiguous_sequences(sequence, differentiated_sequences, position + 1)
        } else {
            // If the amino acid on the current position is a replaceable ambiguous amino acid create a new sequence where the current ambiguous amino acid is sequentially replaced by its actual amino acids.
            // Than pass the new sequence to the next recursion level.
            for replacement_amino_acid in REPLACEABLE_AMBIGIOUS_AMINO_ACID_LOOKUP.get(&current_one_letter_code).unwrap() {
                let mut new_sequence: String = String::from(&sequence[0..position]);
                new_sequence.push(*replacement_amino_acid.get_one_letter_code());
                new_sequence.push_str(&sequence[position + 1..]);
                recursively_differentiate_ambiguous_sequences(new_sequence.as_str(), differentiated_sequences, position + 1);
            }
        }
    } else {
        // If recursion reached the end of the sequence add it to the set of sequences
        differentiated_sequences.insert(String::from(sequence));
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const AMBIGUOUS_SEQUENCE: &'static str = "MDQZTLABBQQILASLZPSR";
    const EXPECTED_DIFFERENTIATED_SEQEUNCES: [&'static str; 16] = [
        "MDQETLANDQQILASLQPSR",
        "MDQETLANDQQILASLEPSR",
        "MDQQTLADNQQILASLQPSR",
        "MDQQTLANDQQILASLQPSR",
        "MDQQTLANNQQILASLEPSR",
        "MDQETLADNQQILASLEPSR",
        "MDQQTLANDQQILASLEPSR",
        "MDQETLADNQQILASLQPSR",
        "MDQQTLADDQQILASLQPSR",
        "MDQETLANNQQILASLQPSR",
        "MDQQTLADDQQILASLEPSR",
        "MDQQTLANNQQILASLQPSR",
        "MDQQTLADNQQILASLEPSR",
        "MDQETLANNQQILASLEPSR",
        "MDQETLADDQQILASLEPSR",
        "MDQETLADDQQILASLQPSR"
    ];
    const AMBIGUOUS_SEQUENCE_WITH_B: &'static str = "MDQTLABBQQILASLPSR";
    const AMBIGUOUS_SEQUENCE_WITH_Z: &'static str = "MDQZTLAQQILASLZPSR";
    const UNAMBIGUOUS_SEQUENCE: &'static str = "MDQTLAQQILASLPSR";

    fn is_sequence_in_expected_differentiated_seqeunces(sequence: &str) -> bool {
        for expected_sequence in &EXPECTED_DIFFERENTIATED_SEQEUNCES {
            if sequence == *expected_sequence {
                return true;
            }
        }
        return false;
    }

    #[test]
    fn test_differentiate_ambiguous_sequences() {
        let differentiated_sequences: HashSet<String> = differentiate_ambiguous_sequences(AMBIGUOUS_SEQUENCE);
        assert_eq!(differentiated_sequences.len(), EXPECTED_DIFFERENTIATED_SEQEUNCES.len());
        for sequence in differentiated_sequences {
            assert_eq!(is_sequence_in_expected_differentiated_seqeunces(&sequence), true);
        }
    }

    #[test]
    fn test_is_sequence_containing_replaceable_ambiguous_amino_acids() {
        assert_eq!(is_sequence_containing_replaceable_ambiguous_amino_acids(AMBIGUOUS_SEQUENCE_WITH_B), true);
        assert_eq!(is_sequence_containing_replaceable_ambiguous_amino_acids(AMBIGUOUS_SEQUENCE_WITH_Z), true);
        assert_eq!(is_sequence_containing_replaceable_ambiguous_amino_acids(AMBIGUOUS_SEQUENCE), true);
        assert_eq!(is_sequence_containing_replaceable_ambiguous_amino_acids(UNAMBIGUOUS_SEQUENCE), false);
    }
}