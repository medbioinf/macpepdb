/// Module containing amino acid information

// std imports
use std::collections::HashMap;

// 3rd party imports
use anyhow::{Result, bail};

// internal imports
use crate::chemistry::molecule::WATER;

pub struct AminoAcid {
    name: &'static str,
    one_letter_code: char,
    three_letter_code: &'static str,
    mono_mass: i64,
    average_mass: i64
}

impl AminoAcid {
    /// Returns the amino acid the given one letter code
    /// 
    /// # Arguments
    /// * `one_letter_code` - One letter code of the amino acid
    /// 
    pub fn get_by_one_letter_code(one_letter_code: char) -> Result<&'static Self> {
        return match one_letter_code {
            'A' => Ok(&ALANINE),
            'B' => Ok(&ASPARAGINE_OR_ASPARTIC_ACID),
            'C' => Ok(&CYSTEINE),
            'D' => Ok(&ASPARTIC_ACID),
            'E' => Ok(&GLUTAMIC_ACID),
            'F' => Ok(&PHENYLALANINE),
            'G' => Ok(&GLYCINE),
            'H' => Ok(&HISTIDINE),
            'I' => Ok(&ISOLEUCINE),
            'J' => Ok(&ISOLEUCINE_OR_LEUCINE),
            'K' => Ok(&LYSINE),
            'L' => Ok(&LEUCINE),
            'M' => Ok(&METHIONINE),
            'N' => Ok(&ASPARAGINE),
            'O' => Ok(&PYRROLYSINE),
            'P' => Ok(&PROLINE),
            'Q' => Ok(&GLUTAMINE),
            'R' => Ok(&ARGININE),
            'S' => Ok(&SERINE),
            'T' => Ok(&THREONINE),
            'U' => Ok(&SELENOCYSTEINE),
            'V' => Ok(&VALINE),
            'W' => Ok(&TRYPTOPHAN),
            'Y' => Ok(&TYROSINE),
            'Z' => Ok(&GLUTAMINE_OR_GLUTAMIC_ACID),
            'X' => Ok(&UNKNOWN),
            _ => bail!("Unknown one letter code: {}", one_letter_code)
        };
    }

    pub fn get_all() -> &'static [&'static AminoAcid; 26] {
        return &ALL;
    }

    pub fn get_name(&self) -> &'static str {
        return &self.name;
    }

    pub fn get_one_letter_code(&self) -> &char {
        return &self.one_letter_code;
    }

    pub fn get_three_letter_code(&self) -> &'static str {
        return &self.three_letter_code;
    }

    pub fn get_mono_mass(&self) -> &i64 {
        return &self.mono_mass;
    }

    pub fn get_average_mass(&self) -> &i64 {
        return &self.average_mass;
    }

}

// Standard amino acids: https://proteomicsresource.washington.edu/protocols06/masses.php
pub const ALANINE:                  AminoAcid = AminoAcid{name: "Alanine",              one_letter_code: 'A',   three_letter_code: "Ala",   mono_mass: mass_to_int!(71.037113805_f64),      average_mass: mass_to_int!(71.0788_f64)};
pub const CYSTEINE:                 AminoAcid = AminoAcid{name: "Cysteine",             one_letter_code: 'C',   three_letter_code: "Cys",   mono_mass: mass_to_int!(103.009184505_f64),     average_mass: mass_to_int!(103.1388_f64)};
pub const ASPARTIC_ACID:            AminoAcid = AminoAcid{name: "Aspartic acid",        one_letter_code: 'D',   three_letter_code: "Asp",   mono_mass: mass_to_int!(115.026943065_f64),     average_mass: mass_to_int!(115.0886_f64)};
pub const GLUTAMIC_ACID:            AminoAcid = AminoAcid{name: "Glutamic acid",        one_letter_code: 'E',   three_letter_code: "Glu",   mono_mass: mass_to_int!(129.042593135_f64),     average_mass: mass_to_int!(129.1155_f64)};
pub const PHENYLALANINE:            AminoAcid = AminoAcid{name: "Phenylalanine",        one_letter_code: 'F',   three_letter_code: "Phe",   mono_mass: mass_to_int!(147.068413945_f64),     average_mass: mass_to_int!(147.1766_f64)};
pub const GLYCINE:                  AminoAcid = AminoAcid{name: "Glycine",              one_letter_code: 'G',   three_letter_code: "Gly",   mono_mass: mass_to_int!(57.021463735_f64),      average_mass: mass_to_int!(57.0519_f64)};
pub const HISTIDINE:                AminoAcid = AminoAcid{name: "Histidine",            one_letter_code: 'H',   three_letter_code: "His",   mono_mass: mass_to_int!(137.058911875_f64),     average_mass: mass_to_int!(137.1411_f64)};
pub const ISOLEUCINE:               AminoAcid = AminoAcid{name: "Isoleucine",           one_letter_code: 'I',   three_letter_code: "Ile",   mono_mass: mass_to_int!(113.084064015_f64),     average_mass: mass_to_int!(113.1594_f64)};
pub const LYSINE:                   AminoAcid = AminoAcid{name: "Lysine",               one_letter_code: 'K',   three_letter_code: "Lys",   mono_mass: mass_to_int!(128.094963050_f64),     average_mass: mass_to_int!(128.1741_f64)};
pub const LEUCINE:                  AminoAcid = AminoAcid{name: "Leucine",              one_letter_code: 'L',   three_letter_code: "Leu",   mono_mass: mass_to_int!(113.084064015_f64),     average_mass: mass_to_int!(113.1594_f64)};
pub const METHIONINE:               AminoAcid = AminoAcid{name: "Methionine",           one_letter_code: 'M',   three_letter_code: "Met",   mono_mass: mass_to_int!(131.040484645_f64),     average_mass: mass_to_int!(131.1926_f64)};
pub const ASPARAGINE:               AminoAcid = AminoAcid{name: "Asparagine",           one_letter_code: 'N',   three_letter_code: "Asn",   mono_mass: mass_to_int!(114.042927470_f64),     average_mass: mass_to_int!(114.1038_f64)};
pub const PYRROLYSINE:              AminoAcid = AminoAcid{name: "Pyrrolysine",          one_letter_code: 'O',   three_letter_code: "Pyl",   mono_mass: mass_to_int!(237.147726925_f64),     average_mass: mass_to_int!(237.29816_f64)};
pub const PROLINE:                  AminoAcid = AminoAcid{name: "Proline",              one_letter_code: 'P',   three_letter_code: "Pro",   mono_mass: mass_to_int!(97.052763875_f64),      average_mass: mass_to_int!(97.1167_f64)};
pub const GLUTAMINE:                AminoAcid = AminoAcid{name: "Glutamine",            one_letter_code: 'Q',   three_letter_code: "Gln",   mono_mass: mass_to_int!(128.05857754_f64),      average_mass: mass_to_int!(128.1307_f64)};
pub const ARGININE:                 AminoAcid = AminoAcid{name: "Arginine",             one_letter_code: 'R',   three_letter_code: "Arg",   mono_mass: mass_to_int!(156.101111050_f64),     average_mass: mass_to_int!(156.1875_f64)};
pub const SERINE:                   AminoAcid = AminoAcid{name: "Serine",               one_letter_code: 'S',   three_letter_code: "Ser",   mono_mass: mass_to_int!(87.032028435_f64),      average_mass: mass_to_int!(87.0782_f64)};
pub const THREONINE:                AminoAcid = AminoAcid{name: "Threonine",            one_letter_code: 'T',   three_letter_code: "Thr",   mono_mass: mass_to_int!(101.047678505_f64),     average_mass: mass_to_int!(101.1051_f64)};
pub const SELENOCYSTEINE:           AminoAcid = AminoAcid{name: "Selenocysteine",       one_letter_code: 'U',   three_letter_code: "SeC",   mono_mass: mass_to_int!(150.953633405_f64),     average_mass: mass_to_int!(150.0379_f64)};
pub const VALINE:                   AminoAcid = AminoAcid{name: "Valine",               one_letter_code: 'V',   three_letter_code: "Val",   mono_mass: mass_to_int!(99.068413945_f64),      average_mass: mass_to_int!(99.1326_f64)};
pub const TRYPTOPHAN:               AminoAcid = AminoAcid{name: "Tryptophan",           one_letter_code: 'W',   three_letter_code: "Trp",   mono_mass: mass_to_int!(186.079312980_f64),     average_mass: mass_to_int!(186.2132_f64)};
pub const TYROSINE:                 AminoAcid = AminoAcid{name: "Tyrosine",             one_letter_code: 'Y',   three_letter_code: "Tyr",   mono_mass: mass_to_int!(163.063328575_f64),     average_mass: mass_to_int!(163.1760_f64)};
// Ambigous amino acids
pub const ASPARAGINE_OR_ASPARTIC_ACID: AminoAcid = AminoAcid{name: "Asparagine or aspartic acid",  one_letter_code: 'B',   three_letter_code: "Asx", mono_mass: mass_to_int!(114.5349352675_f64),  average_mass: mass_to_int!(114.59502_f64)};
pub const ISOLEUCINE_OR_LEUCINE:      AminoAcid = AminoAcid{name: "Isoleucine or Leucine",        one_letter_code: 'J',   three_letter_code: "Xle", mono_mass: mass_to_int!(113.084064015_f64),   average_mass: mass_to_int!(113.1594_f64)};
pub const GLUTAMINE_OR_GLUTAMIC_ACID:  AminoAcid = AminoAcid{name: "Glutamine or glutamic acid",   one_letter_code: 'Z',   three_letter_code: "Glx", mono_mass: mass_to_int!(128.5505853375_f64),  average_mass: mass_to_int!(128.6216_f64)};
// Special amino acids
//// Some Search Engines and Databases used the X Amino Acid for unknown amino acids
pub const UNKNOWN:                  AminoAcid = AminoAcid{name: "Unknown Amino Acid",    one_letter_code: 'X',   three_letter_code: "Xaa",   mono_mass: 0,                                   average_mass: 0};

const ALL: [&'static AminoAcid; 26] = [
    &ALANINE,
    &CYSTEINE,
    &ASPARTIC_ACID,
    &GLUTAMIC_ACID,
    &PHENYLALANINE,
    &GLYCINE,
    &HISTIDINE,
    &ISOLEUCINE,
    &LYSINE,
    &LEUCINE,
    &METHIONINE,
    &ASPARAGINE,
    &PYRROLYSINE,
    &PROLINE,
    &GLUTAMINE,
    &ARGININE,
    &SERINE,
    &THREONINE,
    &SELENOCYSTEINE,
    &VALINE,
    &TRYPTOPHAN,
    &TYROSINE,
    &ASPARAGINE_OR_ASPARTIC_ACID,
    &ISOLEUCINE_OR_LEUCINE,
    &GLUTAMINE_OR_GLUTAMIC_ACID,
    &UNKNOWN
];

lazy_static! {
    pub static ref REPLACEABLE_AMBIGIOUS_AMINO_ACID_LOOKUP: HashMap<char, [&'static AminoAcid; 2]> = collection! {
        'B' => [&ASPARTIC_ACID, &ASPARAGINE],
        'Z' => [&GLUTAMIC_ACID, &GLUTAMINE]
    };
}

pub fn calc_sequence_mass(sequence: &str) -> Result<i64> {
    let mut mass: i64 = WATER.get_mono_mass();
    for c in sequence.chars() {
        mass += AminoAcid::get_by_one_letter_code(c)?.get_mono_mass();
    }
    return Ok(mass);
}


#[cfg(test)]
mod test {
    // internal imports
    use crate::mass::convert::to_int as mass_to_int;
    use super::*;
    

    // Raw values of amino acids withour mass conversion.
    const RAW_AMINO_ACID_VALUES: [(&'static str, char, &'static str, f64, f64); 26] = [
        ("Alanine",                         'A',    "Ala",  71.037113805_f64,   71.0788_f64),
        ("Cysteine",                        'C',    "Cys",  103.009184505_f64,  103.1388_f64),
        ("Aspartic acid",                   'D',    "Asp",  115.026943065_f64,  115.0886_f64),
        ("Glutamic acid",                   'E',    "Glu",  129.042593135_f64,  129.1155_f64),
        ("Phenylalanine",                   'F',    "Phe",  147.068413945_f64,  147.1766_f64),
        ("Glycine",                         'G',    "Gly",  57.021463735_f64,   57.0519_f64),
        ("Histidine",                       'H',    "His",  137.058911875_f64,  137.1411_f64),
        ("Isoleucine",                      'I',    "Ile",  113.084064015_f64,  113.1594_f64),
        ("Lysine",                          'K',    "Lys",  128.094963050_f64,  128.1741_f64),
        ("Leucine",                         'L',    "Leu",  113.084064015_f64,  113.1594_f64),
        ("Methionine",                      'M',    "Met",  131.040484645_f64,  131.1926_f64),
        ("Asparagine",                      'N',    "Asn",  114.042927470_f64,  114.1038_f64),
        ("Pyrrolysine",                     'O',    "Pyl",  237.147726925_f64,  237.29816_f64),
        ("Proline",                         'P',    "Pro",  97.052763875_f64,   97.1167_f64),
        ("Glutamine",                       'Q',    "Gln",  128.05857754_f64,   128.1307_f64),
        ("Arginine",                        'R',    "Arg",  156.101111050_f64,  156.1875_f64),
        ("Serine",                          'S',    "Ser",  87.032028435_f64,   87.0782_f64),
        ("Threonine",                       'T',    "Thr",  101.047678505_f64,  101.1051_f64),
        ("Selenocysteine",                  'U',    "SeC",  150.953633405_f64,  150.0379_f64),
        ("Valine",                          'V',    "Val",  99.068413945_f64,   99.1326_f64),
        ("Tryptophan",                      'W',    "Trp",  186.079312980_f64,  186.2132_f64),
        ("Tyrosine",                        'Y',    "Tyr",  163.063328575_f64,  163.1760_f64),
        ("Asparagine or aspartic acid",     'B',    "Asx",  114.5349352675_f64, 114.59502_f64),
        ("Isoleucine or Leucine",           'J',    "Xle",  113.084064015_f64,  113.1594_f64),
        ("Glutamine or glutamic acid",      'Z',    "Glx",  128.5505853375_f64, 128.6216_f64),
        ("Unknown Amino Acid",              'X',    "Xaa",  0.0_f64,            0.0_f64)
    ];

    // Test if the amino acids attributes are equal to to the tuple values. This ensures, that the tuple and the amino acid is matching and if the mass is converted successfully with the macro.
    fn test_equality_of_amino_acid_attributes_and_raw_value_tuple(amino_acid: &AminoAcid, raw_value_tuple: &(&'static str, char, &'static str, f64, f64)) {
        assert_eq!(amino_acid.name, raw_value_tuple.0);
        assert_eq!(amino_acid.one_letter_code, raw_value_tuple.1);
        assert_eq!(amino_acid.three_letter_code, raw_value_tuple.2);
        assert_eq!(amino_acid.mono_mass, mass_to_int(raw_value_tuple.3));
        assert_eq!(amino_acid.average_mass, mass_to_int(raw_value_tuple.4));
    }

    #[test]
    fn test_get_by_one_letter_code_and_value_correctness() {
        for raw_value_tuple in &RAW_AMINO_ACID_VALUES {
            let amino_acid: &AminoAcid = AminoAcid::get_by_one_letter_code(raw_value_tuple.1).unwrap();
            test_equality_of_amino_acid_attributes_and_raw_value_tuple(amino_acid, raw_value_tuple)
        }  
    }

    #[test]
    fn test_all_array_and_value_correctness() {
        let known_amino_acids: &[&'static AminoAcid; 26] = AminoAcid::get_all();


        // Test if all raw value tuples are found in known_amino_acids
        'tuple_loop: for raw_value_tuple in &RAW_AMINO_ACID_VALUES {
            for amino_acid in known_amino_acids {
                if raw_value_tuple.1 == amino_acid.one_letter_code {
                    test_equality_of_amino_acid_attributes_and_raw_value_tuple(amino_acid, raw_value_tuple);
                    // Start next iteration of outer loop, to omit the panic.
                    continue 'tuple_loop;
                }
            }
            // This panic can only be reached when the matching amino acid was not found in known_amino_acids.
            println!("Did not found {} in `known_amino_acids`.", raw_value_tuple.0);
            panic!("See error above.");
        }

        // Test if all known amino acids are found in the raw value tuples
        'amino_acid_loop: for amino_acid in known_amino_acids {
            for raw_value_tuple in &RAW_AMINO_ACID_VALUES {
                if raw_value_tuple.1 == amino_acid.one_letter_code {
                    test_equality_of_amino_acid_attributes_and_raw_value_tuple(amino_acid, raw_value_tuple);
                    // Start next iteration of outer loop, to omit the panic.
                    continue 'amino_acid_loop;
                }
            }
            // This panic can only be reached when the matching raw value tuple was not found in RAW_AMINO_ACID_VALUES.
            println!("Did not found {} in `RAW_AMINO_ACID_VALUES`.", amino_acid.name);
            panic!("See error above.");
        }
    }
}