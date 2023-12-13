// 3rd party import
use serde::Deserialize;

/// Amino acid entity
///
#[derive(Deserialize, Debug)]
pub struct AminoAcid {
    name: String,
    code: char,
    abbreviation: String,
    mono_mass: f64,
    // not in use yet
    // average_mass: f64,
}

impl AminoAcid {
    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_code(&self) -> &char {
        &self.code
    }

    pub fn get_abbreviation(&self) -> &String {
        &self.abbreviation
    }

    pub fn get_mono_mass(&self) -> &f64 {
        &self.mono_mass
    }

    // pub fn get_average_mass(&self) -> &f64 {
    //     &self.average_mass
    // }
}
