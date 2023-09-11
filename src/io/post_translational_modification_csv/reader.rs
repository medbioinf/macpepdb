// std imports
use std::path::Path;
use std::str::FromStr;

// 3rd party imports
use anyhow::Result;
use dihardts_omicstools::{
    chemistry::amino_acid::get_amino_acid_by_one_letter_code,
    proteomics::post_translational_modifications::{
        ModificationType, Position, PostTranslationalModification as PTM,
    },
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct PostTranslationalModificationCsvRecord {
    name: String,
    amino_acids: String,
    mass_delta: f64,
    mod_type: String,
    position: String,
}

pub struct Reader {}

impl Reader {
    pub fn read(path: &Path) -> Result<Vec<PTM>> {
        let mut reader = csv::Reader::from_path(path)?;
        let mut ptms: Vec<PTM> = Vec::new();
        for record_res in reader.deserialize() {
            let record: PostTranslationalModificationCsvRecord = record_res?;
            for aa_code in record.amino_acids.chars() {
                let amino_acid = get_amino_acid_by_one_letter_code(aa_code)?;
                ptms.push(PTM::new(
                    &record.name.as_str(),
                    amino_acid,
                    record.mass_delta,
                    ModificationType::from_str(&record.mod_type)?,
                    Position::from_str(&record.position)?,
                ));
            }
        }
        return Ok(ptms);
    }
}

#[cfg(test)]
mod test {
    // internal imports
    use super::*;

    #[test]
    fn test_read() {
        let mod_csv = Path::new("test_files/mods.csv");
        let ptms = Reader::read(mod_csv).unwrap();

        assert_eq!(ptms.len(), 2);
    }
}
