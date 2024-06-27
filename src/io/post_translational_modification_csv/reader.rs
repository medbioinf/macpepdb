// std imports
use std::path::Path;

// 3rd party imports
use anyhow::Result;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;

/// Reader for PTMs from a CSV file, e.g.
/// ```csv
/// name,amino_acid,mass_delta,mod_type,position
/// Carbamidomethyl,C,57.021464,Static,Anywhere
/// Oxidation,M,15.994915,Variable,Anywhere
/// ```
///
pub struct Reader {}

impl Reader {
    /// Read PTMs from the given CSV file
    ///
    /// # Arguments
    /// * `path` - The path to the CSV file
    ///
    pub fn read(path: &Path) -> Result<Vec<PTM>> {
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b',')
            .from_path(path)?;

        reader
            .into_deserialize::<PTM>()
            .into_iter()
            .map(|ptm_result| match ptm_result {
                Ok(ptm) => Ok(ptm),
                Err(e) => Err(anyhow::Error::new(e)),
            })
            .collect()
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
