// 3rd party imports
use fancy_regex::Regex;

// internal imports
use crate::biology::digestion_enzyme::enzyme::Enzyme;

/// Name of the enzyme
pub const NAME: &'static str = "trypsin";

lazy_static! {
    static ref CLEAVAGE_SITE_REGEX: Regex = Regex::new("(?<=[KR])(?!P)").unwrap();
}

/// Digestion enzyme Trypsin, which cuts after K and R not followed by P.
///
pub struct Trypsin {
    max_number_of_missed_cleavages: usize,
    min_peptide_length: usize,
    max_peptide_length: usize,
}

impl Enzyme for Trypsin {
    fn new(
        max_number_of_missed_cleavages: usize,
        min_peptide_length: usize,
        max_peptide_length: usize,
    ) -> Self {
        return Self {
            max_number_of_missed_cleavages,
            min_peptide_length,
            max_peptide_length,
        };
    }

    fn get_cleavages_site_regex(&self) -> &Regex {
        return &CLEAVAGE_SITE_REGEX;
    }

    fn get_max_number_of_missed_cleavages(&self) -> usize {
        return self.max_number_of_missed_cleavages;
    }

    fn get_min_peptide_length(&self) -> usize {
        return self.min_peptide_length;
    }

    fn get_max_peptide_length(&self) -> usize {
        return self.max_peptide_length;
    }

    fn get_name(&self) -> &str {
        return NAME;
    }
}

#[cfg(test)]
mod test {
    // std imports
    use std::collections::HashMap;

    // internal imports
    use super::*;

    lazy_static! {
        // Peptides for Leptin (UniProt accession Q257X2, with KP on first position) digested with 3 missed cleavages, length 0 - 60
        // Tested with https://web.expasy.org/peptide_mass/
        // Leucine and Isoleucine are replaced with J already!
        static ref DESIRED_RESULTS: HashMap<String, i16> = collection! {
            "VTGLDFIPGLHPLLSLSKMDQTLAIYQQILASLPSRNVIQISNDLENLRDLLHLLAASK".to_string() => 3,
            "NVIQISNDLENLRDLLHLLAASKSCPLPQVRALESLESLGVVLEASLYSTEVVALSR".to_string() => 3,
            "DLLHLLAASKSCPLPQVRALESLESLGVVLEASLYSTEVVALSRLQGSLQDMLR".to_string() => 3,
            "QRVTGLDFIPGLHPLLSLSKMDQTLAIYQQILASLPSRNVIQISNDLENLR".to_string() => 3,
            "INDISHTQSVSSKQRVTGLDFIPGLHPLLSLSKMDQTLAIYQQILASLPSR".to_string() => 3,
            "SCPLPQVRALESLESLGVVLEASLYSTEVVALSRLQGSLQDMLRQLDLSPGC".to_string() => 3,
            "MDQTLAIYQQILASLPSRNVIQISNDLENLRDLLHLLAASKSCPLPQVR".to_string() => 3,
            "VTGLDFIPGLHPLLSLSKMDQTLAIYQQILASLPSRNVIQISNDLENLR".to_string() => 2,
            "SCPLPQVRALESLESLGVVLEASLYSTEVVALSRLQGSLQDMLR".to_string() => 2,
            "ALESLESLGVVLEASLYSTEVVALSRLQGSLQDMLRQLDLSPGC".to_string() => 2,
            "DLLHLLAASKSCPLPQVRALESLESLGVVLEASLYSTEVVALSR".to_string() => 2,
            "MDQTLAIYQQILASLPSRNVIQISNDLENLRDLLHLLAASK".to_string() => 2,
            "QRVTGLDFIPGLHPLLSLSKMDQTLAIYQQILASLPSR".to_string() => 2,
            "TIVTRINDISHTQSVSSKQRVTGLDFIPGLHPLLSLSK".to_string() => 3,
            "VTGLDFIPGLHPLLSLSKMDQTLAIYQQILASLPSR".to_string() => 1,
            "ALESLESLGVVLEASLYSTEVVALSRLQGSLQDMLR".to_string() => 1,
            "CGPLYRFLWLWPYLSYVEAVPIRKVQDDTK".to_string() => 3,
            "SCPLPQVRALESLESLGVVLEASLYSTEVVALSR".to_string() => 1,
            "INDISHTQSVSSKQRVTGLDFIPGLHPLLSLSK".to_string() => 2,
            "MDQTLAIYQQILASLPSRNVIQISNDLENLR".to_string() => 1,
            "NVIQISNDLENLRDLLHLLAASKSCPLPQVR".to_string() => 2,
            "FLWLWPYLSYVEAVPIRKVQDDTKTLIK".to_string() => 3,
            "KPMRCGPLYRFLWLWPYLSYVEAVPIRK".to_string() => 3,
            "KPMRCGPLYRFLWLWPYLSYVEAVPIR".to_string() => 2,
            "VQDDTKTLIKTIVTRINDISHTQSVSSK".to_string() => 3,
            "CGPLYRFLWLWPYLSYVEAVPIRK".to_string() => 2,
            "FLWLWPYLSYVEAVPIRKVQDDTK".to_string() => 2,
            "CGPLYRFLWLWPYLSYVEAVPIR".to_string() => 1,
            "ALESLESLGVVLEASLYSTEVVALSR".to_string() => 0,
            "TLIKTIVTRINDISHTQSVSSKQR".to_string() => 3,
            "NVIQISNDLENLRDLLHLLAASK".to_string() => 1,
            "TLIKTIVTRINDISHTQSVSSK".to_string() => 2,
            "FLWLWPYLSYVEAVPIRK".to_string() => 1,
            "TIVTRINDISHTQSVSSKQR".to_string() => 2,
            "QRVTGLDFIPGLHPLLSLSK".to_string() => 1,
            "FLWLWPYLSYVEAVPIR".to_string() => 0,
            "MDQTLAIYQQILASLPSR".to_string() => 0,
            "TIVTRINDISHTQSVSSK".to_string() => 1,
            "LQGSLQDMLRQLDLSPGC".to_string() => 1,
            "DLLHLLAASKSCPLPQVR".to_string() => 1,
            "VTGLDFIPGLHPLLSLSK".to_string() => 0,
            "KVQDDTKTLIKTIVTR".to_string() => 3,
            "VQDDTKTLIKTIVTR".to_string() => 2,
            "INDISHTQSVSSKQR".to_string() => 1,
            "NVIQISNDLENLR".to_string() => 0,
            "INDISHTQSVSSK".to_string() => 0,
            "KVQDDTKTLIK".to_string() => 2,
            "VQDDTKTLIK".to_string() => 1,
            "LQGSLQDMLR".to_string() => 0,
            "DLLHLLAASK".to_string() => 0,
            "TLIKTIVTR".to_string() => 1,
            "KPMRCGPLYR".to_string() => 1,
            "SCPLPQVR".to_string() => 0,
            "KVQDDTK".to_string() => 1,
            "QLDLSPGC".to_string() => 0,
            "CGPLYR".to_string() => 0,
            "VQDDTK".to_string() => 0,
            "TIVTR".to_string() => 0,
            "TLIK".to_string() => 0,
            "KPMR".to_string() => 0,
            "QR".to_string() => 0,
            "K".to_string() => 0
        };
    }

    #[test]
    fn test_digest() {
        // Using Leptin (UniProt  accession: Q257X2) with KP on first position to make sure Trypin-implementation skips it
        let leptin: &'static str = "KPMRCGPLYRFLWLWPYLSYVEAVPIRKVQDDTKTLIKTIVTRINDISHTQSVSSKQRVTGLDFIPGLHPLLSLSKMDQTLAIYQQILASLPSRNVIQISNDLENLRDLLHLLAASKSCPLPQVRALESLESLGVVLEASLYSTEVVALSRLQGSLQDMLRQLDLSPGC";
        let trypsin: Trypsin = Trypsin {
            max_number_of_missed_cleavages: 3,
            min_peptide_length: 0,
            max_peptide_length: 60,
        };
        let peptides: HashMap<String, i16> = trypsin.digest(leptin);

        assert_eq!(DESIRED_RESULTS.len(), peptides.len());

        for (peptide, missed_cleavages) in &peptides {
            assert!(
                DESIRED_RESULTS.contains_key(peptide),
                "Peptide {} not found",
                peptide
            );
            assert_eq!(DESIRED_RESULTS[peptide], *missed_cleavages);
        }
    }
}
