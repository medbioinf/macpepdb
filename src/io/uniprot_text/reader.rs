/// Contains reader for UniProt text files.
// std imports
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::path::Path;

// 3rd party imports
use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use fallible_iterator::FallibleIterator;
use log::error;
use tracing::warn;

use crate::entities::domain::Domain;
// internal imports
use crate::entities::protein::Protein;

/// Identifier for reviewed entries
const IS_REVIEWED_STRING: &'static str = "Reviewed;";
/// Identifier for proteome ID
const DR_PROTEOMES_IDENTIFIER: &'static str = "Proteomes;";
/// End index of identifier for proteome ID
const DR_PROTEOME_IDENTIFIED_END: usize = DR_PROTEOMES_IDENTIFIER.len() + 5; // 5 = length of "ID   "
/// Identifier for recommended name
const DE_RECNAME_IDENTIFIER: &'static str = "RecName";
/// Identifier for alternative name
const DE_ALTNAME_IDENTIFIER: &'static str = "AltName";
/// Identifier for full name
const DE_FULL_IDENTIFIER: &'static str = "Full";
/// Attribute for name in gene line
const GN_NAME_ATTRIBUTE: &'static str = "Name=";
/// Attribute for synonyms in gene line
const GN_SYNONYMS_ATTRIBUTE: &'static str = "Synonyms=";

/// Reader for Uniprot text files
/// <https://web.expasy.org/docs/userman.html>
pub struct Reader {
    internal_reader: BufReader<File>,
}

impl Reader {
    /// Creates a new Reader
    ///
    /// # Arguments
    ///
    /// * `uniprot_txt_file_path` - Path to UniProt text file
    ///
    pub fn new(uniprot_txt_file_path: &Path, buffer_size: usize) -> Result<Self> {
        let uniprot_txt_file: File = File::open(uniprot_txt_file_path)?;
        Ok(Self {
            internal_reader: BufReader::with_capacity(buffer_size, uniprot_txt_file),
        })
    }

    /// Resets the reader to the beginning of the file
    ///
    pub fn reset(&mut self) -> Result<()> {
        self.internal_reader.seek(SeekFrom::Start(0))?;
        return Ok(());
    }

    /// Returns the number of entries in the file.
    /// This is much faster than iterating over all entries.
    /// Attention: Resets the reader to the beginning of the file.
    ///
    pub fn count_proteins(&mut self) -> Result<usize> {
        let mut count: usize = 0;
        let mut line = String::new();
        self.reset()?;
        while let Ok(num_bytes) = self.internal_reader.read_line(&mut line) {
            if num_bytes == 0 {
                break;
            }
            if line.starts_with("//") {
                count += 1;
            }
            line.clear();
        }
        self.reset()?;
        Ok(count)
    }
}

impl FallibleIterator for Reader {
    type Item = Protein;
    type Error = anyhow::Error;

    fn next(&mut self) -> Result<Option<Self::Item>> {
        let mut accessions: Vec<String> = Vec::new();
        let mut entry_name: String = String::new();
        let mut name: String = String::new();
        let mut genes: Vec<String> = Vec::new();
        let mut taxonomy_id: i64 = 0;
        let mut proteome_id: String = String::new();
        let mut is_reviewed: bool = false;
        let mut sequence: String = String::new();
        let mut updated_at: i64 = -1;
        let mut domains: Vec<Domain> = Vec::new();

        let mut in_entry: bool = false;
        let mut last_de_line_category: String = String::new();

        let mut domain_start_idx: i64 = 0;
        let mut domain_end_idx: i64 = 0;
        let mut domain_name: String = "".to_string();
        let mut domain_evidence: String;
        let mut is_building_domain = false;
        let mut ft_type: String = "".to_string();

        loop {
            let mut line = String::new();
            if let Ok(num_bytes) = self.internal_reader.read_line(&mut line) {
                if num_bytes == 0 {
                    if in_entry {
                        bail!("reach EOF before end of entry".to_string());
                    }
                    return Ok(None);
                }
                line = line.as_mut_str().trim_end().to_string();
                if line.is_empty() {
                    continue;
                }

                match &line[..2] {
                    "ID" => {
                        // Process ID line by splitting at whitespaces. First element is the entry name, second is the review status.
                        in_entry = true;
                        let mut split = line[5..].split_ascii_whitespace();
                        entry_name = split
                            .next()
                            .ok_or(anyhow::anyhow!("no entry name"))?
                            .to_string();
                        is_reviewed = split
                            .next()
                            .ok_or(anyhow::anyhow!("no review status"))?
                            .to_string()
                            == IS_REVIEWED_STRING;
                    }
                    "AC" => {
                        // Process AC line by splitting at semicolons, trimming whitespaces of each element and adding all to accessions.
                        let split = line[5..].split(";");
                        accessions.extend(
                            split
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty()),
                        );
                    }
                    "OX" => {
                        // Process OX line by parsing the taxonomy ID after 'NCBI_TaxID=' up to the following semicolon
                        // Get end of taxonomy ID which is either the next whitespace or the end of the line
                        let taxonomy_id_end = match line[16..].find(" ") {
                            Some(match_idx) => match_idx + 16,
                            None => line.len() - 1,
                        };
                        taxonomy_id =
                            line[16..taxonomy_id_end].parse::<i64>().with_context(|| {
                                format!(
                                    "could not parse taxonomy ID from line: {},\n parsing: {}",
                                    line,
                                    &line[16..line.len() - 1]
                                )
                            })?;
                    }
                    "DR" => {
                        /*
                         * Process DR line by checking if it is a proteome ID line and if so,
                         * parsing the proteome ID after 'Proteomes;' up to the next semicolon
                         */

                        if &line[5..DR_PROTEOME_IDENTIFIED_END] == DR_PROTEOMES_IDENTIFIER {
                            let proteome_id_end = line[DR_PROTEOME_IDENTIFIED_END..]
                                .find(";")
                                .unwrap_or(line.len() - 1);
                            proteome_id = line[DR_PROTEOME_IDENTIFIED_END
                                ..DR_PROTEOME_IDENTIFIED_END + proteome_id_end]
                                .trim()
                                .to_string();
                        }
                    }
                    "DE" => {
                        /*
                         * Process DE line by checking if it is a recommended or alternative name line and if so,
                         * parsing the full name after 'Full='
                         */

                        if !name.is_empty() {
                            continue;
                        }
                        let category = line[5..12].to_string();
                        if !category.is_empty() {
                            last_de_line_category = category;
                        }
                        if last_de_line_category == DE_RECNAME_IDENTIFIER
                            || last_de_line_category == DE_ALTNAME_IDENTIFIER
                        {
                            let mut split = line[14..].split("=");
                            let subcategory = split
                                .next()
                                .ok_or(anyhow::anyhow!("no subcategory"))?
                                .trim()
                                .to_string();
                            if subcategory == DE_FULL_IDENTIFIER {
                                name = split
                                    .next()
                                    .ok_or(anyhow::anyhow!("no name"))?
                                    .trim()
                                    .to_string();
                                name.pop();
                            }
                        }
                    }
                    "DT" => {
                        /*
                         * Process DT line by parsing the date to unix timestamp.
                         */
                        let date_end = line.find(',').unwrap_or(line.len() - 1);
                        updated_at =
                            NaiveDate::parse_from_str(line[5..date_end].trim(), "%d-%b-%Y")?
                                .and_hms_opt(0, 0, 0)
                                .ok_or(anyhow::anyhow!("no date"))?
                                .timestamp()
                    }
                    "GN" => {
                        /*
                         * Process GN line by parsing the gene name after 'Name=' and the gene synonyms after 'Synonyms='
                         */
                        if let Some(mut name_start) = line.find(GN_NAME_ATTRIBUTE) {
                            name_start += GN_NAME_ATTRIBUTE.len();
                            let name_end = match line[name_start..].find(';') {
                                Some(match_idx) => match_idx + name_start,
                                None => line.len() - 1,
                            };
                            genes.push(line[name_start..name_end].trim().to_string());
                        }
                        if let Some(mut synonyms_start) = line.find(GN_SYNONYMS_ATTRIBUTE) {
                            synonyms_start += GN_SYNONYMS_ATTRIBUTE.len();
                            let synonyms_end = match line[synonyms_start..].find(';') {
                                Some(match_idx) => match_idx + synonyms_start,
                                None => line.len() - 1,
                            };
                            genes.extend(
                                line[synonyms_start..synonyms_end]
                                    .trim()
                                    .split(",")
                                    .map(|s| s.trim().to_string()),
                            );
                        }
                    }
                    "FT" => {
                        if !is_building_domain {
                            ft_type = line[5..13].to_string();
                            if ft_type == "TOPO_DOM"
                                || ft_type == "TRANSMEM"
                                || ft_type == "INTRAMEM"
                            {
                                is_building_domain = true;
                                let indices_list: Vec<_> = line[13..]
                                    .trim()
                                    .replace("<", "")
                                    .replace(">", "")
                                    .replace("?", "")
                                    .split("..")
                                    .map(|s| {
                                        s.parse::<i64>()
                                            .map_err(|x| error!("{} {} {:?}", x, line, accessions))
                                    })
                                    .collect();

                                if indices_list[0].is_err() {
                                    warn!(
                                        "Could not process domain {:?}",
                                        indices_list[0].unwrap_err()
                                    );
                                    is_building_domain = false;
                                } else if indices_list.len() > 1 && indices_list[1].is_err() {
                                    warn!(
                                        "Could not process domain {:?}",
                                        indices_list[1].unwrap_err()
                                    );
                                    is_building_domain = false;
                                } else if indices_list.len() > 1 {
                                    domain_start_idx = indices_list[0].unwrap();
                                    domain_end_idx = indices_list[1].unwrap();
                                } else {
                                    domain_start_idx = indices_list[0].unwrap();
                                    domain_end_idx = indices_list[0].unwrap();
                                }
                            }
                        } else {
                            let s = line[13..].trim();
                            if s.starts_with("/note") && ft_type == "TOPO_DOM" {
                                domain_name = s[7..s.len() - 1].to_string();
                            }
                            if s.starts_with("/evidence") {
                                if domain_name == "" {
                                    if ft_type == "TRANSMEM" {
                                        domain_name = "Transmembrane".to_string();
                                    } else if ft_type == "INTRAMEM" {
                                        domain_name = "Intramembrane".to_string();
                                    }
                                }
                                domain_evidence = s[11..s.len() - 1].to_string();
                                domains.push(Domain::new(
                                    domain_start_idx - 1,
                                    domain_end_idx - 1,
                                    domain_name.clone(),
                                    domain_evidence.clone(),
                                    None,
                                    None,
                                    None,
                                    None,
                                ));
                                domain_name = "".to_string();
                                is_building_domain = false;
                            }
                        }
                    }
                    "  " => {
                        for chunk in line[5..].split_ascii_whitespace() {
                            sequence.push_str(chunk);
                        }
                    }
                    "//" => {
                        let accession = accessions.remove(0);
                        return Ok(Some(Protein::new(
                            accession,
                            accessions,
                            entry_name,
                            name,
                            genes,
                            taxonomy_id,
                            proteome_id,
                            is_reviewed,
                            sequence,
                            updated_at,
                            domains,
                        )));
                    }
                    _ => {
                        continue;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // internal imports
    use super::*;

    const EXPECTED_ACCESSION: [&str; 3] = ["P07477", "P41160", "P78562"];

    const EXPECTED_NAMES: [&str; 3] = [
        "Serine protease 1 {ECO:0000312|HGNC:HGNC:9475}",
        "Leptin",
        "Phosphate-regulating neutral endopeptidase PHEX",
    ];

    const EXPECTED_ENTRY_NAMES: [&str; 3] = ["TRY1_HUMAN", "LEP_MOUSE", "PHEX_HUMAN"];

    const EXPECTED_TAXONOMY_IDS: [i64; 3] = [9606, 10090, 9606];

    const EXPECTED_PROTEOME_IDS: [&'static str; 3] = ["UP000005640", "UP000000589", "UP000005640"];

    const EXPECTED_REVIEW_STATUS: [bool; 3] = [true, false, true];

    const EXPECTED_SEQUENCES: [&'static str; 3] = [
        "MNPLLILTFVAAALAAPFDDDDKIVGGYNCEENSVPYQVSLNSGYHFCGGSLINEQWVVSAGHCYKSRIQVRLGEHNIEVLEGNEQFINAAKIIRHPQYDRKTLNNDIMLIKLSSRAVINARVSTISLPTAPPATGTKCLISGWGNTASSGADYPDELQCLDAPVLSQAKCEASYPGKITSNMFCVGFLEGGKDSCQGDSGGPVVCNGQLQGVVSWGDGCAQKNKPGVYTKVYNYVKWIKNTIAANS",
        "MCWRPLCRFLWLWSYLSYVQAVPIQKVQDDTKTLIKTIVTRINDISHTQSVSAKQRVTGLDFIPGLHPILSLSKMDQTLAVYQQVLTSLPSQNVLQIANDLENLRDLLHLLAFSKSCSLPQTSGLQKPESLDGVLEASLYSTEVVALSRLQGSLQDILQQLDVSPEC",
        "MEAETGSSVETGKKANRGTRIALVVFVGGTLVLGTILFLVSQGLLSLQAKQEYCLKPECIEAAAAILSKVNLSVDPCDNFFRFACDGWISNNPIPEDMPSYGVYPWLRHNVDLKLKELLEKSISRRRDTEAIQKAKILYSSCMNEKAIEKADAKPLLHILRHSPFRWPVLESNIGPEGVWSERKFSLLQTLATFRGQYSNSVFIRLYVSPDDKASNEHILKLDQATLSLAVREDYLDNSTEAKSYRDALYKFMVDTAVLLGANSSRAEHDMKSVLRLEIKIAEIMIPHENRTSEAMYNKMNISELSAMIPQFDWLGYIKKVIDTRLYPHLKDISPSENVVVRVPQYFKDLFRILGSERKKTIANYLVWRMVYSRIPNLSRRFQYRWLEFSRVIQGTTTLLPQWDKCVNFIESALPYVVGKMFVDVYFQEDKKEMMEELVEGVRWAFIDMLEKENEWMDAGTKRKAKEKARAVLAKVGYPEFIMNDTHVNEDLKAIKFSEADYFGNVLQTRKYLAQSDFFWLRKAVPKTEWFTNPTTVNAFYSASTNQIRFPAGELQKPFFWGTEYPRSLSYGAIGVIVGHEFTHGFDNNGRKYDKNGNLDPWWSTESEEKFKEKTKCMINQYSNYYWKKAGLNVKGKRTLGENIADNGGLREAFRAYRKWINDRRQGLEEPLLPGITFTNNQLFFLSYAHVRCNSYRPEAAREQVQIGAHSPPQFRVNGAISNFEEFQKAFNCPPNSTMNRGMDSCRLW"
    ];

    const EXPECTED_UPDATED_AT: [i64; 3] = [1677024000, 791596800, 1677024000];

    //     FT   DOMAIN          24..244
    // FT                   /note="Peptidase S1"
    // FT                   /evidence="ECO:0000255|PROSITE-ProRule:PRU00274"

    lazy_static! {
        static ref EXPECTED_SECONDARY_ACCESSION: Vec<Vec<&'static str>> = vec![
            vec![
                "A1A509", "A6NJ71", "B2R5I5", "Q5NV57", "Q7M4N3", "Q7M4N4", "Q92955", "Q9HAN4",
                "Q9HAN5", "Q9HAN6", "Q9HAN7"
            ],
            vec![],
            vec!["O00678", "Q13646", "Q2M325", "Q93032", "Q99827"],
        ];
        static ref EXPECTED_GENES: Vec<Vec<&'static str>> = vec![
            vec![
                "PRSS1 {ECO:0000312|HGNC:HGNC:9475}",
                "TRP1",
                "TRY1 {ECO:0000312|HGNC:HGNC:9475}",
                "TRYP1"
            ],
            vec!["Lep", "Ob"],
            vec!["PHEX", "PEX"],
        ];
    }

    #[test]
    fn test_reader() {
        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        let mut ctr = 0;

        // let expected_domain: Domain = Domain::new(
        //     23,
        //     243,
        //     "Peptidase S1".to_string(),
        //     "ECO:0000255|PROSITE-ProRule:PRU00274".to_string(),
        //     None,
        //     None,
        //     None,
        //     None,
        // );

        while let Some(protein) = reader.next().unwrap() {
            assert_eq!(
                protein.get_accession(),
                EXPECTED_ACCESSION.get(ctr).unwrap()
            );
            assert_eq!(
                protein.get_secondary_accessions().len(),
                EXPECTED_SECONDARY_ACCESSION.get(ctr).unwrap().len()
            );
            for exp_sec_acc in EXPECTED_SECONDARY_ACCESSION.get(ctr).unwrap() {
                assert!(
                    protein
                        .get_secondary_accessions()
                        .contains(&exp_sec_acc.to_string()),
                    "Secondary accession \"{}\" not found in protein {:?}",
                    exp_sec_acc,
                    protein.get_secondary_accessions()
                );
            }
            assert_eq!(
                protein.get_entry_name(),
                EXPECTED_ENTRY_NAMES.get(ctr).unwrap()
            );
            assert_eq!(protein.get_name(), EXPECTED_NAMES.get(ctr).unwrap());
            assert_eq!(
                protein.get_genes().len(),
                EXPECTED_GENES.get(ctr).unwrap().len()
            );
            for exp_gene in EXPECTED_GENES.get(ctr).unwrap() {
                assert!(
                    protein.get_genes().contains(&exp_gene.to_string()),
                    "Gene \"{}\" not found in protein {:?}",
                    exp_gene,
                    protein.get_genes()
                );
            }
            assert_eq!(
                protein.get_taxonomy_id(),
                EXPECTED_TAXONOMY_IDS.get(ctr).unwrap()
            );
            assert_eq!(
                protein.get_proteome_id(),
                EXPECTED_PROTEOME_IDS.get(ctr).unwrap()
            );
            assert_eq!(
                protein.get_is_reviewed(),
                *EXPECTED_REVIEW_STATUS.get(ctr).unwrap()
            );
            assert_eq!(protein.get_sequence(), EXPECTED_SEQUENCES.get(ctr).unwrap());
            assert_eq!(
                protein.get_updated_at(),
                *EXPECTED_UPDATED_AT.get(ctr).unwrap()
            );

            // if ctr == 0 {
            //     assert_eq!(protein.get_domains(), &vec![expected_domain.to_owned()])
            // }

            ctr += 1;
        }
        assert_eq!(ctr, 3);
    }

    #[test]
    fn test_count_proteins() {
        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        assert_eq!(reader.count_proteins().unwrap(), 3);
    }
}
