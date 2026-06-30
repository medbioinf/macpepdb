use std::fmt::Display;

use itertools::Itertools;
use serde::Serialize;
use thiserror::Error;
use tokio_postgres::Row;

use crate::{
    amino_acid::AminoAcid,
    sequence::{IsBitSequence, IsSimpleSequence, ProteinSequence as Sequence},
};

static NCBI_TAXONOMY_ID_ATTRIBUTE_NAME: &str = "NCBI_TaxID=";

const IS_REVIEWED_BIT: usize = 0;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Taxonomy ID capture `{0}` does not contain ID group")]
    EmptyTaxonomyIdCapture(String),
    #[error("Unable find `{NCBI_TAXONOMY_ID_ATTRIBUTE_NAME}` in OX line `{0}`")]
    MissingTaxonomyIdStart(String),
    #[error("Sequence error in protein: {0}")]
    Sequence(#[from] crate::sequence::Error),
    #[error("Unable to parse taxonomy ID as intege: {0}")]
    TaxonomyIdParsing(std::num::ParseIntError),
    #[error("Row decoding error in protein: {0}")]
    Row(#[from] tokio_postgres::Error),
}

#[derive(Clone, Debug, Serialize)]
pub struct Protein {
    accession: String,
    id: Option<i32>,
    sequence: Sequence,
    taxonomy_id: i32,
    /// Bit flags for e.g. review status, see constants to see what is stored in which bit
    flags: i8,
    genes: Vec<String>,
}

impl Protein {
    pub fn new(
        accession: String,
        id: Option<i32>,
        sequence: Sequence,
        taxonomy_id: i32,
        is_reviewed: bool,
        genes: Vec<String>,
    ) -> Self {
        let mut flags = 0b0000_0000;
        if is_reviewed {
            flags |= 1 << IS_REVIEWED_BIT;
        }

        Self {
            accession,
            id,
            sequence,
            taxonomy_id,
            flags,
            genes,
        }
    }

    pub fn accession(&self) -> &str {
        &self.accession
    }

    pub fn sequence(&self) -> &Sequence {
        &self.sequence
    }

    pub fn is_reviewed(&self) -> bool {
        (self.flags & (1 << IS_REVIEWED_BIT)) != 0
    }

    pub fn id(&self) -> Option<i32> {
        self.id
    }

    pub fn taxonomy_id(&self) -> i32 {
        self.taxonomy_id
    }

    pub fn flags(&self) -> i8 {
        self.flags
    }

    pub fn flags_as_ref(&self) -> &i8 {
        &self.flags
    }

    pub fn genes(&self) -> &Vec<String> {
        &self.genes
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + std::mem::size_of::<String>()
            + self.accession.len()
            + self.sequence.size()
            + std::mem::size_of::<i32>() // 4 for id and taxonomy_id
            + std::mem::size_of::<Vec<String>>() + self.genes.iter().map(|g| std::mem::size_of::<String>() + g.len()).sum::<usize>()
    }
}

/// Splits a UniProt gene-name group value (the part after `=`) on commas that are
/// outside `{...}` evidence annotations, stripping the annotations and trimming each
/// gene name. Works in a single pass with no intermediate allocations.
fn split_genes_stripping_evidence(s: &str) -> impl Iterator<Item = String> + '_ {
    let mut chars = s.chars();
    let mut depth = 0usize;
    std::iter::from_fn(move || {
        let mut gene = String::new();
        loop {
            match chars.next() {
                None => {
                    let t = gene.trim().to_string();
                    return if t.is_empty() { None } else { Some(t) };
                }
                Some('{') => depth += 1,
                Some('}') if depth > 0 => depth -= 1,
                Some(',') if depth == 0 => {
                    let t = gene.trim().to_string();
                    if !t.is_empty() {
                        return Some(t);
                    }
                }
                Some(c) if depth == 0 => gene.push(c),
                _ => {}
            }
        }
    })
}

impl Display for Protein {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "accession  : {}", self.accession)?;
        writeln!(
            f,
            "id         : {}",
            self.id
                .map(|id| id.to_string())
                .unwrap_or("not set".to_string())
        )?;
        let mut prefix = "sequence   : ".to_string();
        for chunk in &self.sequence().amino_acid_bit_codes().chunks(60) {
            writeln!(
                f,
                "{prefix}{}",
                chunk
                    .map(|aa_bit| AminoAcid::by_bit_code(aa_bit).code())
                    .collect::<String>()
            )?;
            prefix = "             ".to_string();
        }
        writeln!(f, "taxonomy ID: {}", self.taxonomy_id)?;
        writeln!(
            f,
            "SwissProt? : {}",
            if self.is_reviewed() { "yes" } else { "no" }
        )?;
        prefix = "gene       : ".to_string();
        for gene in self.genes() {
            writeln!(f, "{prefix}{gene}")?;
            prefix = "             ".to_string();
        }
        Ok(())
    }
}

impl TryFrom<&uniprot_reader::entry::Entry> for Protein {
    type Error = Error;

    fn try_from(entry: &uniprot_reader::entry::Entry) -> Result<Self, Error> {
        let accession = entry
            .accession()
            .find(';')
            .map(|pos| entry.accession()[..pos].trim().to_string())
            .unwrap_or(entry.accession().to_string());

        let is_reviewed = memchr::memmem::find(entry.identification().as_bytes(), b"Reviewed")
            .map(|_| true)
            .unwrap_or(false);

        let genes = entry
            .gene_name()
            .replace("\n", "")
            .split(";") // this splits into groups (names=, synonyms=, ...)
            .map(|token| token.trim())
            .filter(|token| !token.is_empty())
            .flat_map(|token| {
                let equal_idx = token.find("=").map(|idx| idx + 1).unwrap_or(0);
                split_genes_stripping_evidence(&token[equal_idx..])
            })
            .collect::<Vec<String>>();

        Ok(Self::new(
            accession,
            None,
            Sequence::try_from(entry.sequence())?,
            taxonomy_id_from_organism_taxonomy_cross_reference(
                entry.organism_taxonomy_cross_reference(),
            )?,
            is_reviewed,
            genes,
        ))
    }
}

impl TryFrom<(i32, &uniprot_reader::entry::Entry)> for Protein {
    type Error = Error;

    fn try_from((id, entry): (i32, &uniprot_reader::entry::Entry)) -> Result<Self, Error> {
        let mut protein = Self::try_from(entry)?;
        protein.id = Some(id);
        Ok(protein)
    }
}

impl TryFrom<Row> for Protein {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Some(row.try_get("id")?),
            accession: row.try_get("accession")?,
            sequence: row.try_get("sequence")?,
            genes: row.try_get("genes")?,
            taxonomy_id: row.try_get("taxonomy_id")?,
            flags: row.try_get("flags")?,
        })
    }
}

fn taxonomy_id_from_organism_taxonomy_cross_reference(
    organism_taxonomy_cross_reference: &str,
) -> Result<i32, Error> {
    let start = organism_taxonomy_cross_reference
        .find(NCBI_TAXONOMY_ID_ATTRIBUTE_NAME)
        .ok_or(Error::MissingTaxonomyIdStart(
            organism_taxonomy_cross_reference.to_string(),
        ))?
        + NCBI_TAXONOMY_ID_ATTRIBUTE_NAME.len();

    // Taxonomy can end with semicolon or whitespace,
    // e.g `OX   NCBI_TaxID=83333 {ECO:0000312|Proteomes:UP000000625};`or `OX   NCBI_TaxID=83333;`
    // just read until no numeric follows
    organism_taxonomy_cross_reference[start..]
        .chars()
        .take_while(|c| c.is_numeric())
        .collect::<String>()
        .parse()
        .map_err(Error::TaxonomyIdParsing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gene_name_parsing_with_evidence_commas() {
        // Evidence annotations {…} can contain commas; they must not split gene names.
        let raw = "Name=cmoB {ECO:0000255|HAMAP-Rule:MF_01590,\nECO:0000303|PubMed:23676670}; Synonyms=yecP;\nOrderedLocusNames=b1871, JW1860;";
        let cleaned = raw.replace("\n", "");
        let genes: Vec<String> = cleaned
            .split(';')
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .flat_map(|token| {
                let eq = token.find('=').map(|i| i + 1).unwrap_or(0);
                split_genes_stripping_evidence(&token[eq..])
            })
            .collect();
        assert_eq!(genes, vec!["cmoB", "yecP", "b1871", "JW1860"]);
    }
}
