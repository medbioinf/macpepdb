use std::fmt::Display;

use itertools::Itertools;
use macpepdb_web_common::responses::{peptide::PeptideResponse, protein::ProteinResponse};
use serde::Serialize;
use thiserror::Error;
use tokio_postgres::Row;
use uniprot_reader::feature_table::{
    Feature, FeatureTable, Index, NoteOperation, Position, group_by_isoform,
};

use crate::{
    amino_acid::{AminoAcid, AminoAcidBitCode},
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
    #[error("Feature table error in protein: {0}")]
    FeatureTable(#[from] uniprot_reader::feature_table::Error),
    #[error("Isoform feature contain non-fixed position: {0}")]
    FeatureLocation(Position),
    #[error("VAR_SEQ `{id}` position {start}..{end} overlaps another edit for the same isoform")]
    VarSeqOverlap { id: String, start: u32, end: u32 },
    #[error(
        "VAR_SEQ `{id}` position {start}..{end} is out of bounds for a sequence of length {length}"
    )]
    VarSeqOutOfBounds {
        id: String,
        start: u32,
        end: u32,
        length: usize,
    },
    #[error(
        "VAR_SEQ `{id}` note expects `{expected}` at position {start}..{end} but canonical sequence has `{found}`"
    )]
    VarSeqMismatch {
        id: String,
        start: u32,
        end: u32,
        expected: String,
        found: String,
    },
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

    pub(crate) fn id_mut(&mut self) -> &mut Option<i32> {
        &mut self.id
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

    /// Builds the wire response for `GET /api/proteins/{accession}` (full peptide records).
    pub fn to_response(&self, peptides: Vec<PeptideResponse>) -> ProteinResponse<PeptideResponse> {
        ProteinResponse {
            accession: self.accession.clone(),
            id: self.id,
            sequence: self.sequence.to_string(),
            taxonomy_id: self.taxonomy_id,
            is_reviewed: self.is_reviewed(),
            genes: self.genes.clone(),
            peptides,
        }
    }

    /// Builds the wire response for `GET /api/proteins/search/{attribute}` (peptide sequences only).
    pub fn to_summary_response(&self, peptides: Vec<String>) -> ProteinResponse<String> {
        ProteinResponse {
            accession: self.accession.clone(),
            id: self.id,
            sequence: self.sequence.to_string(),
            taxonomy_id: self.taxonomy_id,
            is_reviewed: self.is_reviewed(),
            genes: self.genes.clone(),
            peptides,
        }
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

struct VarSeqEdit {
    /// The `/id` (`VSP_...`), for error messages; empty if absent.
    id: String,
    /// 1-based inclusive, from `Feature::location()`.
    start: u32,
    end: Option<u32>,
    /// Empty for `Missing`.
    replacement: Vec<AminoAcidBitCode>,
    /// The "from" side of a replacement, to sanity-check against the canonical sequence.
    expected: Option<String>,
}

impl VarSeqEdit {
    fn bit_codes_from_str(sequence: &str) -> Result<Vec<AminoAcidBitCode>, Error> {
        sequence
            .chars()
            .map(|code| {
                AminoAcid::by_code(code)
                    .map(|aa| *aa.bit_code())
                    .map_err(crate::sequence::Error::AminoAcid)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(Error::Sequence)
    }

    /// `None` when the feature isn't an edit we understand (e.g. VAR_SEQ text that isn't `"Missing"`
    /// or `"X -> Y"`) — such features are simply skipped, not treated as errors.
    fn new(feature: &Feature) -> Result<Option<VarSeqEdit>, Error> {
        let (replacement, expected) = match feature.note_operation() {
            Some(NoteOperation::Missing) => (Vec::new(), None),
            Some(NoteOperation::Replacement { from, to }) => {
                (Self::bit_codes_from_str(&to)?, Some(from))
            }
            Some(NoteOperation::Other(_)) | None => return Ok(None),
        };

        let start = match feature.location().position().start() {
            Index::Fix(pos) => pos,
            Index::Before(_) => 1, // set to lowest (1 based) index as we only know the variant goes beyond. this seems to be the UniProt behaviour
            _ => return Err(Error::FeatureLocation(feature.location().position())),
        };

        let end = match feature.location().position().end() {
            Index::Fix(pos) => Some(pos),
            Index::After(_) => None, // set to none, inidcating end of the seqeunces as we only the end goes beyond. this seems to be the UniProt behaviour
            _ => return Err(Error::FeatureLocation(feature.location().position())),
        };

        Ok(Some(VarSeqEdit {
            id: feature.id().unwrap_or_default().to_string(),
            start,
            end,
            replacement,
            expected,
        }))
    }
}

/// Collection if canonical and isoforms of protein
///
pub struct Variants {
    proteins: Vec<Protein>,
}

impl Variants {
    pub fn is_empty(&self) -> bool {
        self.proteins.is_empty()
    }

    pub fn len(&self) -> usize {
        self.proteins.len()
    }

    pub fn proteins(&self) -> &[Protein] {
        &self.proteins
    }

    /// Applies a set of VAR_SEQ edits — in canonical-sequence coordinates — to produce one isoform's
    /// bit-code sequence.
    fn apply_var_seq_edits(
        canonical: &[AminoAcidBitCode],
        mut edits: Vec<VarSeqEdit>,
    ) -> Result<Vec<AminoAcidBitCode>, Error> {
        edits.sort_by_key(|edit| edit.start);

        let mut data = Vec::with_capacity(canonical.len());
        let mut cursor = 0usize; // next uncopied canonical index (0-based)

        for edit in &edits {
            let start_idx = edit.start as usize - 1;
            let end = edit.end.unwrap_or(canonical.len() as u32);
            let end_idx = end as usize; // exclusive bound; end is 1-based inclusive

            if start_idx < cursor {
                return Err(Error::VarSeqOverlap {
                    id: edit.id.clone(),
                    start: edit.start,
                    end,
                });
            }
            if end_idx > canonical.len() {
                return Err(Error::VarSeqOutOfBounds {
                    id: edit.id.clone(),
                    start: edit.start,
                    end,
                    length: canonical.len(),
                });
            }
            if let Some(expected) = &edit.expected {
                let found: String = canonical[start_idx..end_idx]
                    .iter()
                    .map(|bit_code| AminoAcid::by_bit_code(bit_code).code())
                    .collect();
                if &found != expected {
                    return Err(Error::VarSeqMismatch {
                        id: edit.id.clone(),
                        start: edit.start,
                        end,
                        expected: expected.clone(),
                        found,
                    });
                }
            }

            data.extend_from_slice(&canonical[cursor..start_idx]);
            data.extend_from_slice(&edit.replacement);
            cursor = end_idx;
        }
        data.extend_from_slice(&canonical[cursor..]);

        Ok(data)
    }
}

impl IntoIterator for Variants {
    type Item = Protein;
    type IntoIter = std::vec::IntoIter<Protein>;

    fn into_iter(self) -> Self::IntoIter {
        self.proteins.into_iter()
    }
}
impl TryFrom<&uniprot_reader::entry::Entry> for Variants {
    type Error = Error;

    fn try_from(entry: &uniprot_reader::entry::Entry) -> Result<Self, Error> {
        let canonical_protein: Protein = Protein::try_from(entry)?;

        let feature_table = FeatureTable::try_from(entry.feature_table())?;
        let isoform_groups = group_by_isoform(feature_table.features());

        let mut variants = Vec::with_capacity(isoform_groups.len() + 1);
        for (isoform_idx, (_label, group)) in isoform_groups.iter().enumerate() {
            let edits = group
                .iter()
                .filter(|feature| feature.key() == "VAR_SEQ")
                .filter_map(|feature| VarSeqEdit::new(feature).transpose())
                .collect::<Result<Vec<_>, Error>>()?;

            if edits.is_empty() {
                continue;
            }

            let sequence_data =
                Self::apply_var_seq_edits(canonical_protein.sequence().data(), edits)?;

            variants.push(Protein::new(
                format!("{}-{}", canonical_protein.accession(), isoform_idx + 2),
                None,
                Sequence::new(sequence_data)?,
                canonical_protein.taxonomy_id(),
                canonical_protein.is_reviewed(),
                canonical_protein.genes().clone(),
            ));
        }

        variants.insert(0, canonical_protein);

        Ok(Variants { proteins: variants })
    }
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

    #[test]
    fn test_apply_var_seq_edits_single_replacement() {
        let canonical = VarSeqEdit::bit_codes_from_str("ACDEFG").unwrap();
        let edits = vec![VarSeqEdit {
            id: "VSP_1".to_string(),
            start: 2,
            end: Some(2),
            replacement: VarSeqEdit::bit_codes_from_str("K").unwrap(),
            expected: Some("C".to_string()),
        }];
        let result =
            Sequence::new(Variants::apply_var_seq_edits(&canonical, edits).unwrap()).unwrap();
        assert_eq!(result.to_string(), "AKDEFG");
    }

    #[test]
    fn test_apply_var_seq_edits_missing() {
        let canonical = VarSeqEdit::bit_codes_from_str("ACDEFG").unwrap();
        let edits = vec![VarSeqEdit {
            id: "VSP_2".to_string(),
            start: 2,
            end: Some(4),
            replacement: Vec::new(),
            expected: None,
        }];
        let result =
            Sequence::new(Variants::apply_var_seq_edits(&canonical, edits).unwrap()).unwrap();
        assert_eq!(result.to_string(), "AFG");
    }

    #[test]
    fn test_apply_var_seq_edits_multiple_non_overlapping() {
        let canonical = VarSeqEdit::bit_codes_from_str("ACDEFGHIKL").unwrap();
        let edits = vec![
            VarSeqEdit {
                id: "1".to_string(),
                start: 1,
                end: Some(1),
                replacement: VarSeqEdit::bit_codes_from_str("K").unwrap(),
                expected: Some("A".to_string()),
            },
            VarSeqEdit {
                id: "2".to_string(),
                start: 8,
                end: Some(10),
                replacement: Vec::new(),
                expected: None,
            },
        ];
        let result =
            Sequence::new(Variants::apply_var_seq_edits(&canonical, edits).unwrap()).unwrap();
        assert_eq!(result.to_string(), "KCDEFGH");
    }

    #[test]
    fn test_apply_var_seq_edits_overlap_errors() {
        let canonical = VarSeqEdit::bit_codes_from_str("ACDEFG").unwrap();
        let edits = vec![
            VarSeqEdit {
                id: "1".to_string(),
                start: 2,
                end: Some(4),
                replacement: Vec::new(),
                expected: None,
            },
            VarSeqEdit {
                id: "2".to_string(),
                start: 3,
                end: Some(5),
                replacement: Vec::new(),
                expected: None,
            },
        ];
        let err = Variants::apply_var_seq_edits(&canonical, edits).unwrap_err();
        assert!(matches!(err, Error::VarSeqOverlap { .. }));
    }

    #[test]
    fn test_apply_var_seq_edits_out_of_bounds_errors() {
        let canonical = VarSeqEdit::bit_codes_from_str("ACDEFG").unwrap();
        let edits = vec![VarSeqEdit {
            id: "1".to_string(),
            start: 5,
            end: Some(10),
            replacement: Vec::new(),
            expected: None,
        }];
        let err = Variants::apply_var_seq_edits(&canonical, edits).unwrap_err();
        assert!(matches!(err, Error::VarSeqOutOfBounds { .. }));
    }

    #[test]
    fn test_apply_var_seq_edits_mismatch_errors() {
        let canonical = VarSeqEdit::bit_codes_from_str("ACDEFG").unwrap();
        let edits = vec![VarSeqEdit {
            id: "1".to_string(),
            start: 2,
            end: Some(2),
            replacement: Vec::new(),
            expected: Some("D".to_string()),
        }];
        let err = Variants::apply_var_seq_edits(&canonical, edits).unwrap_err();
        assert!(matches!(err, Error::VarSeqMismatch { .. }));
    }

    #[test]
    fn test_isoforms_from_entry_with_var_seq() {
        const RAW_ENTRY: &str = concat!(
            "ID   TEST_HUMAN              Reviewed;         10 AA.\n",
            "AC   P99999;\n",
            "OX   NCBI_TaxID=9606;\n",
            "FT   VAR_SEQ         3..5\n",
            "FT                   /note=\"Missing (in isoform 2)\"\n",
            "FT                   /id=\"VSP_000001\"\n",
            "SQ   SEQUENCE   10 AA;  1160 MW;  0000000000000000 CRC64;\n",
            "     ACDEFGHIKL\n",
            "//\n",
        );

        let entry = uniprot_reader::entry::Entry::try_from(RAW_ENTRY.as_bytes().to_vec()).unwrap();
        let proteins: Vec<Protein> = Variants::try_from(&entry).unwrap().into_iter().collect();

        assert_eq!(proteins.len(), 2);
        assert_eq!(proteins[0].accession(), "P99999");
        assert_eq!(proteins[0].sequence().to_string(), "ACDEFGHIKL");
        assert_eq!(proteins[1].accession(), "P99999-2");
        assert_eq!(proteins[1].sequence().to_string(), "ACGHIKL");
    }

    #[test]
    fn test_isoform_resolving() {
        let expected_sequences_file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("A0A1B0GTW7.isoform.plain.txt");

        let a0a1b0gtw7_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("A0A1B0GTW7.txt");

        let expected_sequences = std::fs::read_to_string(expected_sequences_file_path)
            .unwrap()
            .split("\n")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(Sequence::try_from)
            .collect::<Result<Vec<_>, crate::sequence::Error>>()
            .unwrap();

        assert_eq!(expected_sequences.len(), 3);

        let mut byte_reader =
            std::io::BufReader::new(std::fs::File::open(a0a1b0gtw7_path).unwrap());
        let entry_reader = uniprot_reader::reader::Reader::new(&mut byte_reader);
        let entry = entry_reader.into_iter().next().unwrap().unwrap();

        let variants = Variants::try_from(entry.entry()).unwrap();

        assert_eq!(variants.len(), expected_sequences.len());

        for variant in variants.into_iter() {
            assert!(expected_sequences.contains(variant.sequence()));
        }
    }
}
