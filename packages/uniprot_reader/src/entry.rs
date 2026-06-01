use std::fmt::Display;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid entry format, Entry needs to start with `ID` and stops with `//`")]
    InvalidEntryFormat,
    #[error("Unknown line type: {0}")]
    UnknownLineType(String),
}

#[derive(Debug, Default)]
pub struct Entry {
    /// ID
    identification: String,
    /// AC
    accession: String,
    /// DT
    date: String,
    /// DE
    description: String,
    /// GN
    gene_name: String,
    /// OS
    organism_species: String,
    /// OG
    organelle: String,
    /// OC
    organsim_classification: String,
    /// OX
    organism_taxonomy_cross_reference: String,
    /// OH
    organims_host: String,
    /// RN
    reference_number: String,
    /// RP
    reference_position: String,
    /// RC
    reference_comment: String,
    /// RX
    reference_cross_reference: String,
    /// RG
    reference_group: String,
    /// RA
    reference_author: String,
    /// RT
    reference_title: String,
    /// RL
    reference_location: String,
    /// CC
    comment_string: String,
    /// DR
    database_cross_reference: String,
    /// PE
    protein_existence: String,
    /// KW
    keyword: String,
    /// FT
    feature_table: String,
    /// SQ
    sequence_header: String,
    /// Sequence
    sequence: String,
}

impl Entry {
    pub fn identification(&self) -> &str {
        &self.identification
    }

    pub fn accession(&self) -> &str {
        &self.accession
    }

    pub fn date(&self) -> &str {
        &self.date
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn gene_name(&self) -> &str {
        &self.gene_name
    }

    pub fn organism_species(&self) -> &str {
        &self.organism_species
    }

    pub fn organelle(&self) -> &str {
        &self.organelle
    }

    pub fn organsim_classification(&self) -> &str {
        &self.organsim_classification
    }

    pub fn organism_taxonomy_cross_reference(&self) -> &str {
        &self.organism_taxonomy_cross_reference
    }

    pub fn organims_host(&self) -> &str {
        &self.organims_host
    }

    pub fn reference_number(&self) -> &str {
        &self.reference_number
    }

    pub fn reference_position(&self) -> &str {
        &self.reference_position
    }

    pub fn reference_comment(&self) -> &str {
        &self.reference_comment
    }

    pub fn reference_cross_reference(&self) -> &str {
        &self.reference_cross_reference
    }

    pub fn reference_group(&self) -> &str {
        &self.reference_group
    }

    pub fn reference_author(&self) -> &str {
        &self.reference_author
    }

    pub fn reference_title(&self) -> &str {
        &self.reference_title
    }

    pub fn reference_location(&self) -> &str {
        &self.reference_location
    }

    pub fn comment_string(&self) -> &str {
        &self.comment_string
    }

    pub fn database_cross_reference(&self) -> &str {
        &self.database_cross_reference
    }

    pub fn protein_existence(&self) -> &str {
        &self.protein_existence
    }

    pub fn keyword(&self) -> &str {
        &self.keyword
    }

    pub fn feature_table(&self) -> &str {
        &self.feature_table
    }

    pub fn sequence_header(&self) -> &str {
        &self.sequence_header
    }

    pub fn sequence(&self) -> &str {
        &self.sequence
    }

    pub(crate) fn add_line(&mut self, line: &mut Vec<u8>) -> Result<bool, Error> {
        let line_type = line.get(0..2).unwrap_or(b"");

        match line_type {
            b"ID" => {
                self.identification
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"AC" => {
                self.accession
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"DT" => {
                self.date.extend(line.drain(..).skip(5).map(char::from));
            }
            b"DE" => {
                self.description
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"GN" => {
                self.gene_name
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"OS" => {
                self.organism_species
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"OG" => {
                self.organelle
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"OC" => {
                self.organism_species
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"OX" => {
                self.organism_taxonomy_cross_reference
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"OH" => {
                self.organims_host
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"RN" => {
                self.reference_number
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"RP" => {
                self.reference_position
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"RC" => {
                self.reference_comment
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"RX" => {
                self.reference_cross_reference
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"RG" => {
                self.reference_group
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"RA" => {
                self.reference_author
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"RT" => {
                self.reference_title
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"RL" => {
                self.reference_location
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"CC" => {
                self.comment_string
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"DR" => {
                self.database_cross_reference
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"PE" => {
                self.protein_existence
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"KW" => {
                self.keyword.extend(line.drain(..).skip(5).map(char::from));
            }
            b"FT" => {
                self.feature_table
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"SQ" => {
                self.sequence_header
                    .extend(line.drain(..).skip(5).map(char::from));
            }
            b"  " => {
                self.sequence.extend(
                    line.drain(..)
                        .skip(5)
                        .map(char::from)
                        .filter(|c| !c.is_whitespace()),
                );
            }
            b"//" => {
                return Ok(true);
            }
            _ => {
                return Err(Error::UnknownLineType(
                    String::from_utf8_lossy(line_type).to_string(),
                ));
            }
        }
        Ok(false)
    }
}

impl Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for line in self.identification.lines() {
            writeln!(f, "ID   {}", line)?;
        }
        for line in self.accession.lines() {
            writeln!(f, "AC   {}", line)?;
        }
        for line in self.date.lines() {
            writeln!(f, "DT   {}", line)?;
        }
        for line in self.description.lines() {
            writeln!(f, "DE   {}", line)?;
        }
        for line in self.gene_name.lines() {
            writeln!(f, "GN   {}", line)?;
        }
        for line in self.organism_species.lines() {
            writeln!(f, "OS   {}", line)?;
        }
        for line in self.organelle.lines() {
            writeln!(f, "OG   {}", line)?;
        }
        for line in self.organsim_classification.lines() {
            writeln!(f, "OC   {}", line)?;
        }
        for line in self.organism_taxonomy_cross_reference.lines() {
            writeln!(f, "OX   {}", line)?;
        }
        for line in self.organims_host.lines() {
            writeln!(f, "OH   {}", line)?;
        }
        for line in self.reference_number.lines() {
            writeln!(f, "RN   {}", line)?;
        }
        for line in self.reference_position.lines() {
            writeln!(f, "RP   {}", line)?;
        }
        for line in self.reference_comment.lines() {
            writeln!(f, "RC   {}", line)?;
        }
        for line in self.reference_cross_reference.lines() {
            writeln!(f, "RX   {}", line)?;
        }
        for line in self.reference_group.lines() {
            writeln!(f, "RG   {}", line)?;
        }
        for line in self.reference_author.lines() {
            writeln!(f, "RA   {}", line)?;
        }
        for line in self.reference_title.lines() {
            writeln!(f, "RT   {}", line)?;
        }
        for line in self.reference_location.lines() {
            writeln!(f, "RL   {}", line)?;
        }
        for line in self.comment_string.lines() {
            writeln!(f, "CC   {}", line)?;
        }
        for line in self.database_cross_reference.lines() {
            writeln!(f, "DR   {}", line)?;
        }
        for line in self.protein_existence.lines() {
            writeln!(f, "PE   {}", line)?;
        }
        for line in self.keyword.lines() {
            writeln!(f, "KW   {}", line)?;
        }
        for line in self.feature_table.lines() {
            writeln!(f, "FT   {}", line)?;
        }
        for line in self.sequence_header.lines() {
            writeln!(f, "SQ   {}", line)?;
        }
        if !self.sequence.is_empty() {
            for line in self.sequence.as_bytes().chunks(60) {
                write!(f, "    ")?;
                for block in line.chunks(10) {
                    write!(
                        f,
                        " {}",
                        std::str::from_utf8(block).map_err(|_| std::fmt::Error)?
                    )?;
                }
            }
            writeln!(f)?;
        }
        write!(f, "//")
    }
}

impl TryFrom<Vec<u8>> for Entry {
    type Error = Error;

    fn try_from(mut value: Vec<u8>) -> Result<Self, Self::Error> {
        if !value.starts_with(b"ID") || !value.ends_with(b"//\n") {
            return Err(Error::InvalidEntryFormat);
        }

        let mut entry = Entry::default();
        while !value.is_empty() {
            let next_newline = value
                .iter()
                .position(|&b| b == b'\n')
                .ok_or(Error::InvalidEntryFormat)?;

            let mut line = value.drain(..=next_newline).collect::<Vec<u8>>();
            if entry.add_line(&mut line)? {
                break;
            }
        }

        Ok(entry)
    }
}
