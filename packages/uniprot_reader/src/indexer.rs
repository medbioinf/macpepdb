use std::{fmt::Display, io::BufRead};

use thiserror::Error;

/// The inclusive byte range `[start, end]` of a single entry within a UniProt text dump.
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Offset {
    start: u64,
    end: u64,
}

impl Offset {
    /// Creates an offset spanning bytes `start` through `end`, inclusive.
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// The byte position where the entry starts.
    pub fn start(&self) -> u64 {
        self.start
    }

    /// The byte position of the entry's last byte (inclusive).
    pub fn end(&self) -> u64 {
        self.end
    }
}

impl Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Offset {{ start: {}, end: {} }}", self.start, self.end,)
    }
}

/// Errors returned by [`Indexer`].
#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unknown line type: {0}")]
    UnknownLineType(String),
}

/// Scans a UniProt text dump for the `//` entry terminator lines and yields each entry's byte
/// [`Offset`] without parsing its content, so an index can be built cheaply and later used with
/// [`crate::reader::IndexedReader`] for random/parallel access.
pub struct Indexer<'a, R: BufRead> {
    inner: &'a mut R,
    line_buffer: Vec<u8>,
    last_end: u64,
}

impl<'a, R> Indexer<'a, R>
where
    R: BufRead,
{
    /// Wraps a buffered reader positioned at the start of a UniProt text dump.
    pub fn new(content: &'a mut R) -> Self {
        Self {
            inner: content,
            line_buffer: Vec::with_capacity(1024),
            last_end: 0,
        }
    }
}

impl<'a, R> Iterator for Indexer<'a, R>
where
    R: BufRead,
{
    type Item = Result<Offset, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut read_bytes = 0;
        loop {
            self.line_buffer.clear();
            read_bytes += match self.inner.read_until(b'\n', &mut self.line_buffer) {
                Ok(0) => return None,
                Ok(bytes) => bytes,
                Err(err) => return Some(Err(Error::Io(err))),
            };

            // Skip empty lines
            if self.line_buffer.is_empty() || self.line_buffer == b"\n" {
                continue;
            }

            let line_type = self.line_buffer.get(0..2).unwrap_or(b"");

            if line_type == b"//" {
                let offset = Offset {
                    start: self.last_end,
                    end: self.last_end + read_bytes as u64 - 1,
                };
                self.last_end += read_bytes as u64;
                return Some(Ok(offset));
            }
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_indexer() {
        let test_file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("some_human_proteins.uniprot.txt");

        let expected_offsets: &[(u64, u64)] = &[
            (0, 19320),
            (19321, 39107),
            (39108, 58572),
            (58573, 74110),
            (74111, 85411),
            (85412, 103073),
            (103074, 116424),
            (116425, 134135),
            (134136, 143180),
            (143181, 162179),
            (162180, 179420),
        ];

        let mut buf_reader = std::io::BufReader::new(std::fs::File::open(test_file_path).unwrap());

        let uniprot_reader = super::Indexer::new(&mut buf_reader);

        let mut entry_ctr = 0_usize;
        for (offset, expected_offset) in uniprot_reader.zip(expected_offsets) {
            assert!(offset.is_ok());
            entry_ctr += 1;
            let offset = offset.unwrap();
            assert_eq!(offset.start(), expected_offset.0);
            assert_eq!(offset.end(), expected_offset.1);
        }

        assert_eq!(entry_ctr, 11);
    }
}
