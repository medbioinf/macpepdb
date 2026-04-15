use std::{
    fmt::Display,
    io::{BufRead, Seek, SeekFrom},
};

use thiserror::Error;

use crate::{
    entry::{Entry, Error as EntryError},
    indexer::Offset,
};

pub struct Item {
    offset: Offset,
    entry: Entry,
}

impl Item {
    pub fn offset(&self) -> &Offset {
        &self.offset
    }

    pub fn entry(&self) -> &Entry {
        &self.entry
    }
}

impl Display for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Item {{ {}, entry: {} }}",
            self.offset(),
            self.entry.identification().trim()
        )
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unkown line type: {0}")]
    Entry(#[from] EntryError),
}

pub struct Reader<'a, R: BufRead> {
    inner: &'a mut R,
    line_buffer: Vec<u8>,
    last_end: u64,
}

impl<'a, R> Reader<'a, R>
where
    R: BufRead,
{
    pub fn new(content: &'a mut R) -> Self {
        Self {
            inner: content,
            line_buffer: Vec::with_capacity(1024),
            last_end: 0,
        }
    }
}

impl<'a, R> Iterator for Reader<'a, R>
where
    R: BufRead,
{
    type Item = Result<Item, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut entry = Entry::default();
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

            match entry.add_line(&mut self.line_buffer) {
                Ok(false) => continue,
                Ok(true) => {
                    let offset = Item {
                        offset: Offset::new(self.last_end, self.last_end + read_bytes as u64 - 1),
                        entry,
                    };
                    self.last_end += read_bytes as u64;
                    return Some(Ok(offset));
                }
                Err(err) => return Some(Err(err.into())),
            }
        }
    }
}

pub struct IndexedReader<R: BufRead + Seek> {
    inner: R,
}

impl<R> IndexedReader<R>
where
    R: BufRead + Seek,
{
    pub fn new(content: R) -> Self {
        Self { inner: content }
    }

    pub fn read(&mut self, offset: &Offset) -> Result<Entry, Error> {
        self.inner.seek(SeekFrom::Start(offset.start()))?;
        let mut buffer = vec![0; (offset.end() - offset.start() + 1) as usize];
        self.inner.read_exact(&mut buffer)?;
        Entry::try_from(buffer).map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_reader() {
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

        let uniprot_reader = super::Reader::new(&mut buf_reader);

        let mut entry_ctr = 0_usize;
        for (item, expected_offset) in uniprot_reader.zip(expected_offsets) {
            assert!(item.is_ok());
            entry_ctr += 1;
            let item = item.unwrap();
            assert_eq!(item.offset().start(), expected_offset.0);
            assert_eq!(item.offset().end(), expected_offset.1);
        }

        assert_eq!(entry_ctr, 11);
    }
}
