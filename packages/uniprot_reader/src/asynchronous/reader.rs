use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use tokio::io::AsyncBufRead;

use crate::{
    entry::Entry,
    indexer::Offset,
    reader::{Error, Item},
};

/// Async, [`futures::Stream`]-based counterpart of [`crate::reader::Reader`]: parses one
/// [`Item`] (offset + [`Entry`]) at a time from an [`AsyncBufRead`] source, driving the
/// underlying reader with `poll_fill_buf`/`consume` instead of blocking reads so it can be
/// awaited alongside other async I/O (e.g. reading a gzip-decoded stream).
pub struct AsyncReader<'a, R: AsyncBufRead + Unpin + Send> {
    inner: &'a mut R,
    line_buffer: Vec<u8>,
    last_end: u64,
    bytes_in_current_entry: u64,
    current_entry: Entry,
}

impl<'a, R> AsyncReader<'a, R>
where
    R: AsyncBufRead + Unpin + Send,
{
    /// Wraps an [`AsyncBufRead`] source to be consumed entry-by-entry via the [`Stream`] impl.
    pub fn new(content: &'a mut R) -> Self {
        Self {
            inner: content,
            line_buffer: Vec::with_capacity(1024),
            last_end: 0,
            bytes_in_current_entry: 0,
            current_entry: Entry::default(),
        }
    }
}

impl<'a, R> Stream for AsyncReader<'a, R>
where
    R: AsyncBufRead + Unpin + Send,
{
    type Item = Result<Item, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Build one complete line in line_buffer using poll_fill_buf/consume.
            // line_buffer may already contain partial data from a previous Pending return.
            let line_complete = loop {
                let (consume_len, found_newline) = {
                    let filled = match Pin::new(&mut *this.inner).poll_fill_buf(cx) {
                        Poll::Ready(Ok(buf)) => buf,
                        Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(Error::Io(e)))),
                        // line_buffer keeps its partial data; restored on next poll
                        Poll::Pending => return Poll::Pending,
                    };

                    if filled.is_empty() {
                        break false; // EOF
                    }

                    let newline_pos = filled.iter().position(|&b| b == b'\n');
                    let n = newline_pos.map(|p| p + 1).unwrap_or(filled.len());
                    this.line_buffer.extend_from_slice(&filled[..n]);
                    (n, newline_pos.is_some())
                    // `filled` drops here, releasing the borrow on this.inner
                };

                Pin::new(&mut *this.inner).consume(consume_len);
                this.bytes_in_current_entry += consume_len as u64;

                if found_newline {
                    break true;
                }
                // Buffer had no newline; loop to fill more
            };

            if !line_complete && this.line_buffer.is_empty() {
                return Poll::Ready(None);
            }

            // Skip empty lines
            if this.line_buffer.is_empty() || this.line_buffer == b"\n" {
                this.line_buffer.clear();
                continue;
            }

            match this.current_entry.add_line(&mut this.line_buffer) {
                Ok(false) => {
                    this.line_buffer.clear();
                    continue;
                }
                Ok(true) => {
                    this.line_buffer.clear();
                    let read_bytes = this.bytes_in_current_entry;
                    this.bytes_in_current_entry = 0;
                    let item = Item::new(
                        Offset::new(this.last_end, this.last_end + read_bytes - 1),
                        std::mem::take(&mut this.current_entry),
                    );
                    this.last_end += read_bytes;
                    return Poll::Ready(Some(Ok(item)));
                }
                Err(err) => {
                    this.line_buffer.clear();
                    return Poll::Ready(Some(Err(err.into())));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_compression::tokio::bufread::GzipDecoder;
    use futures::{StreamExt, pin_mut};

    use super::*;

    static EXPECTED_OFFSETS: &[(u64, u64)] = &[
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

    async fn test_reader_generic<R: AsyncBufRead + Unpin + Send>(buf_reader: &mut R) {
        let uniprot_reader = AsyncReader::new(buf_reader);

        let mut entry_ctr = 0_usize;
        let entity_stream = uniprot_reader.zip(futures::stream::iter(EXPECTED_OFFSETS));
        pin_mut!(entity_stream);

        while let Some((item, expected_offset)) = entity_stream.next().await {
            if let Err(err) = &item {
                println!("Error parsing entry {}: {err}", entry_ctr + 1);
            }

            assert!(item.is_ok());
            entry_ctr += 1;
            let item = item.unwrap();
            assert_eq!(item.offset().start(), expected_offset.0);
            assert_eq!(item.offset().end(), expected_offset.1);
        }

        assert_eq!(entry_ctr, 11);
    }

    #[tokio::test]
    async fn test_reader() {
        let test_file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("some_human_proteins.uniprot.txt");

        let mut buf_reader = Box::new(tokio::io::BufReader::new(
            tokio::fs::File::open(test_file_path).await.unwrap(),
        ));

        test_reader_generic(&mut buf_reader).await;

        let test_file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("some_human_proteins.uniprot.txt.gz");

        let test_file = tokio::fs::File::open(test_file_path).await.unwrap();
        let buf_reader = tokio::io::BufReader::new(test_file);
        let gz_reader = GzipDecoder::new(buf_reader);
        let mut buf_reader = Box::pin(tokio::io::BufReader::new(gz_reader));
        test_reader_generic(&mut buf_reader).await;
    }
}
