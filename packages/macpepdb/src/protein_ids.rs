use scylla::{
    cluster::metadata::{ColumnType, NativeType},
    deserialize::{FrameSlice, value::DeserializeValue},
    errors::SerializationError,
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
    },
};
use thiserror::Error;

use crate::cql::ensure_not_null_slice;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unexpected CQL value type for protein_ids: got {0:?}, expected Blob")]
    UnexpectedCqlValueType(ColumnType<'static>),
    #[error("protein_ids blob too large for a single CQL cell")]
    CqlValueTooLarge,
    #[error("Truncated varint while decoding protein_ids")]
    TruncatedVarint,
    #[error("Varint overflow while decoding protein_ids")]
    VarintOverflow,
}

/// Zigzag-encode a signed integer so small-magnitude negatives stay small.
#[inline]
fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

#[inline]
fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// Append `value` to `out` as an unsigned LEB128 varint.
fn write_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Read an unsigned LEB128 varint starting at `pos`, returning the value and the
/// number of bytes consumed.
fn read_uvarint(bytes: &[u8], pos: usize) -> Result<(u64, usize), Error> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut consumed = 0;

    loop {
        let byte = *bytes.get(pos + consumed).ok_or(Error::TruncatedVarint)?;
        consumed += 1;
        if shift >= 64 || (shift == 63 && byte > 1) {
            return Err(Error::VarintOverflow);
        }
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    Ok((value, consumed))
}

/// Number of bytes a single unsigned varint occupies (min 1).
fn uvarint_len(mut value: u64) -> usize {
    let mut n = 1;
    while value >= 0x80 {
        value >>= 7;
        n += 1;
    }
    n
}

/// A peptide's associated protein IDs, stored as a delta + varint blob.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ProteinIds(Vec<i32>);

impl ProteinIds {
    pub fn as_slice(&self) -> &[i32] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_vec(self) -> Vec<i32> {
        self.0
    }

    /// Sort + dedup, then delta-encode into a varint blob. The first value is
    /// stored zigzag-encoded; subsequent values are stored as non-negative gaps.
    fn encode(&self) -> Vec<u8> {
        let mut sorted = self.0.clone();
        sorted.sort_unstable();
        sorted.dedup();

        let mut out = Vec::with_capacity(sorted.len() + 4);
        let mut prev: i64 = 0;
        for (i, &id) in sorted.iter().enumerate() {
            let id = id as i64;
            if i == 0 {
                write_uvarint(&mut out, zigzag(id));
            } else {
                write_uvarint(&mut out, (id - prev) as u64);
            }
            prev = id;
        }
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut ids = Vec::new();
        let mut cur: i64 = 0;
        let mut pos = 0;
        let mut first = true;

        while pos < bytes.len() {
            let (raw, consumed) = read_uvarint(bytes, pos)?;
            cur = if first {
                first = false;
                unzigzag(raw)
            } else {
                cur + raw as i64
            };
            ids.push(cur as i32);
            pos += consumed;
        }

        Ok(Self(ids))
    }

    pub fn encoded_len(&self) -> usize {
        if self.0.is_empty() {
            return 0;
        }
        let mut sorted = self.0.clone();
        sorted.sort_unstable();
        sorted.dedup();

        let mut len = uvarint_len(zigzag(sorted[0] as i64));
        let mut prev = sorted[0] as i64;
        for &id in &sorted[1..] {
            let id = id as i64;
            len += uvarint_len((id - prev) as u64);
            prev = id;
        }
        len
    }
}

impl From<Vec<i32>> for ProteinIds {
    fn from(ids: Vec<i32>) -> Self {
        Self(ids)
    }
}

impl AsRef<[i32]> for ProteinIds {
    fn as_ref(&self) -> &[i32] {
        &self.0
    }
}

impl SerializeValue for ProteinIds {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        if !matches!(typ, ColumnType::Native(NativeType::Blob)) {
            return Err(SerializationError::new(Error::UnexpectedCqlValueType(
                typ.clone().into_owned(),
            )));
        }
        writer
            .set_value(self.encode().as_slice())
            .map_err(|_| SerializationError::new(Error::CqlValueTooLarge))
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for ProteinIds {
    fn type_check(typ: &ColumnType) -> Result<(), scylla::errors::TypeCheckError> {
        if !matches!(typ, ColumnType::Native(NativeType::Blob)) {
            return Err(scylla::errors::TypeCheckError::new(
                Error::UnexpectedCqlValueType(typ.clone().into_owned()),
            ));
        }
        Ok(())
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, scylla::errors::DeserializationError> {
        let bytes = ensure_not_null_slice::<&[u8]>(typ, v)?;
        Self::decode(bytes).map_err(scylla::errors::DeserializationError::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(input: Vec<i32>) {
        let ids = ProteinIds::from(input.clone());
        let encoded = ids.encode();
        assert_eq!(
            encoded.len(),
            ids.encoded_len(),
            "encoded_len must match actual encoding for {input:?}"
        );
        let decoded = ProteinIds::decode(&encoded).unwrap();

        // Decoding yields the sorted, deduplicated set.
        let mut expected = input;
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(decoded.into_vec(), expected);
    }

    #[test]
    fn test_round_trip_small() {
        round_trip(vec![1, 5, 6, 9, 2000, 2001]);
    }

    #[test]
    fn test_round_trip_unsorted_with_dups() {
        round_trip(vec![9, 1, 6, 6, 5, 2001, 2000, 1]);
    }

    #[test]
    fn test_round_trip_near_i32_min() {
        let base = i32::MIN;
        round_trip((0..5000).map(|i| base + i).collect());
    }

    #[test]
    fn test_round_trip_full_range() {
        round_trip(vec![i32::MIN, -1, 0, 1, i32::MAX]);
    }

    #[test]
    fn test_round_trip_empty() {
        round_trip(vec![]);
        assert_eq!(ProteinIds::default().encoded_len(), 0);
    }

    #[test]
    fn test_round_trip_single() {
        round_trip(vec![i32::MIN]);
        round_trip(vec![42]);
    }

    #[test]
    fn test_zigzag_inverse() {
        for v in [i64::MIN, -1, 0, 1, 42, i32::MIN as i64, i64::MAX] {
            assert_eq!(unzigzag(zigzag(v)), v);
        }
    }

    #[test]
    fn test_dense_sequential_is_one_byte_per_gap() {
        // 1000 contiguous ids: first value + 999 single-byte gaps of 1.
        let ids = ProteinIds::from((0..1000).collect::<Vec<_>>());
        assert_eq!(ids.encoded_len(), uvarint_len(zigzag(0)) + 999);
    }
}
