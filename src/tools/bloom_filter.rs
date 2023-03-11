// std imports
use core::num::ParseIntError;
use std::io::Cursor;
use std::f64::consts::E;
use std::path::PathBuf;

// 3rd party imports
use anyhow::{Result, bail};
use bitvec::prelude::*;
use murmur3::murmur3_x64_128 as murmur3hash;
use hdf5::{
    File as HDF5File,
    types::{VarLenAscii}
};

pub struct BloomFilter {
    fp_prob: f64, // False positive probability
    size: u128, // Size of the bloom filter. Allowed us a max of u64 but we store it as u128 so it only converted once
    hash_count: u32, // Number of hash functions
    bitvec: BitVec<u8, Msb0>, // Bit vector
}

impl BloomFilter {

    /// Class for Bloom filter, using murmur3 hash function
    /// 
    /// Arguments:
    /// * `fp_prob` - False Positive probability in decimal
    /// * `size` - Size of bloom filter
    /// * `hash_count` - Number of hash functions to use
    /// * `bitvec` - Bit vector
    ///
    pub fn new(fp_prob: f64, size: u64, hash_count: u32, bitvec: BitVec<u8, Msb0>) -> Result<Self> {
        Ok(Self {
            fp_prob,
            hash_count,
            bitvec,
            size: size as u128
        })
    }

    /// Get false positive probability
    /// 
    pub fn get_fp_prob(&self) -> f64 {
        return self.fp_prob;
    }

    /// Get size of bloom filter
    /// 
    pub fn get_size(&self) -> u128 {
        return self.size;
    }

    /// Get number of hash functions
    /// 
    pub fn get_hash_count(&self) -> u32 {
        return self.hash_count;
    }

    /// Get bit vector
    /// 
    pub fn get_bitvec(&self) -> &BitVec<u8, Msb0> {
        return &self.bitvec;
    }

    /// Creates new bloom filter with given parameters. 
    ///
    /// # Arguments
    /// * `items_count` - Number of items expected to be stored in bloom filter
    /// * `fp_prob` - False Positive probability in decimal
    ///
    pub fn new_by_item_count_and_fp_prob(items_count: u64, fp_prob: f64) -> Result<Self> {
        // Size of bit array to use
        let size = Self::calc_size(items_count, fp_prob);

        // Number of hash functions to use
        let hash_count = Self::calc_hash_count(size, items_count)?;

        // Bit array of given size
        let mut bitvec = BitVec::new();

        // initialize all bits as 0
        bitvec.resize(size as usize, false);

        return Self::new(
            fp_prob,
            size,
            hash_count,
            bitvec
        );
    }

    fn calc_item_position(&self, item: &str, seed: u32) -> Result<usize> {
        return Ok(
            (murmur3hash(
                &mut Cursor::new(item),
                seed
            )? % self.size) as usize
        );
    }

    /// Add an item in the filter
    /// 
    /// # Arguments
    ///
    /// * `item` - Item to add
    /// 
    pub fn add(&mut self, item: &str) -> Result<()> {
        for i in 0..self.hash_count {

            // create digest for given item.
            // i work as seed to mmh3.hash() function
            // With different seed, digest created is different
            let digest = self.calc_item_position(item, i)?;

            // set the bit True in bitvec
            self.bitvec.set(digest as usize, true)
        }
        return Ok(());
    }

    /// Check for existence of an item in filter
    /// 
    /// # Arguments
    /// * `item` - Item to search
    ///
    pub fn contains(&self, item: &str) -> Result<bool> {
        for i in 0..self.hash_count {
            let digest = self.calc_item_position(item, i)?;
            if !self.bitvec[digest as usize] {
                return Ok(false);
            }
        }
        return Ok(true);
    } 

    /// Return the size of bit array(m) to used using
    /// following formula
    /// m = -(n * lg(p)) / (lg(2)^2)
    /// 
    /// Rounded up to nearest mutiple of 8
    /// 
    /// # Arguments
    ///
    /// `n` - number of items expected to be stored in filter
    /// `p` - False Positive probability in decimal
    ///
    fn calc_size(n: u64, p: f64) -> u64 {
        let mut m = (
            -(n as f64 * p.log(E))/(2.0_f64.log(E).powi(2))
        ) as u64
        ;
        m += 8 - (m % 8); // round up to nearest multiple of 8
        return m;
    }

    /// Return the hash function(k) to be used using
    /// following formula
    /// k = (m/n) * lg(2)
    /// 
    /// # Arguments
    ///
    /// * `m` - size of bit array
    /// * `n` - number of items expected to be stored in filter
    /// 
    fn calc_hash_count(m: u64, n: u64) -> Result<u32> {
        let k = ((m as f64) / (n as f64)) * 2.0_f64.log(E);
        if k > u32::MAX as f64 {
            bail!("Hash count is too large");
        }
        return Ok(k as u32);
    }

    /// Loads bloom filter from hdf5 file
    /// 
    /// # Arguments
    /// * `path` - Path to hdf5 file
    /// 
    pub fn load(path: &PathBuf) -> Result<Self> {
        let file = HDF5File::open(path)?;
        let size = file.dataset("size")?
            .read_scalar::<u64>()?;
        let hash_count = file.dataset("hash_count")?
            .read_scalar::<u32>()?;
        let fp_prob = file.dataset("fp_prob")?
            .read_scalar::<f64>()?;
        let bytes = match Self::decode_hex(
            &file.dataset("bit_array")?
            .read_scalar::<VarLenAscii>()?
            .as_str()
        ) {
            Ok(bytes) => bytes,
            Err(err) => bail!(format!("Error while decoding hex: {}", err))
        };
        return Self::new(
            fp_prob, 
            size,
            hash_count, 
            BitVec::<u8, Msb0>::from_slice(&bytes)
        );
    }

    pub fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
            .collect()
    }
}


#[cfg(test)]
mod tests {
    // std imports
    use std::path::Path;

    // 3rd party imports
    use fallible_iterator::FallibleIterator;

    // internal imports
    use crate::io::uniprot_text::reader::Reader;
    use super::*;

    #[test]
    fn test_inserting_and_finding() {
        let mut  bloom_filter = BloomFilter::new_by_item_count_and_fp_prob(100, 0.01).unwrap();

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            bloom_filter.add(&protein.get_accession()).unwrap();
        }
        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            assert!(bloom_filter.contains(&protein.get_accession()).unwrap());
        }
    }
}