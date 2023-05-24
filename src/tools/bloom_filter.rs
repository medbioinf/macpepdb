// std imports
use core::num::ParseIntError;
use std::f64::consts::E;
use std::io::Cursor;
use std::path::PathBuf;

// 3rd party imports
use anyhow::{bail, Result};
use bitvec::prelude::*;
use hdf5::{types::VarLenAscii, File as HDF5File};
use murmur3::murmur3_x64_128 as murmur3hash;

pub struct BloomFilter {
    fp_prob: f64,             // False positive probability
    size: u128, // Size of the bloom filter. Allowed us a max of u64 but we store it as u128 so it only converted once
    hash_count: u32, // Number of hash functions
    bitvec: BitBox<u8, Msb0>, // Bit vector
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
    pub fn new(fp_prob: f64, size: u64, hash_count: u32, bitvec: BitBox<u8, Msb0>) -> Result<Self> {
        Ok(Self {
            fp_prob,
            hash_count,
            bitvec,
            size: size as u128,
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
    pub fn get_bitvec(&self) -> &BitBox<u8, Msb0> {
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
        let bitvec = bitvec!(u8, Msb0; 0; size as usize);

        return Self::new(fp_prob, size, hash_count, bitvec.into_boxed_bitslice());
    }

    /// Creates a bloom filter with the given size and false positive probability
    ///
    /// # Arguments
    /// * `size` - Number of bits in bloom filter
    /// * `fp_prob` - False Positive probability in decimal
    ///
    pub fn new_by_size_and_fp_prob(size: u64, fp_prob: f64) -> Result<Self> {
        let rounded_size = size + 8 - (size % 8);

        let (_, hash_count) = Self::calc_item_size_and_hash_count(rounded_size, fp_prob);

        // Bit array of given size
        let bitvec = bitvec!(u8, Msb0; 0; rounded_size as usize);

        return Self::new(
            fp_prob,
            rounded_size,
            hash_count,
            bitvec.into_boxed_bitslice(),
        );
    }

    fn calc_item_position(&self, item: &str, seed: u32) -> Result<usize> {
        return Ok((murmur3hash(&mut Cursor::new(item), seed)? % self.size) as usize);
    }

    /// Add an item in the filter
    ///
    /// # Arguments
    ///
    /// * `item` - Item to add
    ///
    pub fn add(&mut self, item: &str) -> Result<()> {
        for i in 0..self.hash_count {
            // Create hash for given item.
            // `i` works as seed to mmh3.hash() function
            let digest = self.calc_item_position(item, i)?;
            // Set the bit to true
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
    /// Rounded up to nearest multiple of 8
    ///
    /// # Arguments
    ///
    /// `n` - number of items expected to be stored in filter
    /// `p` - False Positive probability in decimal
    ///
    pub fn calc_size(n: u64, p: f64) -> u64 {
        let mut m = (-(n as f64 * p.log(E)) / (2.0_f64.log(E).powi(2))) as u64;
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
    pub fn calc_hash_count(m: u64, n: u64) -> Result<u32> {
        let k = ((m as f64) / (n as f64)) * 2.0_f64.log(E);
        if k > u32::MAX as f64 {
            bail!("Hash count is too large");
        }
        return Ok(k as u32);
    }

    /// Calculates item size and hash count
    /// by increasing the hash_count to fit the maximum possible number of items.
    ///
    /// # Arguments
    /// * `hash_count` - Number of hash functions to use
    /// * `fp_prob` - False Positive probability in decimal
    ///
    pub fn calc_item_size_and_hash_count(size: u64, fp_prob: f64) -> (u64, u32) {
        let size_f = size as f64;
        let mut item_size: u64 = 0;
        for i in 1..=u32::MAX {
            let i_f = i as f64;
            let temp_item_size =
                (size_f / (-i_f / (1_f64 - (fp_prob.ln() / i_f).exp()).ln())).ceil() as u64;
            if item_size > temp_item_size {
                return (item_size, i - 1);
            } else {
                item_size = temp_item_size;
            }
        }
        return (item_size, u32::MAX);
    }

    /// Loads bloom filter from hdf5 file
    ///
    /// # Arguments
    /// * `path` - Path to hdf5 file
    ///
    pub fn load(path: &PathBuf) -> Result<Self> {
        let file = HDF5File::open(path)?;
        let size = file.dataset("size")?.read_scalar::<u64>()?;
        let hash_count = file.dataset("hash_count")?.read_scalar::<u32>()?;
        let fp_prob = file.dataset("fp_prob")?.read_scalar::<f64>()?;
        let bytes = match Self::decode_hex(
            &file
                .dataset("bit_array")?
                .read_scalar::<VarLenAscii>()?
                .as_str(),
        ) {
            Ok(bytes) => bytes,
            Err(err) => bail!(format!("Error while decoding hex: {}", err)),
        };
        return Self::new(
            fp_prob,
            size,
            hash_count,
            BitVec::<u8, Msb0>::from_slice(&bytes).into_boxed_bitslice(),
        );
    }

    /// Saves bloom filter to hdf5 file
    ///
    /// # Arguments
    /// * `path` - Path to hdf5 file
    ///
    pub fn save(&self, path: &PathBuf) -> Result<()> {
        let file = HDF5File::create(path)?;
        file.new_dataset::<u64>()
            .create("size")?
            .write_scalar(&(self.size as u64))?;
        file.new_dataset::<u32>()
            .create("hash_count")?
            .write_scalar(&self.hash_count)?;
        file.new_dataset::<f64>()
            .create("fp_prob")?
            .write_scalar(&self.fp_prob)?;
        // Convert bitvec to hex string
        let s_ascii = Self::encode_hex(&self.bitvec)?
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<String>();
        // Save hex string to hdf5 file
        file.new_dataset::<VarLenAscii>()
            .create("bit_array")?
            .write_scalar(&VarLenAscii::from_ascii(&s_ascii)?)?;
        Ok(())
    }

    pub fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
            .collect()
    }

    pub fn encode_hex(bit_array: &BitBox<u8, Msb0>) -> Result<Vec<u8>> {
        let mut bytes: Vec<u8> = Vec::with_capacity(bit_array.len() / 8);
        for start in (0..bit_array.len()).step_by(8) {
            bytes.push(bit_array[start..(start + 8)].load::<u8>());
        }
        return Ok(bytes);
    }
}

#[cfg(test)]
mod tests {
    // std imports
    use std::env::temp_dir;
    use std::path::Path;

    // 3rd party imports
    use fallible_iterator::FallibleIterator;

    // internal imports
    use super::*;
    use crate::io::uniprot_text::reader::Reader;

    #[test]
    fn test_inserting_and_finding() {
        let mut bloom_filter = BloomFilter::new_by_item_count_and_fp_prob(100, 0.01).unwrap();

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            bloom_filter.add(&protein.get_accession()).unwrap();
        }
        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            assert!(bloom_filter.contains(&protein.get_accession()).unwrap());
        }
    }

    #[test]
    fn test_save_and_load() {
        let mut bloom_filter = BloomFilter::new_by_item_count_and_fp_prob(100, 0.01).unwrap();

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            bloom_filter.add(&protein.get_accession()).unwrap();
        }

        let temp_file = temp_dir().join("macpepdb_bloomfilter_test_save_and_load.h5");
        if temp_file.is_file() {
            std::fs::remove_file(&temp_file).unwrap();
        }

        bloom_filter.save(&temp_file).unwrap();

        let read_bloom_filter = BloomFilter::load(&temp_file).unwrap();

        assert!(bloom_filter.size == read_bloom_filter.size);
        assert!(bloom_filter.hash_count == read_bloom_filter.hash_count);
        assert!(bloom_filter.fp_prob == read_bloom_filter.fp_prob);
        assert!(bloom_filter.bitvec == read_bloom_filter.bitvec);

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            assert!(read_bloom_filter
                .contains(&protein.get_accession())
                .unwrap());
        }

        if temp_file.is_file() {
            std::fs::remove_file(&temp_file).unwrap();
        }
    }
}
