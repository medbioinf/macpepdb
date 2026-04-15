#![feature(prelude_import)]
#[prelude_import]
use std::prelude::rust_2024::*;
#[macro_use]
extern crate std;
pub mod amino_acid {
    use bitvec::{BitArr, bitarr, order::Lsb0, slice::BitSlice};
    use macpepdb_proc_macros::u8_to_amino_acid_bitcode;
    use pastey::paste;
    use thiserror::Error;
    use crate::mass_to_int;
    const BIT_CODE_LEN: usize = 5;
    pub enum Error {
        #[error("Invalid amino acid code: {0}")]
        InvalidAminoAcidCode(char),
        #[error("Invalid amino acid bit code: {0}")]
        InvalidAminoAcidBitCode(String),
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Error {
        #[inline]
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match self {
                Error::InvalidAminoAcidCode(__self_0) => {
                    ::core::fmt::Formatter::debug_tuple_field1_finish(
                        f,
                        "InvalidAminoAcidCode",
                        &__self_0,
                    )
                }
                Error::InvalidAminoAcidBitCode(__self_0) => {
                    ::core::fmt::Formatter::debug_tuple_field1_finish(
                        f,
                        "InvalidAminoAcidBitCode",
                        &__self_0,
                    )
                }
            }
        }
    }
    #[allow(unused_qualifications)]
    #[automatically_derived]
    impl ::thiserror::__private18::Error for Error {}
    #[allow(unused_qualifications)]
    #[automatically_derived]
    impl ::core::fmt::Display for Error {
        fn fmt(&self, __formatter: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            use ::thiserror::__private18::AsDisplay as _;
            #[allow(unused_variables, deprecated, clippy::used_underscore_binding)]
            match self {
                Error::InvalidAminoAcidCode(_0) => {
                    match (_0.as_display(),) {
                        (__display0,) => {
                            __formatter
                                .write_fmt(
                                    format_args!("Invalid amino acid code: {0}", __display0),
                                )
                        }
                    }
                }
                Error::InvalidAminoAcidBitCode(_0) => {
                    match (_0.as_display(),) {
                        (__display0,) => {
                            __formatter
                                .write_fmt(
                                    format_args!("Invalid amino acid bit code: {0}", __display0),
                                )
                        }
                    }
                }
            }
        }
    }
    pub type BitCode = ::bitvec::array::BitArray<
        [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
        Lsb0,
    >;
    pub struct AminoAcid {
        code: char,
        mono_mass: i64,
        bit_code: &'static BitCode,
    }
    impl AminoAcid {
        pub const BIT_CODE_LEN: usize = BIT_CODE_LEN;
        pub fn code(&self) -> char {
            self.code
        }
        pub fn mono_mass(&self) -> i64 {
            self.mono_mass
        }
        pub fn bit_code(&self) -> &BitSlice<u8, Lsb0> {
            &self.bit_code[..Self::BIT_CODE_LEN]
        }
        pub fn foo(&self) -> &'static BitCode {
            self.bit_code
        }
    }
    const GLYCINE_BIT_CODE_RAW: u8 = 6;
    const GLYCINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [GLYCINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const GLYCINE: AminoAcid = AminoAcid {
        code: 'G',
        mono_mass: { (57.021463735 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &GLYCINE_BIT_CODE,
    };
    const ALANINE_BIT_CODE_RAW: u8 = 0;
    const ALANINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [ALANINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const ALANINE: AminoAcid = AminoAcid {
        code: 'A',
        mono_mass: { (71.037113805 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &ALANINE_BIT_CODE,
    };
    const SERINE_BIT_CODE_RAW: u8 = 18;
    const SERINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [SERINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const SERINE: AminoAcid = AminoAcid {
        code: 'S',
        mono_mass: { (87.032028435 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &SERINE_BIT_CODE,
    };
    const PROLINE_BIT_CODE_RAW: u8 = 15;
    const PROLINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [PROLINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const PROLINE: AminoAcid = AminoAcid {
        code: 'P',
        mono_mass: { (97.052763875 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &PROLINE_BIT_CODE,
    };
    const VALINE_BIT_CODE_RAW: u8 = 21;
    const VALINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [VALINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const VALINE: AminoAcid = AminoAcid {
        code: 'V',
        mono_mass: { (99.068413945 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &VALINE_BIT_CODE,
    };
    const THREONINE_BIT_CODE_RAW: u8 = 19;
    const THREONINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [THREONINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const THREONINE: AminoAcid = AminoAcid {
        code: 'T',
        mono_mass: { (101.047678505 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &THREONINE_BIT_CODE,
    };
    const CYSTEINE_BIT_CODE_RAW: u8 = 2;
    const CYSTEINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [CYSTEINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const CYSTEINE: AminoAcid = AminoAcid {
        code: 'C',
        mono_mass: { (103.009184505 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &CYSTEINE_BIT_CODE,
    };
    const LEUCINE_BIT_CODE_RAW: u8 = 11;
    const LEUCINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [LEUCINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const LEUCINE: AminoAcid = AminoAcid {
        code: 'L',
        mono_mass: { (113.084064015 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &LEUCINE_BIT_CODE,
    };
    const ISOLEUCINE_BIT_CODE_RAW: u8 = 8;
    const ISOLEUCINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [ISOLEUCINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const ISOLEUCINE: AminoAcid = AminoAcid {
        code: 'I',
        mono_mass: { (113.084064015 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &ISOLEUCINE_BIT_CODE,
    };
    const ASPARAGINE_BIT_CODE_RAW: u8 = 13;
    const ASPARAGINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [ASPARAGINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const ASPARAGINE: AminoAcid = AminoAcid {
        code: 'N',
        mono_mass: { (114.04292747 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &ASPARAGINE_BIT_CODE,
    };
    const ASPARTIC_ACID_BIT_CODE_RAW: u8 = 3;
    const ASPARTIC_ACID_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [ASPARTIC_ACID_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const ASPARTIC_ACID: AminoAcid = AminoAcid {
        code: 'D',
        mono_mass: { (115.026943065 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &ASPARTIC_ACID_BIT_CODE,
    };
    const GLUTAMINE_BIT_CODE_RAW: u8 = 16;
    const GLUTAMINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [GLUTAMINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const GLUTAMINE: AminoAcid = AminoAcid {
        code: 'Q',
        mono_mass: { (128.05857754 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &GLUTAMINE_BIT_CODE,
    };
    const LYSINE_BIT_CODE_RAW: u8 = 10;
    const LYSINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [LYSINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const LYSINE: AminoAcid = AminoAcid {
        code: 'K',
        mono_mass: { (128.09496305 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &LYSINE_BIT_CODE,
    };
    const GLUTAMIC_ACID_BIT_CODE_RAW: u8 = 4;
    const GLUTAMIC_ACID_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [GLUTAMIC_ACID_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const GLUTAMIC_ACID: AminoAcid = AminoAcid {
        code: 'E',
        mono_mass: { (129.042593135 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &GLUTAMIC_ACID_BIT_CODE,
    };
    const METHIONINE_BIT_CODE_RAW: u8 = 12;
    const METHIONINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [METHIONINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const METHIONINE: AminoAcid = AminoAcid {
        code: 'M',
        mono_mass: { (131.040484645 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &METHIONINE_BIT_CODE,
    };
    const HISTIDINE_BIT_CODE_RAW: u8 = 7;
    const HISTIDINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [HISTIDINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const HISTIDINE: AminoAcid = AminoAcid {
        code: 'H',
        mono_mass: { (137.058911875 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &HISTIDINE_BIT_CODE,
    };
    const PHENYLALANINE_BIT_CODE_RAW: u8 = 5;
    const PHENYLALANINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [PHENYLALANINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const PHENYLALANINE: AminoAcid = AminoAcid {
        code: 'F',
        mono_mass: { (147.068413945 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &PHENYLALANINE_BIT_CODE,
    };
    const SELENOCYSTEINE_BIT_CODE_RAW: u8 = 20;
    const SELENOCYSTEINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [SELENOCYSTEINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const SELENOCYSTEINE: AminoAcid = AminoAcid {
        code: 'U',
        mono_mass: { (150.953633405 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &SELENOCYSTEINE_BIT_CODE,
    };
    const ARGININE_BIT_CODE_RAW: u8 = 17;
    const ARGININE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [ARGININE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const ARGININE: AminoAcid = AminoAcid {
        code: 'R',
        mono_mass: { (156.10111105 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &ARGININE_BIT_CODE,
    };
    const TYROSINE_BIT_CODE_RAW: u8 = 24;
    const TYROSINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [TYROSINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const TYROSINE: AminoAcid = AminoAcid {
        code: 'Y',
        mono_mass: { (163.063328575 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &TYROSINE_BIT_CODE,
    };
    const TRYPTOPHAN_BIT_CODE_RAW: u8 = 22;
    const TRYPTOPHAN_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [TRYPTOPHAN_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const TRYPTOPHAN: AminoAcid = AminoAcid {
        code: 'W',
        mono_mass: { (186.07931298 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &TRYPTOPHAN_BIT_CODE,
    };
    const PYRROLYSINE_BIT_CODE_RAW: u8 = 14;
    const PYRROLYSINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [PYRROLYSINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const PYRROLYSINE: AminoAcid = AminoAcid {
        code: 'O',
        mono_mass: { (237.147726925 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &PYRROLYSINE_BIT_CODE,
    };
    const ASPARAGINE_OR_ASPARTIC_ACID_BIT_CODE_RAW: u8 = 1;
    const ASPARAGINE_OR_ASPARTIC_ACID_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [ASPARAGINE_OR_ASPARTIC_ACID_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const ASPARAGINE_OR_ASPARTIC_ACID: AminoAcid = AminoAcid {
        code: 'B',
        mono_mass: { (114.5349352675 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &ASPARAGINE_OR_ASPARTIC_ACID_BIT_CODE,
    };
    const GLUTAMINE_OR_GLUTAMIC_ACID_BIT_CODE_RAW: u8 = 25;
    const GLUTAMINE_OR_GLUTAMIC_ACID_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [GLUTAMINE_OR_GLUTAMIC_ACID_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const GLUTAMINE_OR_GLUTAMIC_ACID: AminoAcid = AminoAcid {
        code: 'Z',
        mono_mass: { (128.5505853375 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &GLUTAMINE_OR_GLUTAMIC_ACID_BIT_CODE,
    };
    const ISOLEUCINE_OR_LEUCINE_BIT_CODE_RAW: u8 = 9;
    const ISOLEUCINE_OR_LEUCINE_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [ISOLEUCINE_OR_LEUCINE_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const ISOLEUCINE_OR_LEUCINE: AminoAcid = AminoAcid {
        code: 'J',
        mono_mass: { (113.084064015 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &ISOLEUCINE_OR_LEUCINE_BIT_CODE,
    };
    const UNKNOWN_BIT_CODE_RAW: u8 = 23;
    const UNKNOWN_BIT_CODE: BitCode = {
        type This = ::bitvec::array::BitArray<
            [u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)],
            Lsb0,
        >;
        This {
            data: [UNKNOWN_BIT_CODE_RAW],
            ..This::ZERO
        }
    };
    pub const UNKNOWN: AminoAcid = AminoAcid {
        code: 'X',
        mono_mass: { (0 as f64 * crate::mass::MASS_CONVERT_FACTOR) as i64 },
        bit_code: &UNKNOWN_BIT_CODE,
    };
    impl AminoAcid {
        /// Returns a canonical or non-canoncial amino acid by one letter code
        ///
        /// # Arguments
        /// * `code` - One letter code
        ///
        pub fn by_code(code: char) -> Result<&'static AminoAcid, Error> {
            match code.to_ascii_uppercase() {
                'G' => Ok(&GLYCINE),
                'A' => Ok(&ALANINE),
                'S' => Ok(&SERINE),
                'P' => Ok(&PROLINE),
                'V' => Ok(&VALINE),
                'T' => Ok(&THREONINE),
                'C' => Ok(&CYSTEINE),
                'L' => Ok(&LEUCINE),
                'I' => Ok(&ISOLEUCINE),
                'N' => Ok(&ASPARAGINE),
                'D' => Ok(&ASPARTIC_ACID),
                'Q' => Ok(&GLUTAMINE),
                'K' => Ok(&LYSINE),
                'E' => Ok(&GLUTAMIC_ACID),
                'M' => Ok(&METHIONINE),
                'H' => Ok(&HISTIDINE),
                'F' => Ok(&PHENYLALANINE),
                'U' => Ok(&SELENOCYSTEINE),
                'R' => Ok(&ARGININE),
                'Y' => Ok(&TYROSINE),
                'W' => Ok(&TRYPTOPHAN),
                'O' => Ok(&PYRROLYSINE),
                'B' => Ok(&ASPARAGINE_OR_ASPARTIC_ACID),
                'Z' => Ok(&GLUTAMINE_OR_GLUTAMIC_ACID),
                'J' => Ok(&ISOLEUCINE_OR_LEUCINE),
                'X' => Ok(&UNKNOWN),
                _ => Err(Error::InvalidAminoAcidCode(code)),
            }
        }
        /// Returns a canonical or non-canoncial amino acid by MaCPepDB's 5 bit code
        ///
        /// # Arguments
        /// * `bit_code` - 5 bit code
        ///
        pub fn by_bit_code(code: &BitCode) -> Result<&'static AminoAcid, Error> {
            let code_raw: u8 = code.as_raw_slice()[0];
            match code_raw {
                GLYCINE_BIT_CODE_RAW => Ok(&GLYCINE),
                ALANINE_BIT_CODE_RAW => Ok(&ALANINE),
                SERINE_BIT_CODE_RAW => Ok(&SERINE),
                PROLINE_BIT_CODE_RAW => Ok(&PROLINE),
                VALINE_BIT_CODE_RAW => Ok(&VALINE),
                THREONINE_BIT_CODE_RAW => Ok(&THREONINE),
                CYSTEINE_BIT_CODE_RAW => Ok(&CYSTEINE),
                LEUCINE_BIT_CODE_RAW => Ok(&LEUCINE),
                ISOLEUCINE_BIT_CODE_RAW => Ok(&ISOLEUCINE),
                ASPARAGINE_BIT_CODE_RAW => Ok(&ASPARAGINE),
                ASPARTIC_ACID_BIT_CODE_RAW => Ok(&ASPARTIC_ACID),
                GLUTAMINE_BIT_CODE_RAW => Ok(&GLUTAMINE),
                LYSINE_BIT_CODE_RAW => Ok(&LYSINE),
                GLUTAMIC_ACID_BIT_CODE_RAW => Ok(&GLUTAMIC_ACID),
                METHIONINE_BIT_CODE_RAW => Ok(&METHIONINE),
                HISTIDINE_BIT_CODE_RAW => Ok(&HISTIDINE),
                PHENYLALANINE_BIT_CODE_RAW => Ok(&PHENYLALANINE),
                SELENOCYSTEINE_BIT_CODE_RAW => Ok(&SELENOCYSTEINE),
                ARGININE_BIT_CODE_RAW => Ok(&ARGININE),
                TYROSINE_BIT_CODE_RAW => Ok(&TYROSINE),
                TRYPTOPHAN_BIT_CODE_RAW => Ok(&TRYPTOPHAN),
                PYRROLYSINE_BIT_CODE_RAW => Ok(&PYRROLYSINE),
                ASPARAGINE_OR_ASPARTIC_ACID_BIT_CODE_RAW => {
                    Ok(&ASPARAGINE_OR_ASPARTIC_ACID)
                }
                GLUTAMINE_OR_GLUTAMIC_ACID_BIT_CODE_RAW => {
                    Ok(&GLUTAMINE_OR_GLUTAMIC_ACID)
                }
                ISOLEUCINE_OR_LEUCINE_BIT_CODE_RAW => Ok(&ISOLEUCINE_OR_LEUCINE),
                UNKNOWN_BIT_CODE_RAW => Ok(&UNKNOWN),
                _ => Err(Error::InvalidAminoAcidBitCode(code.to_string())),
            }
        }
    }
}
#[macro_use]
pub mod mass {
    /// Constant factor for float conversion to integer.
    pub const MASS_CONVERT_FACTOR: f64 = 1000000000.0;
    /// Converts a mass (Dalton) into the internal integer representation.
    ///
    /// # Arguments
    ///
    /// * `mass` - Mass in Dalton
    ///
    pub fn to_int(mass: f64) -> i64 {
        (mass * MASS_CONVERT_FACTOR) as i64
    }
    /// Converts a mass (Dalton) from the internal integer representation to float.
    ///
    /// # Arguments
    ///
    /// * `mass` - Mass in Dalton
    ///
    pub fn to_float(mass: i64) -> f64 {
        mass as f64 / MASS_CONVERT_FACTOR
    }
}
pub mod sequence {
    use std::fmt::{Debug, Display};
    use bitvec::{order::Lsb0, vec::BitVec};
    use thiserror::Error;
    use crate::amino_acid::{AminoAcid, BitCode, Error as AminoAcidError};
    pub enum Error {
        #[error("{0}")]
        AminoAcid(#[from] AminoAcidError),
        #[error("Unable to convert `{0}` into BitCode: {1}")]
        InvalidBitCode(String, String),
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Error {
        #[inline]
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match self {
                Error::AminoAcid(__self_0) => {
                    ::core::fmt::Formatter::debug_tuple_field1_finish(
                        f,
                        "AminoAcid",
                        &__self_0,
                    )
                }
                Error::InvalidBitCode(__self_0, __self_1) => {
                    ::core::fmt::Formatter::debug_tuple_field2_finish(
                        f,
                        "InvalidBitCode",
                        __self_0,
                        &__self_1,
                    )
                }
            }
        }
    }
    #[allow(unused_qualifications)]
    #[automatically_derived]
    impl ::thiserror::__private18::Error for Error {
        fn source(
            &self,
        ) -> ::core::option::Option<&(dyn ::thiserror::__private18::Error + 'static)> {
            use ::thiserror::__private18::AsDynError as _;
            #[allow(deprecated)]
            match self {
                Error::AminoAcid { 0: source, .. } => {
                    ::core::option::Option::Some(source.as_dyn_error())
                }
                Error::InvalidBitCode { .. } => ::core::option::Option::None,
            }
        }
    }
    #[allow(unused_qualifications)]
    #[automatically_derived]
    impl ::core::fmt::Display for Error {
        fn fmt(&self, __formatter: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            use ::thiserror::__private18::AsDisplay as _;
            #[allow(unused_variables, deprecated, clippy::used_underscore_binding)]
            match self {
                Error::AminoAcid(_0) => {
                    match (_0.as_display(),) {
                        (__display0,) => {
                            __formatter.write_fmt(format_args!("{0}", __display0))
                        }
                    }
                }
                Error::InvalidBitCode(_0, _1) => {
                    match (_0.as_display(), _1.as_display()) {
                        (__display0, __display1) => {
                            __formatter
                                .write_fmt(
                                    format_args!(
                                        "Unable to convert `{0}` into BitCode: {1}",
                                        __display0,
                                        __display1,
                                    ),
                                )
                        }
                    }
                }
            }
        }
    }
    #[allow(deprecated, unused_qualifications)]
    #[automatically_derived]
    impl ::core::convert::From<AminoAcidError> for Error {
        fn from(source: AminoAcidError) -> Self {
            Error::AminoAcid { 0: source }
        }
    }
    pub struct Sequence(BitVec<u8, Lsb0>);
    impl Sequence {
        pub fn len(&self) -> usize {
            self.0.len() / 5
        }
        pub fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
    }
    impl Display for Sequence {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0
                .chunks(AminoAcid::BIT_CODE_LEN)
                .try_for_each(|chunk| {
                    let amino_acid = BitCode::try_from(chunk)
                        .map_err(|err| Error::InvalidBitCode(
                            chunk.to_string(),
                            err.to_string(),
                        ))
                        .and_then(|code| {
                            AminoAcid::by_bit_code(&code).map_err(|err| err.into())
                        });
                    match amino_acid {
                        Ok(amino_acid) => {
                            f.write_fmt(format_args!("{0}", amino_acid.code()))
                        }
                        Err(err) => f.write_fmt(format_args!("?[{0}]", err)),
                    }
                })
        }
    }
    impl Debug for Sequence {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("Sequence({0})", self))
        }
    }
    impl TryFrom<&str> for Sequence {
        type Error = Error;
        fn try_from(value: &str) -> Result<Self, Self::Error> {
            let mut vec = bitvec::vec::BitVec::<
                u8,
                Lsb0,
            >::with_capacity(value.len() * AminoAcid::BIT_CODE_LEN);
            for amino_acid in value.chars().map(AminoAcid::by_code) {
                let amino_acid = amino_acid?;
                {
                    ::std::io::_print(
                        format_args!(
                            "{0}; {1:?}; {2:?}; {3}\n",
                            amino_acid.code(),
                            amino_acid.bit_code(),
                            amino_acid.foo(),
                            amino_acid.bit_code().len(),
                        ),
                    );
                };
                vec.extend_from_bitslice(amino_acid.bit_code());
            }
            Ok(Sequence(vec))
        }
    }
}
