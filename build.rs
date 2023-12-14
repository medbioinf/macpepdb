// std import
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;

// 3rd party imports
use dihardts_omicstools::chemistry::amino_acid::AminoAcid;
use dihardts_omicstools::chemistry::amino_acid::CANONICAL_AMINO_ACIDS;
use dihardts_omicstools::chemistry::amino_acid::NON_CANONICAL_AMINO_ACIDS;

fn amino_acid_name_to_const_name(name: &str) -> String {
    name.replace(" ", "_").to_uppercase()
}

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    // Create internal amino acids with mass as integer
    let amino_acids_rs_path = Path::new(&out_dir).join("amino_acid.rs");
    let mut amino_acid_rs_content: String = "".to_string();
    let mut code_amino_acid_map: HashMap<char, String> = HashMap::new();
    for aa in CANONICAL_AMINO_ACIDS.iter() {
        let const_name = amino_acid_name_to_const_name(aa.get_name());
        let internal_aa = format!(
            r#"
            pub const INTERNAL_{}: InternalAminoAcid = InternalAminoAcid {{
                inner_amino_acid: &{},
                mono_mass_int: mass_to_int!{{{}}},
            }};
            
        "#,
            &const_name,
            &const_name,
            aa.get_mono_mass()
        );
        amino_acid_rs_content.push_str(&internal_aa);
        code_amino_acid_map.insert(*aa.get_code(), const_name);
    }
    for aa in NON_CANONICAL_AMINO_ACIDS.iter() {
        let const_name = amino_acid_name_to_const_name(aa.get_name());
        let internal_aa = format!(
            r#"
            pub const INTERNAL_{}: InternalAminoAcid = InternalAminoAcid {{
                inner_amino_acid: &{},
                mono_mass_int: mass_to_int!{{{}}},
            }};
            
        "#,
            &const_name,
            &const_name,
            aa.get_mono_mass()
        );
        amino_acid_rs_content.push_str(&internal_aa);
        code_amino_acid_map.insert(*aa.get_code(), const_name);
    }
    amino_acid_rs_content.push_str(
        r#"
        /// Returns a canonical or non-canoncial amino acid by one letter code
        /// 
        /// # Arguments 
        /// * `code` - One letter code
        /// 
        pub fn get_internal_amino_acid_by_one_letter_code(code: char) -> Result<&'static InternalAminoAcid> {
            match code.to_ascii_uppercase() {
        "#,
    );
    for (code, const_name) in code_amino_acid_map.iter() {
        amino_acid_rs_content.push_str(&format!(
            r#"
                    '{}' => Ok(&INTERNAL_{}),
            "#,
            code, const_name
        ));
    }
    amino_acid_rs_content.push_str(
        r#"
                _ => bail!("Unknown amino acid code: {}", code),
            }
        }
        "#,
    );

    fs::write(amino_acids_rs_path, amino_acid_rs_content).unwrap();

    // Tell cargo when to rerun the build
    println!("cargo:rerun-if-changed=build.rs");
}
