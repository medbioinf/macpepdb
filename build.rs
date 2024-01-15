// std import
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;

// 3rd party imports
use dihardts_omicstools::chemistry::amino_acid::AminoAcid;
use dihardts_omicstools::chemistry::amino_acid::CANONICAL_AMINO_ACIDS;
use dihardts_omicstools::chemistry::amino_acid::NON_CANONICAL_AMINO_ACIDS;
use dihardts_omicstools::proteomics::proteases::functions::ALL as ALL_PROTEASES;

fn amino_acid_name_to_const_name(name: &str) -> String {
    name.replace(" ", "_").to_uppercase()
}

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let manifest_dir = env::var_os("CARGO_MANIFEST_DIR").unwrap();

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

    // Create an protease enum for CLI
    let protease_rs_path = Path::new(&out_dir).join("protease_choice.rs");
    let protease_rs_template =
        fs::read_to_string(Path::new(&manifest_dir).join("template_files/protease_choice.rs"))
            .unwrap();
    let mut protease_enum_variants: Vec<String> = Vec::new();
    let mut protease_enum_from_string: Vec<String> = Vec::new();
    let mut protease_enum_to_str: Vec<String> = Vec::new();
    let variants_len: usize = ALL_PROTEASES.len();
    let mut variants_with_enum_prefix: Vec<String> = Vec::new();
    for (idx, protease_name) in ALL_PROTEASES.iter().enumerate() {
        // Camel case the name
        let protease_enum_name = protease_name
            .split(' ')
            .map(|part| {
                let mut part = part.to_lowercase();
                part[0..1].make_ascii_uppercase();
                part
            })
            .collect::<Vec<String>>()
            .join("");

        protease_enum_variants.push(format!("{},", protease_enum_name));
        protease_enum_from_string.push(format!(
            "\"{}\" => Ok(ProteaseChoice::{}),",
            protease_name, protease_enum_name,
        ));
        protease_enum_to_str.push(format!(
            "ProteaseChoice::{} => ALL_PROTEASES[{}],",
            protease_enum_name, idx
        ));
        variants_with_enum_prefix.push(format!("ProteaseChoice::{}, ", protease_enum_name));
    }
    // Replace placeholders in template
    let protease_rs_content = protease_rs_template
        .replace("<<VARIANTS>>", &protease_enum_variants.join("\n    "))
        .replace(
            "<<FROM_STR>>",
            &protease_enum_from_string.join("\n            "),
        )
        .replace("<<TO_STR>>", &protease_enum_to_str.join("\n            "))
        .replace("<<VARIANTS_LEN>>", &variants_len.to_string())
        .replace(
            "<<VARIANTS_WITH_ENUM_PREFIX>>",
            &variants_with_enum_prefix.join("\n    "),
        );
    // Write to file
    fs::write(protease_rs_path, protease_rs_content).unwrap();

    // Tell cargo when to rerun the build
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=template_files/protease_choice.rs");
}
