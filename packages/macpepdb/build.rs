use std::env;
use std::fs;
use std::path::Path;

use dihardts_omicstools::chemistry::amino_acid::AminoAcid;
use dihardts_omicstools::chemistry::amino_acid::CANONICAL_AMINO_ACIDS;
use dihardts_omicstools::chemistry::amino_acid::NON_CANONICAL_AMINO_ACIDS;
use dihardts_omicstools::chemistry::element::get_element_by_symbol;
// use dihardts_omicstools::proteomics::proteases::functions::ALL as ALL_PROTEASES;
//

fn create_amino_acid_bit_code(amino_acid: &impl AminoAcid) -> Result<u8, String> {
    let bit_code = *amino_acid.get_code() as u8 - b'A';
    if bit_code >= 2_u8.pow(5) {
        return Err(format!(
            "Amino acid bit code is supposed to be < 2^5, got `{}`",
            bit_code
        ));
    }
    Ok(bit_code)
}

fn create_amino_acid_tuple_for_macro_call(amino_acid: &impl AminoAcid) -> String {
    format!(
        "(\"{}\"; '{}'; {}; {})",
        amino_acid.get_name(),
        amino_acid.get_code(),
        amino_acid.get_mono_mass(),
        create_amino_acid_bit_code(amino_acid).unwrap()
    )
}

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    // let manifest_dir = env::var_os("CARGO_MANIFEST_DIR").unwrap();

    // Create macro call to create internal amino acids with interger mass etc and bit code.
    let amino_acids_rs_path = Path::new(&out_dir).join("amino_acid.rs");

    let mut amino_acids: Vec<String> = CANONICAL_AMINO_ACIDS
        .iter()
        .map(create_amino_acid_tuple_for_macro_call)
        .collect();

    amino_acids.extend(
        NON_CANONICAL_AMINO_ACIDS
            .iter()
            .map(create_amino_acid_tuple_for_macro_call)
            .collect::<Vec<String>>(),
    );

    let amino_acid_rs_content: String = format!(
        r#"
        create_const_amino_acids!([
            {},
        ]);
        "#,
        amino_acids.join(",\n            ")
    );

    fs::write(amino_acids_rs_path, amino_acid_rs_content).unwrap();

    // Molecules
    let molecules_rs_path = Path::new(&out_dir).join("molecules.rs");
    let hydrogen_mono_mass = *get_element_by_symbol("H").unwrap().get_mono_mass();
    let oxigen_mono_mass = *get_element_by_symbol("O").unwrap().get_mono_mass();
    let molecules_rs_content: String = format!(
        r#"
        pub const WATER_MONO_MASS: i64 = mass_to_int!({hydrogen_mono_mass}) + mass_to_int!({hydrogen_mono_mass}) + mass_to_int!({oxigen_mono_mass});
        "#,
    );
    fs::write(molecules_rs_path, molecules_rs_content).unwrap();

    // // Create an protease enum for CLI
    // let protease_rs_path = Path::new(&out_dir).join("protease_choice.rs");
    // let protease_rs_template =
    //     fs::read_to_string(Path::new(&manifest_dir).join("template_files/protease_choice.rs"))
    //         .unwrap();
    // let mut protease_enum_variants: Vec<String> = Vec::new();
    // let mut protease_enum_from_string: Vec<String> = Vec::new();
    // let mut protease_enum_to_str: Vec<String> = Vec::new();
    // let variants_len: usize = ALL_PROTEASES.len();
    // let mut variants_with_enum_prefix: Vec<String> = Vec::new();
    // for (idx, protease_name) in ALL_PROTEASES.iter().enumerate() {
    //     // Camel case the name
    //     let protease_enum_name = protease_name
    //         .split(' ')
    //         .map(|part| {
    //             let mut part = part.to_lowercase();
    //             part[0..1].make_ascii_uppercase();
    //             part
    //         })
    //         .collect::<Vec<String>>()
    //         .join("");

    //     protease_enum_variants.push(format!("{},", protease_enum_name));
    //     protease_enum_from_string.push(format!(
    //         "\"{}\" => Ok(ProteaseChoice::{}),",
    //         protease_name, protease_enum_name,
    //     ));
    //     protease_enum_to_str.push(format!(
    //         "ProteaseChoice::{} => ALL_PROTEASES[{}],",
    //         protease_enum_name, idx
    //     ));
    //     variants_with_enum_prefix.push(format!("ProteaseChoice::{}, ", protease_enum_name));
    // }
    // // Replace placeholders in template
    // let protease_rs_content = protease_rs_template
    //     .replace("<<VARIANTS>>", &protease_enum_variants.join("\n    "))
    //     .replace(
    //         "<<FROM_STR>>",
    //         &protease_enum_from_string.join("\n            "),
    //     )
    //     .replace("<<TO_STR>>", &protease_enum_to_str.join("\n            "))
    //     .replace("<<VARIANTS_LEN>>", &variants_len.to_string())
    //     .replace(
    //         "<<VARIANTS_WITH_ENUM_PREFIX>>",
    //         &variants_with_enum_prefix.join("\n    "),
    //     );
    // // Write to file
    // fs::write(protease_rs_path, protease_rs_content).unwrap();

    // Tell cargo when to rerun the build
    println!("cargo:rerun-if-changed=build.rs");
    // println!("cargo:rerun-if-changed=template_files/protease_choice.rs");
}
