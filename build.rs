// std import
use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

fn main() {
    // Copy the frontend configuration file to the build directory
    let src_config_path_str = match env::var_os("MDBF_CONFIG") {
        Some(var) => var.to_str().unwrap().to_string(),
        None => "frontend.config.toml".to_string(),
    };
    let src_config_path = Path::new(&src_config_path_str);
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_config_path = Path::new(&out_dir).join("config.toml");
    fs::copy(src_config_path, dest_config_path).unwrap();

    // Deal with assets
    let assets_path = Path::new("assets");
    let public_path = Path::new("public");
    //// Compile sass
    let sass_path = assets_path.join("sass");
    let main_sass_path = sass_path.join("index.sass");
    let css_path = public_path.join("index.css");
    Command::new("sass")
        .arg(main_sass_path)
        .arg(css_path)
        .status()
        .unwrap();

    // Tell cargo when to rerun the build
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=frontend.config.toml");
    println!("cargo:rerun-if-env-changed=MDBF_CONFIG");

    // Add all sass files to the watch list
    for entry in fs::read_dir(sass_path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            continue;
        }
        println!("cargo:rerun-if-changed={}", path.to_str().unwrap());
    }
}
