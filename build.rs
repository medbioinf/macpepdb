// std import
use std::env;
use std::fs;
use std::path::Path;

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

    // Tell cargo when to rerun the build
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=frontend.config.toml");
    println!("cargo:rerun-if-env-changed=MDBF_CONFIG");
}
