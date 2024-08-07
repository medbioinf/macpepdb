// # Create absolute path to readme ti increase compatible for different build targets
//  https://gist.github.com/JakeHartnell/2c1fa387f185f5dc46c9429470a2e2be
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Readme.md"))]

// 3rd party imports
#[allow(unused_imports)]
#[macro_use]
extern crate lazy_static;

// Internal imports including macros
/// Contains mass related functions
#[macro_use]
pub mod mass;
/// Contains handy functions
#[macro_use]
pub mod tools;

/// Chemistry related functions and information, e.g. molecule masses
pub mod chemistry;
/// Functions to maintain and access the database
pub mod database;
/// Contains different entities, e.g. proteins, peptides, etc.
pub mod entities;
/// Various functions to prepare queries, access the database, etc.
pub mod functions;
/// Input and output functions
pub mod io;
/// Web API, for details how to use the endpoints look into the function of the `*_controller`-modules
pub mod web;
