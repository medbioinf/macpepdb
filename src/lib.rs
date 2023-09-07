// # Create absolute path to readme ti increase compatible for different build targets
//  https://gist.github.com/JakeHartnell/2c1fa387f185f5dc46c9429470a2e2be
#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Readme.md"))]
// Going nightly for now to use async traits and related features
// Accoirding to [this article](https://blog.rust-lang.org/inside-rust/2023/05/03/stabilizing-async-fn-in-trait.html)
// the feature should be stable in 1.74 (estimated)
#![feature(async_fn_in_trait)]
#![feature(impl_trait_projections)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]
#![feature(async_iterator)]

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

/// Biology related functions, e.g. digestion enzymes
pub mod biology;
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
