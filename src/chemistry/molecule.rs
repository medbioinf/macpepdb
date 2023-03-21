/// Module containing different molecules.
/// As amino acids have a special place in proteomics, they got their own module.
/// 

// 3rd party imports
use anyhow::{Result, bail};

pub struct Molecule {
    name: &'static str,
    mono_mass: i64,
    average_mass: i64,
}

impl Molecule {
    /// Returns the molecule with the given name
    /// ’
    /// # Arguments
    /// * `name` - Name
    /// 
    pub fn get_by_name(name: &str) -> Result<&'static Self> {
        return match name.to_lowercase().as_str() {
            "water" => Ok(&WATER),
            _ => bail!("Unknown name: {}", name)
        };
    }

    pub fn get_all() -> &'static [&'static Molecule; 1] {
        return &ALL;
    }

    pub fn get_name(&self) -> &'static str {
        return &self.name;
    }

    pub fn get_mono_mass(&self) -> i64 {
        return self.mono_mass;
    }

    pub fn get_average_mass(&self) -> i64 {
        return self.average_mass;
    }

}


pub const WATER: Molecule = Molecule{name: "Water", mono_mass: mass_to_int!(18.010564700_f64), average_mass: mass_to_int!(18.015_f64)};

const ALL: [&'static Molecule; 1] = [
    &WATER,
];


#[cfg(test)]
mod test {
    // internal imports
    use crate::mass::convert::to_int as mass_to_int;
    use super::*;
    

    // Raw values of molecules without mass conversion.
    const RAW_MOLECULE_VALUES: [(&'static str, f64, f64); 1] = [
        ("Water", 18.010564700, 18.015),
    ];

    // Test if the molecules attributes are equal to to the tuple values. This ensures, that the tuple and the molecule is matching and if the mass is converted successfully with the macro.
    fn test_equality_of_molecule_attributes_and_raw_value_tuple(molecule: &Molecule, raw_value_tuple: &(&'static str, f64, f64)) {
        assert_eq!(molecule.name, raw_value_tuple.0);
        assert_eq!(molecule.mono_mass, mass_to_int(raw_value_tuple.1));
        assert_eq!(molecule.average_mass, mass_to_int(raw_value_tuple.2));
    }

    #[test]
    fn test_get_by_name_and_value_correctness() {
        for raw_value_tuple in &RAW_MOLECULE_VALUES {
            let molecule: &Molecule = &Molecule::get_by_name(raw_value_tuple.0).unwrap();
            test_equality_of_molecule_attributes_and_raw_value_tuple(molecule, raw_value_tuple)
        }  
    }

    #[test]
    fn test_all_array_and_value_correctness() {
        let molecules: &[&'static Molecule; 1] = Molecule::get_all();


        // Test if all raw value tuples are found in molecules
        'tuple_loop: for raw_value_tuple in &RAW_MOLECULE_VALUES {
            for molecule in molecules {
                if raw_value_tuple.0 == molecule.name {
                    test_equality_of_molecule_attributes_and_raw_value_tuple(molecule, raw_value_tuple);
                    // Start next iteration of outer loop, to omit the panic.
                    continue 'tuple_loop;
                }
            }
            // This panic can only be reached when the matching molecule was not found in molecules.
            println!("Did not found {} in `molecules`.", raw_value_tuple.0);
            panic!("See error above.");
        }

        // Test if all known molecules are found in the raw value tuples
        'molecule_loop: for molecule in molecules {
            for raw_value_tuple in &RAW_MOLECULE_VALUES {
                if raw_value_tuple.0 == molecule.name {
                    test_equality_of_molecule_attributes_and_raw_value_tuple(molecule, raw_value_tuple);
                    // Start next iteration of outer loop, to omit the panic.
                    continue 'molecule_loop;
                }
            }
            // This panic can only be reached when the matching raw value tuple was not found in RAW_MOLECULE_VALUES.
            println!("Did not found {} in `RAW_MOLECULE_VALUES`.", molecule.name);
            panic!("See error above.");
        }
    }
}