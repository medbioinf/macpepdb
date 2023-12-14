// 3rd party imports
use dihardts_omicstools::chemistry::element::get_element_by_symbol;

// internal imports
use crate::mass::convert::to_int as mass_to_int;

lazy_static! {
    pub static ref WATER_MONO_MASS: i64 =
        mass_to_int(*get_element_by_symbol("H").unwrap().get_mono_mass()) * 2
            + mass_to_int(*get_element_by_symbol("O").unwrap().get_mono_mass());
}
