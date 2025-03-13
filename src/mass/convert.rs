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

/// Makro for mass to integer conversion. The `to_int`-method is intentionally not used, so the macro can be used in assignments of constants.
/// Attention: It is not possible to limit the the macros argument to a specific type. Be careful to pass only  
///
// used in build.rs
#[allow(unused_macros)]
macro_rules! mass_to_int {
    ($mass:expr) => {{
        ($mass as f64 * crate::mass::convert::MASS_CONVERT_FACTOR) as i64
    }};
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_mass_int_float_conversion() {
        const DALTON_FLOAT: f64 = 859.49506802369;
        // Due to the conversion we lose two decimal places
        const EXPECTED_DALTON_INT_CONVERSION: i64 = 859495068023;
        const EXPECTED_DALTON_FLOAT_CONVERSION: f64 = 859.495068023;

        let dalton_int: i64 = to_int(DALTON_FLOAT);
        assert_eq!(dalton_int, EXPECTED_DALTON_INT_CONVERSION);
        let dalton_float: f64 = to_float(dalton_int);
        assert_eq!(dalton_float, EXPECTED_DALTON_FLOAT_CONVERSION);
    }

    #[test]
    /// Macros are imported in crate root. So we have to test them here.
    fn test_mass_to_int_macro() {
        const DALTON_FLOAT: f64 = 859.49506802369;
        const EXPECTED_DALTON_INT_CONVERSION: i64 = 859495068023;
        assert_eq!(mass_to_int!(DALTON_FLOAT), EXPECTED_DALTON_INT_CONVERSION)
    }
}
