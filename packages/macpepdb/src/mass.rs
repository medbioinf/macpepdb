/// Constant factor for float conversion to integer.
pub const MASS_CONVERT_FACTOR: f64 = 1_000_000_000.0;

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
#[macro_export]
macro_rules! mass_to_int {
    ($mass:expr) => {{ ($mass as f64 * $crate::mass::MASS_CONVERT_FACTOR) as i64 }};
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
    fn test_mass_to_int_macro() {
        const DALTON_FLOAT: f64 = 859.49506802369;
        const EXPECTED_DALTON_INT_CONVERSION: i64 = 859495068023;
        assert_eq!(mass_to_int!(DALTON_FLOAT), EXPECTED_DALTON_INT_CONVERSION)
    }

    /// Just a little sanity check which make sure it does not make little to now difference
    /// if the tolerance is calculated in MaCPepDB's integer representation instead of the user given float.
    ///
    /// In the used example only the 9th decimal place would be different
    ///
    #[test]
    fn test_tolerance_calc() {
        let tolerance_ppm_f: f64 = 10.0;
        let mass_f = 768.428258998;

        let lower_mass_f = mass_f - (mass_f / 1_000_000.0_f64 * tolerance_ppm_f);
        let upper_mass_f = mass_f + (mass_f / 1_000_000.0_f64 * tolerance_ppm_f);

        let tollerance_ppm_i: i64 = 10;
        let mass_i = mass_to_int!(mass_f);

        let lower_mass_i = mass_i - (mass_i / 1_000_000 * tollerance_ppm_i);
        let upper_mass_i = mass_i + (mass_i / 1_000_000 * tollerance_ppm_i);

        let lower_mass_f_as_i = mass_to_int!(lower_mass_f);
        let upper_mass_f_as_i = mass_to_int!(upper_mass_f);

        // Asserts that the first 11 digits of the mass are equal when calculating the tolerance in MaCPepDB's integer representation,
        // leaving little rounding issue on position 12 (corresponding to the 9 decimal place of the original f64 representation)
        for i in (1..=11_u32).rev() {
            assert_eq!(
                lower_mass_i / 10_i64.pow(i),
                lower_mass_f_as_i / 10_i64.pow(i),
            );

            assert_eq!(
                upper_mass_i / 10_i64.pow(i),
                upper_mass_f_as_i / 10_i64.pow(i),
            );
        }
    }
}
