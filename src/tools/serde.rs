// 3rd party imports
use serde::{Deserialize, Deserializer, Serializer};

// internal imports
use crate::mass::convert::{to_float as mass_to_float, to_int as mass_as_int};

/// Serialize a mass to a float value
/// can be used with serde as `#[serde(serialize_with = "serialize_mass_to_float")]`
///
/// # Arguments
/// * `value` - Mass as integer
/// * `serializer` - Serializer
///
pub fn serialize_mass_to_float<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_f64(mass_to_float(*value))
}

/// Deserialize a mass from a float value
/// can be used with serde as `#[serde(deserialize_with = "deserialize_mass_from_float")]`
///
/// # Arguments
/// * `deserializer` - Deserializer
///
pub fn deserialize_mass_from_int<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(mass_as_int(f64::deserialize(deserializer)?))
}
