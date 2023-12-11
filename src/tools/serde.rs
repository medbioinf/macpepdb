// 3rd party imports
use serde::{Deserialize, Deserializer, Serializer};

// internal imports
use crate::mass::convert::{to_float as mass_to_float, to_int as mass_as_int};

/// Serialize a mass to a float value
/// can be used with serde as `#[serde(serialize_with = "serialize_mass_to_float")]`
/// Useful to provide human readable output with the web API.
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
/// Useful to provide human readable output with the web API.
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

/// Serialize a vector of integer masses to a vector of floats masses for serialization
/// can be used with serde as `#[serde(serialize_with = "serialize_mass_vec_to_float_vec")]`
/// Useful to provide human readable output with the web API.
///
/// # Arguments
/// * `value` - Mass as integer
/// * `serializer` - Serializer
///
pub fn serialize_mass_vec_to_float_vec<S>(
    value: &Vec<i64>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_seq(value.iter().map(|v| mass_to_float(*v)))
}

/// Deserialize a vector of float masses to a vector of integer masses for deserialization
/// can be used with serde as `#[serde(deserialize_with = "deserialize_mass_vec_from_int_vec")]`
/// Useful to provide human readable output with the web API.
///
/// # Arguments
/// * `deserializer` - Deserializer
///
pub fn deserialize_mass_vec_from_int_vec<'de, D>(deserializer: D) -> Result<Vec<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Vec::<f64>::deserialize(deserializer)?
        .into_iter()
        .map(|v| mass_as_int(v))
        .collect())
}
