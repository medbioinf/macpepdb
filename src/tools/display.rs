/// Contains formatting functions for displaying data in a (human-)readable format.
///

/// Converts a number of bytes into a human-readable format.
/// E.g. 11811160064 -> "11.00 GB"
///
/// # Arguments
/// `num_bytes` - The number of bytes to convert.
///
pub fn bytes_to_human_readable(num_bytes: u64) -> String {
    let decimal_places = (0..).take_while(|i| 10u64.pow(*i) <= num_bytes).count();
    let mem_unit = match decimal_places {
        0..=3 => "B",
        4..=6 => "KB",
        7..=9 => "MB",
        10..=12 => "GB",
        _ => "TB",
    };
    let mem_display = match decimal_places {
        0..=3 => num_bytes as f64,
        4..=6 => num_bytes as f64 / 1024_f64,
        7..=9 => num_bytes as f64 / 1024_f64.powf(2.0),
        10..=12 => num_bytes as f64 / 1024_f64.powf(3.0),
        _ => num_bytes as f64 / 1024_f64.powf(4.0),
    };
    format!("{:.2} {}", mem_display, mem_unit)
}
