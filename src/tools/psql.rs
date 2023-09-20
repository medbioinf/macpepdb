// 3rd party imports
use anyhow::Result;

/// Convert place holder to PostgreSQL format
/// e.g. "SELECT * FROM table WHERE id = {} AND partition = {}"
/// to "SELECT * FROM table WHERE id = $1 AND partition = $2"
///
/// # Arguments
/// * `query` - The query to convert
///
pub fn convert_placeholders(query: &str) -> Result<String> {
    let mut num_placeholders: u64 = 1;

    // Processing the queue as byte array is a 2-3 faster than using char iterator or str-slices
    let mut finished_query: Vec<u8> = Vec::with_capacity(query.len());
    let b_query = query.as_bytes();

    let mut i: usize = 0;
    while i < query.len() {
        if i == query.len() - 1 || b_query[i] != b'{' && b_query[i + 1] != b'}' {
            finished_query.push(b_query[i]);
            i += 1;
        } else {
            finished_query.push(b'$');
            finished_query.extend(num_placeholders.to_string().as_bytes());
            num_placeholders += 1;
            i += 2;
        }
    }
    Ok(String::from_utf8(finished_query)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_placeholders() {
        let query = "SELECT * FROM table WHERE id = {} AND partition = {}";
        let expected = "SELECT * FROM table WHERE id = $1 AND partition = $2";

        let actual = convert_placeholders(query).unwrap();
        assert_eq!(expected, actual);
    }
}
