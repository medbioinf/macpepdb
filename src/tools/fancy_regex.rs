/// Module containing functions based on fancy_regex
// 3rd party imports
use fancy_regex::Regex;

/// Splits a string by each match of given regular expression.
/// An empty vector is returned if the regular expression does not match of is faulty.
///
/// # Arguments:
/// * `regex` - fancy_regex::Regex which matches the split positions.
/// * `some_string` - String to split.
///
pub fn split<'a>(regex: &Regex, some_string: &'a str) -> Vec<&'a str> {
    let mut sub_strings: Vec<&'a str> = Vec::new();
    let mut sub_string_start: usize = 0;
    for split_matches in regex.find_iter(some_string) {
        match split_matches {
            Ok(split_match) => {
                sub_strings.push(&some_string[sub_string_start..split_match.start()]);
                sub_string_start = split_match.end();
            }
            Err(_) => return Vec::new(),
        }
    }
    sub_strings.push(&some_string[sub_string_start..some_string.len()]);
    return sub_strings;
}

#[cfg(test)]
mod test {
    use tracing::error;

    use super::*;

    #[test]
    fn test_split() {
        lazy_static! {
            static ref TEST_REGEX: Regex = Regex::new(r"\|").unwrap();
        }
        const TEST_STRING: &'static str = "TEST|STRING|IS|PIPE|SEPARATED;";
        const SPLITTED_TEST_STRING: [&'static str; 5] =
            ["TEST", "STRING", "IS", "PIPE", "SEPARATED;"];

        let splitted_string: Vec<&str> = split(&TEST_REGEX, TEST_STRING);
        assert_eq!(SPLITTED_TEST_STRING.len(), splitted_string.len());

        'sub_string_loop: for sub_sting in splitted_string {
            for desired_sub_string in &SPLITTED_TEST_STRING {
                if *desired_sub_string == sub_sting {
                    continue 'sub_string_loop;
                }
            }
            error!(
                "Did not find substring in '{}' in SPLITTED_TEST_STRING",
                sub_sting
            );
            panic!("See error above.");
        }
    }
}
