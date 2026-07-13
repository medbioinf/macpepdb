use std::collections::HashMap;
use std::fmt;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Expected a feature key/location line but found indented line: `{0}`")]
    UnexpectedContinuation(String),
    #[error("Feature `{0}` is missing a location")]
    MissingLocation(String),
    #[error("Qualifier line `{0}` is not in the form `/name=\"value\"`")]
    MalformedQualifier(String),
    #[error("Qualifier `/{0}=\"...\"` is never closed with a matching quote")]
    UnterminatedQuotedValue(String),
    #[error("Failed to parse position as integer: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
}

/// A feature's start/end index, possibly fuzzy (`<`/`>`) or fully unknown (`?`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Index {
    Fix(u32),
    /// e.g. <3
    Before(u32),
    /// e.g. >3
    After(u32),
    // e.g. ?
    Unknown,
    /// e.g. ?3
    Uncertain(u32),
}

impl Index {
    fn parse(value: &str) -> Result<Self, Error> {
        if value == "?" {
            return Ok(Index::Unknown);
        }
        if let Some(rest) = value.strip_prefix('?') {
            return Ok(Index::Uncertain(rest.parse()?));
        }
        if let Some(rest) = value.strip_prefix('<') {
            return Ok(Index::Before(rest.parse()?));
        }
        if let Some(rest) = value.strip_prefix('>') {
            return Ok(Index::After(rest.parse()?));
        }
        Ok(Index::Fix(value.parse()?))
    }
}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Index::Fix(pos) => write!(f, "{pos}"),
            Index::Before(pos) => write!(f, "<{pos}"),
            Index::After(pos) => write!(f, ">{pos}"),
            Index::Uncertain(pos) => write!(f, "?{pos}"),
            Index::Unknown => write!(f, "?"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    start: Index,
    end: Index,
}

impl Position {
    fn parse(value: &str) -> Result<Self, Error> {
        match value.split_once("..") {
            Some((start, end)) => Ok(Position {
                start: Index::parse(start)?,
                end: Index::parse(end)?,
            }),
            None => {
                let index = Index::parse(value)?;
                Ok(Position {
                    start: index,
                    end: index,
                })
            }
        }
    }

    pub fn start(&self) -> Index {
        self.start
    }

    pub fn end(&self) -> Index {
        self.end
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}..{}", self.start, self.end)
        }
    }
}

/// A feature's location, e.g. `202..301`, `<202..259` or an isoform-qualified `P15005-2:1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    isoform_accession: Option<String>,
    position: Position,
}

impl Location {
    fn parse(value: &str) -> Result<Self, Error> {
        let (isoform_accession, position_str) = match value.split_once(':') {
            Some((accession, rest)) => (Some(accession.to_string()), rest),
            None => (None, value),
        };

        Ok(Location {
            isoform_accession,
            position: Position::parse(position_str)?,
        })
    }

    pub fn isoform_accession(&self) -> Option<&str> {
        self.isoform_accession.as_deref()
    }

    pub fn position(&self) -> Position {
        self.position
    }
}

/// The parsed operation described by a `/note` value, e.g. `"Missing"` or `"A -> B"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoteOperation {
    Missing,
    Replacement { from: String, to: String },
    Other(String),
}

/// Splits a `/note` value into its leading operation text and trailing `(...)` explanation, if any.
fn split_note(note: &str) -> (&str, Option<&str>) {
    let trimmed = note.trim();
    if let Some(open) = trimmed.find('(')
        && trimmed.ends_with(')')
    {
        return (
            trimmed[..open].trim_end(),
            Some(&trimmed[open + 1..trimmed.len() - 1]),
        );
    }
    (trimmed, None)
}

fn parse_note_operation(note: &str) -> NoteOperation {
    let (core, _) = split_note(note);
    if core == "Missing" {
        NoteOperation::Missing
    } else if let Some((from, to)) = core.split_once("->") {
        NoteOperation::Replacement {
            from: from.trim().to_string().replace(char::is_whitespace, ""),
            to: to.trim().to_string().replace(char::is_whitespace, ""),
        }
    } else {
        NoteOperation::Other(core.to_string())
    }
}

/// Extracts every isoform label mentioned in a `/note` explanation, e.g. `(in isoform 3)` ->
/// `["3"]`, `(in isoform 33 kDa)` -> `["33 kDa"]` and the comma/`and`-separated list
/// `(in isoform 4, isoform 5 and isoform 6)` -> `["4", "5", "6"]`. Explanations that don't
/// mention an isoform (e.g. `(in HTX12; ...)`, `(in Ref. 2)`) yield an empty vec.
fn extract_isoform_labels(note: &str) -> Vec<String> {
    let (_, explanation) = split_note(note);
    let Some(mut remaining) = explanation else {
        return Vec::new();
    };
    const MARKER: &str = "isoform ";
    let mut labels = Vec::new();
    while let Some(marker_idx) = remaining.find(MARKER) {
        let after = &remaining[marker_idx + MARKER.len()..];
        let stop_delim = after.find([';', ')']).unwrap_or(after.len());
        let next_marker = after.find(MARKER).unwrap_or(after.len());
        let end = stop_delim.min(next_marker);
        let label = after[..end]
            .trim()
            .trim_end_matches(" and")
            .trim_end_matches(',')
            .trim();
        if !label.is_empty() {
            labels.push(label.to_string());
        }
        remaining = &after[end..];
    }
    labels
}

/// A single UniProt feature-table entry, e.g. a `VAR_SEQ` or `VARIANT` block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Feature {
    key: String,
    location: Location,
    qualifiers: HashMap<String, String>,
}

impl Feature {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn location(&self) -> &Location {
        &self.location
    }

    pub fn qualifier(&self, name: &str) -> Option<&str> {
        self.qualifiers.get(name).map(String::as_str)
    }

    pub fn id(&self) -> Option<&str> {
        self.qualifier("id")
    }

    pub fn note(&self) -> Option<&str> {
        self.qualifier("note")
    }

    pub fn note_operation(&self) -> Option<NoteOperation> {
        self.note().map(parse_note_operation)
    }
}

/// Parses the leading `/name="` of a qualifier line, returning the name, the value collected so
/// far and whether the closing quote has already been seen on this same line.
fn parse_qualifier_start(line: &str) -> Result<(String, String, bool), Error> {
    let rest = line
        .strip_prefix('/')
        .ok_or_else(|| Error::MalformedQualifier(line.to_string()))?;
    let eq_idx = rest
        .find('=')
        .ok_or_else(|| Error::MalformedQualifier(line.to_string()))?;
    let name = rest[..eq_idx].to_string();
    let after_quote = rest[eq_idx + 1..]
        .strip_prefix('"')
        .ok_or_else(|| Error::MalformedQualifier(line.to_string()))?;

    match after_quote.find('"') {
        Some(close_idx) => Ok((name, after_quote[..close_idx].to_string(), true)),
        None => Ok((name, after_quote.to_string(), false)),
    }
}

#[derive(Debug)]
pub struct FeatureTable {
    features: Vec<Feature>,
}

impl FeatureTable {
    pub fn features(&self) -> &[Feature] {
        &self.features
    }

    pub fn into_features(self) -> Vec<Feature> {
        self.features
    }
}

impl TryFrom<&str> for FeatureTable {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut features = Vec::new();
        let mut lines = value.lines().peekable();

        while let Some(header_line) = lines.next() {
            if header_line.trim().is_empty() {
                continue;
            }
            if header_line.starts_with(char::is_whitespace) {
                return Err(Error::UnexpectedContinuation(header_line.to_string()));
            }

            let mut header_parts = header_line.splitn(2, char::is_whitespace);
            let key = header_parts.next().unwrap().to_string();
            let location_str = header_parts.next().unwrap_or("").trim();
            if location_str.is_empty() {
                return Err(Error::MissingLocation(key));
            }
            let location = Location::parse(location_str)?;

            let mut qualifiers = HashMap::new();
            while let Some(next_line) = lines.peek() {
                if !next_line.starts_with(char::is_whitespace) {
                    break;
                }
                let line = lines.next().unwrap().trim_start();
                let (name, mut buffer, mut closed) = parse_qualifier_start(line)?;

                while !closed {
                    let cont_line = lines
                        .next()
                        .ok_or_else(|| Error::UnterminatedQuotedValue(name.clone()))?
                        .trim_start();
                    match cont_line.find('"') {
                        Some(close_idx) => {
                            buffer.push(' ');
                            buffer.push_str(&cont_line[..close_idx]);
                            closed = true;
                        }
                        None => {
                            buffer.push(' ');
                            buffer.push_str(cont_line);
                        }
                    }
                }

                qualifiers.insert(name, buffer);
            }

            features.push(Feature {
                key,
                location,
                qualifiers,
            });
        }

        Ok(FeatureTable { features })
    }
}

pub fn parse(text: &str) -> Result<Vec<Feature>, Error> {
    Ok(FeatureTable::try_from(text)?.into_features())
}

/// Determines which isoform(s) (if any) a feature is associated with: an explicit
/// `<ISOFORM_ACCESSION>:` location prefix takes precedence (label = the accession's suffix
/// after its last `-`, e.g. `P15005-2` -> `"2"`); otherwise falls back to every isoform
/// mentioned in a `/note`, e.g. `"in isoform 4, isoform 5 and isoform 6"` -> `["4", "5", "6"]`.
/// Returns an empty vec if neither mechanism applies.
fn isoform_labels(feature: &Feature) -> Vec<String> {
    if let Some(accession) = feature.location().isoform_accession() {
        let label = match accession.rsplit_once('-') {
            Some((_, suffix)) => suffix.to_string(),
            None => accession.to_string(),
        };
        return vec![label];
    }
    feature
        .note()
        .map(extract_isoform_labels)
        .unwrap_or_default()
}

/// Groups features by the isoform(s) they are associated with — either via an explicit
/// `<ISOFORM_ACCESSION>:` location prefix (e.g. `P15005-2:1`) or a `/note` mention of
/// `"in isoform ..."` (possibly a comma/`and`-separated list naming several isoforms) —
/// preserving the order in which each label was first seen. A feature naming several isoforms
/// is added to every one of their groups. Features referencing no isoform by either mechanism
/// (e.g. `VARIANT`/`CONFLICT` entries describing strains or references) are omitted.
pub fn group_by_isoform(features: &[Feature]) -> Vec<(String, Vec<&Feature>)> {
    let mut groups: Vec<(String, Vec<&Feature>)> = Vec::new();
    for feature in features {
        for label in isoform_labels(feature) {
            match groups.iter_mut().find(|(existing, _)| existing == &label) {
                Some((_, group)) => group.push(feature),
                None => groups.push((label, vec![feature])),
            }
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_fixture() -> Vec<Feature> {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("feature_table.txt");
        let content = std::fs::read_to_string(path).unwrap();
        parse(&content).unwrap()
    }

    #[test]
    fn test_fixture_parses_into_expected_feature_count() {
        assert_eq!(load_fixture().len(), 19);
    }

    #[test]
    fn test_unknown_position() {
        let features = load_fixture();
        let variant = features
            .iter()
            .find(|f| f.key() == "VARIANT" && f.location().position().start() == Index::Unknown)
            .unwrap();
        assert_eq!(variant.location().position().end(), Index::Unknown);
    }

    #[test]
    fn test_fuzzy_start_and_end_positions() {
        let features = load_fixture();
        let var_seqs: Vec<&Feature> = features.iter().filter(|f| f.key() == "VAR_SEQ").collect();

        let fuzzy_start = var_seqs
            .iter()
            .find(|f| matches!(f.location().position().start(), Index::Before(202)))
            .unwrap();
        assert_eq!(fuzzy_start.location().position().end(), Index::Fix(259));

        let fuzzy_end = var_seqs
            .iter()
            .find(|f| matches!(f.location().position().end(), Index::After(259)))
            .unwrap();
        assert_eq!(fuzzy_end.location().position().start(), Index::Fix(202));
    }

    #[test]
    fn test_isoform_qualified_location() {
        let features = load_fixture();
        let init_met = features.iter().find(|f| f.key() == "INIT_MET").unwrap();
        assert_eq!(init_met.location().isoform_accession(), Some("P15005-2"));
        assert_eq!(init_met.location().position().start(), Index::Fix(1));
    }

    #[test]
    fn test_multiline_note_with_slash_continuation() {
        let features = load_fixture();
        let variant = features
            .iter()
            .find(|f| {
                f.key() == "VARIANT" && f.location().position() == Position::parse("29").unwrap()
            })
            .unwrap();
        assert_eq!(
            variant.note_operation(),
            Some(NoteOperation::Replacement {
                from: "M".to_string(),
                to: "V".to_string(),
            })
        );
        assert!(variant.note().unwrap().contains("MT78"));
    }

    #[test]
    fn test_group_by_isoform() {
        let features = load_fixture();
        let groups = group_by_isoform(&features);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, "3");
        assert_eq!(groups[0].1.len(), 3);
        assert!(groups[0].1.iter().all(|f| f.key() == "VAR_SEQ"));

        assert_eq!(groups[1].0, "2");
        assert_eq!(groups[1].1.len(), 4);
        let keys: Vec<&str> = groups[1].1.iter().map(|f| f.key()).collect();
        assert_eq!(keys.iter().filter(|k| **k == "VAR_SEQ").count(), 3);
        assert_eq!(keys.iter().filter(|k| **k == "INIT_MET").count(), 1);
    }

    #[test]
    fn test_group_by_isoform_uses_location_prefix_without_note_mention() {
        let table = "INIT_MET        P15005-2:1\n                /note=\"Removed\"\n";
        let features = parse(table).unwrap();
        let groups = group_by_isoform(&features);
        assert_eq!(groups, vec![("2".to_string(), vec![&features[0]])]);
    }

    #[test]
    fn test_group_by_isoform_supports_non_numeric_label() {
        let table = "VAR_SEQ         1..3\n                /note=\"Missing (in isoform 33 kDa)\"\n";
        let features = parse(table).unwrap();
        let groups = group_by_isoform(&features);
        assert_eq!(groups, vec![("33 kDa".to_string(), vec![&features[0]])]);
    }

    #[test]
    fn test_group_by_isoform_supports_comma_separated_isoform_list() {
        let table = "VAR_SEQ         1..3\n                /note=\"Missing (in isoform 4, isoform 5 and isoform 6)\"\n";
        let features = parse(table).unwrap();
        let groups = group_by_isoform(&features);
        assert_eq!(
            groups,
            vec![
                ("4".to_string(), vec![&features[0]]),
                ("5".to_string(), vec![&features[0]]),
                ("6".to_string(), vec![&features[0]]),
            ]
        );
    }

    #[test]
    fn test_group_by_isoform_returns_no_groups_without_isoform_references() {
        let table = "VARIANT         10\n                /note=\"Missing (in Ref. 3)\"\n";
        let features = parse(table).unwrap();
        assert!(group_by_isoform(&features).is_empty());
    }

    #[test]
    fn test_note_operation_variants() {
        assert_eq!(
            parse_note_operation("Missing (in isoform 2)"),
            NoteOperation::Missing
        );
        assert_eq!(
            parse_note_operation("SEC -> VSR (in isoform 3)"),
            NoteOperation::Replacement {
                from: "SEC".to_string(),
                to: "VSR".to_string(),
            }
        );
        assert_eq!(
            parse_note_operation("Removed"),
            NoteOperation::Other("Removed".to_string())
        );
    }

    #[test]
    fn test_extract_isoform_labels_with_non_numeric_id() {
        assert_eq!(
            extract_isoform_labels("Missing (in isoform 33 kDa)"),
            vec!["33 kDa".to_string()]
        );
        assert_eq!(
            extract_isoform_labels("Missing (in HTX12; uncertain significance)"),
            Vec::<String>::new()
        );
        assert_eq!(extract_isoform_labels("Removed"), Vec::<String>::new());
    }

    #[test]
    fn test_extract_isoform_labels_with_comma_separated_list() {
        assert_eq!(
            extract_isoform_labels("Missing (in isoform 4, isoform 5 and isoform 6)"),
            vec!["4".to_string(), "5".to_string(), "6".to_string()]
        );
        assert_eq!(
            extract_isoform_labels("Missing (in isoform 2 and isoform 3)"),
            vec!["2".to_string(), "3".to_string()]
        );
    }

    #[test]
    fn test_malformed_qualifier_line_errors() {
        let err = FeatureTable::try_from("VARIANT         29\n                note=\"oops\"\n")
            .unwrap_err();
        assert!(matches!(err, Error::MalformedQualifier(_)));
    }

    #[test]
    fn test_unterminated_quoted_value_errors() {
        let err =
            FeatureTable::try_from("VARIANT         29\n                /note=\"never closed\n")
                .unwrap_err();
        assert!(matches!(err, Error::UnterminatedQuotedValue(_)));
    }
}
