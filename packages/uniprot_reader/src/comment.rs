/// A single alternative-product isoform declared in a `CC -!- ALTERNATIVE PRODUCTS:` block.
/// `name` is the curator label (display-only); isoform accessions must be derived from
/// `iso_ids`, not `name` — `Name=` is free text and is not guaranteed to match the
/// `<primary accession>-<n>` accession suffix, while `IsoId=` always is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Isoform {
    name: String,
    iso_ids: Vec<String>,
    feature_ids: Vec<String>,
}

impl Isoform {
    /// The curator-assigned display label (`Name=`). Free text, not an accession.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The isoform's accessions (`IsoId=`), e.g. `P12345-2`.
    pub fn iso_ids(&self) -> &[String] {
        &self.iso_ids
    }

    /// The `VSP_...` feature ids (`Sequence=`) needed to reconstruct this isoform from VAR_SEQ.
    pub fn feature_ids(&self) -> &[String] {
        &self.feature_ids
    }
}

const TOPIC_HEADER: &str = "-!- ALTERNATIVE PRODUCTS:";

fn split_field_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect()
}

/// Parses the `CC -!- ALTERNATIVE PRODUCTS:` topic out of `Entry::comment_string()`, returning
/// every isoform whose `Sequence=` is a list of `VSP_...` feature ids (i.e. every isoform that
/// needs VAR_SEQ-based reconstruction). Isoforms with `Sequence=Displayed`/`External`/
/// `Not described` are omitted. Returns an empty vec if there's no such topic — infallible,
/// best-effort parsing, matching the style of `Feature::note_operation()`/`VarSeqEdit::new` in
/// `macpepdb::protein`.
pub fn parse_alternative_products(comment_string: &str) -> Vec<Isoform> {
    let mut lines = comment_string.lines();
    let Some(_) = lines.by_ref().find(|line| *line == TOPIC_HEADER) else {
        return Vec::new();
    };

    let body_lines: Vec<&str> = lines
        .take_while(|line| line.starts_with(char::is_whitespace))
        .collect();
    let body = body_lines.join(" ");

    let mut isoforms = Vec::new();
    let mut pending_name: Option<String> = None;
    let mut pending_iso_ids: Vec<String> = Vec::new();

    for field in body.split(';').map(str::trim) {
        if let Some(value) = field.strip_prefix("Name=") {
            pending_name = Some(value.trim().to_string());
            pending_iso_ids = Vec::new();
        } else if let Some(value) = field.strip_prefix("IsoId=") {
            pending_iso_ids = split_field_list(value);
        } else if let Some(value) = field.strip_prefix("Sequence=") {
            let value = value.trim();
            let Some(name) = pending_name.take() else {
                continue;
            };
            let iso_ids = std::mem::take(&mut pending_iso_ids);
            if matches!(value, "Displayed" | "External" | "Not described") {
                continue;
            }
            isoforms.push(Isoform {
                name,
                iso_ids,
                feature_ids: split_field_list(value),
            });
        }
    }

    isoforms
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALTERNATIVE_PRODUCTS_EXCERPT: &str = concat!(
        "-!- SUBCELLULAR LOCATION: Membrane {ECO:0000255}; Single-pass type I\n",
        "    membrane protein {ECO:0000255}.\n",
        "-!- ALTERNATIVE PRODUCTS:\n",
        "    Event=Alternative splicing; Named isoforms=3;\n",
        "    Name=1;\n",
        "      IsoId=A0A1B0GTW7-1; Sequence=Displayed;\n",
        "    Name=2;\n",
        "      IsoId=A0A1B0GTW7-2; Sequence=VSP_061525;\n",
        "    Name=3;\n",
        "      IsoId=A0A1B0GTW7-3; Sequence=VSP_061523, VSP_061524, VSP_061526;\n",
        "-!- DISEASE: Heterotaxy, visceral, 12, autosomal (HTX12) [MIM:619702]: A\n",
        "    form of visceral heterotaxy.\n",
    );

    #[test]
    fn test_real_fixture_excerpt() {
        let isoforms = parse_alternative_products(ALTERNATIVE_PRODUCTS_EXCERPT);

        assert_eq!(isoforms.len(), 2);

        assert_eq!(isoforms[0].name(), "2");
        assert_eq!(isoforms[0].iso_ids(), ["A0A1B0GTW7-2".to_string()]);
        assert_eq!(isoforms[0].feature_ids(), ["VSP_061525".to_string()]);

        assert_eq!(isoforms[1].name(), "3");
        assert_eq!(isoforms[1].iso_ids(), ["A0A1B0GTW7-3".to_string()]);
        assert_eq!(
            isoforms[1].feature_ids(),
            [
                "VSP_061523".to_string(),
                "VSP_061524".to_string(),
                "VSP_061526".to_string(),
            ]
        );
    }

    #[test]
    fn test_stops_at_copyright_separator() {
        let comment_string = concat!(
            "-!- ALTERNATIVE PRODUCTS:\n",
            "    Event=Alternative splicing; Named isoforms=1;\n",
            "    Name=2;\n",
            "      IsoId=P12345-2; Sequence=VSP_000001;\n",
            "---------------------------------------------------------------------------\n",
            "Copyrighted by the UniProt Consortium, see https://www.uniprot.org/terms\n",
        );

        let isoforms = parse_alternative_products(comment_string);
        assert_eq!(isoforms.len(), 1);
        assert_eq!(isoforms[0].name(), "2");
    }

    #[test]
    fn test_no_topic_returns_empty() {
        let comment_string = "-!- FUNCTION: Does something.\n    {ECO:0000250}.\n";
        assert!(parse_alternative_products(comment_string).is_empty());
    }

    #[test]
    fn test_external_and_not_described_are_skipped() {
        let comment_string = concat!(
            "-!- ALTERNATIVE PRODUCTS:\n",
            "    Event=Alternative splicing; Named isoforms=2;\n",
            "    Name=1;\n",
            "      IsoId=P12345-1; Sequence=External;\n",
            "    Name=2;\n",
            "      IsoId=P12345-2; Sequence=Not described;\n",
        );

        assert!(parse_alternative_products(comment_string).is_empty());
    }

    #[test]
    fn test_multiline_wrapped_comment_and_note_do_not_break_field_splitting() {
        let comment_string = concat!(
            "-!- ALTERNATIVE PRODUCTS:\n",
            "    Event=Alternative splicing; Named isoforms=1;\n",
            "    Comment=This is a long free text comment that wraps across\n",
            "      several physical CC lines before it is finally terminated;\n",
            "    Name=2;\n",
            "      IsoId=P12345-2; Sequence=VSP_000001;\n",
            "      Note=Another free text note that also wraps across multiple\n",
            "      physical lines before its terminating semicolon;\n",
        );

        let isoforms = parse_alternative_products(comment_string);
        assert_eq!(isoforms.len(), 1);
        assert_eq!(isoforms[0].name(), "2");
        assert_eq!(isoforms[0].feature_ids(), ["VSP_000001".to_string()]);
    }

    #[test]
    fn test_multiple_iso_ids_per_isoform() {
        let comment_string = concat!(
            "-!- ALTERNATIVE PRODUCTS:\n",
            "    Event=Alternative splicing; Named isoforms=1;\n",
            "    Name=2;\n",
            "      IsoId=P12345-2, P12345-3; Sequence=VSP_000001;\n",
        );

        let isoforms = parse_alternative_products(comment_string);
        assert_eq!(isoforms.len(), 1);
        assert_eq!(
            isoforms[0].iso_ids(),
            ["P12345-2".to_string(), "P12345-3".to_string()]
        );
    }
}
