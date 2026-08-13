//! Compares MaCPepDB's memchr-based `Trypsin::full_digest` against the widely-used
//! `[KR](?!P)` regex approach for tryptic cleavage, on the Leptin sequence used in
//! `protease.rs`'s unit tests. Rust's `regex` crate doesn't support look-ahead, so
//! this uses `fancy-regex`, the crate people reach for to express this pattern.
//!
//! The two approaches operate on different representations on purpose: the memchr
//! benchmark runs the real production code over the bit-packed `ProteinSequence`,
//! while the regex benchmark runs over the plain protein `&str` — mirroring how each
//! approach is actually used in practice.

use criterion::{Criterion, criterion_group, criterion_main};
use fancy_regex::Regex;
use macpepdb::protease::{IsProtease, Trypsin};
use macpepdb::sequence::ProteinSequence;

const LEPTIN: &str = "MHWGTLCGFLWLWPYLFYVQAVPIQKVQDDTKTLIKTIVTRINDISHTQSVSSKQKVTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSRNVIQISNDLENLRDLLHVLAFSKSCHLPWASGLETLDSLGGVLEASGYSTEVVALSRLQGSLQDMLWQLDLSPGC";

/// Reproduces `Trypsin::full_digest`'s zero-missed-cleavage semantics on a plain
/// `&str` using the regex `[KR](?!P)`.
fn regex_full_digest<'a>(regex: &Regex, sequence: &'a str) -> Vec<&'a str> {
    let mut peptides = Vec::new();
    let mut last = 0;
    for m in regex.find_iter(sequence) {
        let end = m.unwrap().end();
        peptides.push(&sequence[last..end]);
        last = end;
    }
    if last < sequence.len() {
        peptides.push(&sequence[last..]);
    }
    peptides
}

fn benchmark(c: &mut Criterion) {
    let leptin_sequence = ProteinSequence::try_from(LEPTIN).unwrap();
    let leptin_bit_codes = leptin_sequence.as_ref();
    let regex = Regex::new(r"[KR](?!P)").unwrap();

    assert_eq!(
        Trypsin.full_digest(leptin_bit_codes).len(),
        regex_full_digest(&regex, LEPTIN).len(),
        "memchr and regex digestion must produce the same number of peptides"
    );

    let mut group = c.benchmark_group("trypsin_full_digest_leptin");
    group.bench_function("memchr", |b| {
        b.iter(|| Trypsin.full_digest(leptin_bit_codes));
    });
    group.bench_function("regex", |b| {
        b.iter(|| regex_full_digest(&regex, LEPTIN));
    });
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
