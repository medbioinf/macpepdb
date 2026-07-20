//! A Rust port of the SSRCalc3 algorithm (Krokhin, Craig, Spicer, Ens, Standing, Beavis & Wilkins,
//! *Mol Cell Proteomics* 2004;3(9):908-19) for predicting a peptide's reverse-phase HPLC retention
//! time / hydrophobicity from its amino acid sequence.
//!
//! Ported from `ssrcalc3.js` (itself a perl → C → Java → C# → JavaScript retranslation).
//! Only the live `CleanSequence` + `ScoreSequence` computation path is ported — the source also
//! carries a separate, legacy "PeptidePropertyTool_V7.xls" API surface (`HydrophobicityV1`,
//! `HydrophobClusterV1/V2/V2_1`, `IEP`, ...) that references undefined JS globals/methods and has
//! never run successfully; there is no ground truth to port it against.
//!
//! Several quirks/bugs in the source are preserved intentionally rather than "fixed", because the
//! static weight tables were tuned against this exact (buggy) behavior — see the doc comments on
//! [`helicity::heli1_term_adj`] (`heli1TermAdj`'s wrap-to-last-match scan) and
//! [`helicity`]'s `connector` (an always-true double-negated guard).

/// Isoelectric-point / partial-charge scoring, transcribed from `ssrcalc3.js` (`eMap`,
/// `_partial_charge`, `electric`, `CalcR`, `newiso`, `helectric`).
mod electric;
/// Helicity corrections, transcribed from `ssrcalc3.js` (`helicity1`, `heli1TermAdj`,
/// `helicity2`, `heli2Calc`, `evalH2pattern`, `connector`).
mod helicity;
/// Per-amino-acid parameters used by the SSRCalc3 scoring pipeline.
///
/// Transcribed from the `A300Column` weight table in `ssrcalc3.js` (the only table `create()`
/// actually installs — `A100Column` exists in the source but is never called and is marked
/// "not yet verified", so it is intentionally not ported).
mod params;
/// Static lookup tables transcribed verbatim from `SSRCalc3.init()` in `ssrcalc3.js`.
mod tables;
/// String utilities transcribed from the tail of `ssrcalc3.js` (`ReplaceAAs`, `ContainsAA`,
/// `Backwards`), plus a byte-indexing helper standing in for JS's `undefined`-becomes-`'\0'`
/// out-of-bounds string indexing that several of the ported functions rely on.
mod util;

use params::params_for;
use tables::CLUSTCOMB;
use util::replace_aas;

const CANONICAL_AMINO_ACIDS: &[u8] = b"ACDEFGHIKLMNPQRSTVWY";

// Length-scaling limits/factors (`ssrcalc3.js` lines 568-571).
const SP_LIM: usize = 8;
const LP_LIM: usize = 20;
const LP_S_FAC: f64 = 0.0270;
const SP_S_FAC: f64 = -0.055;

// Missed-cleavage ("undigested") scaling factors (lines 574-575).
const UDF21: f64 = 0.0;
const UDF22: f64 = 0.0;
const UDF31: f64 = 1.0;
const UDF32: f64 = 0.0;

// Total-sum correction factors for buckets 20-30/30-40/40-50/50+ (line 578).
const SUMSCALE1: f64 = 0.27;
const SUMSCALE2: f64 = 0.33;
const SUMSCALE3: f64 = 0.38;
const SUMSCALE4: f64 = 0.447;

// Clusterness scaling (line 581).
const KSCALE: f64 = 0.4;

// Proline-run scores (line 588).
const PPSCORE: f64 = 1.2;
const PPPSCORE: f64 = 3.5;
const PPPPSCORE: f64 = 5.0;

/// Predicts the SSRCalc3 retention-time/hydrophobicity score for `sequence`.
///
/// `sequence` is cleaned first (kept only if one of the 20 canonical uppercase amino acid
/// letters, matching `CleanSequence`'s effective behavior — see the module docs), then scored by
/// the same 8-stage pipeline as the source `ScoreSequence`. Sequences that clean to fewer than 4
/// residues score `0.0`, matching the source's early return.
pub fn score_sequence(sequence: &str) -> f64 {
    let seq = clean_sequence(sequence);
    let sze = seq.len();
    let mut tsum3 = 0.0;
    if sze < 4 {
        return tsum3;
    }

    if sze < 10 {
        tsum3 = params_for(seq[0]).rc1s
            + params_for(seq[1]).rc2s
            + params_for(seq[sze - 1]).rcns
            + params_for(seq[sze - 2]).rcn2s;
        for &b in &seq[2..sze - 2] {
            tsum3 += params_for(b).rcs;
        }
    } else {
        tsum3 = params_for(seq[0]).rc1
            + params_for(seq[1]).rc2
            + params_for(seq[sze - 1]).rcn
            + params_for(seq[sze - 2]).rcn2;
        for &b in &seq[2..sze - 2] {
            tsum3 += params_for(b).rc;
        }
    }

    tsum3 += smallness(sze, tsum3);
    tsum3 -= undigested(&seq);
    tsum3 -= clusterness(&seq);
    tsum3 -= proline(&seq);
    tsum3 *= length_scale(sze);

    if (20.0..30.0).contains(&tsum3) {
        tsum3 -= (tsum3 - 18.0) * SUMSCALE1;
    }
    if (30.0..40.0).contains(&tsum3) {
        tsum3 -= (tsum3 - 18.0) * SUMSCALE2;
    }
    if (40.0..50.0).contains(&tsum3) {
        tsum3 -= (tsum3 - 18.0) * SUMSCALE3;
    }
    if tsum3 >= 50.0 {
        tsum3 -= (tsum3 - 18.0) * SUMSCALE4;
    }

    tsum3 += electric::newiso(&seq, tsum3);
    tsum3 += helicity::helicity1(&seq);
    tsum3 += helicity::helicity2(&seq);
    tsum3 += electric::helectric(&seq);

    tsum3
}

/// Keeps only the 20 canonical uppercase amino acid letters, dropping everything else.
///
/// The source's filter regex (`/[^ACDEFGHIKLMNPQRSTVWY]]*/g`) has a stray trailing `]` — for real
/// peptide input (letters only, no literal `]` characters) that is indistinguishable from this
/// simpler "keep only the 20 canonical letters" filter, which is what's implemented here.
fn clean_sequence(raw_sequence: &str) -> Vec<u8> {
    raw_sequence
        .bytes()
        .filter(|b| CANONICAL_AMINO_ACIDS.contains(b))
        .collect()
}

/// Adjusts short peptides whose average per-residue weight is far from the typical range.
fn smallness(sqlen: usize, tsum: f64) -> f64 {
    let avg = tsum / sqlen as f64;
    if sqlen < 20 && avg < 0.9 {
        return 3.5 * (0.9 - avg);
    }
    if sqlen < 15 && avg > 2.8 {
        return 2.6 * (avg - 2.8);
    }
    0.0
}

/// Penalizes missed-cleavage sites (K/R/H not at the very end of the peptide).
///
/// `DUPLICATE_ORIGINAL_CODE` is `true` in the source, so a backward-looking offset that runs
/// negative wraps around to the right end of the sequence instead of staying at the `'\0'`
/// default — preserved here rather than "fixed", per the module docs.
fn undigested(seq: &[u8]) -> f64 {
    let xx = seq.len() as isize - 1;
    let re = seq[xx as usize];
    let mut csum = 0.0;

    if re == b'R' || re == b'K' || re == b'H' {
        let op1 = seq[(xx - 1) as usize];
        let op2 = seq[(xx - 2) as usize];
        csum = UDF21 * params_for(op1).und_krh + UDF22 * params_for(op2).und_krh;
    }

    let mut dd = 0isize;
    while dd < xx {
        let re = seq[dd as usize];
        if re == b'K' || re == b'R' || re == b'H' {
            let mut op1 = 0u8;
            let mut op2 = 0u8;
            let mut op3 = 0u8;
            let mut op4 = 0u8;

            if dd > 0 && dd - 1 <= xx {
                op1 = seq[(dd - 1) as usize];
            }
            if dd - 2 >= 0 && dd - 2 <= xx {
                op2 = seq[(dd - 2) as usize];
            }
            if dd - 1 < 0 && -(dd - 1) <= xx {
                op1 = seq[(xx + (dd - 1) + 1) as usize];
            }
            if dd - 2 < 0 && -(dd - 2) <= xx {
                op2 = seq[(xx + (dd - 2) + 1) as usize];
            }
            if dd + 1 >= 0 && dd < xx {
                op3 = seq[(dd + 1) as usize];
            }
            if dd + 2 >= 0 && dd + 2 <= xx {
                op4 = seq[(dd + 2) as usize];
            }

            csum += UDF31 * (params_for(op1).und_krh + params_for(op3).und_krh)
                + UDF32 * (params_for(op2).und_krh + params_for(op4).und_krh);
        }
        dd += 1;
    }

    csum
}

/// Penalizes clusters of hydrophobic residues (W/L/I coded `5`, A/M/Y/V coded `1`), keyed by the
/// [`CLUSTCOMB`] pattern table.
fn clusterness(seq: &[u8]) -> f64 {
    let mut cc = Vec::with_capacity(seq.len() + 2);
    cc.push(b'0');
    cc.extend_from_slice(seq);
    cc.push(b'0');

    let cc = replace_aas(&cc, "LIW", b'5');
    let cc = replace_aas(&cc, "AMYV", b'1');
    let cc = replace_aas(&cc, "A-Z", b'0');
    let cc = std::str::from_utf8(&cc).expect("clusterness recoding only ever produces ASCII");

    let mut score = 0.0;
    for &(key, weight) in CLUSTCOMB {
        if cc.contains(key) {
            score += weight;
        }
    }
    score * KSCALE
}

/// Penalizes proline runs.
fn proline(seq: &[u8]) -> f64 {
    if contains_subslice(seq, b"PPPP") {
        PPPPSCORE
    } else if contains_subslice(seq, b"PPP") {
        PPPSCORE
    } else if contains_subslice(seq, b"PP") {
        PPSCORE
    } else {
        0.0
    }
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Scales the score based on peptide length: shorter peptides (< [`SP_LIM`]) are scaled up,
/// longer ones (> [`LP_LIM`]) are scaled down.
fn length_scale(sqlen: usize) -> f64 {
    if sqlen < SP_LIM {
        1.0 + SP_S_FAC * (SP_LIM - sqlen) as f64
    } else if sqlen > LP_LIM {
        1.0 / (1.0 + LP_S_FAC * (sqlen - LP_LIM) as f64)
    } else {
        1.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn workspace_test_data_path(file_name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join(file_name)
    }

    #[test]
    fn matches_reference_js_implementation() {
        let fixture = fs::read_to_string(workspace_test_data_path("ssrcalc3.tsv"))
            .expect("failed to read test_data/ssrcalc3.tsv fixture");

        let mut checked = 0;
        for line in fixture.lines() {
            let (sequence, expected) = line.split_once('\t').expect("malformed fixture line");
            let expected: f64 = expected.parse().expect("malformed expected score");
            let actual = score_sequence(sequence);
            assert!(
                (actual - expected).abs() < 1e-6,
                "score_sequence({sequence:?}) = {actual}, expected {expected}"
            );
            checked += 1;
        }
        assert!(checked > 0, "fixture file was empty");
    }
}
