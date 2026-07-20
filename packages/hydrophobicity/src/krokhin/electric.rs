use super::params::params_for;
use super::util::{contains_aa, replace_aas};

// Isoelectric scaling factors (`ssrcalc3.js` lines 584-585).
const Z01: f64 = -0.03;
const Z02: f64 = 0.60;
const NDELTAWT: f64 = 0.8;
const Z03: f64 = 0.00;
const Z04: f64 = 0.00;
const PDELTAWT: f64 = 1.0;

/// Maps a charged/aromatic residue to its slot in the 7-element charge-count array used by
/// [`electric`]/[`calc_r`]; `-1` for every other residue (matches `AAPARAMS`-index order:
/// K, R, H, D, E, C, Y).
fn e_map(aa: u8) -> Option<usize> {
    match aa {
        b'K' => Some(0),
        b'R' => Some(1),
        b'H' => Some(2),
        b'D' => Some(3),
        b'E' => Some(4),
        b'C' => Some(5),
        b'Y' => Some(6),
        _ => None,
    }
}

fn partial_charge(pk: f64, ph: f64) -> f64 {
    let cr = 10f64.powf(pk - ph);
    cr / (cr + 1.0)
}

fn calc_r(ph: f64, pk0: f64, pk1: f64, aa_cnt: &[u32; 7]) -> f64 {
    partial_charge(pk0, ph)
        + f64::from(aa_cnt[e_map(b'K').unwrap()]) * partial_charge(params_for(b'K').pk, ph)
        + f64::from(aa_cnt[e_map(b'R').unwrap()]) * partial_charge(params_for(b'R').pk, ph)
        + f64::from(aa_cnt[e_map(b'H').unwrap()]) * partial_charge(params_for(b'H').pk, ph)
        - f64::from(aa_cnt[e_map(b'D').unwrap()]) * partial_charge(ph, params_for(b'D').pk)
        - f64::from(aa_cnt[e_map(b'E').unwrap()]) * partial_charge(ph, params_for(b'E').pk)
        - f64::from(aa_cnt[e_map(b'Y').unwrap()]) * partial_charge(ph, params_for(b'Y').pk)
        - partial_charge(ph, pk1)
}

/// Estimates the pH at which the peptide's net charge is closest to zero, via a coarse then fine
/// linear scan (transcribed as-is from the source rather than replaced with a root finder, since
/// the scan's step sizes affect the result).
fn electric(seq: &[u8]) -> f64 {
    let mut aa_cnt = [0u32; 7];

    let pk0 = params_for(seq[0]).ct;
    let pk1 = params_for(seq[seq.len() - 1]).nt;

    for &c in seq {
        if let Some(index) = e_map(c) {
            aa_cnt[index] += 1;
        }
    }

    let step1 = 0.3;
    let mut best = 0.0;
    let mut min = 100_000.0;

    let mut z = 0.01;
    while z <= 14.0 {
        let check = calc_r(z, pk0, pk1, &aa_cnt).abs();
        if check < min {
            min = check;
            best = z;
        }
        z += step1;
    }

    let best1 = best;
    min = 100_000.0;
    let mut z = best1 - step1;
    while z <= best1 + step1 {
        let check = calc_r(z, pk0, pk1, &aa_cnt).abs();
        if check < min {
            min = check;
            best = z;
        }
        z += 0.01;
    }

    best
}

/// Isoelectric-point correction added to the running score total.
pub fn newiso(seq: &[u8], tsum: f64) -> f64 {
    let mass: f64 = seq.iter().map(|&c| params_for(c).amass).sum();
    let pi1 = electric(seq);
    let lmass = 1.8014 * mass.ln();
    let delta1 = pi1 - 19.107 + lmass;

    if delta1 < 0.0 {
        (tsum * Z01 + Z02) * NDELTAWT * delta1
    } else if delta1 > 0.0 {
        (tsum * Z03 + Z04) * PDELTAWT * delta1
    } else {
        0.0
    }
}

/// Short-range electrostatic correction near the C-terminus, active only for peptides of length
/// 4-14 whose 4th-from-last residue is D or E.
pub fn helectric(seq: &[u8]) -> f64 {
    if seq.len() > 14 || seq.len() < 4 {
        return 0.0;
    }
    let mpart = &seq[seq.len() - 4..];

    if mpart[0] == b'D' || mpart[0] == b'E' {
        let mpart = &mpart[1..3];
        if contains_aa(mpart, "PGKRH") {
            return 0.0;
        }
        let mpart = replace_aas(mpart, "LI", b'X');
        let mpart = replace_aas(&mpart, "AVYFWM", b'Z');
        let mpart = replace_aas(&mpart, "GSPCNKQHRTDE", b'U');

        return match mpart.as_slice() {
            b"XX" => 1.0,
            b"ZX" | b"XZ" => 0.5,
            b"ZZ" => 0.4,
            b"XU" | b"UX" => 0.4,
            b"ZU" | b"UZ" => 0.2,
            _ => 0.0,
        };
    }
    0.0
}
