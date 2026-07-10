use std::sync::LazyLock;

/// Weights read by the live `ScoreSequence` path for a single amino acid. Field names follow
/// the source's `RC`/`RC1`/... naming (documented per-field below) rather than being renamed to
/// something more descriptive, so they can be cross-checked against `ssrcalc3.js` line by line.
#[derive(Debug, Clone, Copy, Default)]
pub struct AminoAcidParams {
    /// Regular-peptide retention weight (used for the middle residues of peptides >= 10 aa).
    pub rc: f64,
    /// Regular-peptide first-residue weight.
    pub rc1: f64,
    /// Regular-peptide second-residue weight.
    pub rc2: f64,
    /// Regular-peptide last-residue weight.
    pub rcn: f64,
    /// Regular-peptide penultimate-residue weight.
    pub rcn2: f64,
    /// Short-peptide (< 10 aa) retention weight for middle residues.
    pub rcs: f64,
    /// Short-peptide first-residue weight.
    pub rc1s: f64,
    /// Short-peptide second-residue weight.
    pub rc2s: f64,
    /// Short-peptide last-residue weight.
    pub rcns: f64,
    /// Short-peptide penultimate-residue weight.
    pub rcn2s: f64,
    /// Weight used near undigested (missed-cleavage) K/R/H sites.
    pub und_krh: f64,
    /// Average residue mass in Daltons.
    pub amass: f64,
    /// C-terminus pK.
    pub ct: f64,
    /// N-terminus pK.
    pub nt: f64,
    /// Side-chain pK (used for K/R/H/D/E/Y in the isoelectric-point search).
    pub pk: f64,
    /// Base score used by the helicity-2 pattern evaluator.
    pub h2bascore: f64,
    /// Connector multiplier used by the helicity-2 pattern evaluator.
    pub h2cmult: f64,
}

impl AminoAcidParams {
    #[allow(clippy::too_many_arguments)]
    const fn new(
        rc: f64,
        rc1: f64,
        rc2: f64,
        rcn: f64,
        rcn2: f64,
        rcs: f64,
        rc1s: f64,
        rc2s: f64,
        rcns: f64,
        rcn2s: f64,
        und_krh: f64,
        amass: f64,
        ct: f64,
        nt: f64,
        pk: f64,
        h2bascore: f64,
        h2cmult: f64,
    ) -> Self {
        Self {
            rc,
            rc1,
            rc2,
            rcn,
            rcn2,
            rcs,
            rc1s,
            rc2s,
            rcns,
            rcn2s,
            und_krh,
            amass,
            ct,
            nt,
            pk,
            h2bascore,
            h2cmult,
        }
    }
}

/// All 256 byte values default to an all-zero [`AminoAcidParams`] (mirroring the JS source, which
/// pre-fills `AAPARAMS` for every `String.fromCharCode(0..256)` with a `NULLPARAM` before setting
/// the real entries). Looking up a sentinel/out-of-range byte therefore resolves to zero fields
/// rather than panicking, exactly like the source.
static AA_PARAMS: LazyLock<[AminoAcidParams; 256]> = LazyLock::new(|| {
    let mut table = [AminoAcidParams::default(); 256];

    table[b'A' as usize] = AminoAcidParams::new(
        1.10, 0.35, 0.50, 0.80, -0.10, 0.80, -0.30, 0.10, 0.80, -0.50, 0.00, 71.0370, 3.55, 7.59,
        0.00, 1.0, 1.2,
    );
    table[b'C' as usize] = AminoAcidParams::new(
        0.45, 0.90, 0.20, -0.80, -0.50, 0.50, 0.40, 0.00, -0.80, -0.50, 0.00, 103.0090, 3.55, 7.50,
        0.00, 0.0, 1.0,
    );
    table[b'D' as usize] = AminoAcidParams::new(
        0.15, 0.50, 0.40, -0.50, -0.50, 0.30, 0.30, 0.70, -0.50, -0.50, 0.00, 115.0270, 4.55, 7.50,
        4.05, 0.0, 1.1,
    );
    table[b'E' as usize] = AminoAcidParams::new(
        0.95, 1.00, 0.00, 0.00, -0.10, 0.50, 0.10, 0.00, 0.00, -0.10, 0.00, 129.0430, 4.75, 7.70,
        4.45, 0.0, 1.1,
    );
    table[b'F' as usize] = AminoAcidParams::new(
        10.90, 7.50, 9.50, 10.50, 10.30, 11.10, 8.10, 9.50, 10.50, 10.30, -0.10, 147.0638, 3.55,
        7.50, 0.00, 0.5, 1.0,
    );
    table[b'G' as usize] = AminoAcidParams::new(
        -0.35, 0.20, 0.15, -0.90, -0.70, 0.00, 0.00, 0.10, -0.90, -0.70, 0.00, 57.0210, 3.55, 7.50,
        0.00, 0.0, 0.3,
    );
    table[b'H' as usize] = AminoAcidParams::new(
        -1.45, -0.10, -0.20, -1.30, -1.70, -1.00, 0.10, -0.20, -1.30, -1.70, 0.00, 137.0590, 3.55,
        7.50, 5.98, 0.0, 0.6,
    );
    table[b'I' as usize] = AminoAcidParams::new(
        8.00, 5.20, 6.60, 8.40, 7.70, 7.70, 5.00, 6.80, 8.40, 7.70, 0.15, 113.0840, 3.55, 7.50,
        0.00, 3.5, 1.4,
    );
    table[b'K' as usize] = AminoAcidParams::new(
        -2.05, -0.60, -1.50, -1.90, -1.45, -0.20, -1.40, -1.30, -2.20, -1.45, 0.00, 128.0950, 3.55,
        7.50, 10.00, 0.0, 1.0,
    );
    table[b'L' as usize] = AminoAcidParams::new(
        9.30, 5.55, 7.40, 9.60, 9.30, 9.20, 6.00, 7.90, 9.60, 8.70, 0.30, 113.0840, 3.55, 7.50,
        0.00, 1.6, 1.6,
    );
    table[b'M' as usize] = AminoAcidParams::new(
        6.20, 4.40, 5.70, 5.80, 6.00, 6.20, 5.00, 5.70, 5.80, 6.00, 0.00, 131.0400, 3.55, 7.00,
        0.00, 1.8, 1.0,
    );
    table[b'N' as usize] = AminoAcidParams::new(
        -0.85, 0.20, -0.20, -1.20, -1.10, -0.85, 0.20, -0.20, -1.20, -1.10, 0.00, 114.0430, 3.55,
        7.50, 0.00, 0.0, 0.4,
    );
    table[b'P' as usize] = AminoAcidParams::new(
        2.10, 2.10, 2.10, 0.20, 2.10, 3.00, 1.00, 1.50, 0.20, 2.10, 0.00, 97.0530, 3.55, 8.36,
        0.00, 0.0, 0.3,
    );
    table[b'Q' as usize] = AminoAcidParams::new(
        -0.40, -0.70, -0.20, -0.90, -1.10, -0.40, -0.80, -0.20, -0.90, -1.10, 0.00, 128.0590, 3.55,
        7.50, 0.00, 0.0, 1.0,
    );
    table[b'R' as usize] = AminoAcidParams::new(
        -1.40, 0.50, -1.10, -1.30, -1.10, -0.20, 0.50, -1.10, -1.20, -1.10, 0.00, 156.1010, 3.55,
        7.50, 12.00, 0.0, 1.0,
    );
    table[b'S' as usize] = AminoAcidParams::new(
        -0.15, 0.80, -0.10, -0.80, -1.20, -0.50, 0.40, 0.10, -0.80, -1.20, 0.00, 87.0320, 3.55,
        6.93, 0.00, 0.0, 1.0,
    );
    table[b'T' as usize] = AminoAcidParams::new(
        0.65, 0.80, 0.60, 0.40, 0.00, 0.60, 0.80, 0.40, 0.40, 0.00, 0.00, 101.0480, 3.55, 6.82,
        0.00, 0.0, 1.0,
    );
    table[b'V' as usize] = AminoAcidParams::new(
        5.00, 2.90, 3.40, 5.00, 4.20, 5.10, 2.70, 3.40, 5.00, 4.20, -0.30, 99.0680, 3.55, 7.44,
        0.00, 1.4, 1.2,
    );
    table[b'W' as usize] = AminoAcidParams::new(
        12.25, 11.10, 11.80, 11.00, 12.10, 12.40, 11.60, 11.80, 11.00, 12.10, 0.15, 186.0790, 3.55,
        7.50, 0.00, 1.6, 1.0,
    );
    table[b'Y' as usize] = AminoAcidParams::new(
        4.85, 3.70, 4.50, 4.00, 4.40, 5.10, 4.20, 4.50, 4.00, 4.40, -0.20, 163.0630, 3.55, 7.50,
        10.00, 0.2, 1.0,
    );

    // B/X/Z are present in AAPARAMS (so lookups involving them never fall back to the
    // zero-valued default) even though `CleanSequence` filters them out before scoring reaches
    // this table in practice.
    table[b'B' as usize] = AminoAcidParams::new(
        0.15, 0.50, 0.40, -0.50, -0.50, 0.30, 0.30, 0.70, -0.50, -0.50, 0.00, 115.0270, 4.55, 7.50,
        4.05, 0.0, 1.1,
    );
    table[b'X' as usize] = AminoAcidParams::new(
        0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.0000, 0.00, 0.00, 0.00,
        0.0, 1.0,
    );
    table[b'Z' as usize] = AminoAcidParams::new(
        0.95, 1.00, 0.00, 0.00, -0.10, 0.50, 0.10, 0.00, 0.00, -0.10, 0.00, 129.0430, 4.75, 7.70,
        4.45, 0.0, 1.1,
    );

    table
});

/// Looks up the parameters for `byte`, defaulting to the all-zero [`AminoAcidParams`] for any
/// byte that isn't one of the 20 canonical amino acids (or B/X/Z) — including the `b'\0'`
/// sentinel used throughout this crate for out-of-bounds sequence positions.
pub fn params_for(byte: u8) -> &'static AminoAcidParams {
    &AA_PARAMS[byte as usize]
}
