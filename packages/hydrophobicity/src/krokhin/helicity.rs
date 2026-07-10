use super::params::params_for;
use super::tables::{HLX_SCORE4, HLX_SCORE5, HLX_SCORE6};
use super::util::{at, backwards, replace_aas};

const HELIX1SCALE: f64 = 1.6;
const HELIX2SCALE: f64 = 0.255;

/// Short-helix/turn correction (v3 algorithm).
///
/// The window-length step is not a plain `+1`: on a match the source does `i = i + 1; continue;`
/// inside a `for` loop, which also runs the loop's own `i++` — so a match advances the scan
/// position by 2, while a non-match advances it by 1. That asymmetry is preserved below via the
/// explicit `i += 2` / `i += 1`.
pub fn helicity1(seq: &[u8]) -> f64 {
    let mut hc = seq.to_vec();
    hc = replace_aas(&hc, "PHRK", b'z');
    hc = replace_aas(&hc, "WFIL", b'X');
    hc = replace_aas(&hc, "YMVA", b'Z');
    hc = replace_aas(&hc, "DE", b'O');
    hc = replace_aas(&hc, "GSPCNKQHRT", b'U');

    let sqlen = hc.len();
    let mut sum = 0.0;
    if sqlen < 3 {
        return HELIX1SCALE * sum;
    }

    let mut i = 0usize;
    while i < sqlen - 3 {
        let remaining = sqlen - i;

        if remaining >= 6 {
            let hc6 = &hc[i..i + 6];
            if let Some(&sc6) = HLX_SCORE6.get(std::str::from_utf8(hc6).unwrap())
                && sc6 > 0.0
            {
                sum += sc6 * heli1_term_adj(hc6, i, sqlen);
                i += 2;
                continue;
            }
        }

        if remaining >= 5 {
            let hc5 = &hc[i..i + 5];
            if let Some(&sc5) = HLX_SCORE5.get(std::str::from_utf8(hc5).unwrap())
                && sc5 > 0.0
            {
                sum += sc5 * heli1_term_adj(hc5, i, sqlen);
                i += 2;
                continue;
            }
        }

        if remaining >= 4 {
            let hc4 = &hc[i..i + 4];
            if let Some(&sc4) = HLX_SCORE4.get(std::str::from_utf8(hc4).unwrap())
                && sc4 > 0.0
            {
                sum += sc4 * heli1_term_adj(hc4, i, sqlen);
                i += 2;
                continue;
            }
        }

        i += 1;
    }

    HELIX1SCALE * sum
}

/// Terminal-proximity adjustment for a matched helicity-1 window.
///
/// `DUPLICATE_ORIGINAL_CODE` is `true` in the source, so the scan below never breaks early on the
/// first `O`/`U` byte — it keeps going and keeps the *last* one, which is the (intentionally
/// preserved) behavior the score tables were tuned against.
fn heli1_term_adj(window: &[u8], ix2: usize, sqlen: usize) -> f64 {
    let mut wher = 0usize;
    for (i, &m) in window.iter().enumerate() {
        if m == b'O' || m == b'U' {
            wher = i;
        }
    }
    let wher = wher + ix2;

    if wher < 2 {
        return 0.20;
    }
    if wher < 3 {
        return 0.25;
    }
    if wher < 4 {
        return 0.45;
    }
    if wher > sqlen - 3 {
        return 0.2;
    }
    if wher > sqlen - 4 {
        return 0.75;
    }
    if wher > sqlen - 5 {
        return 0.65;
    }
    1.0
}

/// Connector multiplier between two hydrophobic positions in a helicity-2 pattern (v3 algorithm).
fn connector(acid: u8, lp: u8, rp: u8, ct: &[u8], far1: u8, far2: u8) -> f64 {
    let mut mult = 1.0;

    if ct == b"<-" {
        mult *= 0.2;
    }
    if ct == b"->" {
        mult *= 0.1;
    }

    mult *= params_for(lp).h2cmult;
    if lp != rp {
        mult *= params_for(rp).h2cmult;
    }

    if matches!(acid, b'A' | b'Y' | b'V' | b'M') {
        if lp == b'P' || lp == b'G' || rp == b'P' || rp == b'G' {
            mult = 0.0;
        }
        if ct == b"->" || ct == b"<-" {
            mult = 0.0;
        }
    }

    if matches!(acid, b'L' | b'W' | b'F' | b'I') {
        // The source's guard here is `(!ct.match(/--/) != null)`, which — by JS operator
        // precedence (`!` binds before `!=`) — is a boolean compared against `null` and so is
        // always `true`, regardless of `ct`. The intended "and ct is not `--`" restriction never
        // actually applies; preserved as unconditionally true since the tuned score tables were
        // fit against this behavior.
        if lp == b'P' || lp == b'G' || rp == b'P' || rp == b'G' {
            mult = 0.0;
        }
        if (far1 == b'P' || far1 == b'G' || far2 == b'P' || far2 == b'G')
            && (ct == b"<-" || ct == b"->")
        {
            mult = 0.0;
        }
    }

    mult
}

/// Scores one candidate helicity-2 pattern against the original (uncoded) sequence.
///
/// `etype == b'*'` is the multiplicative scoring pass used while searching for the best pattern;
/// `etype == b'+'` is the additive re-scoring pass used once the best pattern has been found.
fn eval_h2_pattern(pattern: &[u8], testsq: &[u8], posn: isize, etype: u8) -> f64 {
    const OFF1: isize = 2;

    let f01 = pattern[0];
    let mut prod1 = params_for(f01).h2bascore;

    let mut test_aa_l = at(testsq, OFF1 + posn, 0);
    let mut test_aa_r = at(testsq, OFF1 + posn + 2, 0);
    let copy_base = OFF1 + posn + 1;

    let mut mult = connector(f01, test_aa_l, test_aa_r, b"--", 0, 0);
    prod1 *= mult;
    if etype == b'*' {
        prod1 *= 25.0;
    }
    if mult == 0.0 {
        return 0.0;
    }

    let mut acount = 1;
    let mut i = 1isize;
    while (i as usize) < pattern.len() - 2 {
        let fpart: &[u8] = &pattern[i as usize..i as usize + 2];
        let gpart = if (i + 2) < pattern.len() as isize {
            pattern[(i + 2) as usize]
        } else {
            0
        };
        let s3 = params_for(gpart).h2bascore;

        let mut far1 = 0u8;
        let mut far2 = 0u8;
        let iss: isize = if fpart == b"--" {
            0
        } else if fpart == b"<-" {
            far1 = at(testsq, copy_base + i + 1, 0);
            1
        } else if fpart == b"->" {
            far2 = at(testsq, copy_base + i + 3, 0);
            -1
        } else {
            0
        };

        test_aa_l = at(testsq, copy_base + i + 1 + iss, 0);
        test_aa_r = at(testsq, copy_base + i + 3 + iss, 0);

        mult = connector(gpart, test_aa_l, test_aa_r, fpart, far1, far2);

        if etype == b'*' && (mult != 0.0 || acount < 3) {
            prod1 = prod1 * 25.0 * s3 * mult;
        }
        if etype == b'+' {
            prod1 += s3 * mult;
        }
        if mult == 0.0 {
            return prod1;
        }

        acount += 1;
        i += 3;
    }

    prod1
}

/// Builds and scores the best helicity-2 pattern starting from each hydrophobic residue,
/// returning `(hiscore, gscore)` (the source's `HISC`/`GSC`-indexed return array).
fn heli2_calc(seq: &[u8]) -> (f64, f64) {
    if seq.len() < 11 {
        return (0.0, 0.0);
    }

    let prechop = seq;
    let sq_copy = &seq[2..seq.len() - 2];

    let pass1 = replace_aas(sq_copy, "WFILYMVA", b'1');
    let pass1 = replace_aas(&pass1, "GSPCNKQHRTDE", b'0');

    let mut hiscore = 0.0f64;
    let mut best: Vec<u8> = Vec::new();
    let mut best_pos = 0isize;

    for i in 0..pass1.len() {
        if pass1[i] != b'1' {
            continue;
        }

        let lc = &pass1[i..];
        let sq2 = &sq_copy[i..];
        let mut pat: Vec<u8> = Vec::new();
        let mut zap = 0isize;
        let mut subt = 0;

        while zap <= 50 && subt < 2 {
            let f1 = at(lc, zap, b'0');
            let f2 = at(lc, zap - 1, b'0');
            let f3 = at(lc, zap + 1, b'0');

            if f1 == b'1' {
                if zap > 0 {
                    pat.extend_from_slice(b"--");
                }
                pat.push(sq2[zap as usize]);
            } else if f2 == b'1' && f1 == b'0' {
                subt += 1;
                if subt < 2 {
                    pat.extend_from_slice(b"->");
                    pat.push(sq2[(zap - 1) as usize]);
                }
            } else if f3 == b'1' && f1 == b'0' {
                subt += 1;
                if subt < 2 {
                    pat.extend_from_slice(b"<-");
                    pat.push(sq2[(zap + 1) as usize]);
                }
            }

            if f1 == b'0' && f2 == b'0' && f3 == b'0' {
                zap = 1000;
            }
            zap += 3;
        }

        if pat.len() > 4 {
            let skore = eval_h2_pattern(&pat, prechop, i as isize - 1, b'*');
            if skore >= hiscore {
                hiscore = skore;
                best = pat;
                best_pos = i as isize;
            }
        }
    }

    if hiscore > 0.0 {
        let gscore = hiscore;
        let hiscore = eval_h2_pattern(&best, prechop, best_pos - 1, b'+');
        (hiscore, gscore)
    } else {
        (0.0, 0.0)
    }
}

/// Long-helix correction (v3 algorithm): scores both the sequence and its reverse and keeps
/// whichever direction had the higher multiplicative (`gscore`) match.
pub fn helicity2(seq: &[u8]) -> f64 {
    let bk_seq = backwards(seq);
    let (fw_hiscore, fw_gscore) = heli2_calc(seq);
    let (bk_hiscore, bk_gscore) = heli2_calc(&bk_seq);

    let h2_fw_bk = if bk_gscore > fw_gscore {
        bk_hiscore
    } else {
        fw_hiscore
    };

    let len_mult = if seq.len() > 30 { 1.0 } else { 0.0 };
    let no_p_mult = if seq.contains(&b'P') { 0.0 } else { 0.75 };
    let h2_mult = 1.0 + len_mult + no_p_mult;

    HELIX2SCALE * h2_mult * h2_fw_bk
}
