/// Sequence byte at `index`, or `default` if `index` is negative or past the end of `seq`.
///
/// Several source functions (`undigested`, `evalH2pattern`, `heli2Calc`, ...) index a JS string
/// with an offset that can run negative or past the string's length; JS silently returns
/// `undefined`. Depending on what the caller then does with that value, the source either
/// compares it against `'\0'` or against `'0'` — callers pass whichever sentinel byte matches the
/// source at that call site.
pub fn at(seq: &[u8], index: isize, default: u8) -> u8 {
    if index < 0 {
        return default;
    }
    match usize::try_from(index) {
        Ok(index) => seq.get(index).copied().unwrap_or(default),
        Err(_) => default,
    }
}

/// Replaces every byte of `s` that is a member of `aas` with `new_value`.
///
/// `aas` is treated as a literal set of characters to match (via membership, not a
/// range/regex) — **except** for the exact sentinel string `"A-Z"`, which the source special-cases
/// to mean "any uppercase Latin letter" (checked with an actual regex in JS). Any other
/// hyphen-containing string (e.g. `"A-F"`) is treated as the literal 3-character set
/// `{'A', '-', 'F'}`, matching the source's `aas.indexOf(c)` behavior.
pub fn replace_aas(s: &[u8], aas: &str, new_value: u8) -> Vec<u8> {
    let all_aas = aas == "A-Z";
    let aas = aas.as_bytes();
    s.iter()
        .map(|&c| {
            if (!all_aas && aas.contains(&c)) || (all_aas && c.is_ascii_uppercase()) {
                new_value
            } else {
                c
            }
        })
        .collect()
}

/// Returns `true` if any byte of `s` is a member of `aas`.
pub fn contains_aa(s: &[u8], aas: &str) -> bool {
    let aas = aas.as_bytes();
    s.iter().any(|c| aas.contains(c))
}

/// Reverses `s`.
pub fn backwards(s: &[u8]) -> Vec<u8> {
    s.iter().rev().copied().collect()
}
