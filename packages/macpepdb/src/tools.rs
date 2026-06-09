const SIGN_BIT64: u64 = 1u64 << 63;
const SIGN_BIT32: u32 = 1u32 << 31;

pub fn u64_to_i64(v: u64) -> i64 {
    (v ^ SIGN_BIT64) as i64
}

pub fn i64_to_u64(v: i64) -> u64 {
    v as u64 ^ SIGN_BIT64
}

pub fn usize_to_i64(v: usize) -> i64 {
    u64_to_i64(v as u64)
}

pub fn i64_to_usize(v: i64) -> usize {
    i64_to_u64(v) as usize
}

pub fn u32_to_i32(v: u32) -> i32 {
    (v ^ SIGN_BIT32) as i32
}

pub fn i32_to_u32(v: i32) -> u32 {
    v as u32 ^ SIGN_BIT32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u64_to_i64() {
        assert_eq!(u64_to_i64(u64::MIN), i64::MIN);
        assert_eq!(u64_to_i64(1), i64::MIN + 1);

        assert_eq!(u64_to_i64(u64::MAX), i64::MAX);
        assert_eq!(u64_to_i64(u64::MAX - 1), i64::MAX - 1);

        assert_eq!(i64_to_u64(i64::MIN), u64::MIN);
        assert_eq!(i64_to_u64(i64::MIN + 1), 1);

        assert_eq!(i64_to_u64(i64::MAX), u64::MAX);
        assert_eq!(i64_to_u64(i64::MAX - 1), u64::MAX - 1);

        assert_eq!(u64_to_i64(SIGN_BIT64), 0);
    }

    #[test]
    fn test_u32_to_i32() {
        assert_eq!(u32_to_i32(u32::MIN), i32::MIN);
        assert_eq!(u32_to_i32(1), i32::MIN + 1);

        assert_eq!(u32_to_i32(u32::MAX), i32::MAX);
        assert_eq!(u32_to_i32(u32::MAX - 1), i32::MAX - 1);

        assert_eq!(i32_to_u32(i32::MIN), u32::MIN);
        assert_eq!(i32_to_u32(i32::MIN + 1), 1);

        assert_eq!(i32_to_u32(i32::MAX), u32::MAX);
        assert_eq!(i32_to_u32(i32::MAX - 1), u32::MAX - 1);

        assert_eq!(u32_to_i32(SIGN_BIT32), 0);
    }
}
