pub fn u64_to_u8s(x: u64) -> [u8; 8] {
    let b1: u8 = ((x >> 56) & 0xff) as u8;
    let b2: u8 = ((x >> 48) & 0xff) as u8;
    let b3: u8 = ((x >> 40) & 0xff) as u8;
    let b4: u8 = ((x >> 32) & 0xff) as u8;
    let b5: u8 = ((x >> 24) & 0xff) as u8;
    let b6: u8 = ((x >> 16) & 0xff) as u8;
    let b7: u8 = ((x >> 8) & 0xff) as u8;
    let b8: u8 = (x & 0xff) as u8;

    [b1, b2, b3, b4, b5, b6, b7, b8]
}

pub fn u8s_to_u64(xs: [u8; 8]) -> u64 {
    u64::from(xs[0]) * 256 * 256 * 256 * 256 * 256 * 256 * 256
        + u64::from(xs[1]) * 256 * 256 * 256 * 256 * 256 * 256
        + u64::from(xs[2]) * 256 * 256 * 256 * 256 * 256
        + u64::from(xs[3]) * 256 * 256 * 256 * 256
        + u64::from(xs[4]) * 256 * 256 * 256
        + u64::from(xs[5]) * 256 * 256
        + u64::from(xs[6]) * 256
        + u64::from(xs[7])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u64_to_u8s() {
        let n = u64::max_value() - 254;
        let u8s = u64_to_u8s(n);
        assert_eq!(u8s, [255, 255, 255, 255, 255, 255, 255, 1]);

        // and back
        assert_eq!(u8s_to_u64(u8s), n);
    }
}
