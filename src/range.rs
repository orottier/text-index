use std::ops::Bound::{self, Excluded, Included, Unbounded};

pub type Range<R> = (Bound<R>, Bound<R>);

pub fn ranges_overlap<R: Ord>(b1: &Range<R>, b2: &Range<R>) -> bool {
    let lower = match b1.0 {
        Unbounded => &b2.0,
        Included(ref start) => match b2.0 {
            Unbounded => &b1.0,
            Included(ref start2) => {
                if start > start2 {
                    &b1.0
                } else {
                    &b2.0
                }
            }
            Excluded(ref start2) => {
                if start >= start2 {
                    &b1.0
                } else {
                    &b2.0
                }
            }
        },
        Excluded(ref start) => match b2.0 {
            Unbounded => &b1.0,
            Included(ref start2) => {
                if start > start2 {
                    &b1.0
                } else {
                    &b2.0
                }
            }
            Excluded(ref start2) => {
                if start >= start2 {
                    &b1.0
                } else {
                    &b2.0
                }
            }
        },
    };

    let upper = match b1.1 {
        Unbounded => &b2.1,
        Included(ref start) => match b2.1 {
            Unbounded => &b1.1,
            Included(ref start2) => {
                if start <= start2 {
                    &b1.1
                } else {
                    &b2.1
                }
            }
            Excluded(ref start2) => {
                if start < start2 {
                    &b1.1
                } else {
                    &b2.1
                }
            }
        },
        Excluded(ref start) => match &b2.1 {
            Unbounded => &b1.1,
            Included(ref start2) => {
                if start <= start2 {
                    &b1.1
                } else {
                    &b2.1
                }
            }
            Excluded(ref start2) => {
                if start < start2 {
                    &b1.1
                } else {
                    &b2.1
                }
            }
        },
    };

    match lower {
        Unbounded => true,
        Included(ref lower_val) => match upper {
            Unbounded => true,
            Included(ref upper_val) => lower_val <= upper_val,
            Excluded(ref upper_val) => lower_val < upper_val,
        },
        Excluded(ref lower_val) => match upper {
            Unbounded => true,
            Included(ref upper_val) => lower_val < upper_val,
            Excluded(ref upper_val) => lower_val < upper_val,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_both_unbound() {
        let r1: (Bound<u8>, Bound<u8>) = (Unbounded, Unbounded);
        let r2: (Bound<u8>, Bound<u8>) = (Unbounded, Unbounded);
        assert!(ranges_overlap(&r1, &r2));
    }

    #[test]
    fn test_one_unbound() {
        let r1: (Bound<u8>, Bound<u8>) = (Unbounded, Unbounded);
        let r2 = (Included(0), Unbounded);

        assert!(ranges_overlap(&r1, &r2));
        assert!(ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_lower_unbound() {
        let r1: Range<u8> = (Unbounded, Included(8));
        let r2: Range<u8> = (Unbounded, Included(6));

        assert!(ranges_overlap(&r1, &r2));
        assert!(ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_upper_unbound() {
        let r1: Range<u8> = (Included(8), Unbounded);
        let r2: Range<u8> = (Included(6), Unbounded);

        assert!(ranges_overlap(&r1, &r2));
        assert!(ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_one_lower_unbound() {
        let r1: Range<u8> = (Unbounded, Included(8));
        let r2: Range<u8> = (Included(0), Included(2));

        assert!(ranges_overlap(&r1, &r2));
        assert!(ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_one_upper_unbound() {
        let r1: Range<u8> = (Included(0), Unbounded);
        let r2: Range<u8> = (Included(4), Included(6));

        assert!(ranges_overlap(&r1, &r2));
        assert!(ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_full_enclosed() {
        let r1: Range<u8> = (Included(2), Included(4));
        let r2: Range<u8> = (Included(0), Included(6));

        assert!(ranges_overlap(&r1, &r2));
        assert!(ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_overlap() {
        let r1: Range<u8> = (Included(0), Included(4));
        let r2: Range<u8> = (Included(2), Included(6));

        assert!(ranges_overlap(&r1, &r2));
        assert!(ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_no_overlap() {
        let r1: Range<u8> = (Included(0), Included(2));
        let r2: Range<u8> = (Included(4), Included(6));

        assert!(!ranges_overlap(&r1, &r2));
        assert!(!ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_no_overlap_unbound() {
        let r1: Range<u8> = (Unbounded, Included(2));
        let r2: Range<u8> = (Included(4), Unbounded);

        assert!(!ranges_overlap(&r1, &r2));
        assert!(!ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_overlap_excl() {
        let r1: Range<u8> = (Included(0), Included(4));
        let r2: Range<u8> = (Excluded(2), Excluded(6));

        assert!(ranges_overlap(&r1, &r2));
        assert!(ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_overlap_incl() {
        let r1: Range<u8> = (Included(0), Included(4));
        let r2: Range<u8> = (Included(4), Included(6));

        assert!(ranges_overlap(&r1, &r2));
        assert!(ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_no_overlap_excl_one() {
        let r1: Range<u8> = (Included(0), Excluded(4));
        let r2: Range<u8> = (Included(4), Included(6));

        assert!(!ranges_overlap(&r1, &r2));
        assert!(!ranges_overlap(&r2, &r1));
    }

    #[test]
    fn test_no_overlap_excl_both() {
        let r1: Range<u8> = (Included(0), Excluded(4));
        let r2: Range<u8> = (Excluded(4), Included(6));

        assert!(!ranges_overlap(&r1, &r2));
        assert!(!ranges_overlap(&r2, &r1));
    }
}
