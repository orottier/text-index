use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct UnsafeFloat(pub f64);

impl Eq for UnsafeFloat {}
impl Ord for UnsafeFloat {
    fn cmp(&self, other: &UnsafeFloat) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Less)
    }
}
impl PartialOrd for UnsafeFloat {
    fn partial_cmp(&self, other: &UnsafeFloat) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for UnsafeFloat {
    fn eq(&self, other: &UnsafeFloat) -> bool {
        self.0 == other.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort() {
        let mut vec = vec![
            UnsafeFloat(12.0),
            UnsafeFloat(-1000.),
            UnsafeFloat(std::f64::NEG_INFINITY),
        ];
        vec.sort();
        assert_eq!(
            vec,
            vec![
                UnsafeFloat(std::f64::NEG_INFINITY),
                UnsafeFloat(-1000.),
                UnsafeFloat(12.0),
            ]
        );
    }
}
