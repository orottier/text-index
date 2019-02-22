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
