use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Address {
    pub offset: u64,
    pub length: u64,
}
