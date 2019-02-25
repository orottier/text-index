use serde::{Deserialize, Serialize};

use crate::csv_index::Address;
use crate::unsafe_float::UnsafeFloat;

#[derive(Serialize, Deserialize, Debug)]
pub struct Toc<R> {
    // sorted list of byte positions
    addr: Vec<(R, Address)>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TypedToc {
    STR(Toc<String>),
    I64(Toc<i64>),
    F64(Toc<UnsafeFloat>),
}

impl<R: Ord> Toc<R> {
    pub fn new(num_chapters: usize) -> Self {
        Self {
            addr: Vec::with_capacity(num_chapters),
        }
    }

    pub fn push(&mut self, value: (R, Address)) {
        self.addr.push(value);
    }

    pub fn find(self, value: &R) -> Option<Address> {
        let mut prev = Address {
            offset: 0,
            length: 0,
        };

        let mut toc_iter = self.addr.into_iter();
        toc_iter.find(|(key, address)| {
            if key > value {
                true
            } else {
                prev = address.clone();
                false
            }
        });

        if prev.offset == 0 {
            return None;
        }

        Some(prev)
    }
}
