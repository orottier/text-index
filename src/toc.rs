use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use std::error::Error;
use std::fs::File;

use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use std::ops::Bound;
use std::ops::RangeBounds;

use flate2::read::GzDecoder;

use log::debug;

use crate::bits;
use crate::csv_index::Address;
use crate::csv_index::CsvIndex;
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

impl<R: Ord + DeserializeOwned> Toc<R> {
    pub fn new(num_chapters: usize) -> Self {
        Self {
            addr: Vec::with_capacity(num_chapters),
        }
    }

    pub fn push(&mut self, value: (R, Address)) {
        self.addr.push(value);
    }

    pub fn find(self, bounds: &(Bound<R>, Bound<R>)) -> Option<Address> {
        let mut prev = Address {
            offset: 0,
            length: 0,
        };

        let mut toc_iter = self.addr.into_iter();
        toc_iter.find(|(key, address)| {
            if bounds.contains(key) {
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

    pub fn get_index(
        self,
        mut fh: File,
        bounds: &(Bound<R>, Bound<R>),
    ) -> Result<CsvIndex<R>, Box<Error>> {
        if let Some(address) = self.find(bounds) {
            debug!("Seeking {:?}", address);
            fh.seek(SeekFrom::Start(address.offset))?;

            let gzh = fh.take(address.length);
            let gz = GzDecoder::new(gzh);
            let index: CsvIndex<R> = bincode::deserialize_from(gz)?;

            Ok(index)
        } else {
            Ok(CsvIndex::new())
        }
    }
}

impl TypedToc {
    pub fn open(fh: &mut File) -> Result<TypedToc, Box<Error>> {
        let mut reader = BufReader::new(fh);
        let mut size_buffer = [0u8; 8];
        reader.read_exact(&mut size_buffer)?;
        let toc_len = bits::u8s_to_u64(size_buffer);

        let toc_data = (&mut reader).take(toc_len - 8);
        let toc_typed: TypedToc = bincode::deserialize_from(toc_data)?;
        debug!("toc {:?}", toc_typed);

        Ok(toc_typed)
    }
}
