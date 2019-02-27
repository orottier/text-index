use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use std::error::Error;
use std::fs::File;

use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::RangeBounds;

use flate2::read::GzDecoder;

use log::debug;
use std::fmt::Debug;

use crate::bits;
use crate::csv_index::Address;
use crate::csv_index::CsvIndex;
use crate::range::ranges_overlap;
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

impl<R: Ord + DeserializeOwned + Clone + Debug> Toc<R> {
    pub fn new(num_chapters: usize) -> Self {
        Self {
            addr: Vec::with_capacity(num_chapters),
        }
    }

    pub fn push(&mut self, value: (R, Address)) {
        self.addr.push(value);
    }

    fn bounds(&self) -> Vec<(Address, (Bound<R>, Bound<R>))> {
        let mut bounds = Vec::new();

        let mut prev_bound = Unbounded;
        let mut prev_address = Address {
            offset: 0,
            length: 0,
        };

        self.addr.iter().for_each(|(val, address)| {
            if prev_bound != Unbounded {
                bounds.push((
                    prev_address.clone(),
                    (prev_bound.clone(), Excluded(val.clone())),
                ));
            }
            prev_bound = Included(val.clone());
            prev_address = address.clone();
        });

        if prev_bound != Unbounded {
            bounds.push((prev_address, (prev_bound, Unbounded)));
        }

        bounds
    }

    pub fn find(self, bounds: &(Bound<R>, Bound<R>)) -> Vec<Address> {
        let toc_bounds = self.bounds();
        debug!("toc bounds {:?}", toc_bounds);

        toc_bounds
            .into_iter()
            .filter(|(_, toc_bound)| ranges_overlap(bounds, toc_bound))
            .map(|(address, _)| address)
            .collect()
    }

    pub fn get_index(
        self,
        mut fh: &mut File,
        bounds: &(Bound<R>, Bound<R>),
    ) -> Result<Vec<CsvIndex<R>>, Box<Error>> {
        let addresses = self.find(bounds);
        debug!("need to fetch maps {:?}", addresses);

        let maps = addresses
            .into_iter()
            .map(|address| {
                fh.seek(SeekFrom::Start(address.offset)).unwrap();

                let gzh = fh.take(address.length);
                let gz = GzDecoder::new(gzh);
                bincode::deserialize_from(gz).unwrap()
            })
            .collect();

        Ok(maps)
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
