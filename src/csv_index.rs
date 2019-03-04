use std::error::Error;
use std::fs::File;

use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use serde::{Deserialize, Serialize, Serializer};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use crate::address::Address;
use crate::chunked_map::chunk_map;
use crate::range::Range;
use crate::toc::{Toc, TypedToc};
use crate::unsafe_float::UnsafeFloat;

use std::f64;
use std::i64;

use log::{debug, info};

#[derive(Serialize, Deserialize)]
pub struct CsvIndex<R: Ord> {
    map: BTreeMap<R, Vec<Address>>,
}

impl<R: Ord> CsvIndex<R> {
    pub fn new() -> Self {
        CsvIndex {
            map: BTreeMap::new(),
        }
    }

    pub fn from(map: BTreeMap<R, Vec<Address>>) -> Self {
        CsvIndex { map }
    }

    pub fn into_map(self) -> BTreeMap<R, Vec<Address>> {
        self.map
    }

    pub fn entry(&mut self, k: R) -> Entry<R, Vec<Address>> {
        self.map.entry(k)
    }

    pub fn keys(&self) -> std::collections::btree_map::Keys<R, Vec<Address>> {
        self.map.keys()
    }

    pub fn uniques(&self) -> usize {
        self.map.len()
    }

    pub fn print_matching_records<W: Write>(
        &self,
        bounds: Range<R>,
        file: &File,
        mut writer: &mut W,
    ) {
        self.map
            .range(bounds)
            .flat_map(|(_key, vals)| vals.into_iter())
            .for_each(|address| {
                address.print_record(&mut writer, &file);
            });
    }
}

pub enum CsvIndexType {
    STR(CsvIndex<Vec<u8>>),
    I64(CsvIndex<i64>),
    F64(CsvIndex<UnsafeFloat>),
}
impl Serialize for CsvIndexType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CsvIndexType::STR(index) => index.serialize(serializer),
            CsvIndexType::I64(index) => index.serialize(serializer),
            CsvIndexType::F64(index) => index.serialize(serializer),
        }
    }
}

impl CsvIndexType {
    pub fn try_new(csv_type: &str) -> Result<Self, &'static str> {
        match csv_type.to_uppercase().as_ref() {
            "STR" => Ok(CsvIndexType::STR(CsvIndex::<Vec<u8>>::new())),
            "INT" => Ok(CsvIndexType::I64(CsvIndex::<i64>::new())),
            "FLOAT" => Ok(CsvIndexType::F64(CsvIndex::<UnsafeFloat>::new())),
            _ => Err("Unknown operator"),
        }
    }

    #[inline]
    pub fn insert(&mut self, key: Vec<u8>, value: Address) {
        match self {
            CsvIndexType::STR(index) => index.entry(key).or_insert_with(|| vec![]).push(value),
            CsvIndexType::I64(index) => {
                let key = std::str::from_utf8(&key)
                    .unwrap_or("")
                    .parse()
                    .unwrap_or(i64::MIN);
                index.entry(key).or_insert_with(|| vec![]).push(value)
            }
            CsvIndexType::F64(index) => {
                let key = UnsafeFloat(
                    std::str::from_utf8(&key)
                        .unwrap_or("")
                        .parse()
                        .unwrap_or(f64::NEG_INFINITY),
                );
                index.entry(key).or_insert_with(|| vec![]).push(value)
            }
        }
    }

    pub fn uniques(&self) -> usize {
        match &self {
            CsvIndexType::STR(index) => index.uniques(),
            CsvIndexType::I64(index) => index.uniques(),
            CsvIndexType::F64(index) => index.uniques(),
        }
    }

    pub fn print_range(&self) {
        match &self {
            CsvIndexType::STR(index) => {
                info!(
                    "Min value {:?}, max {:?}",
                    index
                        .keys()
                        .next()
                        .map(|b| std::str::from_utf8(b).unwrap_or("INVALID")),
                    index
                        .keys()
                        .next_back()
                        .map(|b| std::str::from_utf8(b).unwrap_or("INVALID")),
                );
            }
            CsvIndexType::I64(index) => {
                info!(
                    "Min value {:?}, max {:?}",
                    index.keys().find(|&&x| x != i64::MIN),
                    index.keys().next_back()
                );
            }
            CsvIndexType::F64(index) => {
                info!(
                    "Min value {:?}, max {:?}",
                    index.keys().find(|&&x| x.0 != f64::NEG_INFINITY),
                    index.keys().next_back()
                );
            }
        }
    }

    pub fn serialize(self, mut fh: File, length: u64) -> Result<(), Box<Error>> {
        let num_chunks = 2 + length as usize / 50000;
        info!("Dividing into {} chunks", num_chunks);

        match self {
            CsvIndexType::STR(index) => {
                let chunked_map = chunk_map(&mut index.into_map(), num_chunks);
                info!("Writing to file");

                let mut toc = Toc::<Vec<u8>>::new(num_chunks);

                // build phantom TOC
                toc.build_empty(&chunked_map);

                // write phantom TOC to file, to get the right offsets
                let typed_toc = TypedToc::STR(toc);
                typed_toc.write_head(&mut fh, 0)?;

                // count size of toc
                let toc_len = fh.seek(SeekFrom::Current(0))?;

                let mut toc = Toc::<Vec<u8>>::new(num_chunks);
                toc.write_maps(&mut fh, chunked_map, toc_len)?;

                let typed_toc = TypedToc::STR(toc);
                debug!("TOC {:?}", typed_toc);
                typed_toc.write_head(&mut fh, toc_len)?;
            }

            CsvIndexType::I64(index) => {
                let chunked_map = chunk_map(&mut index.into_map(), num_chunks);
                info!("Writing to file");

                let mut toc = Toc::<i64>::new(num_chunks);

                // build phantom TOC
                toc.build_empty(&chunked_map);

                // write phantom TOC to file, to get the right offsets
                let typed_toc = TypedToc::I64(toc);
                typed_toc.write_head(&mut fh, 0)?;

                // count size of toc
                let toc_len = fh.seek(SeekFrom::Current(0))?;

                let mut toc = Toc::<i64>::new(num_chunks);
                toc.write_maps(&mut fh, chunked_map, toc_len)?;

                let typed_toc = TypedToc::I64(toc);
                debug!("TOC {:?}", typed_toc);
                typed_toc.write_head(&mut fh, toc_len)?;
            }
            CsvIndexType::F64(index) => {
                let chunked_map = chunk_map(&mut index.into_map(), num_chunks);
                info!("Writing to file");

                let mut toc = Toc::<UnsafeFloat>::new(num_chunks);

                // build phantom TOC
                toc.build_empty(&chunked_map);

                // write phantom TOC to file, to get the right offsets
                let typed_toc = TypedToc::F64(toc);
                typed_toc.write_head(&mut fh, 0)?;

                // count size of toc
                let toc_len = fh.seek(SeekFrom::Current(0))?;

                let mut toc = Toc::<UnsafeFloat>::new(num_chunks);
                toc.write_maps(&mut fh, chunked_map, toc_len)?;

                let typed_toc = TypedToc::F64(toc);
                debug!("TOC {:?}", typed_toc);
                typed_toc.write_head(&mut fh, toc_len)?;
            }
        };

        Ok(())
    }
}
