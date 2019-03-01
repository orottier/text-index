use std::error::Error;
use std::fs::File;

use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::io::{self, StdoutLock};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;

use crate::chunked_map::chunk_map;
use crate::range::Range;
use crate::toc::{Toc, TypedToc};
use crate::unsafe_float::UnsafeFloat;

use std::f64;
use std::i64;

use log::{debug, info};
use std::fmt::Debug;

#[inline]
fn print_record(handle: &mut StdoutLock, mut file: &File, address: &Address) {
    let mut buf = vec![0u8; address.length as usize];
    file.seek(SeekFrom::Start(address.offset))
        .expect("Unable to seek file pos");
    file.read_exact(&mut buf).expect("Unable to read file");

    handle.write_all(&buf).unwrap();
}

#[derive(Serialize, Deserialize)]
pub struct CsvIndex<R: Ord>(pub BTreeMap<R, Vec<Address>>);

impl<R: Ord> CsvIndex<R> {
    pub fn new() -> Self {
        CsvIndex(BTreeMap::new())
    }

    pub fn keys(&self) -> std::collections::btree_map::Keys<R, Vec<Address>> {
        self.0.keys()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Address {
    pub offset: u64,
    pub length: u64,
}

pub fn print_matching_records<R: Ord + Clone + Debug + DeserializeOwned>(
    indexes: Vec<CsvIndex<R>>,
    bounds: Range<R>,
    file: &File,
) {
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    indexes.into_iter().for_each(|index| {
        let b_clone = (bounds.0.clone(), bounds.1.clone());
        index
            .0
            .range(b_clone)
            .flat_map(|(_key, vals)| vals.into_iter())
            .for_each(|address| {
                print_record(&mut handle, &file, address);
            });
    });
}

pub enum CsvIndexType {
    STR(CsvIndex<String>),
    I64(CsvIndex<i64>),
    F64(CsvIndex<UnsafeFloat>),
}
impl Serialize for CsvIndexType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CsvIndexType::STR(index) => index.0.serialize(serializer),
            CsvIndexType::I64(index) => index.0.serialize(serializer),
            CsvIndexType::F64(index) => index.0.serialize(serializer),
        }
    }
}

impl CsvIndexType {
    pub fn new(csv_type: &str) -> Result<Self, &'static str> {
        match csv_type.to_uppercase().as_ref() {
            "STR" => Ok(CsvIndexType::STR(CsvIndex::<String>::new())),
            "INT" => Ok(CsvIndexType::I64(CsvIndex::<i64>::new())),
            "FLOAT" => Ok(CsvIndexType::F64(CsvIndex::<UnsafeFloat>::new())),
            _ => Err("Unknown operator"),
        }
    }

    #[inline]
    pub fn insert(&mut self, key: String, value: Address) {
        match self {
            CsvIndexType::STR(index) => index.0.entry(key).or_insert_with(|| vec![]).push(value),
            CsvIndexType::I64(index) => {
                let key = key.parse().unwrap_or(i64::MIN);
                index.0.entry(key).or_insert_with(|| vec![]).push(value)
            }
            CsvIndexType::F64(index) => {
                let key = UnsafeFloat(key.parse().unwrap_or(f64::NEG_INFINITY));
                index.0.entry(key).or_insert_with(|| vec![]).push(value)
            }
        }
    }

    pub fn len(&self) -> usize {
        match &self {
            CsvIndexType::STR(index) => index.0.len(),
            CsvIndexType::I64(index) => index.0.len(),
            CsvIndexType::F64(index) => index.0.len(),
        }
    }

    pub fn print_range(&self) {
        match &self {
            CsvIndexType::STR(index) => {
                info!(
                    "Min value {:?}, max {:?}",
                    index.keys().next(),
                    index.keys().next_back()
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

    pub fn serialize(&mut self, mut fh: File) -> Result<(), Box<Error>> {
        let num_chunks = 2 + self.len() / 50000;

        match self {
            CsvIndexType::STR(index) => {
                let chunked_map = chunk_map(&mut index.0, num_chunks);
                let mut toc = Toc::<String>::new(num_chunks);

                // build phantom TOC
                toc.build_empty(&chunked_map);

                // write phantom TOC to file, to get the right offsets
                let typed_toc = TypedToc::STR(toc);
                typed_toc.write_head(&mut fh, 0)?;

                // count size of toc
                let toc_len = fh.seek(SeekFrom::Current(0))?;

                let mut toc = Toc::<String>::new(num_chunks);
                toc.write_maps(&mut fh, chunked_map, toc_len)?;

                let typed_toc = TypedToc::STR(toc);
                debug!("TOC {:?}", typed_toc);
                typed_toc.write_head(&mut fh, toc_len)?;
            }

            CsvIndexType::I64(index) => {
                let chunked_map = chunk_map(&mut index.0, num_chunks);
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
                let chunked_map = chunk_map(&mut index.0, num_chunks);
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
