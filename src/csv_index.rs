use std::fs::File;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use flate2::write::GzEncoder;
use flate2::Compression;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::bits;
use crate::chunked_map::chunk_map;
use crate::unsafe_float::UnsafeFloat;

use std::f64;
use std::i64;

pub type CsvToc<R> = Vec<(R, u64)>;
pub type CsvIndex<R> = BTreeMap<R, Vec<(u64, u64)>>;

#[derive(Serialize, Deserialize, Debug)]
pub enum CsvTocType {
    STR(CsvToc<String>),
    I64(CsvToc<i64>),
    F64(CsvToc<UnsafeFloat>),
}

#[derive(Serialize, Deserialize)]
pub enum CsvIndexType {
    STR(CsvIndex<String>),
    I64(CsvIndex<i64>),
    F64(CsvIndex<UnsafeFloat>),
}

impl CsvIndexType {
    #[inline]
    pub fn insert_csv_index(&mut self, key: String, value: (u64, u64)) -> () {
        match self {
            CsvIndexType::STR(index) => index.entry(key).or_insert_with(|| vec![]).push(value),
            CsvIndexType::I64(index) => {
                let key = key.parse().unwrap_or(i64::MIN);
                index.entry(key).or_insert_with(|| vec![]).push(value)
            }
            CsvIndexType::F64(index) => {
                let key = UnsafeFloat(key.parse().unwrap_or(f64::NEG_INFINITY));
                index.entry(key).or_insert_with(|| vec![]).push(value)
            }
        }
    }

    pub fn serialize(&mut self, mut fh: File) {
        let num_chunks = 10;
        match self {
            CsvIndexType::STR(index) => {
                let mut toc: CsvToc<String> = Vec::with_capacity(num_chunks);
                let chunked_map = chunk_map(index, num_chunks);

                // build phantom TOC
                chunked_map.iter().for_each(|(key, _sub_map)| {
                    toc.push((key.to_owned(), 0));
                });
                let typed_toc = CsvTocType::STR(toc);

                // write phantom TOC to file, to get the right offsets
                fh.write(&bits::u64_to_u8s(0));
                bincode::serialize_into(fh.try_clone().unwrap(), &typed_toc);
                let toc_len = fh.seek(SeekFrom::Current(0)).unwrap();

                let mut toc: CsvToc<String> = Vec::with_capacity(num_chunks);

                let mut prev_pos = 0;
                chunked_map.into_iter().for_each(|(key, sub_map)| {
                    let typed_sub = CsvIndexType::STR(sub_map);
                    let gz = GzEncoder::new(fh.try_clone().unwrap(), Compression::fast());
                    bincode::serialize_into(gz, &typed_sub).unwrap();

                    let pos = fh.seek(SeekFrom::Current(0)).unwrap();
                    toc.push((key, prev_pos));

                    prev_pos = pos;
                });

                let typed_toc = CsvTocType::STR(toc);
                println!("TOC {:?}", typed_toc);
                fh.seek(SeekFrom::Start(0)).unwrap();
                fh.write(&bits::u64_to_u8s(toc_len));
                bincode::serialize_into(fh, &typed_toc);
            }
            CsvIndexType::I64(index) => (),
            CsvIndexType::F64(index) => (),
        };
    }
}
