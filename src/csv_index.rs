use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use flate2::read::GzDecoder;
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

    pub fn serialize(&mut self, mut fh: File) -> Result<(), Box<Error>> {
        match self {
            CsvIndexType::STR(index) => {
                let num_chunks = 1 + index.len() / 50000;
                let mut toc: CsvToc<String> = Vec::with_capacity(num_chunks);
                let chunked_map = chunk_map(index, num_chunks);

                // build phantom TOC
                chunked_map.iter().for_each(|(key, _sub_map)| {
                    toc.push((key.to_owned(), 0));
                });
                let typed_toc = CsvTocType::STR(toc);

                // write phantom TOC to file, to get the right offsets
                fh.write(&bits::u64_to_u8s(0))?;
                bincode::serialize_into(&mut fh, &typed_toc)?;
                let toc_len = fh.seek(SeekFrom::Current(0))?;

                let mut toc: CsvToc<String> = Vec::with_capacity(num_chunks);

                let mut prev_pos = toc_len;
                let write_ops: Result<Vec<()>, Box<dyn Error>> = chunked_map
                    .into_iter()
                    .map(|(key, sub_map)| {
                        let typed_sub = CsvIndexType::STR(sub_map);

                        let gz = GzEncoder::new(&mut fh, Compression::fast());
                        bincode::serialize_into(gz, &typed_sub)?;

                        let pos = fh.seek(SeekFrom::Current(0))?;
                        toc.push((key, prev_pos));

                        prev_pos = pos;
                        Ok(())
                    })
                    .collect();
                write_ops?; // propagate error, if any

                let typed_toc = CsvTocType::STR(toc);
                println!("TOC {:?}", typed_toc);
                fh.seek(SeekFrom::Start(0))?;
                fh.write(&bits::u64_to_u8s(toc_len))?;
                bincode::serialize_into(&mut fh, &typed_toc)?;
            }
            CsvIndexType::I64(index) => (),
            CsvIndexType::F64(index) => (),
        };

        Ok(())
    }

    pub fn open(mut fh: File, value: String) -> Result<CsvIndexType, Box<Error>> {
        let mut reader = BufReader::new(&mut fh);
        let mut size_buffer = [0u8; 8];
        reader.read_exact(&mut size_buffer)?;
        let toc_len = bits::u8s_to_u64(size_buffer);

        let toc_data = (&mut reader).take(toc_len - 8);
        let toc_typed: CsvTocType = bincode::deserialize_from(toc_data)?;
        println!("toc {:?}", toc_typed);

        match toc_typed {
            CsvTocType::STR(toc) => {
                let mut prev_pos = 0;
                let mut toc_iter = toc.into_iter();
                let next = toc_iter.find(|(key, pos)| {
                    if *key > value {
                        return true;
                    } else {
                        prev_pos = *pos;
                        return false;
                    }
                });

                println!("Seeking {}", prev_pos);
                fh.seek(SeekFrom::Start(prev_pos))?;
                let mut gzh: Box<dyn Read> = Box::new(fh);
                if let Some((_key, pos)) = next {
                    if pos == 0 {
                        return Ok(CsvIndexType::STR(CsvIndex::new()));
                    }
                    println!("with len {}", pos - prev_pos);
                    gzh = Box::new(gzh.take(pos - prev_pos));
                }

                let gz = GzDecoder::new(gzh);
                let index: CsvIndexType = bincode::deserialize_from(gz)?;
                Ok(index)
            }
            _ => panic!(""),
        }
    }
}
