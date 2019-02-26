use std::error::Error;
use std::fs::File;

use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::io::{self, StdoutLock};

use flate2::write::GzEncoder;
use flate2::Compression;

use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use std::collections::Bound;

use crate::bits;
use crate::chunked_map::chunk_map;
use crate::toc::{Toc, TypedToc};
use crate::unsafe_float::UnsafeFloat;

use std::f64;
use std::i64;

use log::debug;

#[inline]
fn print_record(handle: &mut StdoutLock, mut file: &File, address: &Address) {
    let mut buf = vec![0u8; address.length as usize];
    file.seek(SeekFrom::Start(address.offset))
        .expect("Unable to seek file pos");
    file.read_exact(&mut buf).expect("Unable to read file");

    handle.write_all(&buf).unwrap();
}

pub type CsvIndex<R> = BTreeMap<R, Vec<Address>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Address {
    pub offset: u64,
    pub length: u64,
}

pub fn print_matching_records<R: Ord>(
    index: &CsvIndex<R>,
    bounds: (Bound<R>, Bound<R>),
    file: &File,
) {
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    index
        .range(bounds)
        .flat_map(|(_key, vals)| vals.into_iter())
        .for_each(|address| {
            print_record(&mut handle, file, address);
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
            CsvIndexType::STR(index) => index.serialize(serializer),
            CsvIndexType::I64(index) => index.serialize(serializer),
            CsvIndexType::F64(index) => index.serialize(serializer),
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

    pub fn len(&self) -> usize {
        match &self {
            CsvIndexType::STR(index) => index.len(),
            CsvIndexType::I64(index) => index.len(),
            CsvIndexType::F64(index) => index.len(),
        }
    }

    pub fn serialize(&mut self, mut fh: File) -> Result<(), Box<Error>> {
        match self {
            CsvIndexType::STR(index) => {
                let num_chunks = 1 + index.len() / 50000;
                let mut toc = Toc::<String>::new(num_chunks);
                let chunked_map = chunk_map(index, num_chunks);

                // build phantom TOC
                chunked_map.iter().for_each(|(key, _sub_map)| {
                    toc.push((
                        key.to_owned(),
                        Address {
                            offset: 0,
                            length: 0,
                        },
                    ));
                });
                let typed_toc = TypedToc::STR(toc);

                // write phantom TOC to file, to get the right offsets
                fh.write_all(&bits::u64_to_u8s(0))?;
                bincode::serialize_into(&mut fh, &typed_toc)?;
                let toc_len = fh.seek(SeekFrom::Current(0))?;

                let mut toc = Toc::<String>::new(num_chunks);

                let mut prev_pos = toc_len;
                let write_ops: Result<Vec<()>, Box<dyn Error>> = chunked_map
                    .into_iter()
                    .map(|(key, sub_map)| {
                        let typed_sub = CsvIndexType::STR(sub_map);

                        let gz = GzEncoder::new(&mut fh, Compression::fast());
                        bincode::serialize_into(gz, &typed_sub)?;

                        let pos = fh.seek(SeekFrom::Current(0))?;
                        let address = Address {
                            offset: prev_pos,
                            length: pos - prev_pos,
                        };
                        toc.push((key, address));

                        prev_pos = pos;
                        Ok(())
                    })
                    .collect();
                write_ops?; // propagate error, if any

                let typed_toc = TypedToc::STR(toc);
                debug!("TOC {:?}", typed_toc);

                fh.seek(SeekFrom::Start(0))?;
                fh.write_all(&bits::u64_to_u8s(toc_len))?;
                bincode::serialize_into(&mut fh, &typed_toc)?;
            }
            CsvIndexType::I64(index) => (),
            CsvIndexType::F64(index) => (),
        };

        Ok(())
    }
}
