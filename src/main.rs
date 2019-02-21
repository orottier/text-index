extern crate bincode;
extern crate clap;
extern crate csv;
extern crate serde;

mod unsafe_float;
use crate::unsafe_float::UnsafeFloat;

use clap::{value_t, App, Arg, SubCommand};
use serde::{Deserialize, Serialize};

use std::error::Error;
use std::ops::Bound::{Excluded, Included, Unbounded};

use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

use std::collections::BTreeMap;

use std::f64;
use std::i64;

type CsvIndex<R> = BTreeMap<R, Vec<(u64, u64)>>;

#[derive(Serialize, Deserialize)]
enum CsvIndexType {
    STR(CsvIndex<String>),
    I64(CsvIndex<i64>),
    F64(CsvIndex<UnsafeFloat>),
}

#[inline]
fn insert_csv_index(index: &mut CsvIndexType, key: String, value: (u64, u64)) -> () {
    match index {
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

#[inline]
fn print_record(file: &mut File, byte_pos: u64, len: u64) {
    let mut buf = vec![0u8; len as usize];
    file.seek(SeekFrom::Start(byte_pos))
        .expect("Unable to seek file pos");
    file.read_exact(&mut buf).expect("Unable to read file");

    // may result in invalid utf8 if file has changed after index
    let record = unsafe { std::str::from_utf8_unchecked(&buf) };
    print!("{}", record);
}

enum Operation {
    EQ,
    LT,
    LE,
    GT,
    GE,
}

struct Filter<'a> {
    op: Operation,
    value: &'a str,
    column: usize,
}

fn main() -> Result<(), Box<Error>> {
    let matches = App::new("csv_index")
        .version("0.1")
        .arg(
            Arg::with_name("INPUT")
                .help("Sets the input file to use")
                .required(true)
                .index(1),
        )
        .subcommand(
            SubCommand::with_name("index")
                .help("Build an index for a given column")
                .arg(Arg::with_name("COLUMN").required(true).index(1))
                .arg(Arg::with_name("TYPE").required(false).index(2)),
        )
        .subcommand(
            SubCommand::with_name("filter")
                .help("Filter records on a column value")
                .arg(Arg::with_name("COLUMN").required(true).index(1))
                .arg(Arg::with_name("OP").required(true).index(2))
                .arg(Arg::with_name("VALUE").required(true).index(3)),
        )
        .get_matches();

    let filename = matches
        .value_of("INPUT")
        .expect("required arg cannot be None")
        .to_owned();
    let file = File::open(filename.clone())?;

    if let Some(matches) = matches.subcommand_matches("index") {
        let column = value_t!(matches.value_of("COLUMN"), usize).unwrap_or_else(|e| e.exit());
        let column = column - 1; // index starts at 1

        let csv_type = matches.value_of("TYPE").unwrap_or("STR").to_uppercase();

        let index = index(file, column, csv_type)?;

        let fh = File::create(format!("{}.index.{}", filename, column + 1))?;
        let gz = GzEncoder::new(fh, Compression::fast());
        bincode::serialize_into(gz, &index).unwrap();

        return Ok(());
    }

    if let Some(matches) = matches.subcommand_matches("filter") {
        let column = value_t!(matches.value_of("COLUMN"), usize).unwrap_or_else(|e| e.exit());
        let column = column - 1; // index starts at 1

        let value = matches
            .value_of("VALUE")
            .expect("required arg cannot be None");

        let op_str = matches.value_of("OP").expect("required arg cannot be None");
        let op = match op_str.to_uppercase().as_ref() {
            "LT" => Operation::LT,
            "LE" => Operation::LE,
            "EQ" => Operation::EQ,
            "GE" => Operation::GE,
            "GT" => Operation::GT,
            _ => panic!("Unknown operator"),
        };
        let select = Filter { op, value, column };

        return filter(file, &filename, &select);
    }

    unreachable!();
}

fn index(file: File, column: usize, csv_type: String) -> Result<CsvIndexType, Box<Error>> {
    let mut index = match csv_type.as_ref() {
        "STR" => CsvIndexType::STR(CsvIndex::<String>::new()),
        "INT" => CsvIndexType::I64(CsvIndex::<i64>::new()),
        "FLOAT" => CsvIndexType::F64(CsvIndex::<UnsafeFloat>::new()),
        _ => panic!("Unknown operator"),
    };

    let mut rdr = csv::Reader::from_reader(file);
    let mut record = csv::StringRecord::new();
    let mut prev_value = String::from("");
    let mut prev_pos = 0;
    let mut pos;
    let mut counter = 0u64;

    while rdr.read_record(&mut record)? {
        pos = record.position().unwrap().byte();

        if prev_pos != 0 {
            counter += 1;
            insert_csv_index(&mut index, prev_value.clone(), (prev_pos, pos - prev_pos));
        }

        // no new alloc
        prev_value.clear();
        prev_value.push_str(&record[column]);

        prev_pos = pos;
    }
    if prev_pos != 0 {
        counter += 1;
        pos = record.position().unwrap().byte();
        insert_csv_index(&mut index, prev_value, (prev_pos, pos - prev_pos));
    }

    let unique = match &index {
        CsvIndexType::STR(index) => index.len(),
        CsvIndexType::I64(index) => index.len(),
        CsvIndexType::F64(index) => index.len(),
    };
    println!("Read {} rows with {} unique values", counter, unique);

    Ok(index)
}

fn filter(mut file: File, filename: &str, select: &Filter) -> Result<(), Box<Error>> {
    let fh = File::open(format!("{}.index.{}", filename, select.column + 1))?;
    let gz = GzDecoder::new(fh);
    let index: CsvIndexType = bincode::deserialize_from(gz)?;

    match index {
        CsvIndexType::STR(typed_index) => {
            let bounds = match select.op {
                Operation::EQ => (
                    Included(select.value.to_owned()),
                    Included(select.value.to_owned()),
                ),
                Operation::LE => (Unbounded, Included(select.value.to_owned())),
                Operation::LT => (Unbounded, Excluded(select.value.to_owned())),
                Operation::GT => (Excluded(select.value.to_owned()), Unbounded),
                Operation::GE => (Included(select.value.to_owned()), Unbounded),
            };

            typed_index
                .range(bounds)
                .flat_map(|(_key, vals)| vals.into_iter())
                .for_each(|&(byte_pos, len)| {
                    print_record(&mut file, byte_pos, len);
                });
        }
        CsvIndexType::I64(typed_index) => {
            let value: i64 = select.value.parse().unwrap_or(i64::MIN);
            let bounds = match select.op {
                Operation::EQ => (Included(value), Included(value)),
                Operation::LE => (Excluded(i64::MIN), Included(value)),
                Operation::LT => (Excluded(i64::MIN), Excluded(value)),
                Operation::GT => (Excluded(value), Excluded(i64::MAX)),
                Operation::GE => (Included(value), Excluded(i64::MAX)),
            };

            typed_index
                .range(bounds)
                .flat_map(|(_key, vals)| vals.into_iter())
                .for_each(|&(byte_pos, len)| {
                    print_record(&mut file, byte_pos, len);
                });
        }
        CsvIndexType::F64(typed_index) => {
            let value: UnsafeFloat = UnsafeFloat(select.value.parse().unwrap_or(f64::NEG_INFINITY));
            let lower = Excluded(UnsafeFloat(f64::NEG_INFINITY));
            let upper = Excluded(UnsafeFloat(f64::INFINITY));
            let bounds = match select.op {
                Operation::EQ => (Included(value), Included(value)),
                Operation::LE => (lower, Included(value)),
                Operation::LT => (lower, Excluded(value)),
                Operation::GT => (Excluded(value), upper),
                Operation::GE => (Included(value), upper),
            };

            typed_index
                .range(bounds)
                .flat_map(|(_key, vals)| vals.into_iter())
                .for_each(|&(byte_pos, len)| {
                    print_record(&mut file, byte_pos, len);
                });
        }
    };

    Ok(())
}
