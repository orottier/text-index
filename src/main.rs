extern crate bincode;
extern crate clap;
extern crate csv;

use clap::{value_t, App, Arg, SubCommand};

use std::error::Error;
use std::ops::Bound::{Excluded, Included, Unbounded};

use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use std::collections::BTreeMap;

type CsvIndex = BTreeMap<String, Vec<(u64, u64)>>;

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
                .arg(Arg::with_name("COLUMN").required(true).index(1)),
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

        let index = index(file, column)?;
        let encoded: Vec<u8> = bincode::serialize(&index).unwrap();
        let mut fh = File::create(format!("{}.index.{}", filename, column + 1))?;
        fh.write_all(&encoded)?;

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

fn index(file: File, column: usize) -> Result<CsvIndex, Box<Error>> {
    let mut index = CsvIndex::new();
    let mut rdr = csv::Reader::from_reader(file);

    let mut record = csv::StringRecord::new();
    let mut prev_value = String::from("");
    let mut prev_pos = 0;
    let mut pos;

    while rdr.read_record(&mut record)? {
        pos = record.position().unwrap().byte();

        if prev_pos != 0 {
            index
                .entry(prev_value.clone())
                .or_insert_with(|| vec![])
                .push((prev_pos, pos - prev_pos));
        }

        // no new alloc
        prev_value.clear();
        prev_value.push_str(&record[column]);

        prev_pos = pos;
    }
    if prev_pos != 0 {
        pos = record.position().unwrap().byte();
        index
            .entry(prev_value)
            .or_insert_with(|| vec![])
            .push((prev_pos, pos - prev_pos));
    }

    println!("Read {} rows", index.len());

    Ok(index)
}

fn filter(mut file: File, filename: &str, select: &Filter) -> Result<(), Box<Error>> {
    let fh = File::open(format!("{}.index.{}", filename, select.column + 1))?;
    let index: CsvIndex = bincode::deserialize_from(fh)?;

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

    index
        .range(bounds)
        .flat_map(|(_key, vals)| vals.into_iter())
        .for_each(|&(byte_pos, len)| {
            let mut buf = vec![0u8; len as usize];
            file.seek(SeekFrom::Start(byte_pos))
                .expect("Unable to seek file pos");
            file.read_exact(&mut buf).expect("Unable to read file");

            // may result in invalid utf8 if file has changed after index
            let record = unsafe { std::str::from_utf8_unchecked(&buf) };
            print!("{}", record);
        });

    Ok(())
}
