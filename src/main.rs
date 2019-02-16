extern crate bincode;
extern crate clap;
extern crate csv;
extern crate multimap;

use clap::{value_t, App, Arg, SubCommand};

use multimap::MultiMap;
use std::error::Error;

use std::fs::File;
use std::io::Write;

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
                .arg(Arg::with_name("VALUE").required(true).index(2)),
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

        return filter(file, &filename, column, value);
    }

    unreachable!();
}

fn index(file: File, column: usize) -> Result<MultiMap<String, u64>, Box<Error>> {
    let mut index = MultiMap::<String, u64>::new();
    let mut rdr = csv::Reader::from_reader(file);

    for result in rdr.records() {
        let record = result?;
        let pos = record.position().unwrap().byte();
        index.insert(record[column].to_owned(), pos);
        println!("{} @ {}", record[column].to_owned(), pos);
    }

    println!("{:?}", index.get_vec("1"));

    Ok(index)
}

fn filter(file: File, filename: &str, column: usize, value: &str) -> Result<(), Box<Error>> {
    let mut rdr = csv::Reader::from_reader(file);

    let fh = File::open(format!("{}.index.{}", filename, column + 1))?;
    let index: MultiMap<String, u64> = bincode::deserialize_from(fh)?;

    let mut record = csv::StringRecord::new();
    index
        .get_vec(value)
        .unwrap_or(&vec![])
        .into_iter()
        .for_each(|&byte_pos| {
            let mut pos = csv::Position::new();
            pos.set_byte(byte_pos);
            rdr.seek(pos).unwrap();
            assert!(rdr.read_record(&mut record).unwrap()); // must be true unless underlying file has changed since index

            println!("{:?}", record);
        });

    Ok(())
}
