extern crate bincode;
extern crate clap;
extern crate csv;
extern crate multimap;

use clap::{value_t, App, Arg, SubCommand};

use multimap::MultiMap;
use std::error::Error;

use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
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

fn index(file: File, column: usize) -> Result<MultiMap<String, (u64, u64)>, Box<Error>> {
    let mut index = MultiMap::<String, (u64, u64)>::new();
    let mut rdr = csv::Reader::from_reader(file);

    let mut record = csv::StringRecord::new();
    let mut prev_value = String::from("");
    let mut prev_pos = 0;
    let mut pos;

    while rdr.read_record(&mut record)? {
        pos = record.position().unwrap().byte();
        println!("read   {} @ {}", record[column].to_owned(), pos);

        if prev_pos != 0 {
            println!("insert {} @ {}", prev_value, pos - prev_pos);
            index.insert(prev_value.clone(), (prev_pos, pos - prev_pos));
        }

        // no new alloc
        prev_value.clear();
        prev_value.push_str(&record[column]);

        prev_pos = pos;
    }
    if prev_pos != 0 {
        pos = record.position().unwrap().byte();
        println!("insert {} @ {}", prev_value, pos - prev_pos);
        index.insert(prev_value, (prev_pos, pos - prev_pos));
    }

    println!("{:?}", index.get_vec("1"));

    Ok(index)
}

fn filter(mut file: File, filename: &str, column: usize, value: &str) -> Result<(), Box<Error>> {
    let fh = File::open(format!("{}.index.{}", filename, column + 1))?;
    let index: MultiMap<String, (u64, u64)> = bincode::deserialize_from(fh)?;

    index
        .get_vec(value)
        .unwrap_or(&vec![])
        .into_iter()
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
