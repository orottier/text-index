mod bits;
mod chunked_map;
mod csv_index;
mod filter;
mod unsafe_float;

use env_logger::Env;
use log::info;

use crate::csv_index::{print_matching_records, Address, CsvIndexType};
use crate::filter::{Filter, Operator};

use clap::{value_t, App, Arg, SubCommand};

use std::error::Error;

use std::fs::File;

fn main() -> Result<(), Box<Error>> {
    let env = Env::default().filter_or("RUST_LOG", "debug");
    env_logger::init_from_env(env);

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
                .about("Build an index for a given column")
                .arg(
                    Arg::with_name("COLUMN")
                        .required(true)
                        .index(1)
                        .help("Column number (starts at 1)"),
                )
                .arg(
                    Arg::with_name("TYPE")
                        .required(false)
                        .index(2)
                        .help("Type (str(default), int, float)"),
                ),
        )
        .subcommand(
            SubCommand::with_name("filter")
                .about("Filter records on a column value")
                .arg(
                    Arg::with_name("COLUMN")
                        .required(true)
                        .index(1)
                        .help("Column number (starts at 1)"),
                )
                .arg(
                    Arg::with_name("OP")
                        .required(true)
                        .index(2)
                        .help("Operator (eq, lt, le, gt, ge, in, sw)"),
                )
                .arg(
                    Arg::with_name("VALUE")
                        .required(true)
                        .index(3)
                        .help("Value"),
                )
                .arg(
                    Arg::with_name("VALUE2")
                        .required(false)
                        .index(4)
                        .help("Value2 (when operator is `in`)"),
                ),
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

        let csv_type = matches.value_of("TYPE").unwrap_or("STR");

        let mut index = index(file, column, &csv_type)?;

        let fh = File::create(format!("{}.index.{}", filename, column + 1))?;
        index.serialize(fh)?;

        return Ok(());
    }

    if let Some(matches) = matches.subcommand_matches("filter") {
        let column = value_t!(matches.value_of("COLUMN"), usize).unwrap_or_else(|e| e.exit());
        let column = column - 1; // index starts at 1

        let value = matches
            .value_of("VALUE")
            .expect("required arg cannot be None");

        let value2 = matches.value_of("VALUE2").unwrap_or("");

        let op_str = matches.value_of("OP").expect("required arg cannot be None");
        let op = Operator::from(op_str)?;

        let select = Filter {
            op,
            value,
            value2,
            column,
        };

        return filter(file, &filename, &select);
    }

    unreachable!();
}

fn index(file: File, column: usize, csv_type: &str) -> Result<CsvIndexType, Box<Error>> {
    let mut index = CsvIndexType::new(csv_type)?;

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

            let address = Address {
                offset: prev_pos,
                length: (pos - prev_pos) as u32,
            };
            index.insert(prev_value.clone(), address);
        }

        // no new alloc
        prev_value.clear();
        prev_value.push_str(&record[column]);

        prev_pos = pos;
    }
    if prev_pos != 0 {
        counter += 1;

        pos = record.position().unwrap().byte();
        let address = Address {
            offset: prev_pos,
            length: (pos - prev_pos) as u32,
        };
        index.insert(prev_value, address);
    }

    info!("Read {} rows with {} unique values", counter, index.len());

    Ok(index)
}

fn filter(file: File, filename: &str, select: &Filter) -> Result<(), Box<Error>> {
    let fh = File::open(format!("{}.index.{}", filename, select.column + 1))?;
    let csv_index = CsvIndexType::open(fh, select.value.to_owned())?;

    match csv_index {
        CsvIndexType::STR(typed_index) => {
            let bounds = select.string_bounds();
            print_matching_records(&typed_index, bounds, &file);
        }
        CsvIndexType::I64(typed_index) => {
            let bounds = select.int_bounds();
            print_matching_records(&typed_index, bounds, &file);
        }
        CsvIndexType::F64(typed_index) => {
            let bounds = select.float_bounds();
            print_matching_records(&typed_index, bounds, &file);
        }
    };

    Ok(())
}
