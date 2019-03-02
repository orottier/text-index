mod bits;
mod chunked_map;
mod csv_index;
mod filter;
mod index;
mod range;
mod toc;
mod unsafe_float;

use env_logger::Env;
use log::{debug, info};
use std::time::Instant;

use crate::csv_index::{print_matching_records, CsvIndexType};
use crate::filter::{Filter, Operator};
use crate::toc::TypedToc;

use clap::{value_t, App, Arg, SubCommand};

use std::error::Error;
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::thread;

fn main() -> Result<(), Box<Error>> {
    let matches = App::new("csv_index")
        .version("0.1")
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Verbosity (-v, -vv supported)"),
        )
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

    let default_log = match matches.occurrences_of("v") {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    let env = Env::default().filter_or("RUST_LOG", default_log);
    env_logger::init_from_env(env);

    let filename = matches
        .value_of("INPUT")
        .expect("required arg cannot be None")
        .to_owned();

    if let Some(matches) = matches.subcommand_matches("index") {
        let column = value_t!(matches.value_of("COLUMN"), usize).unwrap_or_else(|e| e.exit());
        let column = column - 1; // index starts at 1

        let csv_type = matches.value_of("TYPE").unwrap_or("STR");

        let mut index = index(&filename, column, &csv_type)?;

        let fh = File::create(format!("{}.index.{}", filename, column + 1))?;
        index.serialize(fh)?;

        return Ok(());
    }

    if let Some(matches) = matches.subcommand_matches("filter") {
        let mut file = File::open(filename.clone())?;

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

        return filter(&mut file, &filename, &select);
    }

    unreachable!();
}

fn index(filename: &str, column: usize, csv_type: &str) -> Result<CsvIndexType, Box<dyn Error>> {
    let start = Instant::now();

    let csv_index = Arc::new(Mutex::new(CsvIndexType::new(csv_type)?));

    let mut handles = Vec::new();
    for i in 1..5 {
        let thread_index = Arc::clone(&csv_index);
        let thread_file = File::open(filename)?;
        let handle =
            thread::spawn(move || index::scan_file(&thread_file, column, &thread_index, i));

        handles.push(handle);
    }

    let counter = handles
        .into_iter()
        .map(|handle| handle.join().unwrap_or_else(|_| panic!("Thread problem")))
        .collect::<Result<Vec<u64>, Box<dyn Error + Send>>>()
        .unwrap()
        .iter()
        .sum::<u64>();

    let index = Arc::try_unwrap(csv_index)
        .unwrap_or_else(|_| panic!("Arc problem"))
        .into_inner()?;

    info!("Read {} rows with {} unique values", counter, index.len());
    let elapsed = start.elapsed().as_secs();
    if elapsed > 0 {
        info!("Records/sec: {}", counter / elapsed);
    }

    index.print_range();

    Ok(index)
}

fn filter(file: &mut File, filename: &str, select: &Filter) -> Result<(), Box<Error>> {
    let mut fh = File::open(format!("{}.index.{}", filename, select.column + 1))?;
    let typed_toc = TypedToc::open(&mut fh)?;

    match typed_toc {
        TypedToc::STR(typed_toc) => {
            let bounds = select.string_bounds();
            let index = typed_toc.get_index(&mut fh, &bounds)?;
            print_matching_records(index, bounds, &file);
        }
        TypedToc::I64(typed_toc) => {
            let bounds = select.int_bounds();
            let index = typed_toc.get_index(&mut fh, &bounds)?;
            print_matching_records(index, bounds, &file);
        }
        TypedToc::F64(typed_toc) => {
            let bounds = select.float_bounds();
            let index = typed_toc.get_index(&mut fh, &bounds)?;
            print_matching_records(index, bounds, &file);
        }
    };

    Ok(())
}
