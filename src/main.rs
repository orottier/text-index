mod address;
mod bits;
mod chunked_map;
mod csv_index;
mod csv_reader;
mod filter;
mod index;
mod range;
mod toc;
mod unsafe_float;

use env_logger::Env;

use clap::{value_t, App, Arg, SubCommand};

use std::error::Error;
use std::fs::File;

fn main() -> Result<(), Box<Error>> {
    let matches = App::new("csv_index")
        .version("0.1")
        .arg(
            Arg::with_name("VERBOSITY")
                .short("v")
                .multiple(true)
                .help("Verbose output (-v, -vv supported)"),
        )
        .arg(
            Arg::with_name("THREADS")
                .value_name("THREADS")
                .short("t")
                .help("Max number of THREADS")
                .takes_value(true)
                .empty_values(false),
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

    let default_log = match matches.occurrences_of("VERBOSITY") {
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

    let threads = value_t!(matches.value_of("THREADS"), u64).unwrap_or(2);

    if let Some(matches) = matches.subcommand_matches("index") {
        let column = value_t!(matches.value_of("COLUMN"), usize).unwrap_or_else(|e| e.exit());
        let column = column - 1; // index starts at 1

        let csv_type = matches.value_of("TYPE").unwrap_or("STR");

        if threads == 0 {
            panic!("Thread count must be larger than 0");
        }

        let mut index = index::index(&filename, column, &csv_type, threads)?;

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
        let op = filter::Operator::from(op_str)?;

        let filter = filter::Filter::from(op, &value, &value2, column);

        let stdout = std::io::stdout();
        let writer = stdout.lock();

        return filter.execute(&mut file, &filename, writer);
    }

    Err("Use one of the subcommands (index, filter, ..)")?
}
