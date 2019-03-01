use crate::csv_index::{Address, CsvIndexType};
use log::debug;

use std::error::Error;
use std::fs::File;
use std::sync::{Arc, Mutex};

pub fn scan_file(
    file: &File,
    column: usize,
    index: &Arc<Mutex<CsvIndexType>>,
) -> Result<u64, Box<dyn Error + Send>> {
    let mut locked_index = index.lock().unwrap();

    let mut rdr = csv::Reader::from_reader(file);
    let mut record = csv::StringRecord::new();
    let mut prev_value = String::from("");
    let mut prev_pos = 0;
    let mut pos;
    let mut counter = 0u64;

    while rdr.read_record(&mut record).unwrap() {
        pos = record.position().unwrap().byte();

        if prev_pos != 0 {
            counter += 1;

            let address = Address {
                offset: prev_pos,
                length: pos - prev_pos,
            };
            locked_index.insert(prev_value.clone(), address);
        }

        // no new alloc
        prev_value.clear();
        prev_value.push_str(&record[column]);

        prev_pos = pos;

        if counter % 1_000_000 == 0 {
            debug!("Processed {}M items", counter / 1_000_000);
        }
    }
    if prev_pos != 0 {
        counter += 1;

        pos = record.position().unwrap().byte();
        let address = Address {
            offset: prev_pos,
            length: pos - prev_pos,
        };
        locked_index.insert(prev_value, address);
    }

    Ok(counter)
}
