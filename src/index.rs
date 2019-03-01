use crate::csv_index::{Address, CsvIndexType};
use log::debug;

use std::error::Error;
use std::fs::File;
use std::sync::{Arc, Mutex};

pub fn scan_file(
    file: &File,
    column: usize,
    index: &Arc<Mutex<CsvIndexType>>,
    pid: usize,
) -> Result<u64, Box<dyn Error + Send>> {
    let mut rdr = csv::Reader::from_reader(file);
    let mut record = csv::StringRecord::new();
    let mut prev_value = String::from("");
    let mut prev_pos = 0;
    let mut pos;

    let mut counter = 0;
    let mut temp_results = Vec::new();

    while rdr.read_record(&mut record).unwrap() {
        pos = record.position().unwrap().byte();

        if prev_pos != 0 {
            let address = Address {
                offset: prev_pos,
                length: pos - prev_pos,
            };
            temp_results.push((prev_value.clone(), address));
        }

        // no new alloc
        prev_value.clear();
        prev_value.push_str(&record[column]);

        prev_pos = pos;

        if temp_results.len() % 100_000 == 0 {
            counter += temp_results.len() as u64;
            debug!("THREAD{}: Processed {} items", pid, counter);

            let mut locked_index = index.lock().unwrap();
            while let Some((value, address)) = temp_results.pop() {
                locked_index.insert(value, address);
            }
        }
    }
    if prev_pos != 0 {
        pos = record.position().unwrap().byte();
        let address = Address {
            offset: prev_pos,
            length: pos - prev_pos,
        };

        temp_results.push((prev_value, address));
    }

    counter += temp_results.len() as u64;

    let mut locked_index = index.lock().unwrap();
    while let Some((value, address)) = temp_results.pop() {
        locked_index.insert(value, address);
    }

    Ok(counter)
}
