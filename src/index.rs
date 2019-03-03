use crate::csv_index::CsvIndexType;
use crate::csv_reader::CsvReader;
use log::{debug, trace};

use std::error::Error;
use std::fs::File;

use std::sync::{Arc, Mutex};

pub fn scan_file(
    file: &File,
    column: usize,
    index: &Arc<Mutex<CsvIndexType>>,
    pid: u64,
    chunk_size: u64,
) -> Result<u64, Box<dyn Error + Send>> {
    let offset = pid * chunk_size;
    let reader = CsvReader::new(file, column, offset, chunk_size);

    if offset > 0 {
        debug!(
            "THREAD{} skipping: {}",
            pid,
            std::str::from_utf8(reader.padding()).unwrap_or("invalid UTF8")
        );
    }

    let mut counter = 0;
    let mut temp_results = vec![];

    reader.for_each(|(address, value)| {
        trace!("THREAD{} read: {:?}", pid, value);

        temp_results.push((value, address));

        if temp_results.len() % 100_000 == 0 {
            counter += temp_results.len() as u64;
            debug!("THREAD{}: Processed {} items", pid, counter);

            let mut locked_index = index.lock().unwrap();
            while let Some((value, address)) = temp_results.pop() {
                locked_index.insert(value, address);
            }
        }
    });

    counter += temp_results.len() as u64;

    let mut locked_index = index.lock().unwrap();
    while let Some((value, address)) = temp_results.pop() {
        locked_index.insert(value, address);
    }

    Ok(counter)
}
