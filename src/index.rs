use crate::csv_index::{Address, CsvIndexType};
use log::{debug, trace};

use std::error::Error;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use std::sync::{Arc, Mutex};

pub fn scan_file(
    mut file: &File,
    column: usize,
    index: &Arc<Mutex<CsvIndexType>>,
    pid: u64,
    chunk_size: u64,
) -> Result<u64, Box<dyn Error + Send>> {
    let mut offset = pid * chunk_size;

    let input: Box<dyn Read> = if pid == 0 {
        Box::new(file)
    } else {
        debug!("Thread {} seeking {}", pid, offset);
        file.seek(SeekFrom::Start(offset)).unwrap(); //todo
        let mut reader = BufReader::with_capacity(1 << 16, file);
        let mut buf = vec![];
        reader.read_until(10u8, &mut buf).unwrap(); // jump to newline
        offset += buf.len() as u64;
        debug!("skipping leftover {:?}", buf);

        Box::new(reader)
    };

    let mut rdr = csv::Reader::from_reader(input);
    let mut record = csv::StringRecord::new();
    let mut prev_value = String::from("");
    let mut prev_pos = 0;
    let mut pos;

    let mut counter = 0;
    let mut temp_results = Vec::new();

    while prev_pos < chunk_size {
        let success = rdr.read_record(&mut record).unwrap();
        if !success {
            break; // EOF
        }
        trace!("THREAD{} read: {:?}", pid, record);
        pos = record.position().unwrap().byte();

        if prev_pos != 0 {
            let address = Address {
                offset: offset + prev_pos,
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
            offset: offset + prev_pos,
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
