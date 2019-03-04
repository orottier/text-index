use crate::csv_index::CsvIndexType;
use crate::csv_reader::CsvReader;

use log::{debug, info, trace};
use std::time::Instant;

use std::error::Error;
use std::fs::File;

use std::sync::{Arc, Mutex};
use std::thread;

pub fn index(
    filename: &str,
    column: usize,
    csv_type: &str,
    threads: u64,
) -> Result<(CsvIndexType, u64), Box<dyn Error>> {
    let file_size = std::fs::metadata(filename)?.len();
    debug!("file size {}", file_size);
    let chunk_size = file_size / threads;

    let start = Instant::now();

    let csv_index = Arc::new(Mutex::new(CsvIndexType::try_new(csv_type)?));

    let mut handles = Vec::new();
    for i in 0..threads {
        let thread_index = Arc::clone(&csv_index);
        let thread_file = File::open(filename)?;
        let handle = thread::Builder::new()
            .name(format!("reader_{}", i))
            .spawn(move || index_chunk(&thread_file, column, &thread_index, i, chunk_size))?;

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

    info!(
        "Read {} rows with {} unique values",
        counter,
        index.uniques()
    );
    let elapsed = start.elapsed().as_secs();
    if elapsed > 0 {
        info!("Records/sec: {}", counter / elapsed);
    }

    index.print_range();

    Ok((index, counter))
}

fn index_chunk(
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
    let mut temp_results = Vec::with_capacity(100_000.min((chunk_size / 1000) as usize));

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
