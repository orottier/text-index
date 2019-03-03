use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use crate::address::Address;
use csv::{ByteRecord, ReaderBuilder};

pub struct CsvReader<'a> {
    rdr: csv::Reader<Box<dyn 'a + Read>>,
    column: usize,
    padding: Vec<u8>,
    offset: u64,
    chunk_size: u64,

    // current record being read, to prevent allocations in loop
    record: ByteRecord,
}

impl<'a> CsvReader<'a> {
    pub fn new<R: 'a + Read + Seek>(
        mut input: R,
        column: usize,
        mut offset: u64,
        chunk_size: u64,
    ) -> Self {
        let mut padding = vec![];

        let rdr: csv::Reader<Box<dyn Read>> = if offset == 0 {
            ReaderBuilder::new().from_reader(Box::new(input))
        } else {
            input.seek(SeekFrom::Start(offset)).unwrap(); //todo
            let mut reader = BufReader::with_capacity(1 << 16, input);
            reader.read_until(10u8, &mut padding).unwrap(); // jump to newline
            offset += padding.len() as u64;

            ReaderBuilder::new()
                .has_headers(false)
                .from_reader(Box::new(reader))
        };

        Self {
            rdr,
            column,
            padding,
            offset,
            chunk_size,
            record: ByteRecord::new(),
        }
    }

    pub fn padding(&self) -> &[u8] {
        &self.padding
    }
}

impl<'a> Iterator for CsvReader<'a> {
    type Item = (Address, Vec<u8>);

    fn next(&mut self) -> Option<(Address, Vec<u8>)> {
        let success = self.rdr.read_byte_record(&mut self.record).unwrap();
        if !success {
            return None;
        }

        let pos = self.record.position().unwrap().byte();
        if pos > self.chunk_size {
            return None;
        }

        let address = Address {
            offset: pos + self.offset,
            length: self.rdr.position().byte() - pos,
        };

        Some((address, self.record[self.column].to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_full() {
        let input = std::io::Cursor::new(
            "\
city,country,pop
Boston,United States,4628910
Amsterdam,Netherlands,7500000
",
        );
        let mut reader = CsvReader::new(input, 0, 0, 1000);

        assert_eq!(reader.padding(), vec![].as_slice());

        let item = reader.next();
        assert!(item.is_some());

        let (address, record) = item.unwrap();
        assert_eq!(
            address,
            Address {
                offset: 17,
                length: 29
            }
        );
        assert_eq!(record, b"Boston".to_owned());

        let item = reader.next();
        assert!(item.is_some());

        let (address, record) = item.unwrap();
        assert_eq!(
            address,
            Address {
                offset: 46,
                length: 30
            }
        );
        assert_eq!(record, b"Amsterdam".to_owned());

        let item = reader.next();
        assert!(item.is_none());
    }

    #[test]
    fn test_read_chunk() {
        let input = std::io::Cursor::new(
            "\
city,country,pop
Boston,United States,4628910
Amsterdam,Netherlands,7500000
",
        );
        let mut reader = CsvReader::new(input, 0, 0, 40);

        assert_eq!(reader.padding(), vec![].as_slice());

        let item = reader.next();
        assert!(item.is_some());

        let (address, record) = item.unwrap();
        assert_eq!(
            address,
            Address {
                offset: 17,
                length: 29
            }
        );
        assert_eq!(record, b"Boston".to_owned());

        let item = reader.next();
        assert!(item.is_none());
    }

    #[test]
    fn test_read_offset() {
        let input = std::io::Cursor::new(
            "\
city,country,pop
Boston,United States,4628910
Amsterdam,Netherlands,7500000
",
        );
        let mut reader = CsvReader::new(input, 0, 25, 1000);

        assert_eq!(
            std::str::from_utf8(reader.padding()),
            Ok("nited States,4628910\n")
        );

        let item = reader.next();
        assert!(item.is_some());

        let (address, record) = item.unwrap();
        assert_eq!(
            address,
            Address {
                offset: 46,
                length: 30
            }
        );
        assert_eq!(record, b"Amsterdam".to_owned());

        let item = reader.next();
        assert!(item.is_none());
    }
}
