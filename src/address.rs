use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Address {
    pub offset: u64,
    pub length: u64,
}

impl Address {
    #[inline]
    pub fn print_record<W: Write>(&self, handle: &mut W, mut file: &File) {
        let mut buf = vec![0u8; self.length as usize];
        file.seek(SeekFrom::Start(self.offset))
            .expect("Unable to seek file pos");
        file.read_exact(&mut buf).expect("Unable to read file");

        handle.write_all(&buf).unwrap();
    }
}
