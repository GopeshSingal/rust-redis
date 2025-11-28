use std::fs::OpenOptions;
use std::io::{Write, BufWriter};
use std::sync::{Arc, Mutex};

pub struct Aof {
    writer: Mutex<BufWriter<std::fs::File>>,
}

impl Aof {
    pub fn new(path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
        })
    }

    pub fn append(&self, data: &str) {
        let mut w = self.writer.lock().unwrap();
        w.write_all(data.as_bytes()).unwrap();
        w.flush().unwrap();
    }
}