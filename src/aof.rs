use std::path::{Path, PathBuf};

use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

use crate::errors::RedisError;
use crate::resp::{encode_frame, parse_frame, Frame};

#[derive(Debug)]
pub struct Aof {
    path: PathBuf,
    writer: Mutex<BufWriter<tokio::fs::File>>,
}

impl Aof {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, RedisError> {
        let path = path.as_ref().to_path_buf();

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(Self {
            path,
            writer: Mutex::new(BufWriter::new(file)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn append_frame(&self, frame: &Frame) -> Result<(), RedisError> {
        let bytes = encode_frame(frame);
        let mut w = self.writer.lock().await;
        w.write_all(&bytes).await?;
        w.flush().await?;
        Ok(())
    }
}

pub fn parse_frames_from_bytes(bytes: &[u8]) -> Result<Vec<Frame>, RedisError> {
    let mut frames = Vec::new();
    let mut offset = 0;

    while offset < bytes.len() {
        match parse_frame(&bytes[offset..]) {
            Ok((frame, used)) => {
                frames.push(frame);
                offset += used;
            }
            Err(crate::resp::parser::ParseError::Incomplete) => {
                break;
            }
            Err(e) => {
                return Err(RedisError::Other(format!(
                            "AOF parse error at offset {}: {}",
                            offset, e
                )));
            }
        }
    }
    Ok(frames)
}
