use std::path::{Path, PathBuf};
use std::time::Duration;
use std::sync::Arc;

use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

use crate::errors::RedisError;
use crate::resp::{encode_frame, parse_frame, Frame};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofFsync {
    Always,
    EverySec,
    No,
}

impl AofFsync {
    pub fn parse(s: &str) -> Result<Self, RedisError> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Ok(AofFsync::Always),
            "everysec" => Ok(AofFsync::EverySec),
            "no" => Ok(AofFsync::No),
            _ => Err(RedisError::Other(
                    "ERR --aof-fsync must be one of: always | everysec | no".into(),
            )),
        }
    }
}

#[derive(Debug)]
struct AofInner {
    writer: BufWriter<tokio::fs::File>,
}

#[derive(Debug)]
pub struct Aof {
    path: PathBuf,
    fsync: AofFsync,
    inner: Mutex<AofInner>
}

impl Aof {
    pub async fn open(path: impl AsRef<Path>, fsync: AofFsync) -> Result<Arc<Self>, RedisError> {
        let path = path.as_ref().to_path_buf();

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
       
        let aof = Arc::new(Self {
            path,
            fsync,
            inner: Mutex::new(AofInner {
                writer: BufWriter::new(file),
            }),
        });
        
        if fsync == AofFsync::EverySec {
            let cloned = aof.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    let _ = cloned.flush_and_sync().await;
                }
            });
        }

        Ok(aof)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    
    pub fn fsync_policy(&self) -> AofFsync {
        self.fsync
    }

    pub async fn append_frame(&self, frame: &Frame) -> Result<(), RedisError> {
        let bytes = encode_frame(frame);

        let mut inner = self.inner.lock().await;
        inner.writer.write_all(&bytes).await?;

        inner.writer.flush().await?;

        match self.fsync {
            AofFsync::Always => {
                inner.writer.get_ref().sync_data().await?;
            }
            AofFsync::EverySec | AofFsync::No => {
            }
        }

        Ok(())
    }

    async fn flush_and_sync(&self) -> Result<(), RedisError> {
        let mut inner = self.inner.lock().await;

        inner.writer.flush().await?;
        inner.writer.get_ref().sync_data().await?;
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
