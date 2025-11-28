use tokio::net::TcpStream;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::resp::{Frame, parse_frame, encode_frame};

pub struct Connection {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            reader: BufReader::new(stream),
        }
    }

    pub fn new_from_reader<R>(reader: R) -> Self 
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let rd = BufReader::new(reader);
        Self {
            reader: rd,
            writer: None,
        }
    }

    pub async fn read_frame(&mut self) -> std::io::Result<Option<Frame>> {
        let mut first = [0u8; 1];
        let n = self.reader.read(&mut first).await?;
        if n == 0 {
            return Ok(None);
        }

        let mut buf = vec![first[0]];

        loop {
            if let Ok((frame, _used)) = parse_frame(&buf) {
                return Ok(Some(frame));
            }

            let mut chunk = [0u8; 1024];
            let n = self.reader.read(&mut chunk).await?;
            if n == 0 {
                if buf.is_empty() {
                    return Ok(None);
                } else {
                    if let Ok((frame, _used)) = parse_frame(&buf) {
                        return Ok(Some(frame));
                    }
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "incomplete frame",
                    ));
                }
            }
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> std::io::Result<()> {
        let bytes = encode_frame(frame);
        let stream = self.reader.get_mut();
        stream.write_all(&bytes).await?;
        stream.flush().await
    }
}