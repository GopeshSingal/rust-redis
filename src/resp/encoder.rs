use super::parser::Frame;

pub fn encode_frame(frame: &Frame) -> Vec<u8> {
    match frame {
        Frame::Simple(s) => {
            let mut out = Vec::new();
            out.extend_from_slice(b"+");
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
            out
        }
        Frame::Error(s) => {
            let mut out = Vec::new();
            out.extend_from_slice(b"-");
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
            out
        }
        Frame::Integer(i) => {
            let mut out = Vec::new();
            out.extend_from_slice(b":");
            out.extend_from_slice(i.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out
        }
        Frame::Bulk(data) => {
            let mut out = Vec::new();
            out.extend_from_slice(b"$");
            out.extend_from_slice(data.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
            out
        }
        Frame::Array(items) => {
            let mut out = Vec::new();
            out.extend_from_slice(b"*");
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                out.extend_from_slice(&encode_frame(item));
            }
            out
        }
        Frame::Null => b"$-1\r\n".to_vec(),
    }
}