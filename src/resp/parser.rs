use std::str;

#[derive(Debug, Clone)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Array(Vec<Frame>),
    Null,
}

#[derive(Debug)]
pub enum ParseError {
    Incomplete,
    Invalid(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Incomplete => write!(f, "incomplete frame"),
            ParseError::Invalid(s) => write!(f, "invalid frame: {}", s),
        }
    }
}

impl std::error::Error for ParseError {}

pub fn parse_frame(src: &[u8]) -> Result<(Frame, usize), ParseError> {
    if src.is_empty() {
        return Err(ParseError::Incomplete);
    }

    match src[0] {
        b'+' => parse_simple(src),
        b'-' => parse_error(src),
        b':' => parse_integer(src),
        b'$' => parse_bulk(src),
        b'*' => parse_array(src),
        _ => Err(ParseError::Invalid("unknown frame type".into())),
    }
}

fn find_crlf(src: &[u8]) -> Option<usize> {
    src.windows(2).position(|w| w == b"\r\n")
}

fn parse_simple(src: &[u8]) -> Result<(Frame, usize), ParseError> {
    if let Some(pos) = find_crlf(src) {
        let line = &src[1..pos];
        let s = String::from_utf8(line.to_vec())
            .map_err(|e| ParseError::Invalid(format!("utf8: {}", e)))?;
        Ok((Frame::Simple(s), pos + 2))
    } else {
        Err(ParseError::Incomplete)
    }
}

fn parse_error(src: &[u8]) -> Result<(Frame, usize), ParseError> {
    if let Some(pos) = find_crlf(src) {
        let line = &src[1..pos];
        let s = String::from_utf8(line.to_vec())
            .map_err(|e| ParseError::Invalid(format!("utf8: {}", e)))?;
        Ok((Frame::Error(s), pos + 2))
    } else {
        Err(ParseError::Incomplete)
    }
}

fn parse_integer(src: &[u8]) -> Result<(Frame, usize), ParseError> {
    if let Some(pos) = find_crlf(src) {
        let line = &src[1..pos];
        let s = str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(format!("utf8: {}", e)))?;
        let val: i64 = s
            .parse()
            .map_err(|e| ParseError::Invalid(format!("parse int: {}", e)))?;
        Ok((Frame::Integer(val), pos + 2))
    } else {
        Err(ParseError::Incomplete)
    }
}

fn parse_bulk(src: &[u8]) -> Result<(Frame, usize), ParseError> {
    if let Some(pos) = find_crlf(src) {
        let line = &src[1..pos];
        let s = str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(format!("utf8: {}", e)))?;
        let len: isize = s
            .parse()
            .map_err(|e| ParseError::Invalid(format!("parse bulk len: {}", e)))?;

        if len == -1 {
            return Ok((Frame::Null, pos + 2));
        }

        let len = len as usize;
        let start = pos + 2;
        let end = start + len;

        if src.len() < end + 2 {
            return Err(ParseError::Incomplete);
        }

        if &src[end..end + 2] != b"\r\n" {
            return Err(ParseError::Invalid("bulk missing CRLF".into()));
        }

        let data = src[start..end].to_vec();
        Ok((Frame::Bulk(data), end + 2))
    } else {
        Err(ParseError::Incomplete)
    }
}

fn parse_array(src: &[u8]) -> Result<(Frame, usize), ParseError> {
    if let Some(pos) = find_crlf(src) {
        let line = &src[1..pos];
        let s = str::from_utf8(line)
            .map_err(|e| ParseError::Invalid(format!("utf8: {}", e)))?;
        let count: isize = s
            .parse()
            .map_err(|e| ParseError::Invalid(format!("parse array len: {}", e)))?;
        
        if count == -1 {
            return Ok((Frame::Null, pos + 2));
        }

        let mut items = Vec::with_capacity(count as usize);
        let mut offset = pos + 2;

        for _ in 0..count {
            let (frame, used) = parse_frame(&src[offset..])?;
            items.push(frame);
            offset += used;
        }

        Ok((Frame::Array(items), offset))
    } else {
        Err(ParseError::Incomplete)
    }
}