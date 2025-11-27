use thiserror::Error;

use crate::resp::parser::ParseError;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("unknown command")]
    UnknownCommand,

    #[error("wrong type")]
    WrongType,

    #[error("{0}")]
    Other(String),
}