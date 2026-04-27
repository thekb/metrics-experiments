use std::fmt::{self, Display};

#[derive(Debug)]
pub enum LogError {
    Conflict(String),
    CorruptSegment(String),
    InvalidArgument(String),
    Io(std::io::Error),
    NotFound(String),
    Store(String),
}

impl Display for LogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict(message) => write!(f, "conflict: {message}"),
            Self::CorruptSegment(message) => write!(f, "corrupt segment: {message}"),
            Self::InvalidArgument(message) => write!(f, "invalid argument: {message}"),
            Self::Io(error) => write!(f, "io error: {error}"),
            Self::NotFound(message) => write!(f, "not found: {message}"),
            Self::Store(message) => write!(f, "store error: {message}"),
        }
    }
}

impl std::error::Error for LogError {}

impl From<std::io::Error> for LogError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub type Result<T> = std::result::Result<T, LogError>;
