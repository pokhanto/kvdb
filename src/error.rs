use std::{fmt, io};

#[derive(Debug)]
pub enum DbError {
    LogFileOpen,
    LogFileWrite,
    KeyNotFound,
    Io(io::Error),
    Serde(serde_json::Error),
    String(String),
    Unknown,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::LogFileOpen => write!(f, "Can't open log file."),
            DbError::LogFileWrite => write!(f, "Can't write to log file."),
            DbError::KeyNotFound => write!(f, "Key not found."),
            DbError::Io(error) => write!(f, "IO error. {}", error),
            DbError::Serde(error) => write!(f, "Serde error. {}", error),
            DbError::String(message) => write!(f, "{}", message),
            DbError::Unknown => write!(f, "Unknown error."),
        }
    }
}

impl From<io::Error> for DbError {
    fn from(error: io::Error) -> DbError {
        DbError::Io(error)
    }
}

impl From<serde_json::Error> for DbError {
    fn from(error: serde_json::Error) -> DbError {
        DbError::Serde(error)
    }
}

impl std::error::Error for DbError {}

/// Result
pub type Result<T> = std::result::Result<T, DbError>;
