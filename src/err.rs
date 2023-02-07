use std::error::Error;
use std::fmt;
use std::io;
use std::num::ParseIntError;

#[derive(Debug)]
pub enum RedisError {
    Parse(String),
    IO(String),
    Type,
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisError::Type => {
                write!(f, "Operation against a key holding the wrong kind of value")
            }
            RedisError::Parse(message) => write!(f, "{message}"),
            RedisError::IO(message) => write!(f, "{message}"),
        }
    }
}

impl Error for RedisError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl From<io::Error> for RedisError {
    fn from(value: io::Error) -> Self {
        RedisError::IO(format!("IO error: {value}"))
    }
}

impl From<ParseIntError> for RedisError {
    fn from(value: ParseIntError) -> Self {
        RedisError::Parse(format!("Invalid input: {value}"))
    }
}
