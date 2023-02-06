use std::fmt;
use std::error::Error;
use std::io;
use std::num::ParseIntError;

#[derive(Debug)]
pub enum RedisError {
    ParseError(String),
    IOError(String),
    TypeError,
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisError::TypeError => 
                write!(f, "Operation against a key holding the wrong kind of value"),
            RedisError::ParseError(message) => 
                write!(f, "{}", message),
            RedisError::IOError(message) =>
                write!(f, "{}", message),
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
        RedisError::IOError(format!("IO error: {}", value))
    }
}

impl From<ParseIntError> for RedisError {
    fn from(value: ParseIntError) -> Self {
        RedisError::ParseError(format!("Invalid input: {}", value))
    }
}