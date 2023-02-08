use crate::err::RedisError;

pub mod parser;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Ping,
    TtlCount,
    CommandDocs,
    Get(String),
    Set(String, Vec<u8>),
    SetEx(String, Vec<u8>, usize),
    Lpush(String, Vec<Vec<u8>>),
    Rpush(String, Vec<Vec<u8>>),
    LpushX(String, Vec<Vec<u8>>),
    RpushX(String, Vec<Vec<u8>>),
    Lpop(String, usize),
    Rpop(String, usize),
}
