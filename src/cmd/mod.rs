pub mod parser;

#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
  Ping,
  CommandDocs,
  DbSize,
  Get(&'a str),
  Set(&'a str, &'a [u8]),
  SetEx(&'a str, &'a [u8], usize),
  Lpush(&'a str, Vec<Vec<u8>>),
  Rpush(&'a str, Vec<Vec<u8>>),
  LpushX(&'a str, Vec<Vec<u8>>),
  RpushX(&'a str, Vec<Vec<u8>>),
  Lpop(&'a str, usize),
  Rpop(&'a str, usize),
  Del(Vec<&'a str>),
  Incr(&'a str),
}
