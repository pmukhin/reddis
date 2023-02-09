use crate::cmd::Command;
use crate::err::RedisError;

use std::{fmt, num::ParseIntError};

use nom::{
  branch::alt,
  bytes::complete::{escaped, tag, tag_no_case, take_while},
  character::complete::{alphanumeric1 as alphanumeric, char, digit0, one_of},
  combinator::{cut, map, opt},
  error::{
    context, convert_error, ContextError, Error, ErrorKind, ParseError, VerboseError,
    VerboseErrorKind,
  },
  multi::separated_list0,
  number::complete::double,
  sequence::{delimited, preceded, separated_pair, terminated},
  Err, IResult,
};

enum CmdCode {
  Ping,
  Set,
  Get,
  SetEx,
  Lpush,
  Rpush,
  LpushX,
  RpushX,
  Lpop,
  Rpop,
  Del,
  Incr,
  DbSize,
  CommandDocs,
}

fn value_len<'a>(i: &'a str) -> IResult<&'a str, usize, ParseFailure> {
  let (i, _) = tag("$")(i)?;
  let (i, _u) = take_while(|c: char| c.is_numeric())(i)?;
  let (i, _) = tag("\r\n")(i)?;

  Ok((i, _u.parse::<usize>().unwrap()))
}

fn cmd<'a>(i: &'a str) -> IResult<&'a str, CmdCode, ParseFailure> {
  let (i, _) = opt(value_len)(i)?;
  let (i, v) = alt((
    map(tag_no_case("PING"), |_| CmdCode::Ping),
    map(tag_no_case("SET"), |_| CmdCode::Set),
    map(tag_no_case("GET"), |_| CmdCode::Get),
    map(tag_no_case("SETEX"), |_| CmdCode::SetEx),
    map(tag_no_case("LPUSHX"), |_| CmdCode::LpushX),
    map(tag_no_case("RPUSHX"), |_| CmdCode::RpushX),
    map(tag_no_case("LPUSH"), |_| CmdCode::Lpush),
    map(tag_no_case("RPUSH"), |_| CmdCode::Rpush),
    map(tag_no_case("LPOP"), |_| CmdCode::Lpop),
    map(tag_no_case("RPOP"), |_| CmdCode::Rpop),
    map(tag_no_case("DEL"), |_| CmdCode::Del),
    map(tag_no_case("INCR"), |_| CmdCode::Incr),
    map(tag_no_case("DBSIZE"), |_| CmdCode::DbSize),
    map(tag_no_case("COMMAND"), |_| CmdCode::CommandDocs),
  ))(i)?;
  let (i, _) = tag("\r\n")(i)?;

  Ok((i, v))
}

fn u_number<'a>(i: &'a str) -> IResult<&'a str, usize, ParseFailure> {
  let (i, v) = string(i)?;
  Ok((i, v.parse::<usize>().unwrap()))
}

fn value<'a>(i: &'a str) -> IResult<&'a str, &'a str, ParseFailure> {
  let (i, _) = tag("$")(i)?;
  let (i, size_str) = digit0(i)?;
  let str_size = size_str.parse::<usize>().unwrap();
  let (i, _) = tag("\r\n")(i)?;
  let value = &i[0..str_size];

  Ok((&i[str_size..], value))
}

fn string<'a>(i: &'a str) -> IResult<&'a str, &'a str, ParseFailure> {
  let (i, value) = value(i)?;
  let (i, _) = tag("\r\n")(i)?;

  Ok((i, value))
}

fn push<'a, F>(i: &'a str, f: F) -> IResult<&'a str, Command, ParseFailure>
where
  F: Fn(&'a str, Vec<&'a [u8]>) -> Command<'a>,
{
  let (i, key) = string(i)?;
  let (i, raw_values) = separated_list0(tag("\r\n"), value)(i)?;
  let values = raw_values
    .iter()
    .map(|v| v.as_bytes())
    .collect::<Vec<_>>();

  Ok((i, f(key, values)))
}

fn pop<'a, F>(i: &'a str, f: F) -> IResult<&'a str, Command, ParseFailure>
where
  F: Fn(&'a str, usize) -> Command<'a>,
{
  let (i, key) = string(i)?;
  let (i, count) = u_number(i)?;

  Ok((i, f(key, count)))
}

fn root<'a>(i: &'a str) -> IResult<&'a str, Command, ParseFailure> {
  let (i, cmd) = cmd(i)?;
  match cmd {
    CmdCode::Set => {
      let (i, key) = string(i)?;
      let (i, value) = string(i)?;
      Ok((i, Command::Set(key, value.as_bytes())))
    }
    CmdCode::Get => {
      let (i, key) = string(i)?;
      Ok((i, Command::Get(key)))
    }
    CmdCode::SetEx => {
      let (i, key) = string(i)?;
      let (i, value) = string(i)?;
      let (i, ttl) = u_number(i)?;
      let cmd = Command::SetEx(key, value.as_bytes(), ttl);
      Ok((i, cmd))
    }
    CmdCode::Lpush => push(i, Command::Lpush),
    CmdCode::Rpush => push(i, Command::Rpush),
    CmdCode::LpushX => push(i, Command::LpushX),
    CmdCode::RpushX => push(i, Command::RpushX),
    CmdCode::Lpop => pop(i, Command::Lpop),
    CmdCode::Rpop => pop(i, Command::Rpop),
    CmdCode::CommandDocs => Ok((i, Command::CommandDocs)),
    CmdCode::Ping => Ok((i, Command::Ping)),
    CmdCode::Incr => {
      let (i, key) = string(i)?;
      Ok((i, Command::Incr(key)))
    }
    CmdCode::Del => {
      let (i, raw_values) = separated_list0(tag("\r\n"), value)(i)?;
      let values = raw_values.iter().map(|v| *v).collect::<Vec<_>>();
      Ok((i, Command::Del(values)))
    }
    CmdCode::DbSize => Ok((i, Command::DbSize))
  }
}

#[derive(Debug)]
pub struct ParseFailure(String);

impl fmt::Display for ParseFailure {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "parsing failure: `{:?}`", self.0)
  }
}

pub fn parse<'a>(i: &'a str) -> Result<Command, RedisError> {
  let (_, cmd) = root(i)?;
  Ok(cmd)
}

impl From<nom::Err<ParseFailure>> for RedisError {
  fn from(value: nom::Err<ParseFailure>) -> Self {
    match value {
      Err::Incomplete(_) => todo!(),
      Err::Error(e) => RedisError::Parse(format!("{}", e)),
      Err::Failure(_) => todo!(),
    }
  }
}

impl From<ParseIntError> for ParseFailure {
  fn from(value: ParseIntError) -> Self {
    ParseFailure(format!("can't parse int: {value}"))
  }
}

impl ParseError<&str> for ParseFailure {
  fn from_error_kind(input: &str, kind: ErrorKind) -> Self {
    ParseFailure(format!("{:?}, {}", input, kind.description()))
  }

  fn append(input: &str, kind: ErrorKind, other: Self) -> Self {
    ParseFailure(format!(
      "{}, kind = {}, other = {}",
      input,
      kind.description(),
      other
    ))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cmd::Command;

  #[test]
  fn test_get() {
    let raw_cmd = "$3\r\nGET\r\n$3\r\naaa\r\n";
    assert_eq!(parse(raw_cmd).unwrap(), Command::Get("aaa"));
  }

  #[test]
  fn test_ping() {
    let raw_cmd = "PING\r\n";
    assert_eq!(parse(raw_cmd).unwrap(), Command::Ping);
  }

  #[test]
  fn test_set() {
    let raw_cmd = "$3\r\nSET\r\n$3\r\naaa\r\n$3\r\naaa\r\n";
    assert_eq!(
      parse(raw_cmd).unwrap(),
      Command::Set("aaa", "aaa".as_bytes())
    );
  }

  #[test]
  fn test_lpush() {
    let raw_cmd =
      "$5\r\nLPUSH\r\n$3\r\naaa\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n";
    assert_eq!(
      parse(raw_cmd).unwrap(),
      Command::Lpush(
        "aaa",
        vec!["1", "2", "3", "4", "5"]
          .iter()
          .map(|v| v.as_bytes())
          .collect(),
      )
    );
  }

  #[test]
  fn test_rpush() {
    let raw_cmd =
      "$5\r\nRPUSH\r\n$3\r\naaa\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n";
    assert_eq!(
      parse(raw_cmd).unwrap(),
      Command::Rpush(
        "aaa",
        vec!["1", "2", "3", "4", "5"]
          .iter()
          .map(|v| v.as_bytes())
          .collect(),
      )
    );
  }

  #[test]
  fn test_lpushx() {
    let raw_cmd =
      "$6\r\nLPUSHX\r\n$3\r\naaa\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n";
    assert_eq!(
      parse(raw_cmd).unwrap(),
      Command::LpushX(
        "aaa",
        vec!["1", "2", "3", "4", "5"]
          .iter()
          .map(|v| v.as_bytes())
          .collect(),
      )
    );
  }

  #[test]
  fn test_rpushx() {
    let raw_cmd =
      "$6\r\nRPUSHX\r\n$3\r\naaa\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n";
    assert_eq!(
      parse(raw_cmd).unwrap(),
      Command::RpushX(
        "aaa",
        vec!["1", "2", "3", "4", "5"]
          .iter()
          .map(|v| v.as_bytes())
          .collect(),
      )
    );
  }

  #[test]
  fn test_lpop() {
    let raw_cmd = "$4\r\nLPOP\r\n$2\r\naa\r\n$1\r\n2\r\n";
    assert_eq!(parse(raw_cmd).unwrap(), Command::Lpop("aa", 2));
  }

  #[test]
  fn test_rpop() {
    let raw_cmd = "$4\r\nRPOP\r\n$2\r\naa\r\n$1\r\n2\r\n";
    assert_eq!(parse(raw_cmd).unwrap(), Command::Rpop("aa", 2));
  }

  #[test]
  fn test_del() {
    let raw_cmd = "$3\r\nDEL\r\n$3\r\naaa\r\n$3\r\nbbb\r\n$3\r\nccc\r\n";
    assert_eq!(
      parse(raw_cmd).unwrap(),
      Command::Del(vec!["aaa", "bbb", "ccc"])
    );
  }
}
