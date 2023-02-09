#![feature(let_chains)]

mod cmd;
mod err;
mod redis;

use cmd::parser::parse;
use cmd::Command;

use std::borrow::Cow;
use log::{info, warn};
use redis::Redis;

use std::env;
use std::error::Error;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::io::{AsyncRead, BufReader};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};

use simple_logger::SimpleLogger;

use err::RedisError;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  SimpleLogger::new().init()?;

  let addr = env::args()
    .nth(1)
    .unwrap_or_else(|| "0.0.0.0:6380".to_string());

  let listener = TcpListener::bind(&addr).await?;
  let redis = Arc::new(Redis::new().await);

  loop {
    let (mut socket, _) = listener.accept().await.unwrap();
    let r = redis.clone();
    tokio::spawn(async move {
      Session::new(&mut socket, r).run().await;
    });
  }
}

struct Session<'a> {
  write: WriteHalf<'a>,
  read: BufReader<ReadHalf<'a>>,
  redis: Arc<Redis>,
}

#[derive(Debug)]
enum OpResult {
  Ok,
  Nothing,
  Integer(u64),
  EmptyString,
  SimpleString(String),
  BulkString(Vec<String>),
  Array(Vec<String>),
}

impl From<&'static str> for OpResult {
  fn from(value: &'static str) -> Self {
    OpResult::SimpleString(value.to_string())
  }
}

impl From<usize> for OpResult {
  fn from(value: usize) -> Self {
    OpResult::Integer(value as u64)
  }
}

impl From<Vec<Vec<u8>>> for OpResult {
  fn from(value: Vec<Vec<u8>>) -> Self {
    OpResult::Array(
      value
        .iter()
        .map(|v| String::from_utf8(v.to_vec()).unwrap())
        .collect::<Vec<_>>(),
    )
  }
}

async fn read_cmd<'a, T: AsyncRead + Unpin>(read: &mut BufReader<T>) -> Result<String, RedisError> {
  let mut r = String::new();
  read.read_line(&mut r).await?;

  let mut cmd = String::new();
  if r.chars().nth(0) == Some('*') {
    let rest = &r.trim()[1..];
    let mut cmd_parts_count = rest.parse::<usize>()?;

    r.clear();
    while cmd_parts_count > 0 {
      read.read_line(&mut r).await?;
      if r.chars().nth(0) != Some('$') {
        cmd_parts_count -= 1;
      }
      cmd.push_str(r.as_str());
      r.clear();
    }
    cmd = cmd.to_string();
  } else {
    cmd = r.to_string();
  }
  Ok(cmd)
}

impl<'a> Session<'a> {
  pub fn new(socket: &'a mut TcpStream, redis: Arc<Redis>) -> Session<'a> {
    let (read_half, write) = socket.split();
    let read = BufReader::new(read_half);

    Session { read, write, redis }
  }

  async fn handle_cmd(&mut self) -> Result<OpResult, RedisError> {
    let cmd = read_cmd(&mut self.read).await?;

    if cmd.trim().is_empty() {
      return Ok(OpResult::Nothing);
    }

    let command = parse(cmd.as_str())?;

    match command {
      Command::Ping => Ok(OpResult::from("PONG")),
      Command::CommandDocs => Ok(OpResult::BulkString(Vec::new())),
      Command::Get(key) => match self.redis.get(key).await? {
        Option::None => Ok(OpResult::EmptyString),
        Option::Some(v) => {
          let s = String::from_utf8(v.to_vec()).unwrap();
          Ok(OpResult::SimpleString(s))
        }
      },
      Command::Set(key, value) => {
        info!("set: {}", key);
        self.redis.set(key, value).await;

        Ok(OpResult::Ok)
      }
      Command::SetEx(key, value, ttl) => {
        info!("setex: {}, {}", key, ttl);
        self.redis.setex(key, value, ttl).await;

        Ok(OpResult::Ok)
      }
      Command::Lpush(key, values) => {
        info!("lpush: {}, {:?}", key, values);
        let len = self.redis.push(key, values, true, true).await?;

        Ok(OpResult::from(len))
      }
      Command::Rpush(key, values) => {
        info!("rpush: {}, {:?}", key, values);
        let len = self.redis.push(key, values, true, false).await?;

        Ok(OpResult::from(len))
      }
      Command::LpushX(key, values) => {
        info!("lpushx: {}, {:?}", key, values);
        let len = self.redis.push(key, values, false, true).await?;

        Ok(OpResult::from(len))
      }
      Command::RpushX(key, values) => {
        info!("rpushx: {}, {:?}", key, values);
        let len = self.redis.push(key, values, false, false).await?;

        Ok(OpResult::from(len))
      }
      Command::Lpop(key, times) => {
        info!("lpop: {}!", key);
        let v = self.redis.pop(key, times, true).await?;
        Ok(OpResult::from(v))
      }
      Command::Rpop(key, times) => {
        info!("rpop: {}!", key);
        let v = self.redis.pop(key, times, false).await?;
        Ok(OpResult::from(v))
      }
      Command::Del(keys) => {
        info!("delete {:?}!", keys);
        let del_keys_count: usize = self.redis.delete(&keys).await;

        Ok(OpResult::Integer(del_keys_count as u64))
      }
      Command::Incr(key) => {
        info!("incr! {:?}", key);
        let v: u64 = self.redis.incr(key).await?;

        Ok(OpResult::Integer(v))
      }
      Command::DbSize => Ok(OpResult::Integer(self.redis.keys_count().await as u64))
    }
  }

  pub async fn run(&mut self) {
    loop {
      let output = self.handle_cmd().await;

      let raw_output: Cow<'static, str> = match output {
        Ok(OpResult::Ok) => "+OK\r\n".into(),
        Ok(OpResult::EmptyString) => "$-1\r\n".into(),
        Ok(OpResult::SimpleString(elem)) => {
          let mut s = String::new();
          s.push_str(format!("${}\r\n", elem.len()).as_str());
          s.push_str(&elem);
          s.push_str("\r\n");
          s.into()
        }
        Ok(OpResult::Nothing) => "\0".into(), // to close connection if it's
        Ok(OpResult::Array(v)) if v.is_empty() => "*-1\r\n".into(),
        Ok(OpResult::Array(v)) => {
          let mut s = String::new();
          s.push_str(format!("*{}\r\n", v.len()).as_str());
          v.iter().for_each(|elem| {
            s.push_str(format!("${}\r\n", elem.len()).as_str());
            s.push_str(elem);
            s.push_str("\r\n");
          });
          s.into()
        }
        Ok(OpResult::Integer(v)) => format!(":{v}\r\n").into(),
        Ok(OpResult::BulkString(_)) => "$-1\r\n".into(),
        Err(e @ RedisError::Type) => format!("-WRONGTYPE {e}\r\n").into(),
        Err(RedisError::Parse(msg)) => {
          warn!("parse error: {msg}");
          format!("-ERR {msg}\r\n").into()
        }
        Err(e) => format!("-ERR {e}\r\n").into(),
      };

      self
        .write
        .write_all(raw_output.as_bytes())
        .await
        .expect("can't write response");
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::read_cmd;
  use tokio::io::BufReader;

  #[tokio::test]
  async fn test_read_cmd() {
    let test_input =
      b"*7\r\n$5\r\nLPUSH\r\n$3\r\naaa\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"
        as &[u8];
    let mut buf = BufReader::new(test_input);

    assert_eq!(
      read_cmd(&mut buf).await.unwrap(),
      "$5\r\nLPUSH\r\n$3\r\naaa\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$1\r\n4\r\n$1\r\n5\r\n"
    )
  }
}
