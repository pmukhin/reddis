#![feature(let_chains)]

mod cmd;
mod err;
mod redis;

use cmd::parser::parse;
use cmd::Command;

use log::info;
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
  Integer(usize),
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
    OpResult::Integer(value)
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
    let (read, write) = socket.split();

    Session {
      read: BufReader::new(read),
      write,
      redis,
    }
  }

  async fn handle_cmd(&mut self) -> Result<OpResult, RedisError> {
    let cmd = read_cmd(&mut self.read).await?;

    info!("raw command=`{:?}`", &cmd);
    let command = parse(cmd.as_str())?;

    match command {
      Command::Ping => Ok(OpResult::from("PONG")),
      Command::CommandDocs => Ok(OpResult::BulkString(Vec::new())),
      Command::Get(key) => match self.redis.get(&key).await? {
        Option::None => Ok(OpResult::EmptyString),
        Option::Some(v) => {
          let s = String::from_utf8(v).unwrap();
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
        let v = self.redis.pop(&key, times, true).await?;
        Ok(OpResult::from(v))
      }
      Command::Rpop(key, times) => {
        info!("rpop: {}!", key);
        let v = self.redis.pop(&key, times, false).await?;
        Ok(OpResult::from(v))
      }
      Command::Del(keys) => {
        info!("delete {:?}!", keys);
        let del_keys_count: usize = self.redis.delete(&keys).await;

        Ok(OpResult::Integer(del_keys_count))
      }
    }
  }

  pub async fn run(&mut self) {
    loop {
      let output = self.handle_cmd().await;

      info!("responding with {:?}", output);

      let raw_output: String = match output {
        Ok(OpResult::Ok) => "+OK\r\n".to_string(),
        Ok(OpResult::EmptyString) => "$-1\r\n".to_string(),
        Ok(OpResult::SimpleString(elem)) => {
          let mut s = String::new();
          s.push_str(format!("${}\r\n", elem.len()).as_str());
          s.push_str(&elem);
          s.push_str("\r\n");
          s
        }
        Ok(OpResult::Array(v)) if v.is_empty() => "*-1\r\n".to_string(),
        Ok(OpResult::Array(v)) => {
          let mut s = String::new();
          s.push_str(format!("*{}\r\n", v.len()).as_str());
          v.iter().for_each(|elem| {
            s.push_str(format!("${}\r\n", elem.len()).as_str());
            s.push_str(elem);
            s.push_str("\r\n");
          });
          s
        }
        Ok(OpResult::Integer(v)) => format!(":{v}\r\n"),
        Ok(OpResult::BulkString(_)) => "$-1\r\n".to_string(),
        Err(e @ RedisError::Type) => format!("-WRONGTYPE {e}\r\n"),
        Err(e) => format!("-ERR {e}\r\n"),
      };

      self
        .write
        .write_all(raw_output.as_bytes())
        .await
        .expect("can't write response");
    }
  }
}

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
