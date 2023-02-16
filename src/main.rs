#![feature(let_chains)]
#![allow(unused)]

mod cmd;
mod err;
mod journal;
mod redis;
mod value;

use cmd::parser::parse;

use log::warn;
use redis::Redis;
use std::borrow::Cow;

use value::RedisValue;

use std::env;
use std::error::Error;
use std::marker::Send;
use std::sync::Arc;

use journal::{Disabled, Simple, Writer};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::io::{AsyncRead, BufReader};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};

use simple_logger::SimpleLogger;

use err::RedisError;

use clap::Parser;

#[derive(clap::Parser)]
struct Cli {
  /// addr to listen on
  addr: Option<String>,
  /// Persistence level
  journal: Option<String>,
  /// The path to the file to read
  journal_path: Option<std::path::PathBuf>,
}

async fn start_with_no_journal(listener: &TcpListener) -> Result<(), Box<dyn Error>> {
  let redis = Arc::new(Redis::new(Disabled {}).await);
  start(redis, listener).await
}

async fn start<W: Writer + Send + Sync + 'static>(
  redis: Arc<Redis<W>>,
  listener: &TcpListener,
) -> Result<(), Box<dyn Error>> {
  loop {
    let (mut socket, _) = listener.accept().await.unwrap();
    let r = redis.clone();
    tokio::spawn(async move { Session::new(&mut socket, r).run().await });
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  SimpleLogger::new().init()?;

  let args = Cli::parse();

  let addr = args.addr.unwrap_or_else(|| "0.0.0.0:6380".to_string());

  let listener = TcpListener::bind(&addr).await?;
  let journal = args.journal.unwrap_or("disabled".to_string());

  if journal == "disabled" {
    start_with_no_journal(&listener).await
  } else {
    todo!()
    // start_with_no_simple_journaling(&listener)
  }
}

struct Session<'a, W: Writer> {
  write: WriteHalf<'a>,
  read: BufReader<ReadHalf<'a>>,
  redis: Arc<Redis<W>>,
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

impl<'a, W: Writer + Send> Session<'a, W> {
  pub fn new(socket: &'a mut TcpStream, redis: Arc<Redis<W>>) -> Session<'a, W> {
    let (read_half, write) = socket.split();
    let read = BufReader::new(read_half);

    Session { read, write, redis }
  }

  async fn handle_cmd(&mut self) -> Result<RedisValue, RedisError> {
    let cmd = read_cmd(&mut self.read).await?;

    if cmd.trim().is_empty() {
      return Ok(RedisValue::Nothing);
    }

    let command = parse(cmd.as_str())?;

    self.redis.exec(&command).await
    // do something else with the command?
  }

  pub async fn run(&mut self) {
    loop {
      let output = self.handle_cmd().await;

      let raw_output: Cow<'static, str> = match output {
        Ok(RedisValue::Ok) => "+OK\r\n".into(),
        Ok(RedisValue::EmptyString) => "$-1\r\n".into(),
        Ok(RedisValue::SimpleString(elem)) => {
          let mut s = String::new();
          s.push_str(format!("${}\r\n", elem.len()).as_str());
          for ch in &*elem {
            s.push(*ch as char);
          }
          s.push_str("\r\n");
          s.into()
        }
        Ok(RedisValue::Nothing) => "\0".into(), // to close connection if it's
        Ok(RedisValue::Array(v)) if v.is_empty() => "*-1\r\n".into(),
        Ok(RedisValue::Array(v)) => {
          let mut s = String::new();
          s.push_str(format!("*{}\r\n", v.len()).as_str());
          v.iter().for_each(|elem| {
            s.push_str(format!("${}\r\n", elem.len()).as_str());
            s.push_str(elem);
            s.push_str("\r\n");
          });
          s.into()
        }
        Ok(RedisValue::Integer(v)) => format!(":{v}\r\n").into(),
        Ok(RedisValue::BulkString(_)) => "$-1\r\n".into(),
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
