#![feature(let_chains)]

mod cmd;
mod redis;
mod err;

use cmd::Command;

use log::info;
use redis::Redis;

use std::env;
use std::error::Error;
use std::fmt::format;
use std::os::macos::raw;
use std::sync::Arc;

use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
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
        let (socket, _) = listener.accept().await.unwrap();
        start_session(socket, redis.clone()).await;
    }
}

async fn start_session(mut socket: TcpStream, redis: Arc<Redis>) {
    tokio::spawn(async move {
        Session::new(&mut socket, redis).run().await;
    });
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
    Array(Vec<String>)
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

impl<'a> Session<'a> {
    pub fn new(socket: &'a mut TcpStream, redis: Arc<Redis>) -> Session<'a> {
        let (read, write) = socket.split();

        Session {
            read: BufReader::new(read),
            write,
            redis,
        }
    }

    async fn read_cmd(&mut self) -> Result<String, RedisError> {
        let mut r = String::new();
        self.read.read_line(&mut r).await?;

        let mut cmd = String::new();
        if r.chars().nth(0) == Some('*') {
            let rest = &r.trim()[1..];
            let mut cmd_parts_count = rest.parse::<usize>()?;

            r.clear();
            while cmd_parts_count > 0 {
                self.read.read_line(&mut r).await?;
                if r.chars().nth(0) == Some('$') {
                    r.clear();
                    continue;
                }
                cmd_parts_count -= 1;
                cmd.push_str(r.trim());
                cmd.push(' ');
                r.clear();
            }
            cmd = cmd.trim().to_string();
        } else {
            cmd = r.trim().to_string();
        }

        Ok(cmd)
    }

    async fn handle_cmd(&mut self) -> Result<OpResult, RedisError> {
        let cmd = self.read_cmd().await?;

        info!("raw command=`{:?}`", &cmd);
        let command = cmd::parse_command(cmd.as_str())?;

        match command {
            Command::Ping => Ok(OpResult::from("PONG")),
            Command::CommandDocs => Ok(OpResult::BulkString(Vec::new())),
            Command::Get(key) => match self.redis.get(&key).await? {
                Option::None => Ok(OpResult::EmptyString),
                Option::Some(v) => {
                    let s = String::from_utf8(v).unwrap();
                    Ok(OpResult::SimpleString(s))
                },
            },
            Command::TtlCount => Ok(OpResult::from(self.redis.ttl_keys().await)),
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
            Command::Lpush(key, values, may_create) => {
                info!("lpush: {}, {:?}, {}", key, values, may_create);
                let len = self.redis.lpush(key, values, may_create).await?;

                Ok(OpResult::from(len))
            }
            Command::Rpush(key, values, may_create) => {
                info!("rpush: {}, {:?}, {}", key, values, may_create);
                let len = self.redis.rpush(key, values, may_create).await?;

                Ok(OpResult::from(len))
            }
            Command::Lpop(key) => {
                info!("lpop: {}!", key);
                match self.redis.lpop(&key, 1).await? {
                    v => Ok( OpResult::Array( v.iter().map(|v| String::from_utf8(v.to_vec()).unwrap()).collect::<Vec<_>>() ) ),
                }
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
                },
                Ok(OpResult::Integer(v)) => format!(":{}\r\n", v),
                Ok(OpResult::BulkString(s)) => "$-1\r\n".to_string(),
                Err(e @ RedisError::TypeError) => format!("-WRONGTYPE {}\r\n", e),
                Err(e) => format!("-ERR {}\r\n", e),
            };

            self.write.write_all(raw_output.as_bytes()).await.unwrap();
        }
    }
}
