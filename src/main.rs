#![feature(let_chains)]

mod cmd;
mod redis;

use cmd::{CmdError, Command};

use log::info;
use redis::Redis;

use std::env;
use std::error::Error;
use std::sync::Arc;

use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};

use simple_logger::SimpleLogger;

use crate::redis::RedisError;

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

impl<'a> Session<'a> {
    pub fn new(socket: &'a mut TcpStream, redis: Arc<Redis>) -> Session<'a> {
        let (read, write) = socket.split();

        Session {
            read: BufReader::new(read),
            write,
            redis,
        }
    }

    async fn handle_cmd(&mut self) -> Result<String, Box<dyn Error>> {
        let mut r = String::new();
        self.read.read_line(&mut r).await?;

        let raw_command = r.trim().to_string();
        info!("raw command=`{}`", raw_command);
        let command = cmd::parse_command(raw_command);

        match command {
            Ok(Command::Ping) => Ok(String::from("PONG")),
            Ok(Command::Get(key)) => match self.redis.get(&key).await {
                Ok(Option::None) => Ok(String::from("NONE")),
                Ok(Option::Some(v)) => Ok(String::from_utf8(v)?),
                Err(e @ RedisError::TypeError) => Ok(format!("{}", e)),
            },
            Ok(Command::TtlCount) => Ok(format!("{}", self.redis.ttl_keys().await)),
            Ok(Command::Set(key, value)) => {
                info!("set: {}", key);
                self.redis.set(key, value).await;
                Ok("+OK".to_string())
            }
            Ok(Command::SetEx(key, value, ttl)) => {
                info!("setex: {}, {}", key, ttl);
                self.redis.setex(key, value, ttl).await;

                Ok("+OK".to_string())
            }
            Ok(Command::Lpush(key, values, may_create)) => {
                info!("lpush: {}, {:?}, {}", key, values, may_create);
                let len = self.redis.lpush(key, values, may_create).await?;

                Ok(format!("(integer) {}", len))
            }
            Ok(Command::Rpush(key, values, may_create)) => {
                info!("rpush: {}, {:?}, {}", key, values, may_create);
                let len = self.redis.rpush(key, values, may_create).await?;

                Ok(format!("(integer) {}", len))
            }
            Ok(Command::Lpop(key)) => {
                info!("lpop: {}!", key);
                match self.redis.lpop(&key, 1).await {
                    Ok(v) if v.is_empty() => Ok(String::from("(nil)")),
                    Ok(v) => Ok(String::from_utf8(v[0].to_vec())?),
                    Err(e @ RedisError::TypeError) => Ok(format!("{}", e)),
                }
            }
            Err(CmdError::ParseError(message)) => Ok(format!("-ERR {}", message)),
        }
    }

    pub async fn run(&mut self) {
        loop {
            let output = self.handle_cmd().await.unwrap();

            self.write.write_all(output.as_bytes()).await.unwrap();
            self.write.write_u8('\n' as u8).await.unwrap();
        }
    }
}
