mod cmd;
mod redis;

use log::info;
use redis::Redis;
use std::env;
use std::error::Error;
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};

use simple_logger::SimpleLogger;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new().init()?;

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:8082".to_string());

    let listener = TcpListener::bind(&addr).await?;
    let redis = Arc::new(Redis::new().await);

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        start(socket, redis.clone()).await;
    }
}

async fn start(mut socket: TcpStream, redis: Arc<Redis>) {
    tokio::spawn(async move {
        let r = redis.clone();
        Session::new(&mut socket, r).start_session().await;
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

        let command = cmd::parse_command(r.trim().to_string());

        match command {
            Ok(cmd::Command::Ping) => Ok(String::from("PONG")),
            Ok(cmd::Command::Get(key)) => {
                let entry = self.redis.get(&key).await;
                match entry {
                    Option::None => Ok(String::from("NONE")),
                    Option::Some(v) => Ok(String::from_utf8(v)?),
                }
            }
            Ok(cmd::Command::Set(key, value)) => {
                info!("set: {}", key);
                let _ = self.redis.set(key, value).await;
                Ok("+OK".to_string())
            }
            Ok(cmd::Command::SetEx(key, value, ttl)) => {
                info!("setex: {}, {}", key, ttl);
                self.redis.setex(key, value, ttl).await;

                Ok("+OK".to_string())
            }
            Err(cmd::CmdError::ParseError(message)) => Ok(format!("-ERR {}", message)),
        }
    }

    pub async fn start_session(&mut self) {
        loop {
            let output = self.handle_cmd().await.unwrap();

            self.write.write_all(output.as_bytes()).await.unwrap();
            self.write.write_u8('\n' as u8).await.unwrap();
        }
    }
}
