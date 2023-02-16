use crate::cmd::Command;

use async_trait::async_trait;
use log::info;
use tokio::{fs::File, sync::Mutex, io::AsyncWriteExt};

#[async_trait]
pub trait Writer {
  async fn write<'a>(&self, cmd: &'a Command<'a>);
}

pub struct Journal {
  writer: dyn Writer,
}

pub struct Simple {
  file: Mutex<File>
}

impl Simple {
  pub fn make(file: Mutex<File>) -> Simple {
    Simple { file }
  }
}

#[async_trait]
impl Writer for Simple {
  async fn write<'cmd>(&self, cmd: &'cmd Command<'cmd>) {
    let mut f = self.file.lock().await;
    f.write_all(format!("attempt to simple log a command: {:?}\r\n", cmd).as_bytes()).await;
  }
}

pub struct Disabled;

#[async_trait]
impl Writer for Disabled {
  async fn write<'a>(&self, cmd: &'a Command<'a>) {
    info!("attempt to log a command: {:?}", cmd);
  }
}
