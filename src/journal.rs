use crate::cmd::Command;

use async_trait::async_trait;
use log::info;
use std::fs::File;

#[async_trait]
pub trait Writer {
  async fn write<'a>(&self, cmd: &'a Command<'a>);
}

pub struct Journal {
  writer: dyn Writer,
}

pub struct Simple {}

#[async_trait]
impl Writer for Simple {
  async fn write<'a>(&self, cmd: &'a Command<'a>) {
    info!("attempt to simple log a command: {:?}", cmd);
  }
}

pub struct Disabled;

#[async_trait]
impl Writer for Disabled {
  async fn write<'a>(&self, cmd: &'a Command<'a>) {
    info!("attempt to log a command: {:?}", cmd);
  }
}

unsafe impl Send for Disabled {}

unsafe impl Send for Simple {}
