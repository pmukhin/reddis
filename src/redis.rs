use crate::cmd::Command;
use crate::err::RedisError;
use crate::journal::{Journal, Writer};
use crate::value::RedisValue;

use log::info;
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, LinkedList};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

static INITIAL_CAPACITY: usize = 256;

enum Value {
  Raw(Arc<Vec<u8>>),
  List(LinkedList<Vec<u8>>),
}

struct SharedData {
  dict: HashMap<String, Value>,
  ttl_heap: BinaryHeap<Reverse<(u64, String)>>,
}

pub struct Redis<W: Writer> {
  shared_data: Arc<RwLock<SharedData>>,
  journal: W,
}

impl<W: Writer + Send> Redis<W> {
  pub async fn new(writer: W) -> Redis<W> {
    let shared_data = RwLock::new(SharedData {
      dict: HashMap::with_capacity(INITIAL_CAPACITY),
      ttl_heap: BinaryHeap::new(),
    });
    let arc = Arc::new(shared_data);
    spawn_ttl_heap_cleaner(arc.clone()).await;

    Redis {
      shared_data: arc,
      journal: writer,
    }
  }

  pub async fn exec<'a>(&self, cmd: &'a Command<'a>) -> Result<RedisValue, RedisError> {
    match cmd {
      Command::Set(key, value) => {
        self.set(key, value).await;
        Ok(RedisValue::Ok)
      }
      Command::Get(key) => match self.get(key).await? {
        Option::None => Ok(RedisValue::EmptyString),
        Option::Some(v) => Ok(RedisValue::SimpleString(v)),
      },
      c @ Command::SetEx(key, value, ttl) => {
        self.journal.write(c).await;
        self.setex(key, value, *ttl).await;
        Ok(RedisValue::Ok)
      }
      Command::Ping => Ok(RedisValue::from("PONG")),
      Command::CommandDocs => Ok(RedisValue::BulkString(Vec::new())),
      Command::DbSize => Ok(RedisValue::Integer(self.keys_count().await as i64)),
      Command::Config => Ok(RedisValue::BulkString(Vec::new())),
      c @ Command::Lpush(key, value) => {
        self.journal.write(c).await;
        self.push(key, value, true, true).await?;
        Ok(RedisValue::Ok)
      }
      c @ Command::Rpush(key, value) => {
        self.journal.write(c).await;
        self.push(key, value, true, false).await?;
        Ok(RedisValue::Ok)
      }
      c @ Command::LpushX(key, value) => {
        self.journal.write(c).await;
        self.push(key, value, false, true).await?;
        Ok(RedisValue::Ok)
      }
      c @ Command::RpushX(key, value) => {
        self.journal.write(c).await;
        self.push(key, value, false, false).await?;
        Ok(RedisValue::Ok)
      }
      c @ Command::Lpop(key, times) => {
        info!("lpop: {}!", key);
        self.journal.write(c).await;
        let v = self.pop(key, *times, true).await?;
        Ok(RedisValue::from(v))
      }
      c @ Command::Rpop(key, times) => {
        info!("rpop: {}!", key);
        self.journal.write(c).await;
        let v = self.pop(key, *times, false).await?;
        Ok(RedisValue::from(v))
      }
      c @ Command::Del(keys) => {
        info!("delete {:?}!", keys);
        self.journal.write(c).await;
        let del_keys_count: usize = self.delete(&keys).await;

        Ok(RedisValue::Integer(del_keys_count as i64))
      }
      c @ Command::Incr(key) => {
        self.journal.write(c).await;
        Ok(RedisValue::Integer(self.incr(key).await?))
      }
    }
  }

  async fn set(&self, key: &str, value: &[u8]) {
    self
      .shared_data
      .write()
      .await
      .dict
      .insert(key.to_string(), Value::Raw(Arc::new(value.to_vec())));
  }

  async fn setex(&self, key: &str, value: &[u8], ttl: usize) {
    let s_data = &mut self.shared_data.write().await;

    s_data
      .dict
      .insert(key.to_string(), Value::Raw(Arc::new(value.to_vec())));

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let ttl_value = now.add(Duration::from_secs(ttl as u64));

    s_data
      .ttl_heap
      .push(Reverse((ttl_value.as_secs(), key.to_string())));

    info!(
      "pushed 1 elem into ttl_heap, ttl_heap_len={}",
      s_data.ttl_heap.len()
    );
  }

  async fn get(&self, key: &str) -> Result<Option<Arc<Vec<u8>>>, RedisError> {
    let read_from = self.shared_data.read().await;
    let value_opt = read_from.dict.get(key);

    match value_opt {
      Some(Value::Raw(data)) => Ok(Some(Arc::clone(data))),
      Some(_) => Result::Err(RedisError::Type),
      None => Ok(None),
    }
  }

  async fn push(
    &self,
    key: &str,
    values: &Vec<&[u8]>,
    allow_creation: bool,
    front: bool,
  ) -> Result<usize, RedisError> {
    let mut write_from = self.shared_data.write().await;

    match write_from.dict.get_mut(key) {
      Some(&mut Value::List(ref mut ll)) => {
        values.iter().for_each(|v| ll.push_front(v.to_vec()));
        Ok(ll.len())
      }
      Some(_) => Result::Err(RedisError::Type),
      None if !allow_creation => Ok(values.len()),
      None => {
        let mut ll = LinkedList::new();

        values.iter().for_each(|v| {
          if front {
            ll.push_front(v.to_vec());
          } else {
            ll.push_back(v.to_vec());
          }
        });
        write_from.dict.insert(key.to_string(), Value::List(ll));

        Ok(values.len())
      }
    }
  }

  async fn pop(
    &self,
    key: &str,
    mut times: usize,
    front: bool,
  ) -> Result<Vec<Vec<u8>>, RedisError> {
    match self.shared_data.write().await.dict.get_mut(key) {
      None => Ok(Vec::new()),
      Some(&mut Value::List(ref mut ll)) => {
        let mut r = Vec::new();
        while times > 0 && let Some(v) = if front { ll.pop_front() } else { ll.pop_back() } {
                    times -= 1;
                    r.push(v);
                }
        Ok(r)
      }
      Some(_) => Result::Err(RedisError::Type),
    }
  }

  async fn delete(&self, keys: &[&str]) -> usize {
    let mut write_handle = self.shared_data.write().await;
    let mut count = 0;
    for key in keys {
      if write_handle.dict.contains_key(*key) {
        count += 1;
        write_handle.dict.remove(*key);
      }
    }
    count
  }

  async fn keys_count(&self) -> usize {
    let read_handle = self.shared_data.read().await;
    read_handle.dict.len()
  }

  async fn incr(&self, key: &str) -> Result<i64, RedisError> {
    let mut write_handle = self.shared_data.write().await;
    match write_handle.dict.get(key) {
      Some(Value::Raw(v)) => {
        let v: Result<i64, RedisError> = match String::from_utf8_lossy(v) {
          Cow::Borrowed(v) => v.parse::<i64>().or(Err(RedisError::Type)),
          _ => Err(RedisError::Type),
        };

        let new_value = v? + 1;
        write_handle.dict.insert(
          key.to_owned(),
          Value::Raw(Arc::new(new_value.to_string().as_bytes().to_vec())),
        );
        Ok(new_value)
      }
      None => {
        write_handle.dict.insert(
          key.to_owned(),
          Value::Raw(Arc::new("1".as_bytes().to_vec())),
        );
        Ok(1)
      }
      Some(_) => Result::Err(RedisError::Type),
    }
  }
}

async fn spawn_ttl_heap_cleaner(shared_data: Arc<RwLock<SharedData>>) {
  tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
      interval.tick().await;

      let s_data = &mut shared_data.write().await;

      if s_data.ttl_heap.is_empty() {
        continue;
      };

      let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

      while let Some(Reverse((w, key))) = s_data.ttl_heap.pop() {
        if w >= now {
          s_data.ttl_heap.push(Reverse((w, key)));
          break;
        }
        info!("deleting stale key={}", key);

        s_data.dict.remove(&key).unwrap();
      }
    }
  });
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use crate::journal::Disabled;

  #[tokio::test]
  async fn test_redis_set() {
    let redis = super::Redis::new(Disabled {}).await;
    for i in 0..100 {
      redis
        .set(
          format!("key_{}", i).as_str(),
          format!("value_{}", i).as_bytes(),
        )
        .await
    }
    for i in 0..100 {
      let key = format!("key_{}", i);
      let value = Arc::new(format!("value_{}", i).as_bytes().to_vec());

      assert_eq!(redis.get(&key).await.unwrap(), Some(value));
    }
  }
}
