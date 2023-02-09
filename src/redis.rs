use crate::err::RedisError;
use log::info;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, LinkedList};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

static INITIAL_CAPACITY: usize = 256;

enum Value {
  Raw(Vec<u8>),
  Integer(u64),
  List(LinkedList<Vec<u8>>),
}

struct SharedData {
  dict: HashMap<String, Value>,
  ttl_heap: BinaryHeap<Reverse<(u64, String)>>,
}

pub struct Redis {
  shared_data: Arc<RwLock<SharedData>>,
}

impl Redis {
  pub async fn new() -> Redis {
    let shared_data = RwLock::new(SharedData {
      dict: HashMap::with_capacity(INITIAL_CAPACITY),
      ttl_heap: BinaryHeap::new(),
    });
    let arc = Arc::new(shared_data);
    spawn_ttl_heap_cleaner(arc.clone()).await;

    Redis { shared_data: arc }
  }

  pub async fn set(&self, key: &str, value: &[u8]) {
    self
      .shared_data
      .write()
      .await
      .dict
      .insert(key.to_string(), Value::Raw(value.to_vec()));
  }

  pub async fn setex(&self, key: &str, value: &[u8], ttl: usize) {
    let s_data = &mut self.shared_data.write().await;

    s_data
      .dict
      .insert(key.to_string(), Value::Raw(value.to_vec()));

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

  pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, RedisError> {
    let read_from = self.shared_data.read().await;
    let value_opt = read_from.dict.get(key);

    match value_opt {
      Some(Value::Raw(data)) => Ok(Some(data.to_vec())),
      Some(Value::Integer(i)) => Ok(Some(i.to_string().as_str().as_bytes().to_vec())),
      Some(_) => Result::Err(RedisError::Type),
      None => Ok(None),
    }
  }

  pub async fn push(
    &self,
    key: &str,
    values: Vec<Vec<u8>>,
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

  pub async fn pop(
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

  pub async fn delete(&self, keys: &[&str]) -> usize {
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

  pub async fn keys_count(&self) -> usize {
    let read_handle = self.shared_data.read().await;
    read_handle.dict.len()
  }

  pub async fn incr(&self, key: &str) -> Result<u64, RedisError> {
    let mut write_handle = self.shared_data.write().await;
    match write_handle.dict.get(key) {
      Some(Value::Integer(v)) => {
        let new_value = v + 1;
        write_handle
          .dict
          .insert(key.to_owned(), Value::Integer(new_value));
        Ok(new_value)
      }
      None => {
        write_handle.dict.insert(key.to_owned(), Value::Integer(1));
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

mod tests {
  #[tokio::test]
  async fn test_redis_set() {
    let redis = super::Redis::new().await;
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
      let value = format!("value_{}", i).as_bytes().to_vec();

      assert_eq!(redis.get(&key).await.unwrap(), Some(value));
    }
  }
}
