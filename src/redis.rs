use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, LinkedList};
use std::ops::Add;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::err::RedisError;
use log::info;

static INITIAL_CAPACITY: usize = 256;

enum Value {
    Raw(Vec<u8>),
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

    pub async fn ttl_keys(&self) -> usize {
        self.shared_data.read().unwrap().ttl_heap.len()
    }

    pub async fn set(&self, key: String, value: Vec<u8>) {
        self.shared_data
            .try_write()
            .expect("Unable to lock mutex")
            .dict
            .insert(key, Value::Raw(value));
    }

    pub async fn setex(&self, key: String, value: Vec<u8>, ttl: u64) {
        let s_data = &mut self.shared_data.write().unwrap();

        s_data.dict.insert(key.clone(), Value::Raw(value));

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let ttl_value = now.add(Duration::from_secs(ttl));

        s_data
            .ttl_heap
            .push(Reverse((ttl_value.as_secs(), key.to_string())));

        info!(
            "pushed 1 elem into ttl_heap, ttl_heap_len={}",
            s_data.ttl_heap.len()
        );
    }

    pub async fn get(&self, key: &String) -> Result<Option<Vec<u8>>, RedisError> {
        let read_from = self.shared_data.read().expect("Unable to lock mutex");

        let value_opt = read_from.dict.get(key);

        match value_opt {
            Some(Value::Raw(data)) => Ok(Some(data.to_vec())),
            Some(_) => Result::Err(RedisError::TypeError),
            None => Ok(None),
        }
    }

    async fn do_push(
        &self,
        key: String,
        values: Vec<Vec<u8>>,
        allow_creation: bool,
        push_f: bool,
    ) -> Result<usize, RedisError> {
        let mut write_from = self.shared_data.write().expect("mutex is poisoned");

        match write_from.dict.get_mut(&key) {
            Some(&mut Value::List(ref mut ll)) => {
                values.iter().for_each(|v| ll.push_front(v.to_vec()));
                Ok(ll.len())
            }
            Some(_) => Result::Err(RedisError::TypeError),
            None => {
                if !allow_creation {
                    return Ok(values.len());
                }

                let mut ll = LinkedList::new();

                values.iter().for_each(|v| {
                    if push_f {
                        ll.push_front(v.to_vec())
                    } else {
                        ll.push_back(v.to_vec())
                    }
                });
                write_from.dict.insert(key, Value::List(ll));

                Ok(values.len())
            }
        }
    }

    pub async fn lpush(
        &self,
        key: String,
        values: Vec<Vec<u8>>,
        allow_creation: bool,
    ) -> Result<usize, RedisError> {
        self.do_push(key, values, allow_creation, true).await
    }

    pub async fn rpush(
        &self,
        key: String,
        values: Vec<Vec<u8>>,
        allow_creation: bool,
    ) -> Result<usize, RedisError> {
        self.do_push(key, values, allow_creation, false).await
    }

    pub async fn lpop(&self, key: &String, mut times: usize) -> Result<Vec<Vec<u8>>, RedisError> {
        match self.shared_data.write().unwrap().dict.get_mut(key) {
            None => Ok(Vec::new()),
            Some(&mut Value::List(ref mut ll)) => {
                let mut r = Vec::new();
                while times > 0 && let Some(v) = ll.pop_front() {
                    times -= 1;
                    r.push(v);
                }
                Ok(r)
            }
            Some(_) => Result::Err(RedisError::TypeError),
        }
    }
}

async fn spawn_ttl_heap_cleaner(shared_data: Arc<RwLock<SharedData>>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            let s_data = &mut shared_data.write().unwrap();

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
                    format!("key_{}", i),
                    format!("value_{}", i).as_bytes().to_vec(),
                )
                .await
        }
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i).as_bytes().to_vec();
            assert!(matches!(redis.get(&key).await, Ok(Some(value))));
        }
        assert_eq!(redis.ttl_keys().await, 0);
    }
}
