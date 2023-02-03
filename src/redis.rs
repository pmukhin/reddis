use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::ops::Add;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::info;

static INITIAL_CAPACITY: usize = 256;

struct SharedData {
    dict: HashMap<String, Vec<u8>>,
    ttl_heap: BinaryHeap<Reverse<(u64, String)>>,
}

pub struct Redis {
    shared_data: Arc<RwLock<SharedData>>,
}

impl Redis {
    pub async fn new() -> Redis {
        let shared_data = Arc::new(RwLock::from(SharedData {
            dict: HashMap::with_capacity(INITIAL_CAPACITY),
            ttl_heap: BinaryHeap::new(),
        }));

        spawn_ttl_heap_cleaner(shared_data.clone()).await;

        Redis {
            shared_data: shared_data.clone(),
        }
    }

    pub async fn ttl_keys(&self) -> usize {
        self.shared_data.read().unwrap().ttl_heap.len()
    }

    pub async fn set(&self, key: String, value: Vec<u8>) {
        self.shared_data.write().unwrap()
            .dict
            .insert(key, value);
    }

    pub async fn setex(&self, key: String, value: Vec<u8>, ttl: u64) {
        self.set(key.clone(), value).await;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let ttl_value = now.add(Duration::from_secs(ttl));

        self.shared_data
            .write()
            .unwrap()
            .ttl_heap
            .push(Reverse((ttl_value.as_secs(), key.to_string())));
    }

    pub async fn get(&self, key: &String) -> Option<Vec<u8>> {
        self.shared_data
            .read()
            .unwrap()
            .dict
            .get(key)
            .map(|x| x.to_vec())
    }
}

async fn spawn_ttl_heap_cleaner(shared_data: Arc<RwLock<SharedData>>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            let ttl_heap = &shared_data.read().unwrap().ttl_heap;
            if ttl_heap.is_empty() {
                continue;
            }

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let shared_data = &mut shared_data.write().unwrap();

            while let Some(Reverse((w, key))) = ttl_heap.peek() {
                if *w >= now {
                    break;
                }

                info!("deleting stale key={}", key);

                shared_data.dict.remove(key).unwrap();
                shared_data.ttl_heap.pop().unwrap();
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
            assert_eq!(redis.get(&key).await, Some(value));
        }
        assert_eq!(redis.ttl_keys().await, 0);
    }
}
