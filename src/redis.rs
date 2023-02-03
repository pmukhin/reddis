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
        let shared_data = RwLock::new(SharedData {
            dict: HashMap::with_capacity(INITIAL_CAPACITY),
            ttl_heap: BinaryHeap::new(),
        });
        let arc = Arc::new(shared_data);
        spawn_ttl_heap_cleaner(arc.clone()).await;

        Redis { shared_data: arc.clone() }
    }

    pub async fn ttl_keys(&self) -> usize {
        self.shared_data.read().unwrap().ttl_heap.len()
    }

    pub async fn set(&self, key: String, value: Vec<u8>) {
        self.shared_data
            .try_write()
            .expect("Unable to lock mutex")
            .dict
            .insert(key, value);
    }

    pub async fn setex(&self, key: String, value: Vec<u8>, ttl: u64) {
        let s_data = &mut self.shared_data.write().unwrap();

        s_data.dict.insert(key.clone(), value);

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let ttl_value = now.add(Duration::from_secs(ttl));

        s_data.ttl_heap
            .push(Reverse((ttl_value.as_secs(), key.to_string())));

        info!("pushed 1 elem into ttl_heap, ttl_heap_len={}", s_data.ttl_heap.len());
    }

    pub async fn get(&self, key: &String) -> Option<Vec<u8>> {
        self.shared_data
            .try_read()
            .expect("Unable to lock mutex")
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

            let s_data = &mut shared_data.write().unwrap();

            if s_data.ttl_heap.is_empty() { continue };

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            while let Some(Reverse((w, key))) = s_data.ttl_heap.pop() {
                if w >= now {
                    s_data.ttl_heap.push(Reverse((w, key)));
                    break 
                };

                info!("deleting stale key={}", key);
                
                // s_data.ttl_heap.pop().unwrap();
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
            assert_eq!(redis.get(&key).await, Some(value));
        }
        assert_eq!(redis.ttl_keys().await, 0);
    }
}
