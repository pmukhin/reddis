use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::ops::Add;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::sync::atomic::Ordering;

use log::info;

static INITIAL_CAPACITY: usize = 256;

struct SharedData {
    dict: Mutex<HashMap<String, Vec<u8>>>,
    ttl_heap: Mutex<BinaryHeap<Reverse<(u64, String)>>>,
    ttl_entries_count: AtomicU32,
}

pub struct Redis {
    shared_data: Arc<SharedData>,
}

impl Redis {
    pub async fn new() -> Redis {
        let shared_data = Arc::new(SharedData {
            dict: Mutex::from(HashMap::with_capacity(INITIAL_CAPACITY)),
            ttl_heap: Mutex::from(BinaryHeap::new()),
            ttl_entries_count: AtomicU32::new(0),
        });

        spawn_ttl_heap_cleaner(shared_data.clone()).await;

        Redis {
            shared_data: shared_data.clone(),
        }
    }

    pub async fn ttl_keys(&self) -> u32 {
        self.shared_data
            .ttl_entries_count
            .load(Ordering::SeqCst)
    }

    pub async fn set(&self, key: String, value: Vec<u8>) {
        self.shared_data
            .dict
            .lock()
            .expect("Unable to lock mutex")
            .insert(key, value);
    }

    pub async fn setex(&self, key: String, value: Vec<u8>, ttl: u64) {
        self.set(key.clone(), value).await;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let ttl_value = now.add(Duration::from_secs(ttl));

        self.shared_data
            .ttl_heap
            .lock()
            .expect("Unable to lock mutex")
            .push(Reverse((ttl_value.as_secs(), key.to_string())));

        self.shared_data
            .ttl_entries_count
            .fetch_add(1, Ordering::SeqCst);
    }

    pub async fn get(&self, key: &String) -> Option<Vec<u8>> {
        self.shared_data
            .dict
            .lock()
            .expect("Unable to lock mutex")
            .get(key)
            .map(|x| x.to_vec())
    }
}

async fn spawn_ttl_heap_cleaner(shared_data: Arc<SharedData>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;
            if shared_data.ttl_entries_count.load(Ordering::SeqCst) == 0 {
                continue;
            }

            let mut ttl_heap = shared_data.ttl_heap.lock().unwrap();
            if ttl_heap.is_empty() {
                continue;
            }

            let mut d = shared_data.dict.lock().unwrap();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            while let Some(Reverse((w, key))) = ttl_heap.peek() {
                if *w >= now {
                    break;
                }
                info!("deleting stale key={}", key);
                let _ = d.remove(key);
                ttl_heap.pop().unwrap();
                shared_data.ttl_entries_count.fetch_sub(1, Ordering::SeqCst);
            }
        }
    });
}
