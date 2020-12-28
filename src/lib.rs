use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use tokio::sync::Notify;
use tokio::time::timeout;

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct Item<T>
where
    T: Ord,
{
    expiration: Option<Reverse<Instant>>,
    inner: T,
}

struct SharedInner<T>
where
    T: Ord,
{
    storage: Mutex<BinaryHeap<Item<T>>>,
    notify: Notify,
}

#[derive(Clone)]
pub struct TimedQueue<T>
where
    T: Ord,
{
    inner: Arc<SharedInner<T>>,
}

impl<T> TimedQueue<T>
where
    T: Ord,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SharedInner {
                storage: Mutex::new(BinaryHeap::new()),
                notify: Notify::new(),
            }),
        }
    }
    pub fn enqueue(&self, t: T, expiration: Option<Instant>) {
        self.inner.storage.lock().unwrap().push(Item {
            expiration: expiration.map(|e| Reverse(e)),
            inner: t,
        });
        self.inner.notify.notify_one();
    }

    pub async fn dequeue(&self) -> (T, Option<Instant>) {
        loop {
            let now = Instant::now();
            let mut lock = self.inner.storage.lock().unwrap();
            let (ready, duration) = match lock.peek() {
                Some(Item {
                    expiration: Some(Reverse(expiration)),
                    ..
                }) => {
                    if *expiration < now {
                        (true, None)
                    } else {
                        (false, Some(*expiration - now))
                    }
                }
                Some(Item {
                    expiration: None, ..
                }) => (true, None),
                None => (false, None),
            };
            if ready {
                let Item {
                    expiration,
                    inner: item,
                } = lock.pop().unwrap();
                break (item, expiration.map(|Reverse(expiration)| expiration));
            }
            std::mem::drop(lock);
            if let Some(duration) = duration {
                let _ = timeout(duration, self.inner.notify.notified()).await;
            } else {
                self.inner.notify.notified().await;
            }
        }
    }
}