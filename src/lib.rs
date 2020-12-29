//! `timed-queue` provides `TimedQueue`, a set of objects and the minimum time at which they should be returned.
//!
//! # Example
//! Imagine the "new messages" queue of an SMTP server implementation. Delivery should be attempted immediately for new messages.
//! Messages for which delivery fails should be retried after 30 minutes.
//!
//!```
//! fn server_loop<I: IntoIterator<Item = MailMessage>>(tq: TimedQueue<MailMessage>, messages: I) {
//!     for m in messages {
//!         tq.enqueue(m, None);
//!     }
//! }
//!
//! async fn delivery_loop(tq: TimedQueue<MailMessage>) {
//!     loop {
//!         let (msg, _) = tq.dequeue().await;
//!         if try_deliver(msg).await.is_err() {
//!             tq.enqueue(msg, Some(Instant::now() + Duration::from_secs(30 * 60)));
//!         }
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let tq = TimedQueue::new();
//!     let tq2 = tq.clone();
//!     std::thread::spawn(move || server_loop(tq, get_message_stream()));
//!     tokio::spawn(delivery_loop(tq2));
//! }
//! ```
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::Notify;
use tokio::time::timeout;

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct Item<T>
where
    T: Ord,
{
    expiration: Reverse<Option<Instant>>,
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
            expiration: Reverse(expiration),
            inner: t,
        });
        self.inner.notify.notify_one();
    }

    fn peek_inner(&self) -> Result<(T, Option<Instant>), Option<Duration>> {
        let now = Instant::now();
        let mut lock = self.inner.storage.lock().unwrap();
        let (ready, duration) = match lock.peek() {
            Some(Item {
                expiration: Reverse(Some(expiration)),
                ..
            }) => {
                if *expiration < now {
                    (true, None)
                } else {
                    (false, Some(*expiration - now))
                }
            }
            Some(Item {
                expiration: Reverse(None),
                ..
            }) => (true, None),
            None => (false, None),
        };
        if ready {
            let Item {
                expiration: Reverse(expiration),
                inner: item,
            } = lock.pop().unwrap();
            Ok((item, expiration))
        } else {
            Err(duration)
        }
    }

    pub async fn dequeue(&self) -> (T, Option<Instant>) {
        loop {
            match self.peek_inner() {
                Ok((item, duration)) => {
                    break (item, duration);
                }
                Err(Some(duration)) => {
                    let _ = timeout(duration, self.inner.notify.notified()).await;
                }
                Err(None) => {
                    self.inner.notify.notified().await;
                }
            }
        }
    }
}
