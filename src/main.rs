use std::io::BufRead;
use std::time::Duration;
use std::time::Instant;

use timed_queue::TimedQueue;

fn put_them(tq: TimedQueue<usize>) {
    let stdin = std::io::stdin();
    for (idx, line) in stdin.lock().lines().enumerate() {
        let line = line.unwrap();
        let dur: u64 = line.parse().unwrap();
        tq.enqueue(idx, Some(Instant::now() + Duration::from_secs(dur)));
    }
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let tq = TimedQueue::new();
    std::thread::spawn({
        let tq = tq.clone();
        move || put_them(tq)
    });

    loop {
        let next = tq.dequeue().await.0;
        println!("Fired: {}", next)
    }
}
