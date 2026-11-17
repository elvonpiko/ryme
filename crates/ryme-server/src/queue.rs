//! A short queue that keeps the newest items.
//!
//! Voice on TCP cannot wait. When the socket is behind, the frames that matter
//! are the ones just captured, so a full queue drops the oldest.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

struct Slots<T> {
    items: VecDeque<T>,
    closed: bool,
}

struct Inner<T> {
    cap: usize,
    slots: Mutex<Slots<T>>,
    notify: Notify,
    senders: AtomicUsize,
}

pub struct NewestSender<T> {
    inner: Arc<Inner<T>>,
}

pub struct NewestReceiver<T> {
    inner: Arc<Inner<T>>,
}

pub fn newest<T>(cap: usize) -> (NewestSender<T>, NewestReceiver<T>) {
    let inner = Arc::new(Inner {
        cap: cap.max(1),
        slots: Mutex::new(Slots {
            items: VecDeque::new(),
            closed: false,
        }),
        notify: Notify::new(),
        senders: AtomicUsize::new(1),
    });
    (
        NewestSender {
            inner: Arc::clone(&inner),
        },
        NewestReceiver { inner },
    )
}

impl<T> NewestSender<T> {
    /// Keep `item`. If the queue is already full, the oldest item is dropped.
    pub fn offer(&self, item: T) {
        let mut slots = self.inner.slots.lock().unwrap();
        if slots.closed {
            return;
        }
        while slots.items.len() >= self.inner.cap {
            slots.items.pop_front();
        }
        slots.items.push_back(item);
        drop(slots);
        self.inner.notify.notify_one();
    }
}

impl<T> Clone for NewestSender<T> {
    fn clone(&self) -> Self {
        self.inner.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for NewestSender<T> {
    fn drop(&mut self) {
        if self.inner.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.slots.lock().unwrap().closed = true;
            self.inner.notify.notify_waiters();
        }
    }
}

impl<T> NewestReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        loop {
            // Register the waiter before checking, so an offer cannot land
            // between the empty check and the sleep.
            let notified = self.inner.notify.notified();
            {
                let mut slots = self.inner.slots.lock().unwrap();
                if let Some(item) = slots.items.pop_front() {
                    return Some(item);
                }
                if slots.closed {
                    return None;
                }
            }
            notified.await;
        }
    }
}

/// Ask the kernel for a small TCP send buffer.
///
/// The default buffer can hold seconds of voice, so the six-frame queue never
/// fills and a stall is replayed later. The kernel may raise this floor.
pub fn limit_send_buffer(stream: &tokio::net::TcpStream) {
    let bytes: i32 = 4096;
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        unsafe extern "C" {
            fn setsockopt(
                sockfd: i32,
                level: i32,
                optname: i32,
                optval: *const u8,
                optlen: u32,
            ) -> i32;
        }
        #[cfg(target_os = "linux")]
        let (level, name) = (1, 7);
        #[cfg(not(target_os = "linux"))]
        let (level, name) = (0xffff, 0x1001);
        let _ = unsafe {
            setsockopt(
                stream.as_raw_fd(),
                level,
                name,
                &bytes as *const i32 as *const u8,
                4,
            )
        };
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::{AsRawSocket, RawSocket};
        #[link(name = "ws2_32")]
        unsafe extern "system" {
            fn setsockopt(
                s: RawSocket,
                level: i32,
                optname: i32,
                optval: *const i8,
                optlen: i32,
            ) -> i32;
        }
        let _ = unsafe {
            setsockopt(
                stream.as_raw_socket(),
                0xffff,
                0x1001,
                &bytes as *const i32 as *const i8,
                4,
            )
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_full_queue_keeps_the_newest_items() {
        let (tx, mut rx) = newest(2);
        tx.offer(1);
        tx.offer(2);
        tx.offer(3);
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    }

    #[tokio::test]
    async fn the_receiver_ends_when_the_sender_is_dropped() {
        let (tx, mut rx) = newest::<u8>(2);
        tx.offer(4);
        drop(tx);
        assert_eq!(rx.recv().await, Some(4));
        assert_eq!(rx.recv().await, None);
    }
}
