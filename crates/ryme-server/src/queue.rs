//! Voice queues.
//!
//! A full queue keeps the newest frames. A fair queue gives each speaker their
//! own slots, drops a frame once it has waited too long, and serves speakers
//! in turn so one talker cannot replace another.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

/// Unsent voice older than this is dropped, even when the queue is not full.
pub const VOICE_MAX_AGE: Duration = Duration::from_millis(100);

struct Stamped<T> {
    item: T,
    at: Instant,
}

struct Slots<T> {
    items: VecDeque<Stamped<T>>,
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
        self.offer_at(item, Instant::now());
    }

    pub fn offer_at(&self, item: T, at: Instant) {
        let mut slots = self.inner.slots.lock().unwrap();
        if slots.closed {
            return;
        }
        while slots.items.len() >= self.inner.cap {
            slots.items.pop_front();
        }
        slots.items.push_back(Stamped { item, at });
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
    #[allow(dead_code)]
    pub async fn recv(&mut self) -> Option<T> {
        self.recv_within(Duration::MAX).await
    }

    /// The next frame that is still younger than `max_age`. Older frames are
    /// dropped here. Bytes already inside TLS are not recalled.
    pub async fn recv_within(&mut self, max_age: Duration) -> Option<T> {
        loop {
            // Register the waiter before checking, so an offer cannot land
            // between the empty check and the sleep.
            let notified = self.inner.notify.notified();
            {
                let mut slots = self.inner.slots.lock().unwrap();
                while let Some(stamped) = slots.items.pop_front() {
                    if stamped.at.elapsed() <= max_age {
                        return Some(stamped.item);
                    }
                }
                if slots.closed {
                    return None;
                }
            }
            notified.await;
        }
    }
}

struct Lane<T> {
    items: VecDeque<Stamped<T>>,
}

struct FairSlots<T> {
    lanes: HashMap<u32, Lane<T>>,
    turn: VecDeque<u32>,
    total: usize,
    closed: bool,
    evicted: HashMap<u32, u64>,
    expired: HashMap<u32, u64>,
}

struct FairInner<T> {
    per_speaker: usize,
    total_cap: usize,
    max_age: Duration,
    slots: Mutex<FairSlots<T>>,
    notify: Notify,
    senders: AtomicUsize,
}

pub struct FairSender<T> {
    inner: Arc<FairInner<T>>,
}

pub struct FairReceiver<T> {
    inner: Arc<FairInner<T>>,
}

pub fn fair<T>(
    per_speaker: usize,
    total: usize,
    max_age: Duration,
) -> (FairSender<T>, FairReceiver<T>) {
    let inner = Arc::new(FairInner {
        per_speaker: per_speaker.max(1),
        total_cap: total.max(1),
        max_age,
        slots: Mutex::new(FairSlots {
            lanes: HashMap::new(),
            turn: VecDeque::new(),
            total: 0,
            closed: false,
            evicted: HashMap::new(),
            expired: HashMap::new(),
        }),
        notify: Notify::new(),
        senders: AtomicUsize::new(1),
    });
    (
        FairSender {
            inner: Arc::clone(&inner),
        },
        FairReceiver { inner },
    )
}

impl<T> FairSender<T> {
    pub fn offer(&self, speaker: u32, item: T) {
        self.offer_at(speaker, item, Instant::now());
    }

    pub fn offer_at(&self, speaker: u32, item: T, at: Instant) {
        let mut slots = self.inner.slots.lock().unwrap();
        if slots.closed {
            return;
        }
        expire_fair(&mut slots, self.inner.max_age);
        let per = self.inner.per_speaker;
        loop {
            let full = slots
                .lanes
                .get(&speaker)
                .is_some_and(|lane| lane.items.len() >= per);
            if !full {
                break;
            }
            if let Some(lane) = slots.lanes.get_mut(&speaker) {
                lane.items.pop_front();
            }
            slots.total = slots.total.saturating_sub(1);
            *slots.evicted.entry(speaker).or_insert(0) += 1;
        }
        while slots.total >= self.inner.total_cap {
            if !drop_oldest(&mut slots) {
                break;
            }
        }
        let was_empty = slots
            .lanes
            .get(&speaker)
            .is_none_or(|lane| lane.items.is_empty());
        slots
            .lanes
            .entry(speaker)
            .or_insert_with(|| Lane {
                items: VecDeque::new(),
            })
            .items
            .push_back(Stamped { item, at });
        slots.total += 1;
        if was_empty {
            slots.turn.push_back(speaker);
        }
        drop(slots);
        self.inner.notify.notify_one();
    }

    #[allow(dead_code)]
    pub fn evicted(&self, speaker: u32) -> u64 {
        self.inner
            .slots
            .lock()
            .unwrap()
            .evicted
            .get(&speaker)
            .copied()
            .unwrap_or(0)
    }

    #[allow(dead_code)]
    pub fn expired(&self, speaker: u32) -> u64 {
        self.inner
            .slots
            .lock()
            .unwrap()
            .expired
            .get(&speaker)
            .copied()
            .unwrap_or(0)
    }
}

fn drop_oldest<T>(slots: &mut FairSlots<T>) -> bool {
    let Some(speaker) = slots
        .lanes
        .iter()
        .filter(|(_, lane)| !lane.items.is_empty())
        .max_by_key(|(_, lane)| lane.items.len())
        .map(|(speaker, _)| *speaker)
    else {
        return false;
    };
    let removed = slots
        .lanes
        .get_mut(&speaker)
        .and_then(|lane| lane.items.pop_front())
        .is_some();
    if !removed {
        return false;
    }
    slots.total = slots.total.saturating_sub(1);
    *slots.evicted.entry(speaker).or_insert(0) += 1;
    let empty = slots
        .lanes
        .get(&speaker)
        .is_some_and(|lane| lane.items.is_empty());
    if empty {
        slots.turn.retain(|id| *id != speaker);
    }
    true
}

fn expire_fair<T>(slots: &mut FairSlots<T>, max_age: Duration) {
    let mut counted = Vec::new();
    let mut empty = Vec::new();
    for (speaker, lane) in slots.lanes.iter_mut() {
        let mut n = 0u64;
        while lane
            .items
            .front()
            .is_some_and(|item| item.at.elapsed() > max_age)
        {
            lane.items.pop_front();
            n += 1;
        }
        if n > 0 {
            counted.push((*speaker, n));
        }
        if lane.items.is_empty() {
            empty.push(*speaker);
        }
    }
    for (speaker, n) in counted {
        slots.total = slots.total.saturating_sub(n as usize);
        *slots.expired.entry(speaker).or_insert(0) += n;
    }
    if !empty.is_empty() {
        slots.turn.retain(|id| !empty.contains(id));
        for speaker in empty {
            slots.lanes.remove(&speaker);
        }
    }
    bound_retired(&mut slots.evicted, &slots.lanes);
    bound_retired(&mut slots.expired, &slots.lanes);
}

fn bound_retired<T>(counts: &mut HashMap<u32, u64>, lanes: &HashMap<u32, Lane<T>>) {
    if counts.len() <= 64 {
        return;
    }
    counts.retain(|speaker, _| lanes.contains_key(speaker));
}

impl<T> Clone for FairSender<T> {
    fn clone(&self) -> Self {
        self.inner.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for FairSender<T> {
    fn drop(&mut self) {
        if self.inner.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.slots.lock().unwrap().closed = true;
            self.inner.notify.notify_waiters();
        }
    }
}

impl<T> FairReceiver<T> {
    pub async fn recv(&mut self) -> Option<(u32, T)> {
        loop {
            let notified = self.inner.notify.notified();
            {
                let mut slots = self.inner.slots.lock().unwrap();
                expire_fair(&mut slots, self.inner.max_age);
                if let Some(speaker) = slots.turn.pop_front() {
                    let item = slots
                        .lanes
                        .get_mut(&speaker)
                        .and_then(|lane| lane.items.pop_front());
                    if let Some(stamped) = item {
                        slots.total = slots.total.saturating_sub(1);
                        let more = slots
                            .lanes
                            .get(&speaker)
                            .is_some_and(|lane| !lane.items.is_empty());
                        if more {
                            slots.turn.push_back(speaker);
                        }
                        return Some((speaker, stamped.item));
                    }
                }
                if slots.closed && slots.total == 0 {
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

    #[tokio::test]
    async fn an_old_frame_is_skipped_even_when_the_queue_has_room() {
        let (tx, mut rx) = newest(4);
        tx.offer_at(1, Instant::now() - Duration::from_millis(200));
        tx.offer_at(2, Instant::now());
        assert_eq!(rx.recv_within(VOICE_MAX_AGE).await, Some(2));
    }

    #[tokio::test]
    async fn two_speakers_are_both_kept_and_served_in_turn() {
        let (tx, mut rx) = fair::<&'static [u8]>(4, 16, VOICE_MAX_AGE);
        tx.offer(1, b"a1");
        tx.offer(2, b"b1");
        tx.offer(1, b"a2");
        assert_eq!(rx.recv().await, Some((1, &b"a1"[..])));
        assert_eq!(rx.recv().await, Some((2, &b"b1"[..])));
        assert_eq!(rx.recv().await, Some((1, &b"a2"[..])));
    }

    #[tokio::test]
    async fn a_speaker_over_the_cap_loses_their_oldest_frame() {
        let (tx, mut rx) = fair::<u8>(2, 8, VOICE_MAX_AGE);
        tx.offer(7, 1);
        tx.offer(7, 2);
        tx.offer(7, 3);
        assert_eq!(tx.evicted(7), 1);
        assert_eq!(tx.expired(7), 0);
        assert_eq!(rx.recv().await, Some((7, 2)));
        assert_eq!(rx.recv().await, Some((7, 3)));
    }

    #[tokio::test]
    async fn an_old_speaker_frame_expires_without_counting_as_a_cap_drop() {
        let (tx, mut rx) = fair::<u8>(4, 8, VOICE_MAX_AGE);
        tx.offer_at(3, 1, Instant::now() - Duration::from_millis(200));
        tx.offer(3, 2);
        assert_eq!(rx.recv().await, Some((3, 2)));
        assert_eq!(tx.expired(3), 1);
        assert_eq!(tx.evicted(3), 0);
    }
}
