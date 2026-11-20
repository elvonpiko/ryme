//! A small adaptive jitter buffer.
//!
//! Each packet carries the sender's sample timestamp. Playback is a local
//! clock: one pull is one 10 ms frame. A slot that was already played or
//! concealed is never played later. If speech arrives so late that the
//! listener would sit behind the talker, the play head moves to a short
//! recent run and the expired frames are dropped. A short utterance can
//! start before a full cushion has arrived.

use std::collections::BTreeMap;

const JUMP: i32 = 64;
const FRAME_SAMPLES: u32 = 480;
const REPRIME: u32 = 4;
/// Pulls to wait, with audio already queued, before a short utterance starts.
const START_HOLD: u32 = 3;
const WINDOW: i32 = 1_048_576;

#[derive(Debug, PartialEq, Eq)]
pub enum Play {
    /// Not enough audio has arrived to start, or the cushion is being rebuilt.
    Wait,
    Packet(Vec<u8>),
    /// The frame that was due never arrived.
    Loss,
}

struct Slot {
    timestamp: u32,
    payload: Vec<u8>,
}

pub struct Jitter {
    next: Option<u32>,
    play_ts: Option<u32>,
    started: bool,
    start: usize,
    min_depth: usize,
    max_depth: usize,
    target: usize,
    queued: BTreeMap<u32, Slot>,
    underrun: u32,
    comfortable: u32,
    /// Highest sequence already played or concealed.
    played: Option<u32>,
    holds: u32,
    dropped: u64,
}

impl Jitter {
    /// QUIC. A few frames of cushion, and a short maximum wait.
    pub fn new() -> Self {
        Self::profile(3, 2, 8)
    }

    /// TCP. Loss hides in the kernel for longer, so the cushion can grow
    /// further before a missing frame gives up.
    pub fn tcp() -> Self {
        Self::profile(5, 3, 20)
    }

    /// The socket changed. Keep the audio already queued and use the new budget.
    pub fn follow_path(&mut self, tcp: bool) {
        let (start, min_depth, max_depth) = if tcp { (5, 3, 20) } else { (3, 2, 8) };
        self.start = start;
        self.min_depth = min_depth;
        self.max_depth = max_depth;
        if self.target > max_depth {
            self.target = max_depth;
        }
        if self.target < min_depth {
            self.target = min_depth.max(1);
        }
        self.trim_to_max();
    }

    fn profile(start: usize, min_depth: usize, max_depth: usize) -> Self {
        Self {
            next: None,
            play_ts: None,
            started: false,
            start,
            min_depth,
            max_depth,
            target: start,
            queued: BTreeMap::new(),
            underrun: 0,
            comfortable: 0,
            played: None,
            holds: 0,
            dropped: 0,
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    pub fn push(&mut self, seq: u32, timestamp: u32, payload: Vec<u8>) {
        if let Some(played) = self.played {
            if behind(played, seq) {
                return;
            }
        }
        if self.started {
            self.note_talkspurt(seq, timestamp);
        }
        self.queued.insert(seq, Slot { timestamp, payload });
        self.catch_live();
    }

    /// A pause, then speech whose timestamp jumped, is a new talkspurt.
    /// Concealing the whole pause would play the first syllable late.
    fn note_talkspurt(&mut self, seq: u32, timestamp: u32) {
        if !self.queued.is_empty() {
            return;
        }
        let Some(next) = self.next else {
            return;
        };
        let seq_gap = seq.wrapping_sub(next) as i32;
        let ts_gap = self
            .play_ts
            .map(|play| timestamp.wrapping_sub(play) as i32)
            .unwrap_or(0);
        if seq_gap > JUMP || ts_gap > JUMP * FRAME_SAMPLES as i32 {
            self.started = false;
            self.next = None;
            self.play_ts = None;
            self.played = None;
            self.underrun = 0;
            self.comfortable = 0;
            self.holds = 0;
            self.target = self.start;
        }
    }

    /// Speech sitting further behind the newest packet than the budget allows
    /// is expired. Keep a cushion at the live edge and do not play the stall.
    fn catch_live(&mut self) {
        if !self.started {
            self.trim_to_max();
            return;
        }
        let Some(next) = self.next else {
            return;
        };
        let Some(newest) = self.ring_newest() else {
            return;
        };
        let late = newest.wrapping_sub(next) as i32;
        if late <= self.max_depth as i32 {
            return;
        }
        let keep = (self.target.max(1) as u32).saturating_sub(1);
        let start = newest.wrapping_sub(keep);
        self.drop_before(start);
        self.next = Some(start);
        self.play_ts = self.queued.get(&start).map(|slot| slot.timestamp);
        self.played = Some(start.wrapping_sub(1));
        self.underrun = 0;
    }

    fn trim_to_max(&mut self) {
        while self.queued.len() > self.max_depth {
            let Some(oldest) = self.ring_oldest() else {
                break;
            };
            self.queued.remove(&oldest);
            self.dropped = self.dropped.saturating_add(1);
        }
    }

    fn drop_before(&mut self, start: u32) {
        let stale: Vec<u32> = self
            .queued
            .keys()
            .copied()
            .filter(|seq| behind(start.wrapping_sub(1), *seq))
            .collect();
        for seq in stale {
            self.queued.remove(&seq);
            self.dropped = self.dropped.saturating_add(1);
        }
    }

    fn ring_oldest(&self) -> Option<u32> {
        let keys: Vec<u32> = self.queued.keys().copied().collect();
        if keys.len() <= 1 {
            return keys.first().copied();
        }
        let mut best_gap = keys[0].wrapping_sub(*keys.last().unwrap());
        let mut oldest = keys[0];
        for pair in keys.windows(2) {
            let gap = pair[1].wrapping_sub(pair[0]);
            if gap > best_gap {
                best_gap = gap;
                oldest = pair[1];
            }
        }
        Some(oldest)
    }

    fn ring_newest(&self) -> Option<u32> {
        self.ring_oldest().map(|oldest| {
            let mut newest = oldest;
            for seq in self.queued.keys().copied() {
                if seq.wrapping_sub(oldest) > newest.wrapping_sub(oldest) {
                    newest = seq;
                }
            }
            newest
        })
    }

    fn prime(&mut self) {
        if self.started || self.queued.is_empty() {
            return;
        }
        let Some(oldest) = self.ring_oldest() else {
            return;
        };
        let contiguous = self
            .played
            .is_some_and(|played| oldest == played.wrapping_add(1));
        if self.queued.len() < self.target && self.holds < START_HOLD && !contiguous {
            return;
        }
        let timestamp = self.queued.get(&oldest).map(|slot| slot.timestamp);
        self.next = Some(oldest);
        self.play_ts = timestamp;
        self.started = true;
        self.holds = 0;
    }

    /// The packet that would play after the one `pull` just reported, still queued.
    pub fn upcoming(&self) -> Option<&[u8]> {
        self.next
            .and_then(|seq| self.queued.get(&seq).map(|slot| slot.payload.as_slice()))
    }

    pub fn pull(&mut self) -> Play {
        if !self.started {
            if !self.queued.is_empty() {
                self.holds = self.holds.saturating_add(1);
            }
            self.prime();
            if !self.started {
                return Play::Wait;
            }
        }
        let Some(next) = self.next else {
            return Play::Wait;
        };
        if let Some(slot) = self.queued.remove(&next) {
            self.advance_from(slot.timestamp);
            self.underrun = 0;
            self.comfortable = self.comfortable.saturating_add(1);
            if self.comfortable > 100 && self.target > self.min_depth {
                self.target -= 1;
                self.comfortable = 0;
            }
            Play::Packet(slot.payload)
        } else {
            self.comfortable = 0;
            self.underrun = self.underrun.saturating_add(1);
            if self.underrun >= REPRIME {
                self.target = (self.target + 1).min(self.max_depth);
                self.underrun = 0;
                self.started = false;
                self.next = None;
                self.play_ts = None;
                self.holds = 0;
                Play::Wait
            } else {
                let ts = self.play_ts.unwrap_or(0);
                self.advance_from(ts);
                Play::Loss
            }
        }
    }

    fn advance_from(&mut self, timestamp: u32) {
        if let Some(next) = self.next {
            self.played = Some(next);
            self.next = Some(next.wrapping_add(1));
        }
        self.play_ts = Some(timestamp.wrapping_add(FRAME_SAMPLES));
    }
}

fn behind(played: u32, seq: u32) -> bool {
    let gap = played.wrapping_sub(seq) as i32;
    (0..WINDOW).contains(&gap)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push(jitter: &mut Jitter, seq: u32, payload: u8) {
        jitter.push(seq, seq.wrapping_mul(FRAME_SAMPLES), vec![payload]);
    }

    #[test]
    fn a_stall_counts_the_frames_it_skips() {
        let mut jitter = Jitter::new();
        for seq in 1..=30 {
            push(&mut jitter, seq, seq as u8);
        }
        assert!(jitter.dropped() > 0);
    }

    #[test]
    fn playback_waits_for_three_frames_then_stays_in_order() {
        let mut jitter = Jitter::new();
        assert_eq!(jitter.pull(), Play::Wait);
        push(&mut jitter, 1, 1);
        push(&mut jitter, 2, 2);
        assert_eq!(jitter.pull(), Play::Wait);
        push(&mut jitter, 3, 3);
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        assert_eq!(jitter.pull(), Play::Packet(vec![2]));
        assert_eq!(jitter.pull(), Play::Packet(vec![3]));
    }

    #[test]
    fn a_late_packet_is_dropped_and_a_hole_is_a_loss() {
        let mut jitter = Jitter::new();
        push(&mut jitter, 1, 1);
        push(&mut jitter, 2, 2);
        push(&mut jitter, 4, 4);
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        push(&mut jitter, 1, 9);
        assert_eq!(jitter.pull(), Play::Packet(vec![2]));
        assert_eq!(jitter.pull(), Play::Loss);
        assert_eq!(jitter.upcoming(), Some(&[4][..]));
        assert_eq!(jitter.pull(), Play::Packet(vec![4]));
    }

    #[test]
    fn a_packet_one_ahead_waits_for_the_missing_frame() {
        let mut jitter = Jitter::new();
        for seq in 1..=3 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        assert_eq!(jitter.pull(), Play::Packet(vec![2]));
        assert_eq!(jitter.pull(), Play::Packet(vec![3]));
        push(&mut jitter, 5, 5);
        push(&mut jitter, 4, 4);
        assert_eq!(jitter.pull(), Play::Packet(vec![4]));
        assert_eq!(jitter.pull(), Play::Packet(vec![5]));
    }

    #[test]
    fn a_lost_frame_is_concealed_before_the_one_after_it() {
        let mut jitter = Jitter::new();
        for seq in 1..=3 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        assert_eq!(jitter.pull(), Play::Packet(vec![2]));
        assert_eq!(jitter.pull(), Play::Packet(vec![3]));
        push(&mut jitter, 5, 5);
        assert_eq!(jitter.pull(), Play::Loss);
        assert_eq!(jitter.pull(), Play::Packet(vec![5]));
    }

    #[test]
    fn a_burst_before_playback_keeps_the_newest_cushion() {
        let mut jitter = Jitter::new();
        for seq in 1..=50 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![43]));
        assert_eq!(jitter.pull(), Play::Packet(vec![44]));
    }

    #[test]
    fn a_stall_returns_to_live_speech() {
        let mut jitter = Jitter::tcp();
        for seq in 1..=5 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        for seq in 2..=30 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![19]));
        assert_eq!(jitter.pull(), Play::Packet(vec![20]));
    }

    #[test]
    fn a_concealed_slot_is_not_replayed() {
        let mut jitter = Jitter::new();
        for seq in 1..=3 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        assert_eq!(jitter.pull(), Play::Packet(vec![2]));
        assert_eq!(jitter.pull(), Play::Packet(vec![3]));
        assert_eq!(jitter.pull(), Play::Loss);
        assert_eq!(jitter.pull(), Play::Loss);
        assert_eq!(jitter.pull(), Play::Loss);
        assert_eq!(jitter.pull(), Play::Wait);
        for seq in 4..=7 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![7]));
    }

    #[test]
    fn a_short_utterance_starts_before_the_full_cushion() {
        let mut jitter = Jitter::tcp();
        push(&mut jitter, 1, 1);
        assert_eq!(jitter.pull(), Play::Wait);
        assert_eq!(jitter.pull(), Play::Wait);
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
    }

    #[test]
    fn a_path_change_keeps_the_queue_and_uses_the_new_budget() {
        let mut jitter = Jitter::tcp();
        assert_eq!(jitter.max_depth, 20);
        push(&mut jitter, 1, 1);
        jitter.follow_path(false);
        assert_eq!(jitter.max_depth, 8);
        assert_eq!(jitter.min_depth, 2);
        assert!(jitter.queued.contains_key(&1));
    }

    #[test]
    fn a_push_to_talk_pause_starts_again_instead_of_concealing_the_gap() {
        let mut jitter = Jitter::new();
        for seq in 1..=3 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        assert_eq!(jitter.pull(), Play::Packet(vec![2]));
        assert_eq!(jitter.pull(), Play::Packet(vec![3]));
        for seq in 80..=82 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![80]));
        assert_eq!(jitter.pull(), Play::Packet(vec![81]));
    }

    #[test]
    fn sequence_numbers_wrap_and_a_late_one_is_dropped() {
        let mut jitter = Jitter::new();
        let first = u32::MAX - 1;
        push(&mut jitter, first, 1);
        push(&mut jitter, u32::MAX, 2);
        push(&mut jitter, 0, 3);
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        push(&mut jitter, first.wrapping_sub(1), 9);
        assert_eq!(jitter.pull(), Play::Packet(vec![2]));
        assert_eq!(jitter.pull(), Play::Packet(vec![3]));
    }
}
