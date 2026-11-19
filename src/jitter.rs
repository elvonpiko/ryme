//! A small adaptive jitter buffer.
//!
//! Each packet carries the sender's sample timestamp. Playback is a local
//! clock: one pull is one 10 ms frame, and a missing frame is concealed when
//! that frame is due. A later packet does not move the play head, so a frame
//! that arrives before the next callback can still be played. After several
//! losses the buffer stops and fills again. A burst that is genuinely late is
//! skipped on purpose. A long healthy stretch shortens the wait by one frame.

use std::collections::BTreeMap;

const COMFORT: u32 = 100;
const JUMP: i32 = 64;
const FRAME_SAMPLES: u32 = 480;
const REPRIME: u32 = 4;

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
    min_depth: usize,
    max_depth: usize,
    target: usize,
    queued: BTreeMap<u32, Slot>,
    underrun: u32,
    comfortable: u32,
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

    fn profile(start: usize, min_depth: usize, max_depth: usize) -> Self {
        Self {
            next: None,
            play_ts: None,
            started: false,
            min_depth,
            max_depth,
            target: start,
            queued: BTreeMap::new(),
            underrun: 0,
            comfortable: 0,
        }
    }

    pub fn push(&mut self, seq: u32, timestamp: u32, payload: Vec<u8>) {
        if self.started {
            if let Some(next) = self.next {
                let late = next.wrapping_sub(seq) as i32;
                if late > 0 {
                    return;
                }
            }
            self.note_talkspurt(seq, timestamp);
        }
        self.queued.insert(seq, Slot { timestamp, payload });
        self.trim_burst();
        self.prime();
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
            self.underrun = 0;
            self.comfortable = 0;
        }
    }

    /// Keep a cushion of `target` plus one frame. Anything older is a stale
    /// burst and the play head moves to what remains. One packet sitting
    /// ahead of a missing frame never fills the cushion, so it stays put.
    fn trim_burst(&mut self) {
        let keep = self.target + 1;
        if self.queued.len() <= keep {
            return;
        }
        while self.queued.len() > keep {
            let Some(oldest) = self.ring_oldest() else {
                break;
            };
            self.queued.remove(&oldest);
        }
        let Some(oldest) = self.ring_oldest() else {
            return;
        };
        let Some(next) = self.next else {
            return;
        };
        if oldest.wrapping_sub(next) as i32 > 0 {
            let timestamp = self.queued.get(&oldest).map(|slot| slot.timestamp);
            self.next = Some(oldest);
            self.play_ts = timestamp;
            self.underrun = 0;
            self.comfortable = 0;
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

    fn prime(&mut self) {
        if self.started || self.queued.len() < self.target {
            return;
        }
        let Some(oldest) = self.ring_oldest() else {
            return;
        };
        let timestamp = self.queued.get(&oldest).map(|slot| slot.timestamp);
        self.next = Some(oldest);
        self.play_ts = timestamp;
        self.started = true;
    }

    /// The packet that would play after the one `pull` just reported, still queued.
    pub fn upcoming(&self) -> Option<&[u8]> {
        self.next
            .and_then(|seq| self.queued.get(&seq).map(|slot| slot.payload.as_slice()))
    }

    pub fn pull(&mut self) -> Play {
        if !self.started {
            self.prime();
            if !self.started {
                return Play::Wait;
            }
        }
        self.shrink_if_deep();
        let Some(next) = self.next else {
            return Play::Wait;
        };
        if let Some(slot) = self.queued.remove(&next) {
            self.advance_from(slot.timestamp);
            self.comfortable = self.comfortable.saturating_add(1);
            self.underrun = 0;
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
                self.comfortable = 0;
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
            self.next = Some(next.wrapping_add(1));
        }
        self.play_ts = Some(timestamp.wrapping_add(FRAME_SAMPLES));
    }

    fn shrink_if_deep(&mut self) {
        if self.comfortable <= COMFORT || self.target <= self.min_depth {
            return;
        }
        if self.queued.len() > self.min_depth {
            if let Some(next) = self.next {
                if self.queued.remove(&next).is_some() {
                    let ts = self.play_ts.unwrap_or(0);
                    self.advance_from(ts);
                }
            }
        }
        self.target -= 1;
        self.comfortable = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push(jitter: &mut Jitter, seq: u32, payload: u8) {
        jitter.push(seq, seq.wrapping_mul(FRAME_SAMPLES), vec![payload]);
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
    fn a_deep_healthy_buffer_skips_a_frame() {
        let mut jitter = Jitter::new();
        for seq in 1..=20 {
            push(&mut jitter, seq, seq as u8);
        }
        let mut played = Vec::new();
        let mut seq = 21u32;
        for _ in 0..150 {
            push(&mut jitter, seq, seq as u8);
            seq = seq.wrapping_add(1);
            if let Play::Packet(packet) = jitter.pull() {
                played.push(packet[0]);
            }
        }
        let gaps = played
            .windows(2)
            .filter(|pair| pair[1] != pair[0].wrapping_add(1))
            .count();
        assert!(gaps >= 1, "played {played:?}");
    }

    #[test]
    fn a_burst_plays_from_the_newest_frames() {
        let mut jitter = Jitter::new();
        for seq in 1..=50 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![47]));
        assert_eq!(jitter.pull(), Play::Packet(vec![48]));
    }

    #[test]
    fn tcp_waits_longer_and_still_drops_a_burst() {
        let mut jitter = Jitter::tcp();
        for seq in 1..=4 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Wait);
        push(&mut jitter, 5, 5);
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        for seq in 6..=40 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![35]));
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
    fn a_delay_step_reprimes_and_keeps_the_late_frames() {
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
        assert_eq!(jitter.pull(), Play::Packet(vec![4]));
        assert_eq!(jitter.pull(), Play::Packet(vec![5]));
        assert_eq!(jitter.pull(), Play::Packet(vec![6]));
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
