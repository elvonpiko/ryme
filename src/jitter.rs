//! A small adaptive jitter buffer.
//!
//! Each packet carries the sender's sample timestamp. Playback is a local
//! clock: one pull is one 10 ms frame, and a missing frame is concealed when
//! that frame is due. A later packet does not move the play head. The same
//! packet is played whether it arrived on QUIC or TCP. When packets sit late,
//! the wait grows, up to a maximum. It shrinks again in a gap, not by skipping
//! a frame in the middle of a sentence. A pile that arrives before playback
//! starts keeps only the newest maximum, so the first word is not half a
//! second behind.

use std::collections::BTreeMap;

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
    start: usize,
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
            start,
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
        self.note_lateness();
        self.trim_oldest();
    }

    /// Packets waiting past the cushion are late. The wait grows to meet them.
    fn note_lateness(&mut self) {
        if !self.started {
            return;
        }
        let depth = self.queued.len();
        if depth > self.target {
            self.target = depth.min(self.max_depth);
        }
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
            self.target = self.start;
        }
    }

    /// Past the maximum wait, the oldest frames go. The play head stays, so a
    /// burst is not jumped to its newest packet. Before playback starts, what
    /// remains is the newest cushion.
    fn trim_oldest(&mut self) {
        while self.queued.len() > self.max_depth {
            let Some(oldest) = self.ring_oldest() else {
                break;
            };
            self.queued.remove(&oldest);
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
        let Some(next) = self.next else {
            return Play::Wait;
        };
        if let Some(slot) = self.queued.remove(&next) {
            self.advance_from(slot.timestamp);
            self.comfortable = self.comfortable.saturating_add(1);
            self.underrun = 0;
            Play::Packet(slot.payload)
        } else {
            if self.queued.is_empty() && self.comfortable > 0 && self.target > self.min_depth {
                self.target -= 1;
            }
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
    fn a_late_run_grows_the_wait_and_a_gap_shrinks_it() {
        let mut jitter = Jitter::new();
        for seq in 1..=3 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        assert_eq!(jitter.pull(), Play::Packet(vec![2]));
        assert_eq!(jitter.pull(), Play::Packet(vec![3]));
        for seq in 4..=12 {
            push(&mut jitter, seq, seq as u8);
        }
        assert_eq!(jitter.target, 8);
        assert_eq!(jitter.pull(), Play::Loss);
        for seq in 5..=12 {
            assert_eq!(jitter.pull(), Play::Packet(vec![seq as u8]));
        }
        let grown = jitter.target;
        assert_eq!(jitter.pull(), Play::Loss);
        assert!(jitter.target < grown);
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
    fn a_burst_during_playback_stays_in_order() {
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
        assert!(jitter.target > 5);
        assert_eq!(jitter.pull(), Play::Loss);
        assert_eq!(jitter.pull(), Play::Loss);
        assert_eq!(jitter.pull(), Play::Loss);
        assert_eq!(jitter.pull(), Play::Wait);
        assert_eq!(jitter.pull(), Play::Packet(vec![21]));
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
