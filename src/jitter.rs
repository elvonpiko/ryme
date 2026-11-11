//! A small adaptive jitter buffer.
//!
//! Playback waits until a few frames have arrived, then emits one frame per
//! tick in sequence order. A missing frame is reported as a loss so the Opus
//! decoder can conceal it. If losses keep happening the wait grows. If the
//! buffer stays deep and healthy, one frame is skipped so the delay can fall.

use std::collections::BTreeMap;

const START: usize = 3;
const MIN_DEPTH: usize = 2;
const MAX_DEPTH: usize = 8;
const COMFORT: u32 = 100;
const JUMP: i32 = 64;

#[derive(Debug, PartialEq, Eq)]
pub enum Play {
    /// Not enough audio has arrived to start.
    Wait,
    Packet(Vec<u8>),
    /// The next sequence number never arrived.
    Loss,
}

pub struct Jitter {
    next: Option<u32>,
    started: bool,
    target: usize,
    queued: BTreeMap<u32, Vec<u8>>,
    underrun: u32,
    comfortable: u32,
}

impl Jitter {
    pub fn new() -> Self {
        Self {
            next: None,
            started: false,
            target: START,
            queued: BTreeMap::new(),
            underrun: 0,
            comfortable: 0,
        }
    }

    pub fn push(&mut self, seq: u32, payload: Vec<u8>) {
        // A jump is measured from the newest frame we still hold. Measuring
        // from the play head made a short burst look like the other side had
        // restarted, and the buffer wiped itself.
        let newest = self.queued.keys().next_back().copied().or(self.next);
        if let Some(newest) = newest {
            let gap = seq.wrapping_sub(newest) as i32;
            if gap > JUMP {
                self.restart();
            }
        }
        if let Some(next) = self.next {
            let late = next.wrapping_sub(seq) as i32;
            if late > 0 {
                return;
            }
        }
        self.queued.insert(seq, payload);
        if self.queued.len() > 24 {
            if let Some(oldest) = self.queued.keys().next().copied() {
                self.queued.remove(&oldest);
                if self.next == Some(oldest) {
                    self.next = Some(oldest.wrapping_add(1));
                }
            }
        }
        if !self.started && self.queued.len() >= self.target {
            self.next = self.queued.keys().next().copied();
            self.started = self.next.is_some();
        }
    }

    pub fn pull(&mut self) -> Play {
        if !self.started {
            return Play::Wait;
        }
        self.shrink_if_deep();
        let Some(next) = self.next else {
            return Play::Wait;
        };
        self.next = Some(next.wrapping_add(1));
        if let Some(packet) = self.queued.remove(&next) {
            self.comfortable = self.comfortable.saturating_add(1);
            Play::Packet(packet)
        } else {
            self.comfortable = 0;
            self.underrun = self.underrun.saturating_add(1);
            if self.underrun >= 4 {
                self.target = (self.target + 1).min(MAX_DEPTH);
                self.underrun = 0;
            }
            Play::Loss
        }
    }

    fn shrink_if_deep(&mut self) {
        if self.comfortable > COMFORT
            && self.queued.len() > self.target + 2
            && self.target > MIN_DEPTH
        {
            if let Some(next) = self.next {
                self.queued.remove(&next);
                self.next = Some(next.wrapping_add(1));
            }
            self.target -= 1;
            self.comfortable = 0;
        }
    }

    fn restart(&mut self) {
        self.queued.clear();
        self.started = false;
        self.next = None;
        self.underrun = 0;
        self.comfortable = 0;
        self.target = START;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn playback_waits_for_three_frames_then_stays_in_order() {
        let mut jitter = Jitter::new();
        assert_eq!(jitter.pull(), Play::Wait);
        jitter.push(1, vec![1]);
        jitter.push(2, vec![2]);
        assert_eq!(jitter.pull(), Play::Wait);
        jitter.push(3, vec![3]);
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        assert_eq!(jitter.pull(), Play::Packet(vec![2]));
        assert_eq!(jitter.pull(), Play::Packet(vec![3]));
    }

    #[test]
    fn a_late_packet_is_dropped_and_a_hole_is_a_loss() {
        let mut jitter = Jitter::new();
        jitter.push(1, vec![1]);
        jitter.push(2, vec![2]);
        jitter.push(4, vec![4]);
        assert_eq!(jitter.pull(), Play::Packet(vec![1]));
        jitter.push(1, vec![9]);
        assert_eq!(jitter.pull(), Play::Packet(vec![2]));
        assert_eq!(jitter.pull(), Play::Loss);
        assert_eq!(jitter.pull(), Play::Packet(vec![4]));
    }

    #[test]
    fn a_deep_healthy_buffer_skips_a_frame() {
        let mut jitter = Jitter::new();
        for seq in 1..=20 {
            jitter.push(seq, vec![seq as u8]);
        }
        let mut played = Vec::new();
        let mut seq = 21u8;
        for _ in 0..150 {
            jitter.push(seq as u32, vec![seq]);
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
}
