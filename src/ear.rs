//! Microphone in, speakers out.
//!
//! The sound callback only moves samples through a lock-free ring. Encoding,
//! the jitter buffer, and the socket sit on a normal thread, one 10 ms frame
//! at a time. Opus is 48 kHz mono. A device that runs at another rate is
//! resampled on that thread.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use magnum_opus::{Application, Bitrate, Channels, Decoder, Encoder};
use rtrb::{Consumer, Producer, RingBuffer};
use ryme_proto::{decode_voice, encode_voice, VoicePacket};
use ryme_server::VoicePipe;
use tokio::sync::mpsc::UnboundedSender;

use crate::jitter::{Jitter, Play};

const RATE: u32 = 48_000;
const FRAME: usize = 480;
const RING: usize = 16_384;
// Laptop mics often sit well below a full-scale voice. Lift before the meter
// and the encoder, then light the ring a little under a quiet speaking level.
const MIC_GAIN: f32 = 2.5;
const SPEECH: f32 = 0.015;

#[derive(Clone, Copy)]
pub struct Mix {
    pub gain: f32,
    pub quiet: bool,
}

pub struct Ear {
    stop: Arc<AtomicBool>,
    muted: Arc<AtomicBool>,
    deaf: Arc<AtomicBool>,
    live: Arc<AtomicBool>,
    ptt: Arc<AtomicBool>,
    denoise: Arc<AtomicBool>,
    vkey: Arc<AtomicBool>,
    levels: Arc<Mutex<HashMap<u32, Mix>>>,
    pub mic: bool,
    pub speakers: bool,
    thread: Option<JoinHandle<()>>,
}

impl Ear {
    pub fn set_muted(&self, muted: bool) {
        self.muted.store(muted, Ordering::Relaxed);
    }

    pub fn set_deaf(&self, deaf: bool) {
        self.deaf.store(deaf, Ordering::Relaxed);
    }

    pub fn set_live(&self, live: bool) {
        self.live.store(live, Ordering::Relaxed);
    }

    /// Push-to-talk holds the microphone until V is down. Off means open mic.
    pub fn set_ptt(&self, ptt: bool) {
        self.ptt.store(ptt, Ordering::Relaxed);
    }

    pub fn set_denoise(&self, on: bool) {
        self.denoise.store(on, Ordering::Relaxed);
    }

    /// V, as seen by the window. The audio thread also reads the keyboard.
    pub fn set_vkey(&self, down: bool) {
        self.vkey.store(down, Ordering::Relaxed);
    }

    pub fn set_levels(&self, levels: HashMap<u32, Mix>) {
        *self.levels.lock().unwrap() = levels;
    }
}

impl Drop for Ear {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub fn open(pipe: VoicePipe, me: u32, speaking: UnboundedSender<(u32, bool)>) -> Ear {
    let stop = Arc::new(AtomicBool::new(false));
    let muted = Arc::new(AtomicBool::new(false));
    let deaf = Arc::new(AtomicBool::new(false));
    let live = Arc::new(AtomicBool::new(true));
    let ptt = Arc::new(AtomicBool::new(false));
    let denoise = Arc::new(AtomicBool::new(true));
    let vkey = Arc::new(AtomicBool::new(false));
    let levels = Arc::new(Mutex::new(HashMap::new()));
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let thread = thread::spawn({
        let stop = stop.clone();
        let muted = muted.clone();
        let deaf = deaf.clone();
        let live = live.clone();
        let ptt = ptt.clone();
        let denoise = denoise.clone();
        let vkey = vkey.clone();
        let levels = levels.clone();
        move || {
            run(
                pipe, me, speaking, stop, muted, deaf, live, ptt, denoise, vkey, levels, ready_tx,
            )
        }
    });
    let opened = ready_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap_or(Opened {
            mic: false,
            speakers: false,
        });
    Ear {
        stop,
        muted,
        deaf,
        live,
        ptt,
        denoise,
        vkey,
        levels,
        mic: opened.mic,
        speakers: opened.speakers,
        thread: Some(thread),
    }
}

struct Opened {
    mic: bool,
    speakers: bool,
}

fn run(
    pipe: VoicePipe,
    me: u32,
    speaking: UnboundedSender<(u32, bool)>,
    stop: Arc<AtomicBool>,
    muted: Arc<AtomicBool>,
    deaf: Arc<AtomicBool>,
    live: Arc<AtomicBool>,
    ptt: Arc<AtomicBool>,
    denoise: Arc<AtomicBool>,
    vkey: Arc<AtomicBool>,
    levels: Arc<Mutex<HashMap<u32, Mix>>>,
    ready: std::sync::mpsc::SyncSender<Opened>,
) {
    let (cap_rate, mut cap_rx, mic, cap_thread) = start_capture(stop.clone());
    let (play_rate, mut play_tx, speakers_on, play_thread) = start_playback(stop.clone());
    let _ = ready.send(Opened {
        mic,
        speakers: speakers_on,
    });

    let mut encoder = Encoder::new(RATE, Channels::Mono, Application::Voip).ok();
    if let Some(encoder) = encoder.as_mut() {
        let _ = encoder.set_bitrate(Bitrate::Bits(64_000));
        let _ = encoder.set_vbr(true);
        let _ = encoder.set_inband_fec(true);
        let _ = encoder.set_packet_loss_perc(8);
    }
    let mut tone = Tone::new();
    let mut speakers: HashMap<u32, Mouth> = HashMap::new();
    let mut shown: HashMap<u32, bool> = HashMap::new();
    let mut mine = Meter::default();
    let mut seq = 0u32;
    let mut cap_pace = Pace::new(cap_rate, RATE);
    let mut play_pace = Pace::new(RATE, play_rate);
    let mut cap_48 = Vec::<f32>::new();
    let mut pending_play = Vec::<f32>::new();
    let frame_device = (play_rate / 100).max(1) as usize;

    while !stop.load(Ordering::Relaxed) {
        while let Some(packet) = pipe.try_recv() {
            ingest(&mut speakers, packet);
        }
        let keyed = if ptt.load(Ordering::Relaxed) {
            vkey.load(Ordering::Relaxed) || key_held()
        } else {
            true
        };
        if muted.load(Ordering::Relaxed) || !live.load(Ordering::Relaxed) || !keyed {
            tell(&speaking, &mut shown, me, false);
        }
        if encoder.is_some() {
            pull_capture(&mut cap_rx, &mut cap_pace, &mut cap_48);
            while cap_48.len() >= FRAME {
                let mut frame = [0f32; FRAME];
                frame.copy_from_slice(&cap_48[..FRAME]);
                cap_48.drain(..FRAME);
                shape_frame(&mut frame, denoise.load(Ordering::Relaxed), &mut tone);
                let sending =
                    live.load(Ordering::Relaxed) && !muted.load(Ordering::Relaxed) && keyed;
                let talking = sending && mine.tick(&frame);
                tell(&speaking, &mut shown, me, talking);
                if sending {
                    if let Some(encoder) = encoder.as_mut() {
                        send_frame(encoder, &pipe, seq, &frame);
                    }
                }
                seq = seq.wrapping_add(1);
            }
        }
        if play_tx.is_some() {
            let filled = RING - play_tx.as_ref().map(|tx| tx.slots()).unwrap_or(RING);
            if filled + pending_play.len() < frame_device * 2 {
                let mut mix = [0f32; FRAME];
                let heard = {
                    let levels = levels.lock().unwrap().clone();
                    mix_frame(&mut speakers, &mut mix, &speaking, &mut shown, &levels)
                };
                if deaf.load(Ordering::Relaxed) {
                    mix.fill(0.0);
                }
                if heard {
                    play_pace.convert(&mix, &mut pending_play);
                }
            }
            if let Some(tx) = play_tx.as_mut() {
                let written = {
                    let (done, _) = tx.push_partial_slice(&pending_play);
                    done.len()
                };
                pending_play.drain(..written);
            }
        }
        thread::sleep(Duration::from_millis(2));
    }
    if let Some(thread) = cap_thread {
        let _ = thread.join();
    }
    if let Some(thread) = play_thread {
        let _ = thread.join();
    }
}

struct Mouth {
    jitter: Jitter,
    decoder: Decoder,
    meter: Meter,
    losses: u32,
}

fn ingest(speakers: &mut HashMap<u32, Mouth>, packet: Vec<u8>) {
    let Ok(voice) = decode_voice(&packet) else {
        return;
    };
    if voice.payload.len() > ryme_proto::MAX_VOICE_PAYLOAD {
        return;
    }
    let mouth = match speakers.entry(voice.speaker) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let Ok(decoder) = Decoder::new(RATE, Channels::Mono) else {
                return;
            };
            entry.insert(Mouth {
                jitter: Jitter::new(),
                decoder,
                meter: Meter::default(),
                losses: 0,
            })
        }
    };
    mouth.jitter.push(voice.seq, voice.payload);
}

fn mix_frame(
    speakers: &mut HashMap<u32, Mouth>,
    mix: &mut [f32; FRAME],
    speaking: &UnboundedSender<(u32, bool)>,
    shown: &mut HashMap<u32, bool>,
    levels: &HashMap<u32, Mix>,
) -> bool {
    let ids: Vec<u32> = speakers.keys().copied().collect();
    let mut heard = false;
    let mut gone = Vec::new();
    for id in ids {
        let Some(mouth) = speakers.get_mut(&id) else {
            continue;
        };
        let mut pcm = [0f32; FRAME];
        match mouth.jitter.pull() {
            Play::Wait => {}
            Play::Packet(packet) => {
                mouth.losses = 0;
                if mouth.decoder.decode_float(&packet, &mut pcm, false).is_ok() {
                    tell(speaking, shown, id, mouth.meter.tick(&pcm));
                    if blend(&mut pcm, levels.get(&id).copied()) {
                        add(mix, &pcm);
                        heard = true;
                    }
                }
            }
            Play::Loss => {
                mouth.losses = mouth.losses.saturating_add(1);
                let fec = mouth.jitter.upcoming().map(|packet| packet.to_vec());
                if let Some(packet) = fec {
                    let _ = mouth.decoder.decode_float(&packet, &mut pcm, true);
                } else {
                    let _ = mouth.decoder.decode_float(&[], &mut pcm, false);
                }
                if mouth.losses > 30 {
                    gone.push(id);
                } else if blend(&mut pcm, levels.get(&id).copied()) {
                    add(mix, &pcm);
                    heard = true;
                }
            }
        }
    }
    for id in gone {
        speakers.remove(&id);
        tell(speaking, shown, id, false);
    }
    for sample in mix.iter_mut() {
        *sample = soft(*sample);
    }
    heard
}

fn add(mix: &mut [f32], pcm: &[f32]) {
    for (slot, sample) in mix.iter_mut().zip(pcm) {
        *slot += sample;
    }
}

fn soft(sample: f32) -> f32 {
    let sample = sample * 0.8;
    sample / (1.0 + sample.abs())
}

struct Tone {
    prev_x: f32,
    prev_y: f32,
    floor: f32,
    gate: f32,
}

impl Tone {
    fn new() -> Self {
        Self {
            prev_x: 0.0,
            prev_y: 0.0,
            floor: 0.008,
            gate: 1.0,
        }
    }
}

/// High-pass, then an optional downward expander, then a soft peak.
/// The expander is what Soften noise does. It does not add a look-ahead.
fn shape_frame(frame: &mut [f32], soften: bool, tone: &mut Tone) {
    const POLE: f32 = 0.986;
    for sample in frame.iter_mut() {
        let x = *sample;
        let y = POLE * (tone.prev_y + x - tone.prev_x);
        tone.prev_x = x;
        tone.prev_y = y;
        *sample = y;
    }
    let mut energy = 0.0f32;
    for sample in frame.iter() {
        energy += sample * sample;
    }
    let rms = (energy / frame.len().max(1) as f32).sqrt();
    if soften {
        // Follow the quiet parts only. A held voice must not become the new floor
        // and then duck itself.
        if rms <= tone.floor {
            tone.floor = tone.floor * 0.80 + rms * 0.20;
        } else if rms < tone.floor * 2.0 {
            tone.floor = tone.floor * 0.99 + rms * 0.01;
        }
        let thresh = (tone.floor * 2.8).max(0.0015);
        let target = if rms < thresh {
            let ratio = (rms / thresh).clamp(0.0, 1.0);
            (ratio * ratio).max(0.015)
        } else {
            1.0
        };
        tone.gate += (target - tone.gate) * 0.45;
    } else {
        tone.gate += (1.0 - tone.gate) * 0.45;
    }
    if (tone.gate - 1.0).abs() > 0.001 {
        for sample in frame.iter_mut() {
            *sample *= tone.gate;
        }
    }
    for sample in frame.iter_mut() {
        let x = *sample * MIC_GAIN;
        *sample = x / (1.0 + x.abs());
    }
}

fn blend(pcm: &mut [f32], level: Option<Mix>) -> bool {
    let level = level.unwrap_or(Mix {
        gain: 1.0,
        quiet: false,
    });
    if level.quiet || level.gain <= 0.001 {
        return false;
    }
    if (level.gain - 1.0).abs() > 0.001 {
        for sample in pcm.iter_mut() {
            *sample *= level.gain;
        }
    }
    true
}

fn send_frame(encoder: &mut Encoder, pipe: &VoicePipe, seq: u32, frame: &[f32]) {
    let mut packet = [0u8; 400];
    let Ok(n) = encoder.encode_float(frame, &mut packet) else {
        return;
    };
    let Ok(body) = encode_voice(&VoicePacket {
        speaker: 0,
        seq,
        timestamp: seq.wrapping_mul(FRAME as u32),
        flags: 0,
        payload: packet[..n].to_vec(),
    }) else {
        return;
    };
    pipe.send(body);
}

fn tell(
    speaking: &UnboundedSender<(u32, bool)>,
    shown: &mut HashMap<u32, bool>,
    id: u32,
    on: bool,
) {
    if shown.get(&id).copied() == Some(on) {
        return;
    }
    shown.insert(id, on);
    let _ = speaking.send((id, on));
}

#[derive(Default)]
struct Meter {
    hang: u8,
    on: bool,
}

impl Meter {
    fn tick(&mut self, frame: &[f32]) -> bool {
        let mut energy = 0.0f32;
        for sample in frame {
            energy += sample * sample;
        }
        let rms = (energy / frame.len().max(1) as f32).sqrt();
        if rms > SPEECH {
            self.hang = 30;
            self.on = true;
        } else if self.hang > 0 {
            self.hang -= 1;
            self.on = true;
        } else {
            self.on = false;
        }
        self.on
    }
}

struct Pace {
    from: u32,
    to: u32,
    pos: f64,
    held: Vec<f32>,
}

impl Pace {
    fn new(from: u32, to: u32) -> Self {
        Self {
            from: from.max(1),
            to: to.max(1),
            pos: 0.0,
            held: Vec::new(),
        }
    }

    fn convert(&mut self, input: &[f32], output: &mut Vec<f32>) {
        if self.from == self.to {
            output.extend_from_slice(input);
            return;
        }
        self.held.extend_from_slice(input);
        let step = self.from as f64 / self.to as f64;
        while self.pos + 1.0 < self.held.len() as f64 {
            let index = self.pos.floor() as usize;
            let frac = (self.pos - index as f64) as f32;
            let sample = self.held[index] + (self.held[index + 1] - self.held[index]) * frac;
            output.push(sample);
            self.pos += step;
        }
        let drop = self.pos.floor() as usize;
        if drop > 0 && drop <= self.held.len() {
            self.held.drain(..drop);
            self.pos -= drop as f64;
        }
    }
}

fn start_capture(
    stop: Arc<AtomicBool>,
) -> (u32, Option<Consumer<f32>>, bool, Option<JoinHandle<()>>) {
    let (producer, consumer) = RingBuffer::<f32>::new(RING);
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let thread = thread::spawn(move || capture_thread(stop, producer, ready_tx));
    match ready_rx.recv_timeout(Duration::from_secs(2)) {
        Ok((true, rate)) => (rate, Some(consumer), true, Some(thread)),
        _ => (RATE, None, false, Some(thread)),
    }
}

fn start_playback(
    stop: Arc<AtomicBool>,
) -> (u32, Option<Producer<f32>>, bool, Option<JoinHandle<()>>) {
    let (producer, consumer) = RingBuffer::<f32>::new(RING);
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let thread = thread::spawn(move || playback_thread(stop, consumer, ready_tx));
    match ready_rx.recv_timeout(Duration::from_secs(2)) {
        Ok((true, rate)) => (rate, Some(producer), true, Some(thread)),
        _ => (RATE, None, false, Some(thread)),
    }
}

#[cfg(target_os = "linux")]
fn capture_thread(
    stop: Arc<AtomicBool>,
    mut producer: Producer<f32>,
    ready: std::sync::mpsc::SyncSender<(bool, u32)>,
) {
    let Some(pcm) = alsa::open(true) else {
        let _ = ready.send((false, RATE));
        return;
    };
    let _ = ready.send((true, pcm.rate));
    let mut buf = vec![0i16; FRAME * pcm.channels as usize];
    while !stop.load(Ordering::Relaxed) {
        let read = unsafe {
            alsa::snd_pcm_readi(pcm.pcm, buf.as_mut_ptr().cast(), FRAME as std::ffi::c_ulong)
        };
        if read <= 0 {
            if !alsa::again(read) {
                unsafe { alsa::snd_pcm_recover(pcm.pcm, read as i32, 1) };
            }
            thread::sleep(Duration::from_millis(2));
            continue;
        }
        let frames = (read as usize).min(FRAME);
        push_i16(
            &mut producer,
            &buf[..frames * pcm.channels as usize],
            pcm.channels as usize,
        );
    }
    unsafe { alsa::snd_pcm_close(pcm.pcm) };
}

#[cfg(target_os = "linux")]
fn playback_thread(
    stop: Arc<AtomicBool>,
    mut consumer: Consumer<f32>,
    ready: std::sync::mpsc::SyncSender<(bool, u32)>,
) {
    let Some(pcm) = alsa::open(false) else {
        let _ = ready.send((false, RATE));
        return;
    };
    let _ = ready.send((true, pcm.rate));
    let channels = pcm.channels as usize;
    let mut buf = vec![0i16; FRAME * channels];
    while !stop.load(Ordering::Relaxed) {
        for frame in 0..FRAME {
            let sample = consumer.pop().unwrap_or(0.0);
            let sample = (sample.clamp(-1.0, 1.0) * 32767.0) as i16;
            for channel in 0..channels {
                buf[frame * channels + channel] = sample;
            }
        }
        let mut sent = 0usize;
        while sent < FRAME && !stop.load(Ordering::Relaxed) {
            let wrote = unsafe {
                alsa::snd_pcm_writei(
                    pcm.pcm,
                    buf[sent * channels..].as_ptr().cast(),
                    (FRAME - sent) as std::ffi::c_ulong,
                )
            };
            if wrote <= 0 {
                if !alsa::again(wrote) {
                    unsafe { alsa::snd_pcm_recover(pcm.pcm, wrote as i32, 1) };
                }
                thread::sleep(Duration::from_millis(2));
                continue;
            }
            let wrote = (wrote as usize).min(FRAME - sent);
            sent += wrote;
        }
    }
    unsafe { alsa::snd_pcm_close(pcm.pcm) };
}

#[cfg(target_os = "linux")]
mod alsa {
    use std::ffi::{c_char, c_int, c_long, c_ulong, c_void, CString};

    #[link(name = "libasound.so.2", kind = "dylib", modifiers = "+verbatim")]
    unsafe extern "C" {
        pub fn snd_pcm_open(
            pcm: *mut *mut c_void,
            name: *const c_char,
            stream: c_int,
            mode: c_int,
        ) -> c_int;
        pub fn snd_pcm_close(pcm: *mut c_void) -> c_int;
        pub fn snd_pcm_prepare(pcm: *mut c_void) -> c_int;
        pub fn snd_pcm_nonblock(pcm: *mut c_void, nonblock: c_int) -> c_int;
        pub fn snd_pcm_recover(pcm: *mut c_void, err: c_int, silent: c_int) -> c_int;
        pub fn snd_pcm_readi(pcm: *mut c_void, buffer: *mut c_void, size: c_ulong) -> c_long;
        pub fn snd_pcm_writei(pcm: *mut c_void, buffer: *const c_void, size: c_ulong) -> c_long;
        fn snd_pcm_hw_params_malloc(ptr: *mut *mut c_void) -> c_int;
        fn snd_pcm_hw_params_free(ptr: *mut c_void);
        fn snd_pcm_hw_params_any(pcm: *mut c_void, params: *mut c_void) -> c_int;
        fn snd_pcm_hw_params_set_access(
            pcm: *mut c_void,
            params: *mut c_void,
            access: c_int,
        ) -> c_int;
        fn snd_pcm_hw_params_set_format(
            pcm: *mut c_void,
            params: *mut c_void,
            format: c_int,
        ) -> c_int;
        fn snd_pcm_hw_params_set_channels(
            pcm: *mut c_void,
            params: *mut c_void,
            channels: u32,
        ) -> c_int;
        fn snd_pcm_hw_params_set_rate_near(
            pcm: *mut c_void,
            params: *mut c_void,
            rate: *mut u32,
            dir: *mut c_int,
        ) -> c_int;
        fn snd_pcm_hw_params_set_period_size_near(
            pcm: *mut c_void,
            params: *mut c_void,
            val: *mut c_ulong,
            dir: *mut c_int,
        ) -> c_int;
        fn snd_pcm_hw_params_set_buffer_size_near(
            pcm: *mut c_void,
            params: *mut c_void,
            val: *mut c_ulong,
        ) -> c_int;
        fn snd_pcm_hw_params(pcm: *mut c_void, params: *mut c_void) -> c_int;
        fn snd_pcm_hw_params_get_rate(
            params: *const c_void,
            rate: *mut u32,
            dir: *mut c_int,
        ) -> c_int;
    }

    pub struct Pcm {
        pub pcm: *mut c_void,
        pub rate: u32,
        pub channels: u32,
    }

    pub fn open(capture: bool) -> Option<Pcm> {
        let name = CString::new("default").ok()?;
        let mut pcm = std::ptr::null_mut();
        let stream = if capture { 1 } else { 0 };
        if unsafe { snd_pcm_open(&mut pcm, name.as_ptr(), stream, 0) } < 0 || pcm.is_null() {
            return None;
        }
        let mut params = std::ptr::null_mut();
        if unsafe { snd_pcm_hw_params_malloc(&mut params) } < 0 {
            unsafe { snd_pcm_close(pcm) };
            return None;
        }
        let mut rate = 48_000u32;
        let mut dir = 0;
        let mut period = super::FRAME as c_ulong;
        let mut buffer = period * 4;
        let mut channels = 1u32;
        let ok = unsafe {
            snd_pcm_hw_params_any(pcm, params) >= 0
                && snd_pcm_hw_params_set_access(pcm, params, 3) >= 0
                && snd_pcm_hw_params_set_format(pcm, params, 2) >= 0
                && snd_pcm_hw_params_set_rate_near(pcm, params, &mut rate, &mut dir) >= 0
                && {
                    if snd_pcm_hw_params_set_channels(pcm, params, 1) >= 0 {
                        channels = 1;
                        true
                    } else {
                        channels = 2;
                        snd_pcm_hw_params_set_channels(pcm, params, 2) >= 0
                    }
                }
                && {
                    let _ =
                        snd_pcm_hw_params_set_period_size_near(pcm, params, &mut period, &mut dir);
                    let _ = snd_pcm_hw_params_set_buffer_size_near(pcm, params, &mut buffer);
                    true
                }
                && snd_pcm_hw_params(pcm, params) >= 0
                && snd_pcm_prepare(pcm) >= 0
                && snd_pcm_nonblock(pcm, 1) >= 0
        };
        let _ = unsafe { snd_pcm_hw_params_get_rate(params, &mut rate, &mut dir) };
        unsafe { snd_pcm_hw_params_free(params) };
        if !ok {
            unsafe { snd_pcm_close(pcm) };
            return None;
        }
        Some(Pcm {
            pcm,
            rate,
            channels,
        })
    }

    /// EAGAIN and EINTR mean the device has nothing ready. Closing the call
    /// checks the stop flag on that pause instead of waiting out a blocking read.
    pub fn again(result: c_long) -> bool {
        matches!(result as i32, 0 | -4 | -11)
    }
}

#[cfg(not(target_os = "linux"))]
fn capture_thread(
    stop: Arc<AtomicBool>,
    mut producer: Producer<f32>,
    ready: std::sync::mpsc::SyncSender<(bool, u32)>,
) {
    use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
    let Some(device) = cpal::default_host().default_input_device() else {
        let _ = ready.send((false, RATE));
        return;
    };
    let Ok(supported) = device.default_input_config() else {
        let _ = ready.send((false, RATE));
        return;
    };
    let chosen = prefer_f32(&device, supported, true);
    let channels = chosen.channels() as usize;
    let rate = chosen.sample_rate();
    let config = stream_config(&chosen);
    let Ok(stream) = device.build_input_stream_raw(
        config,
        chosen.sample_format(),
        move |data, _| push_captured(&mut producer, data, channels),
        |_| {},
        None,
    ) else {
        let _ = ready.send((false, RATE));
        return;
    };
    if stream.play().is_err() {
        let _ = ready.send((false, RATE));
        return;
    }
    let _ = ready.send((true, rate));
    while !stop.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(not(target_os = "linux"))]
fn playback_thread(
    stop: Arc<AtomicBool>,
    mut consumer: Consumer<f32>,
    ready: std::sync::mpsc::SyncSender<(bool, u32)>,
) {
    use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
    let Some(device) = cpal::default_host().default_output_device() else {
        let _ = ready.send((false, RATE));
        return;
    };
    let Ok(supported) = device.default_output_config() else {
        let _ = ready.send((false, RATE));
        return;
    };
    let chosen = prefer_f32(&device, supported, false);
    let channels = chosen.channels() as usize;
    let rate = chosen.sample_rate();
    let config = stream_config(&chosen);
    let Ok(stream) = device.build_output_stream_raw(
        config,
        chosen.sample_format(),
        move |data, _| fill_captured(data, channels, &mut consumer),
        |_| {},
        None,
    ) else {
        let _ = ready.send((false, RATE));
        return;
    };
    if stream.play().is_err() {
        let _ = ready.send((false, RATE));
        return;
    }
    let _ = ready.send((true, rate));
    while !stop.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(not(target_os = "linux"))]
fn stream_config(supported: &cpal::SupportedStreamConfig) -> cpal::StreamConfig {
    cpal::StreamConfig {
        channels: supported.channels(),
        sample_rate: supported.sample_rate(),
        buffer_size: cpal::BufferSize::Default,
    }
}

/// Shared-mode Windows and macOS devices are often 24-bit. Asking for float
/// lets the host convert; the callback still accepts the formats it might get.
#[cfg(not(target_os = "linux"))]
fn prefer_f32(
    device: &cpal::Device,
    fallback: cpal::SupportedStreamConfig,
    input: bool,
) -> cpal::SupportedStreamConfig {
    use cpal::traits::DeviceTrait;
    if input {
        device
            .supported_input_configs()
            .ok()
            .map(|ranges| choose_f32(ranges, fallback.clone()))
            .unwrap_or(fallback)
    } else {
        device
            .supported_output_configs()
            .ok()
            .map(|ranges| choose_f32(ranges, fallback.clone()))
            .unwrap_or(fallback)
    }
}

#[cfg(not(target_os = "linux"))]
fn choose_f32(
    mut ranges: impl Iterator<Item = cpal::SupportedStreamConfigRange>,
    fallback: cpal::SupportedStreamConfig,
) -> cpal::SupportedStreamConfig {
    let rate = fallback.sample_rate();
    let channels = fallback.channels();
    ranges
        .find(|range| {
            range.channels() == channels
                && range.sample_format() == cpal::SampleFormat::F32
                && range.min_sample_rate() <= rate
                && range.max_sample_rate() >= rate
        })
        .map(|range| range.with_sample_rate(rate))
        .unwrap_or(fallback)
}

#[cfg(not(target_os = "linux"))]
fn push_captured(producer: &mut Producer<f32>, data: &cpal::Data, channels: usize) {
    use cpal::SampleFormat::*;
    match data.sample_format() {
        F32 => {
            if let Some(samples) = data.as_slice::<f32>() {
                push_f32(producer, samples, channels);
            }
        }
        F64 => {
            if let Some(samples) = data.as_slice::<f64>() {
                push_mapped(producer, samples, channels, |sample| sample as f32);
            }
        }
        I8 => {
            if let Some(samples) = data.as_slice::<i8>() {
                push_mapped(producer, samples, channels, |sample| {
                    sample as f32 / i8::MAX as f32
                });
            }
        }
        I16 => {
            if let Some(samples) = data.as_slice::<i16>() {
                push_i16(producer, samples, channels);
            }
        }
        I24 => {
            if let Some(samples) = data.as_slice::<cpal::I24>() {
                push_mapped(producer, samples, channels, |sample| {
                    sample.inner() as f32 / 8_388_608.0
                });
            }
        }
        I32 => {
            if let Some(samples) = data.as_slice::<i32>() {
                push_mapped(producer, samples, channels, |sample| {
                    sample as f32 / i32::MAX as f32
                });
            }
        }
        U8 => {
            if let Some(samples) = data.as_slice::<u8>() {
                push_mapped(producer, samples, channels, |sample| {
                    (sample as f32 - 128.0) / 128.0
                });
            }
        }
        U16 => {
            if let Some(samples) = data.as_slice::<u16>() {
                push_mapped(producer, samples, channels, |sample| {
                    (sample as f32 - 32_768.0) / 32_768.0
                });
            }
        }
        _ => {}
    }
}

#[cfg(not(target_os = "linux"))]
fn fill_captured(data: &mut cpal::Data, channels: usize, consumer: &mut Consumer<f32>) {
    use cpal::SampleFormat::*;
    match data.sample_format() {
        F32 => {
            if let Some(samples) = data.as_slice_mut::<f32>() {
                fill_f32(samples, channels, consumer);
            }
        }
        F64 => {
            if let Some(samples) = data.as_slice_mut::<f64>() {
                fill_mapped(samples, channels, consumer, |sample| sample as f64);
            }
        }
        I16 => {
            if let Some(samples) = data.as_slice_mut::<i16>() {
                fill_i16(samples, channels, consumer);
            }
        }
        I24 => {
            if let Some(samples) = data.as_slice_mut::<cpal::I24>() {
                fill_mapped(samples, channels, consumer, |sample| {
                    cpal::I24::new_unchecked((sample.clamp(-1.0, 1.0) * 8_388_607.0) as i32)
                });
            }
        }
        I32 => {
            if let Some(samples) = data.as_slice_mut::<i32>() {
                fill_mapped(samples, channels, consumer, |sample| {
                    (sample.clamp(-1.0, 1.0) * i32::MAX as f32) as i32
                });
            }
        }
        _ => {}
    }
}

#[cfg(not(target_os = "linux"))]
fn push_mapped<T: Copy>(
    producer: &mut Producer<f32>,
    samples: &[T],
    channels: usize,
    map: impl Fn(T) -> f32,
) {
    if channels <= 1 {
        for sample in samples {
            if producer.push(map(*sample)).is_err() {
                break;
            }
        }
        return;
    }
    for frame in samples.chunks(channels) {
        let sample = frame.iter().map(|sample| map(*sample)).sum::<f32>() / channels as f32;
        if producer.push(sample).is_err() {
            break;
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn fill_mapped<T: Copy>(
    samples: &mut [T],
    channels: usize,
    consumer: &mut Consumer<f32>,
    map: impl Fn(f32) -> T,
) {
    if channels <= 1 {
        for sample in samples {
            *sample = map(consumer.pop().unwrap_or(0.0));
        }
        return;
    }
    for frame in samples.chunks_mut(channels) {
        let mapped = map(consumer.pop().unwrap_or(0.0));
        for channel in frame {
            *channel = mapped;
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn push_f32(producer: &mut Producer<f32>, samples: &[f32], channels: usize) {
    if channels <= 1 {
        let _ = producer.push_partial_slice(samples);
        return;
    }
    for frame in samples.chunks(channels) {
        let sample = frame.iter().copied().sum::<f32>() / channels as f32;
        if producer.push(sample).is_err() {
            break;
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn fill_f32(samples: &mut [f32], channels: usize, consumer: &mut Consumer<f32>) {
    if channels <= 1 {
        let (_done, rest) = consumer.pop_partial_slice(samples);
        rest.fill(0.0);
        return;
    }
    for frame in samples.chunks_mut(channels) {
        let sample = consumer.pop().unwrap_or(0.0);
        for channel in frame {
            *channel = sample;
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn fill_i16(samples: &mut [i16], channels: usize, consumer: &mut Consumer<f32>) {
    for frame in samples.chunks_mut(channels.max(1)) {
        let sample = consumer.pop().unwrap_or(0.0);
        let sample = (sample.clamp(-1.0, 1.0) * 32767.0) as i16;
        for channel in frame {
            *channel = sample;
        }
    }
}

fn pull_capture(rx: &mut Option<Consumer<f32>>, pace: &mut Pace, into: &mut Vec<f32>) {
    let Some(rx) = rx else {
        return;
    };
    let mut scratch = [0f32; 1024];
    loop {
        let n = {
            let (got, _) = rx.pop_partial_slice(&mut scratch);
            got.len()
        };
        if n == 0 {
            break;
        }
        pace.convert(&scratch[..n], into);
    }
}

fn push_i16(producer: &mut Producer<f32>, samples: &[i16], channels: usize) {
    if channels <= 1 {
        for sample in samples {
            let sample = *sample as f32 / 32768.0;
            if producer.push(sample).is_err() {
                break;
            }
        }
        return;
    }
    for frame in samples.chunks(channels) {
        let sample =
            frame.iter().map(|sample| *sample as f32).sum::<f32>() / (channels as f32 * 32768.0);
        if producer.push(sample).is_err() {
            break;
        }
    }
}

/// V, wherever the card is. A desktop we cannot read does not mute an open mic
/// that the switch has put into push-to-talk.
fn key_held() -> bool {
    #[cfg(windows)]
    {
        return win_key_held();
    }
    #[cfg(target_os = "linux")]
    {
        return linux_key_held().unwrap_or(false) || evdev_key_held();
    }
    #[cfg(target_os = "macos")]
    {
        return mac_key_held();
    }
    #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
    {
        true
    }
}

#[cfg(windows)]
fn win_key_held() -> bool {
    #[link(name = "user32")]
    extern "system" {
        fn GetAsyncKeyState(key: i32) -> i16;
    }
    // VK_V. The high bit is set while it is down.
    unsafe { GetAsyncKeyState(0x56) < 0 }
}

#[cfg(target_os = "macos")]
fn mac_key_held() -> bool {
    #[link(name = "CoreGraphics", kind = "framework")]
    extern "C" {
        fn CGEventSourceKeyState(state: i32, key: u16) -> bool;
    }
    // HID system state, kVK_ANSI_V.
    unsafe { CGEventSourceKeyState(1, 0x09) }
}

#[cfg(target_os = "linux")]
fn evdev_key_held() -> bool {
    use std::cell::RefCell;
    thread_local! {
        static BOARD: RefCell<Option<Evdev>> = const { RefCell::new(None) };
    }
    BOARD.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            *slot = Some(Evdev::open());
        }
        slot.as_mut().map(|board| board.down()).unwrap_or(false)
    })
}

#[cfg(target_os = "linux")]
struct Evdev {
    files: Vec<std::fs::File>,
    held: i32,
}

#[cfg(target_os = "linux")]
impl Evdev {
    fn open() -> Self {
        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;
        let mut files = Vec::new();
        for index in 0..32 {
            let path = format!("/dev/input/event{index}");
            let Ok(file) = OpenOptions::new()
                .read(true)
                .custom_flags(0x800)
                .open(&path)
            else {
                continue;
            };
            files.push(file);
        }
        Self { files, held: 0 }
    }

    fn down(&mut self) -> bool {
        use std::io::Read;
        let mut dead = Vec::new();
        for (index, file) in self.files.iter_mut().enumerate() {
            let mut buf = [0u8; 24];
            loop {
                match file.read(&mut buf) {
                    Ok(24) => {
                        let kind = u16::from_ne_bytes([buf[16], buf[17]]);
                        let code = u16::from_ne_bytes([buf[18], buf[19]]);
                        let value = i32::from_ne_bytes([buf[20], buf[21], buf[22], buf[23]]);
                        // EV_KEY, KEY_V. Repeat (2) does not change the hold.
                        if kind == 1 && code == 47 {
                            if value == 1 {
                                self.held = self.held.saturating_add(1);
                            } else if value == 0 {
                                self.held = self.held.saturating_sub(1);
                            }
                        }
                    }
                    Ok(0) => {
                        dead.push(index);
                        break;
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        dead.push(index);
                        break;
                    }
                    Ok(_) => break,
                }
            }
        }
        for index in dead.into_iter().rev() {
            self.files.swap_remove(index);
        }
        self.held > 0
    }
}

#[cfg(target_os = "linux")]
fn linux_key_held() -> Option<bool> {
    use std::cell::{Cell, RefCell};
    thread_local! {
        static KEYS: RefCell<Option<XKeys>> = const { RefCell::new(None) };
        static FAILED: Cell<bool> = const { Cell::new(false) };
    }
    if FAILED.with(|failed| failed.get()) {
        return None;
    }
    KEYS.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            match XKeys::open() {
                Some(keys) => *slot = Some(keys),
                None => {
                    FAILED.with(|failed| failed.set(true));
                    return None;
                }
            }
        }
        slot.as_mut().and_then(|keys| keys.down())
    })
}

#[cfg(target_os = "linux")]
struct XKeys {
    conn: x11rb::rust_connection::RustConnection,
    codes: Vec<u8>,
    at: std::time::Instant,
    down: bool,
}

#[cfg(target_os = "linux")]
impl XKeys {
    fn open() -> Option<Self> {
        use x11rb::connection::Connection;
        use x11rb::protocol::xproto::ConnectionExt;
        let (conn, _) = x11rb::connect(None).ok()?;
        let (first, count) = {
            let setup = conn.setup();
            (
                setup.min_keycode,
                setup
                    .max_keycode
                    .saturating_sub(setup.min_keycode)
                    .saturating_add(1),
            )
        };
        let reply = conn.get_keyboard_mapping(first, count).ok()?.reply().ok()?;
        let per = reply.keysyms_per_keycode as usize;
        if per == 0 {
            return None;
        }
        let mut codes = Vec::new();
        for (index, group) in reply.keysyms.chunks(per).enumerate() {
            // XK_v and XK_V.
            if group.iter().any(|sym| *sym == 0x76 || *sym == 0x56) {
                codes.push(first.saturating_add(index as u8));
            }
        }
        if codes.is_empty() {
            return None;
        }
        Some(Self {
            conn,
            codes,
            at: std::time::Instant::now() - Duration::from_secs(1),
            down: false,
        })
    }

    fn down(&mut self) -> Option<bool> {
        use x11rb::protocol::xproto::ConnectionExt;
        if self.at.elapsed() < Duration::from_millis(20) {
            return Some(self.down);
        }
        let reply = self.conn.query_keymap().ok()?.reply().ok()?;
        self.at = std::time::Instant::now();
        self.down = self.codes.iter().any(|code| {
            let code = *code as usize;
            reply
                .keys
                .get(code / 8)
                .map(|byte| byte & (1 << (code % 8)) != 0)
                .unwrap_or(false)
        });
        Some(self.down)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soften_pulls_room_noise_down_and_leaves_a_voice() {
        let mut tone = Tone::new();
        let mut noise = [0f32; FRAME];
        for (index, sample) in noise.iter_mut().enumerate() {
            *sample = (index as f32 * 0.37).sin() * 0.0015;
        }
        shape_frame(&mut noise, true, &mut tone);
        let noise_rms = rms(&noise);
        let mut voice = [0f32; FRAME];
        for (index, sample) in voice.iter_mut().enumerate() {
            *sample = (index as f32 * 0.15).sin() * 0.2;
        }
        for _ in 0..8 {
            shape_frame(&mut voice.clone(), true, &mut tone);
        }
        shape_frame(&mut voice, true, &mut tone);
        let voice_rms = rms(&voice);
        assert!(
            noise_rms < 0.02,
            "noise should be softened, rms {noise_rms}"
        );
        assert!(
            voice_rms > noise_rms * 4.0,
            "voice {voice_rms} should stay well above the noise {noise_rms}"
        );
    }

    #[test]
    fn a_held_voice_does_not_become_the_noise_floor() {
        let mut tone = Tone::new();
        let mut frame = [0f32; FRAME];
        for (index, sample) in frame.iter_mut().enumerate() {
            *sample = (index as f32 * 0.13).sin() * 0.12;
        }
        for _ in 0..400 {
            let mut pass = frame;
            shape_frame(&mut pass, true, &mut tone);
        }
        shape_frame(&mut frame, true, &mut tone);
        let voice = rms(&frame);
        assert!(
            voice > 0.05,
            "a held voice was ducked to {voice} (floor {})",
            tone.floor
        );
    }

    #[test]
    fn a_hot_mic_is_rounded_instead_of_squared_off() {
        let mut tone = Tone::new();
        let mut frame = [0f32; FRAME];
        for (index, sample) in frame.iter_mut().enumerate() {
            *sample = (index as f32 * 0.2).sin() * 0.9;
        }
        shape_frame(&mut frame, false, &mut tone);
        let peak = frame
            .iter()
            .fold(0.0f32, |peak, sample| peak.max(sample.abs()));
        assert!(peak < 0.95, "peak {peak}");
        assert!(peak > 0.4, "peak {peak}");
    }

    fn rms(frame: &[f32]) -> f32 {
        let energy: f32 = frame.iter().map(|sample| sample * sample).sum();
        (energy / frame.len() as f32).sqrt()
    }

    #[test]
    fn quiet_room_stays_dark_and_speech_lights_the_ring() {
        let mut meter = Meter::default();
        assert!(!meter.tick(&[0.004; FRAME]));
        assert!(meter.tick(&[0.05; FRAME]));
        assert!(meter.tick(&[0.0; FRAME]));
    }

    #[test]
    fn a_ten_ms_frame_roundtrips_through_opus() {
        let mut encoder = Encoder::new(RATE, Channels::Mono, Application::Voip).unwrap();
        encoder.set_bitrate(Bitrate::Bits(64_000)).unwrap();
        let mut pcm = [0f32; FRAME];
        for (index, sample) in pcm.iter_mut().enumerate() {
            *sample = (index as f32 * 0.07).sin() * 0.2;
        }
        let mut packet = [0u8; 400];
        let size = encoder.encode_float(&pcm, &mut packet).unwrap();
        assert!(size > 0 && size <= 400);
        let mut decoder = Decoder::new(RATE, Channels::Mono).unwrap();
        let mut out = [0f32; FRAME];
        let decoded = decoder
            .decode_float(&packet[..size], &mut out, false)
            .unwrap();
        assert_eq!(decoded, FRAME);
        let energy: f32 = out.iter().map(|sample| sample * sample).sum();
        assert!(energy > 0.1, "energy {energy}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_default_sound_device_can_be_opened() {
        let mic = super::alsa::open(true);
        let speakers = super::alsa::open(false);
        let mic_ok = mic.is_some();
        let speakers_ok = speakers.is_some();
        if let Some(pcm) = mic {
            unsafe { super::alsa::snd_pcm_close(pcm.pcm) };
        }
        if let Some(pcm) = speakers {
            unsafe { super::alsa::snd_pcm_close(pcm.pcm) };
        }
        assert!(
            mic_ok && speakers_ok,
            "microphone {mic_ok}, speakers {speakers_ok}"
        );
    }
}
