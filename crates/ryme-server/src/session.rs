use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ryme_proto::{
    decode_client_frame, encode_server, stamp_speaker, ClientMsg, ProtoError, ServerMsg,
};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;

use crate::hub::{Hub, Step};
use crate::identity;

const CONTROL_QUEUE: usize = 16;
const MAX_CONNECTIONS: usize = 64;
const CONTROL_PER_SECOND: u32 = 30;
const VOICE_PER_SECOND: u32 = 120;
const ADMIT_WAIT: Duration = Duration::from_secs(5);
const WRITE_WAIT: Duration = Duration::from_secs(2);

static OPEN: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
struct VoiceNote {
    seq: u64,
    packet: Bytes,
}

#[derive(Clone)]
struct VoiceTx {
    next: Arc<AtomicU64>,
    tx: watch::Sender<Option<VoiceNote>>,
}

impl VoiceTx {
    fn pair() -> (Self, watch::Receiver<Option<VoiceNote>>) {
        let (tx, rx) = watch::channel(None);
        (
            Self {
                next: Arc::new(AtomicU64::new(0)),
                tx,
            },
            rx,
        )
    }

    /// Replace any voice packet the writer has not sent yet.
    fn offer(&self, packet: Bytes) {
        let seq = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.tx.send(Some(VoiceNote { seq, packet }));
    }
}

#[derive(Clone)]
struct Pipes {
    control: Sender<ServerMsg>,
    voice: VoiceTx,
    close: Sender<()>,
}

pub(crate) struct World {
    hub: Hub,
    pipes: HashMap<u32, Pipes>,
    data: Option<std::path::PathBuf>,
}

impl World {
    fn new(pass: [u8; 32], data: Option<std::path::PathBuf>) -> Self {
        let mut hub = Hub::new(pass);
        if let Some(dir) = &data {
            if std::fs::read(dir.join("locked")).is_ok() {
                hub.set_locked(true);
            }
            if let Ok(text) = std::fs::read_to_string(dir.join("banned")) {
                for line in text.lines() {
                    if let Ok(card) = line.trim().parse::<u64>() {
                        hub.ban(card);
                    }
                }
            }
        }
        Self {
            hub,
            pipes: HashMap::new(),
            data,
        }
    }

    fn handle(&mut self, user: Option<u32>, msg: ClientMsg, pipes: &Pipes) -> Option<u32> {
        let admitted = if user.is_none() {
            if let ClientMsg::Hello { name, card, .. } = &msg {
                Some((name.clone(), *card))
            } else {
                None
            }
        } else {
            None
        };
        let step = self.hub.handle(user, msg);
        if let (Some((name, card)), Some(id)) = (admitted, step.user_id) {
            tracing::info!(id, card = format!("{card:016x}"), %name, "admitted");
        }
        if let Some(old) = step.replace {
            if let Some(old_pipes) = self.pipes.remove(&old) {
                let _ = old_pipes.close.try_send(());
            }
        }
        if let Some(id) = step.user_id {
            self.pipes.insert(id, pipes.clone());
        }
        self.dispatch(&step, pipes);
        step.user_id
    }

    fn voice(&mut self, from: u32, packet: Bytes) {
        let mut packet = packet.to_vec();
        if stamp_speaker(&mut packet, from).is_err() {
            return;
        }
        let targets = self.hub.voice_targets(from, &packet);
        let packet = Bytes::from(packet);
        for id in targets {
            if let Some(pipes) = self.pipes.get(&id) {
                offer_voice(&pipes.voice, packet.clone());
            }
        }
    }

    fn allow(&mut self, name: &str) {
        self.hub.allow(name);
    }

    fn disconnect(&mut self, id: u32) {
        let notices = self.hub.disconnect(id);
        self.pipes.remove(&id);
        for (peer, msg) in notices {
            if let Some(pipes) = self.pipes.get(&peer) {
                offer_control(pipes, msg);
            }
        }
    }

    fn kick(&mut self, id: u32) {
        if let Some(pipes) = self.pipes.get(&id) {
            let _ = pipes.close.try_send(());
        }
        self.disconnect(id);
    }

    fn set_locked(&mut self, locked: bool) {
        self.hub.set_locked(locked);
        if let Some(dir) = &self.data {
            let path = dir.join("locked");
            if locked {
                let _ = std::fs::write(&path, b"1\n");
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    fn ban_card(&mut self, card: u64, banned: bool) {
        if banned {
            self.hub.ban(card);
            let ids: Vec<u32> = self
                .hub_cards()
                .into_iter()
                .filter(|(_, existing)| *existing == card)
                .map(|(id, _)| id)
                .collect();
            for id in ids {
                self.kick(id);
            }
        } else {
            self.hub.unban(card);
        }
        self.store_bans();
    }

    fn hub_cards(&self) -> Vec<(u32, u64)> {
        self.hub.cards()
    }

    fn store_bans(&self) {
        let Some(dir) = &self.data else {
            return;
        };
        let mut lines = String::new();
        for card in self.hub.banned_cards() {
            lines.push_str(&format!("{card}\n"));
        }
        let _ = std::fs::write(dir.join("banned"), lines);
    }

    fn dispatch(&self, step: &Step, self_pipes: &Pipes) {
        for msg in &step.to_self {
            offer_control(self_pipes, msg.clone());
        }
        for (peer, msg) in &step.to_others {
            if let Some(pipes) = self.pipes.get(peer) {
                offer_control(pipes, msg.clone());
            }
        }
    }
}

/// Take a connection slot. False when the server is already full.
pub(crate) fn claim_connection() -> bool {
    let open = OPEN.fetch_add(1, Ordering::Relaxed);
    if open >= MAX_CONNECTIONS {
        OPEN.fetch_sub(1, Ordering::Relaxed);
        false
    } else {
        true
    }
}

pub(crate) fn release_connection() {
    OPEN.fetch_sub(1, Ordering::Relaxed);
}

pub async fn run(conn: quinn::Connection, world: Arc<Mutex<World>>) {
    let peer = conn.remote_address();
    tracing::info!(%peer, "connected");
    let (fail_tx, mut fail_rx) = watch::channel(false);
    let admitted = tokio::time::Instant::now() + ADMIT_WAIT;
    let accepted = tokio::time::timeout(ADMIT_WAIT, conn.accept_bi()).await;
    let Ok(Ok((send, mut recv))) = accepted else {
        tracing::info!(%peer, "disconnected");
        conn.close(quinn::VarInt::from_u32(0), b"bye");
        return;
    };

    let (control_tx, control_rx) = tokio::sync::mpsc::channel(CONTROL_QUEUE);
    let (voice_tx, voice_rx) = VoiceTx::pair();
    let (close_tx, mut close_rx) = tokio::sync::mpsc::channel(1);
    let pipes = Pipes {
        control: control_tx,
        voice: voice_tx,
        close: close_tx,
    };
    let writer = tokio::spawn(write_out(send, conn.clone(), control_rx, voice_rx, fail_tx));

    let mut user_id = None;
    let mut buf = Vec::new();
    let mut tmp = [0u8; 2048];
    let mut pace = Pace::new();
    loop {
        if user_id.is_none() && tokio::time::Instant::now() >= admitted {
            break;
        }
        tokio::select! {
            _ = fail_rx.changed() => break,
            _ = close_rx.recv() => break,
            _ = tokio::time::sleep_until(admitted), if user_id.is_none() => break,
            read = recv.read(&mut tmp) => {
                match read {
                    Ok(Some(n)) if n > 0 => {
                        buf.extend_from_slice(&tmp[..n]);
                        if buf.len() > ryme_proto::MAX_FRAME {
                            break;
                        }
                        match drain_frames(&mut buf, &world, &mut user_id, &pipes, &mut pace) {
                            Ok(true) => {}
                            Ok(false) | Err(_) => break,
                        }
                    }
                    _ => break,
                }
            }
            datagram = conn.read_datagram() => {
                match datagram {
                    Ok(packet) => {
                        if !pace.allow(true) {
                            break;
                        }
                        let Some(id) = user_id else { continue };
                        world.lock().expect("hub").voice(id, packet);
                    }
                    Err(_) => break,
                }
            }
        }
    }

    if let Some(id) = user_id {
        world.lock().expect("hub").disconnect(id);
    }
    drop(pipes);
    writer.abort();
    conn.close(quinn::VarInt::from_u32(0), b"bye");
    tracing::info!(%peer, "disconnected");
}

fn drain_frames(
    buf: &mut Vec<u8>,
    world: &Arc<Mutex<World>>,
    user_id: &mut Option<u32>,
    pipes: &Pipes,
    pace: &mut Pace,
) -> Result<bool, ProtoError> {
    loop {
        match decode_client_frame(buf)? {
            Some((msg, n)) => {
                buf.drain(..n);
                if !pace.allow(false) {
                    return Ok(false);
                }
                let mut world = world.lock().expect("hub");
                if let Some(id) = world.handle(*user_id, msg, pipes) {
                    *user_id = Some(id);
                }
            }
            None => return Ok(true),
        }
    }
}

async fn write_out(
    mut send: quinn::SendStream,
    conn: quinn::Connection,
    mut control_rx: Receiver<ServerMsg>,
    mut voice_rx: watch::Receiver<Option<VoiceNote>>,
    fail: watch::Sender<bool>,
) {
    loop {
        tokio::select! {
            biased;
            changed = voice_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let packet = voice_rx.borrow_and_update().as_ref().map(|note| note.packet.clone());
                let Some(packet) = packet else { continue };
                if conn.send_datagram(packet).is_err() {
                    let _ = fail.send(true);
                    break;
                }
            }
            msg = control_rx.recv() => {
                let Some(msg) = msg else { break };
                let Ok(frame) = encode_server(&msg) else {
                    let _ = fail.send(true);
                    break;
                };
                let wrote = tokio::time::timeout(WRITE_WAIT, send.write_all(&frame)).await;
                let ok = matches!(wrote, Ok(Ok(())));
                if !ok {
                    let _ = fail.send(true);
                    break;
                }
            }
        }
    }
}

fn offer_voice(tx: &VoiceTx, packet: Bytes) {
    tx.offer(packet);
}

fn offer_control(pipes: &Pipes, msg: ServerMsg) {
    if pipes.control.try_send(msg).is_err() {
        let _ = pipes.close.try_send(());
    }
}

struct Pace {
    window: Instant,
    control: u32,
    voice: u32,
}

impl Pace {
    fn new() -> Self {
        Self {
            window: Instant::now(),
            control: 0,
            voice: 0,
        }
    }

    fn allow(&mut self, voice: bool) -> bool {
        if self.window.elapsed() >= Duration::from_secs(1) {
            self.window = Instant::now();
            self.control = 0;
            self.voice = 0;
        }
        if voice {
            self.voice += 1;
            self.voice <= VOICE_PER_SECOND
        } else {
            self.control += 1;
            self.control <= CONTROL_PER_SECOND
        }
    }
}

pub(crate) fn world(pass: [u8; 32], data: Option<std::path::PathBuf>) -> Arc<Mutex<World>> {
    Arc::new(Mutex::new(World::new(pass, data)))
}

pub(crate) fn allow(world: &Mutex<World>, name: &str) {
    world.lock().expect("hub").allow(name);
}

fn apply_host(text: &str, world: &Mutex<World>) {
    let text = text.trim();
    let mut hub = world.lock().expect("hub");
    if let Some(id) = text.strip_prefix("drop ").and_then(|id| id.parse().ok()) {
        hub.kick(id);
    } else if text == "lock" {
        hub.set_locked(true);
    } else if text == "unlock" {
        hub.set_locked(false);
    } else if let Some(card) = text.strip_prefix("ban ").and_then(parse_card) {
        hub.ban_card(card, true);
    } else if let Some(card) = text.strip_prefix("unban ").and_then(parse_card) {
        hub.ban_card(card, false);
    }
}

/// A card from the server log. Sixteen hex digits, or a decimal number.
fn parse_card(text: &str) -> Option<u64> {
    let text = text.trim();
    let card = if let Some(hex) = text.strip_prefix("0x") {
        u64::from_str_radix(hex, 16).ok()?
    } else if text.len() == 16 && text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        u64::from_str_radix(text, 16).ok()?
    } else {
        text.parse().ok()?
    };
    (card != 0).then_some(card)
}

/// Local control socket. The first 32 bytes are the host key. Then one line:
/// `drop <id>`, `lock`, `unlock`, `ban <card>`, or `unban <card>`.
/// Anything else is ignored. The host key never leaves this machine.
pub(crate) async fn listen_host(addr: SocketAddr, key: [u8; 32], world: Arc<Mutex<World>>) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(_) => {
            tracing::info!(%addr, "host control not listening");
            return;
        }
    };
    tracing::info!(%addr, "host control listening");
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        let world = Arc::clone(&world);
        tokio::spawn(async move {
            let _ = sock.set_nodelay(true);
            let mut got = [0u8; 32];
            let read =
                tokio::time::timeout(Duration::from_secs(2), read_exact(&mut sock, &mut got));
            if !matches!(read.await, Ok(Ok(()))) || !identity::same(&got, &key) {
                return;
            }
            let mut line = Vec::new();
            let mut byte = [0u8; 1];
            while line.len() < 64 {
                let read =
                    tokio::time::timeout(Duration::from_secs(2), read_exact(&mut sock, &mut byte));
                if !matches!(read.await, Ok(Ok(()))) {
                    return;
                }
                if byte[0] == b'\n' {
                    break;
                }
                line.push(byte[0]);
            }
            let Ok(text) = std::str::from_utf8(&line) else {
                return;
            };
            apply_host(text, &world);
        });
    }
}

async fn read_exact(sock: &mut tokio::net::TcpStream, buf: &mut [u8]) -> std::io::Result<()> {
    use tokio::io::AsyncReadExt;
    sock.read_exact(buf).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_congested_voice_queue_keeps_the_newest_packet() {
        let (tx, mut rx) = VoiceTx::pair();
        offer_voice(&tx, Bytes::from_static(b"stale"));
        offer_voice(&tx, Bytes::from_static(b"fresh"));
        rx.changed().await.unwrap();
        let note = rx.borrow().clone().unwrap();
        assert_eq!(note.seq, 2);
        assert_eq!(note.packet.as_ref(), b"fresh");
    }

    #[test]
    fn a_client_past_the_rate_is_not_allowed_another_frame() {
        let mut pace = Pace::new();
        for _ in 0..CONTROL_PER_SECOND {
            assert!(pace.allow(false));
        }
        assert!(!pace.allow(false));
        for _ in 0..VOICE_PER_SECOND {
            assert!(pace.allow(true));
        }
        assert!(!pace.allow(true));
    }

    #[test]
    fn a_card_from_the_log_is_hex() {
        assert_eq!(parse_card("0000000000000004"), Some(4));
        assert_eq!(parse_card("4"), Some(4));
        assert_eq!(parse_card("0"), None);
        assert_eq!(parse_card("zz"), None);
    }
}
