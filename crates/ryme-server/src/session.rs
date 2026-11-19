use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ryme_proto::{
    decode_tagged, encode_server, encode_tagged, stamp_speaker, take_client_frame, ClientFrame,
    ClientMsg, ProtoError, ServerMsg, TCP_CONTROL, TCP_VOICE,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;

use crate::hub::{Hub, Step};
use crate::identity;
use crate::queue::{self, FairReceiver, FairSender};

/// Room history is up to 200 lines, sent when someone joins. The queue has to
/// hold that burst. A frame that does not fit is dropped; it does not end the call.
const CONTROL_QUEUE: usize = 256;
const VOICE_PER_SPEAKER: usize = 8;
const VOICE_PER_LISTENER: usize = 32;
const MAX_CONNECTIONS: usize = 64;
const CONTROL_PER_SECOND: u32 = 30;
// A TCP read can deliver a backlog in one go. The outbound queue keeps the
// newest frames of that burst. This cap only stops a flood.
const VOICE_PER_SECOND: u32 = 500;
const ADMIT_WAIT: Duration = Duration::from_secs(5);
// A slow TCP write must not be cancelled. Two seconds was cutting a voice
// frame in half on a laggy link and the session then dropped and reconnected.
const WRITE_WAIT: Duration = Duration::from_secs(15);

static OPEN: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
struct Pipes {
    control: Sender<ServerMsg>,
    voice: FairSender<Bytes>,
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

    /// Stamp the speaker and clone each listener's queue. The caller offers
    /// outside the world lock, so one slow queue does not stall the room.
    fn route_voice(&mut self, from: u32, packet: Bytes) -> Vec<(FairSender<Bytes>, Bytes)> {
        let mut packet = packet.to_vec();
        if stamp_speaker(&mut packet, from).is_err() {
            return Vec::new();
        }
        let targets = self.hub.voice_targets(from, &packet);
        let packet = Bytes::from(packet);
        targets
            .into_iter()
            .filter_map(|id| {
                self.pipes
                    .get(&id)
                    .map(|pipes| (pipes.voice.clone(), packet.clone()))
            })
            .collect()
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
    let (voice_tx, voice_rx) =
        queue::fair(VOICE_PER_SPEAKER, VOICE_PER_LISTENER, queue::VOICE_MAX_AGE);
    let (close_tx, mut close_rx) = tokio::sync::mpsc::channel(1);
    let pipes = Pipes {
        control: control_tx,
        voice: voice_tx,
        close: close_tx,
    };
    let control_task = tokio::spawn(write_control(send, control_rx, fail_tx.clone()));
    let voice_task = tokio::spawn(write_datagrams(conn.clone(), voice_rx, fail_tx));

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
                        if user_id.is_none() && ryme_proto::decode_probe(&packet).is_some() {
                            let _ = conn.send_datagram(packet);
                            continue;
                        }
                        // A burst over the cap is discarded. Hanging up here is
                        // what made a live microphone reconnect every few seconds.
                        if !pace.allow(true) {
                            continue;
                        }
                        let Some(id) = user_id else { continue };
                        deliver_voice(&world, id, packet);
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
    control_task.abort();
    voice_task.abort();
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
        match take_client_frame(buf)? {
            Some((ClientFrame::Refused, n)) => {
                buf.drain(..n);
                // A bad line is a no. It is not a reason to drop the call.
                let _ = pipes.control.try_send(ServerMsg::Reject {
                    reason: "message was not sent".into(),
                });
                continue;
            }
            Some((ClientFrame::Message(msg), n)) => {
                buf.drain(..n);
                if let ClientMsg::Ping { token } = msg {
                    // A full control queue drops the echo. It does not close the call.
                    let _ = pipes.control.try_send(ServerMsg::Pong { token });
                    continue;
                }
                if let ClientMsg::Probe { pass, nonce } = msg {
                    // A probe must not take the seat. The live call stays up
                    // until a datagram echo has proved this path.
                    let ack = world.lock().expect("hub").hub.admits(&pass);
                    let reply = if ack {
                        ServerMsg::ProbeAck { nonce }
                    } else {
                        ServerMsg::Reject {
                            reason: "invite was retired".into(),
                        }
                    };
                    let _ = pipes.control.try_send(reply);
                    continue;
                }
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

async fn write_control(
    mut send: quinn::SendStream,
    mut control_rx: Receiver<ServerMsg>,
    fail: watch::Sender<bool>,
) {
    loop {
        let Some(msg) = control_rx.recv().await else {
            break;
        };
        let Ok(frame) = encode_server(&msg) else {
            let _ = fail.send(true);
            break;
        };
        let wrote = tokio::time::timeout(WRITE_WAIT, send.write_all(&frame)).await;
        if !matches!(wrote, Ok(Ok(()))) {
            let _ = fail.send(true);
            break;
        }
    }
}

async fn write_datagrams(
    conn: quinn::Connection,
    mut voice_rx: FairReceiver<Bytes>,
    fail: watch::Sender<bool>,
) {
    loop {
        let Some((_speaker, packet)) = voice_rx.recv().await else {
            break;
        };
        if conn.send_datagram(packet).is_err() {
            // A full datagram buffer drops this packet. Closing here would
            // end the call. A dead connection is the case that stops the writer.
            if conn.close_reason().is_some() {
                let _ = fail.send(true);
                break;
            }
        }
    }
}

fn deliver_voice(world: &Arc<Mutex<World>>, from: u32, packet: Bytes) {
    let routed = {
        let mut world = world.lock().expect("hub");
        world.route_voice(from, packet)
    };
    for (voice, packet) in routed {
        offer_voice(&voice, packet);
    }
}

fn offer_voice(voice: &FairSender<Bytes>, packet: Bytes) {
    let Some(speaker) = packet.get(1..5) else {
        return;
    };
    let Ok(speaker) = speaker.try_into() else {
        return;
    };
    voice.offer(u32::from_le_bytes(speaker), packet);
}

pub(crate) async fn run_tcp<S>(peer: SocketAddr, stream: S, world: Arc<Mutex<World>>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tracing::info!(%peer, transport = "tcp", "connected");
    let (fail_tx, mut fail_rx) = watch::channel(false);
    let (mut read, write) = tokio::io::split(stream);
    let (control_tx, control_rx) = tokio::sync::mpsc::channel(CONTROL_QUEUE);
    let (voice_tx, voice_rx) =
        queue::fair(VOICE_PER_SPEAKER, VOICE_PER_LISTENER, queue::VOICE_MAX_AGE);
    let (close_tx, mut close_rx) = tokio::sync::mpsc::channel(1);
    let pipes = Pipes {
        control: control_tx,
        voice: voice_tx,
        close: close_tx,
    };
    let writer = tokio::spawn(write_tcp(write, control_rx, voice_rx, fail_tx));

    let admitted = tokio::time::Instant::now() + ADMIT_WAIT;
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
            read = read_some(&mut read, &mut tmp) => {
                match read {
                    Ok(n) if n > 0 => {
                        buf.extend_from_slice(&tmp[..n]);
                        if !drain_tcp(&mut buf, &world, &mut user_id, &pipes, &mut pace) {
                            break;
                        }
                    }
                    _ => break,
                }
            }
        }
    }

    if let Some(id) = user_id {
        world.lock().expect("hub").disconnect(id);
    }
    drop(pipes);
    writer.abort();
    tracing::info!(%peer, transport = "tcp", "disconnected");
}

fn drain_tcp(
    buf: &mut Vec<u8>,
    world: &Arc<Mutex<World>>,
    user_id: &mut Option<u32>,
    pipes: &Pipes,
    pace: &mut Pace,
) -> bool {
    loop {
        let frame = match decode_tagged(buf) {
            Ok(Some((tag, payload, n))) => {
                let owned = payload.to_vec();
                buf.drain(..n);
                (tag, owned)
            }
            Ok(None) => return true,
            Err(_) => return false,
        };
        match frame.0 {
            TCP_CONTROL => {
                let mut body = frame.1;
                match drain_frames(&mut body, world, user_id, pipes, pace) {
                    Ok(true) if body.is_empty() => {}
                    _ => return false,
                }
            }
            TCP_VOICE => {
                if !pace.allow(true) {
                    continue;
                }
                let Some(id) = *user_id else { continue };
                deliver_voice(world, id, Bytes::from(frame.1));
            }
            _ => return false,
        }
    }
}

async fn read_some<R>(read: &mut R, tmp: &mut [u8]) -> std::io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    read.read(tmp).await
}

async fn write_tcp<W>(
    mut write: W,
    mut control_rx: Receiver<ServerMsg>,
    mut voice_rx: FairReceiver<Bytes>,
    fail: watch::Sender<bool>,
) where
    W: AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            biased;
            msg = control_rx.recv() => {
                let Some(msg) = msg else { break };
                let Ok(frame) = encode_server(&msg) else {
                    let _ = fail.send(true);
                    break;
                };
                if !write_tagged(&mut write, TCP_CONTROL, &frame).await {
                    let _ = fail.send(true);
                    break;
                }
            }
            packet = voice_rx.recv() => {
                let Some((_speaker, packet)) = packet else { break };
                // The queue already dropped frames that waited too long.
                // A write that does not finish is a stuck writer. Closing is
                // the only safe step: another frame on a partial tag would
                // corrupt the byte stream.
                let started = tokio::time::Instant::now();
                let wrote = tokio::time::timeout(WRITE_WAIT, write_frame(&mut write, TCP_VOICE, &packet)).await;
                if started.elapsed() > Duration::from_millis(200) {
                    tracing::info!(
                        ms = started.elapsed().as_millis() as u64,
                        "voice write"
                    );
                }
                if !matches!(wrote, Ok(Ok(()))) {
                    tracing::info!("voice writer stalled, closing the connection");
                    let _ = fail.send(true);
                    break;
                }
            }
        }
    }
}

async fn write_frame<W>(write: &mut W, tag: u8, payload: &[u8]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let frame = encode_tagged(tag, payload)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "frame"))?;
    write.write_all(&frame).await?;
    write.flush().await
}

async fn write_tagged<W>(write: &mut W, tag: u8, payload: &[u8]) -> bool
where
    W: AsyncWrite + Unpin,
{
    let wrote = tokio::time::timeout(WRITE_WAIT, write_frame(write, tag, payload)).await;
    matches!(wrote, Ok(Ok(())))
}

fn offer_control(pipes: &Pipes, msg: ServerMsg) {
    let _ = pipes.control.try_send(msg);
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
    async fn two_speakers_both_wait_for_one_listener() {
        let (tx, mut rx) = queue::fair(4, 16, queue::VOICE_MAX_AGE);
        offer_voice(&tx, Bytes::from(speaker_packet(1, b"a")));
        offer_voice(&tx, Bytes::from(speaker_packet(2, b"b")));
        let (first, _) = rx.recv().await.unwrap();
        let (second, _) = rx.recv().await.unwrap();
        assert_eq!([first, second], [1, 2]);
    }

    fn speaker_packet(speaker: u32, payload: &[u8]) -> Vec<u8> {
        let mut packet = vec![1];
        packet.extend_from_slice(&speaker.to_le_bytes());
        packet.extend_from_slice(&[0; 9]);
        packet.extend_from_slice(payload);
        packet
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
