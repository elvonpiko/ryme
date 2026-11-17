use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use ryme_proto::{
    decode_server_frame, decode_tagged, encode_client, encode_tagged, parse_invite, ClientMsg,
    Member, ServerMsg, TCP_CONTROL, TCP_VOICE,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::identity::{client_config, tls_client_config};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub user_id: u32,
    pub name: String,
    pub muted: bool,
    pub card: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Joined {
        channel: String,
        members: Vec<Peer>,
        rooms: Vec<String>,
    },
    PeerJoined {
        user_id: u32,
        name: String,
        card: u64,
    },
    PeerLeft {
        user_id: u32,
    },
    Chat {
        user_id: u32,
        text: String,
    },
    /// A line already in the channel when this client arrived.
    Past {
        name: String,
        text: String,
        mine: bool,
    },
    Muted {
        user_id: u32,
        muted: bool,
    },
    Left,
    Rejected {
        reason: String,
    },
    Disconnected,
}

enum Cmd {
    Chat(String),
    Mute(bool),
    Deafen(bool),
    Join(String),
    Leave,
    Logout,
}

pub struct Live {
    pub me: u32,
    pub channel: String,
    pub members: Vec<Peer>,
    pub rooms: Vec<String>,
    pub link: Link,
    pub events: UnboundedReceiver<Event>,
    /// Datagrams in and out. The audio thread owns this; control stays on `link`.
    pub voice: VoicePipe,
    /// Secret for the next hello, so a reconnect replaces this session.
    pub resume: [u8; 32],
    /// The QUIC handshake did not finish, so this session is TLS on TCP.
    pub tcp: bool,
    /// Latest round trip in milliseconds. `None` until the first sample.
    pub delay: tokio::sync::watch::Receiver<Option<u32>>,
}

/// Voice packets cross here so the audio thread never waits on the socket.
pub struct VoicePipe {
    incoming: std::sync::mpsc::Receiver<Vec<u8>>,
    outgoing: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl VoicePipe {
    pub fn send(&self, packet: Vec<u8>) {
        let _ = self.outgoing.try_send(packet);
    }

    /// A sender the bench can move onto its own task. `send` drops a packet when
    /// the queue is full; waiting here means a slow path is not counted as sent.
    pub fn queue(&self) -> tokio::sync::mpsc::Sender<Vec<u8>> {
        self.outgoing.clone()
    }

    pub fn try_recv(&self) -> Option<Vec<u8>> {
        self.incoming.try_recv().ok()
    }
}

#[derive(Clone)]
pub struct Link {
    cmd: UnboundedSender<Cmd>,
}

impl Link {
    pub fn chat(&self, text: impl Into<String>) {
        let _ = self.cmd.send(Cmd::Chat(text.into()));
    }

    pub fn mute(&self, muted: bool) {
        let _ = self.cmd.send(Cmd::Mute(muted));
    }

    pub fn deafen(&self, deafened: bool) {
        let _ = self.cmd.send(Cmd::Deafen(deafened));
    }

    pub fn join(&self, channel: impl Into<String>) {
        let _ = self.cmd.send(Cmd::Join(channel.into()));
    }

    pub fn leave(&self) {
        let _ = self.cmd.send(Cmd::Leave);
    }

    pub fn logout(&self) {
        let _ = self.cmd.send(Cmd::Logout);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LinkError {
    Invite,
    Unreachable,
    Timeout,
    Pin,
    Address,
    Rejected(String),
}

const TCP_AFTER: Duration = Duration::from_millis(300);
const TCP_VOICE_QUEUE: usize = 6;

enum Opened {
    Quic(quinn::Endpoint, quinn::Connection),
    Tcp(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
}

pub async fn connect(
    invite: &str,
    name: &str,
    card: u64,
    resume: [u8; 32],
) -> Result<Live, LinkError> {
    let parsed = parse_invite(invite).map_err(|_| LinkError::Invite)?;
    let addrs = addresses(&parsed.host, parsed.port)?;
    let mut last = LinkError::Unreachable;
    for addr in addrs {
        match open(addr, parsed.fingerprint).await {
            Ok(Opened::Quic(endpoint, conn)) => {
                return finish_quic(endpoint, conn, name, card, parsed.pass, resume).await;
            }
            Ok(Opened::Tcp(stream)) => {
                return finish_tcp(stream, name, card, parsed.pass, resume).await;
            }
            Err(LinkError::Pin) => return Err(LinkError::Pin),
            Err(err) => last = err,
        }
    }
    Err(last)
}

/// TLS on TCP, without trying QUIC. The integration test calls this.
#[cfg(test)]
pub(crate) async fn connect_tcp(
    addr: SocketAddr,
    fingerprint: [u8; 32],
    name: &str,
    card: u64,
    pass: [u8; 32],
    resume: [u8; 32],
) -> Result<Live, LinkError> {
    let stream = dial_tcp(addr, fingerprint).await?;
    finish_tcp(stream, name, card, pass, resume).await
}

async fn open(addr: SocketAddr, fingerprint: [u8; 32]) -> Result<Opened, LinkError> {
    let mut quic = tokio::spawn(dial(addr, fingerprint));
    let mut tcp = tokio::spawn(async move {
        tokio::time::sleep(TCP_AFTER).await;
        dial_tcp(addr, fingerprint).await
    });
    tokio::select! {
        q = &mut quic => match q {
            Ok(Ok((endpoint, conn))) => {
                tcp.abort();
                Ok(Opened::Quic(endpoint, conn))
            }
            Ok(Err(LinkError::Pin)) => {
                tcp.abort();
                Err(LinkError::Pin)
            }
            Ok(Err(err)) => match tcp.await {
                Ok(Ok(stream)) => Ok(Opened::Tcp(stream)),
                Ok(Err(LinkError::Pin)) => Err(LinkError::Pin),
                _ => Err(err),
            },
            Err(_) => Err(LinkError::Unreachable),
        },
        t = &mut tcp => match t {
            Ok(Ok(stream)) => {
                quic.abort();
                Ok(Opened::Tcp(stream))
            }
            Ok(Err(LinkError::Pin)) => {
                quic.abort();
                Err(LinkError::Pin)
            }
            _ => match quic.await {
                Ok(Ok((endpoint, conn))) => Ok(Opened::Quic(endpoint, conn)),
                Ok(Err(err)) => Err(err),
                Err(_) => Err(LinkError::Unreachable),
            },
        },
    }
}

async fn finish_quic(
    endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    name: &str,
    card: u64,
    pass: [u8; 32],
    resume: [u8; 32],
) -> Result<Live, LinkError> {
    let (mut send, mut recv) = conn.open_bi().await.map_err(|_| LinkError::Unreachable)?;

    write_msg(
        &mut send,
        &ClientMsg::Hello {
            name: name.to_string(),
            card,
            pass,
            resume,
        },
    )
    .await?;
    let mut buf = Vec::new();
    let (me, resume) = match read_msg(&mut recv, &mut buf).await? {
        ServerMsg::Welcome { user_id, resume } => (user_id, resume),
        ServerMsg::Reject { reason } => return Err(LinkError::Rejected(reason)),
        _ => return Err(LinkError::Unreachable),
    };

    write_msg(
        &mut send,
        &ClientMsg::Join {
            channel: "Lobby".into(),
        },
    )
    .await?;
    let (channel, members, rooms) = match read_msg(&mut recv, &mut buf).await? {
        ServerMsg::Joined {
            channel,
            members,
            rooms,
        } => (channel, peers(members), rooms),
        ServerMsg::Reject { reason } => return Err(LinkError::Rejected(reason)),
        _ => return Err(LinkError::Unreachable),
    };

    let (cmd_tx, cmd_rx) = unbounded_channel();
    let (event_tx, events) = unbounded_channel();
    let (voice_out_tx, voice_out_rx) = tokio::sync::mpsc::channel(32);
    let (voice_in_tx, voice_in_rx) = std::sync::mpsc::sync_channel(64);
    let (delay_tx, delay) = tokio::sync::watch::channel(None);
    tokio::spawn(run(
        endpoint,
        conn,
        send,
        recv,
        buf,
        cmd_rx,
        event_tx,
        voice_out_rx,
        voice_in_tx,
        delay_tx,
    ));

    Ok(Live {
        me,
        channel,
        members,
        rooms,
        link: Link { cmd: cmd_tx },
        events,
        voice: VoicePipe {
            incoming: voice_in_rx,
            outgoing: voice_out_tx,
        },
        resume,
        tcp: false,
        delay,
    })
}

async fn finish_tcp(
    stream: tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
    name: &str,
    card: u64,
    pass: [u8; 32],
    resume: [u8; 32],
) -> Result<Live, LinkError> {
    let (mut read, mut write) = tokio::io::split(stream);
    let mut acc = Vec::new();
    write_control(
        &mut write,
        &ClientMsg::Hello {
            name: name.to_string(),
            card,
            pass,
            resume,
        },
    )
    .await?;
    let (me, resume) = match read_control(&mut read, &mut acc).await? {
        ServerMsg::Welcome { user_id, resume } => (user_id, resume),
        ServerMsg::Reject { reason } => return Err(LinkError::Rejected(reason)),
        _ => return Err(LinkError::Unreachable),
    };
    write_control(
        &mut write,
        &ClientMsg::Join {
            channel: "Lobby".into(),
        },
    )
    .await?;
    let (channel, members, rooms) = match read_control(&mut read, &mut acc).await? {
        ServerMsg::Joined {
            channel,
            members,
            rooms,
        } => (channel, peers(members), rooms),
        ServerMsg::Reject { reason } => return Err(LinkError::Rejected(reason)),
        _ => return Err(LinkError::Unreachable),
    };

    let (cmd_tx, cmd_rx) = unbounded_channel();
    let (event_tx, events) = unbounded_channel();
    let (voice_out_tx, voice_out_rx) = tokio::sync::mpsc::channel(TCP_VOICE_QUEUE);
    let (voice_in_tx, voice_in_rx) = std::sync::mpsc::sync_channel(64);
    let (dead_tx, dead_rx) = tokio::sync::watch::channel(false);
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let clock = std::time::Instant::now();
    let (pending_tx, pending_rx) = tokio::sync::watch::channel(None::<u64>);
    let (delay_tx, delay) = tokio::sync::watch::channel(None);
    tokio::spawn(write_tcp_out(
        write,
        voice_out_rx,
        cmd_rx,
        dead_tx.clone(),
        stop_tx,
        clock,
        pending_tx,
    ));
    tokio::spawn(read_tcp_in(
        read,
        acc,
        event_tx.clone(),
        voice_in_tx,
        dead_tx,
        stop_rx,
        clock,
        pending_rx,
        delay_tx,
    ));
    tokio::spawn(watch_tcp(dead_rx, event_tx));

    Ok(Live {
        me,
        channel,
        members,
        rooms,
        link: Link { cmd: cmd_tx },
        events,
        voice: VoicePipe {
            incoming: voice_in_rx,
            outgoing: voice_out_tx,
        },
        resume,
        tcp: true,
        delay,
    })
}

async fn dial(
    addr: SocketAddr,
    fingerprint: [u8; 32],
) -> Result<(quinn::Endpoint, quinn::Connection), LinkError> {
    let mut endpoint =
        quinn::Endpoint::client(bind_for(addr).parse().unwrap()).map_err(|_| LinkError::Address)?;
    endpoint
        .set_default_client_config(client_config(fingerprint).map_err(|_| LinkError::Unreachable)?);
    let connecting = endpoint
        .connect(addr, "ryme")
        .map_err(|_| LinkError::Unreachable)?;
    let conn = tokio::time::timeout(Duration::from_secs(4), connecting)
        .await
        .map_err(|_| LinkError::Timeout)?
        .map_err(classify)?;
    Ok((endpoint, conn))
}

async fn dial_tcp(
    addr: SocketAddr,
    fingerprint: [u8; 32],
) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, LinkError> {
    let stream = tokio::time::timeout(Duration::from_secs(4), tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| LinkError::Timeout)?
        .map_err(|_| LinkError::Unreachable)?;
    stream
        .set_nodelay(true)
        .map_err(|_| LinkError::Unreachable)?;
    let config = tls_client_config(fingerprint).map_err(|_| LinkError::Unreachable)?;
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    let name = rustls::pki_types::ServerName::try_from("ryme").map_err(|_| LinkError::Unreachable)?;
    tokio::time::timeout(Duration::from_secs(4), connector.connect(name, stream))
        .await
        .map_err(|_| LinkError::Timeout)?
        .map_err(classify)
}

async fn write_control<W>(write: &mut W, msg: &ClientMsg) -> Result<(), LinkError>
where
    W: AsyncWrite + Unpin,
{
    let frame = encode_client(msg).map_err(|_| LinkError::Unreachable)?;
    if write_one(write, TCP_CONTROL, &frame).await {
        Ok(())
    } else {
        Err(LinkError::Unreachable)
    }
}

async fn write_one<W>(write: &mut W, tag: u8, payload: &[u8]) -> bool
where
    W: AsyncWrite + Unpin,
{
    let Ok(frame) = encode_tagged(tag, payload) else {
        return false;
    };
    write.write_all(&frame).await.is_ok() && write.flush().await.is_ok()
}

async fn read_control<R>(read: &mut R, acc: &mut Vec<u8>) -> Result<ServerMsg, LinkError>
where
    R: AsyncRead + Unpin,
{
    let mut tmp = [0u8; 2048];
    loop {
        if let Some(msg) = take_control(acc)? {
            return Ok(msg);
        }
        let n = tokio::time::timeout(Duration::from_secs(4), read.read(&mut tmp))
            .await
            .map_err(|_| LinkError::Timeout)?
            .map_err(|_| LinkError::Unreachable)?;
        if n == 0 {
            return Err(LinkError::Unreachable);
        }
        acc.extend_from_slice(&tmp[..n]);
    }
}

fn take_control(acc: &mut Vec<u8>) -> Result<Option<ServerMsg>, LinkError> {
    loop {
        match decode_tagged(acc).map_err(|_| LinkError::Unreachable)? {
            Some((tag, payload, n)) => {
                let owned = payload.to_vec();
                acc.drain(..n);
                if tag != TCP_CONTROL {
                    continue;
                }
                match decode_server_frame(&owned).map_err(|_| LinkError::Unreachable)? {
                    Some((msg, used)) if used == owned.len() => return Ok(Some(msg)),
                    _ => return Err(LinkError::Unreachable),
                }
            }
            None => return Ok(None),
        }
    }
}

async fn write_tcp_out<W>(
    mut write: W,
    mut voice_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    mut cmd_rx: UnboundedReceiver<Cmd>,
    dead: tokio::sync::watch::Sender<bool>,
    stop: tokio::sync::watch::Sender<bool>,
    clock: std::time::Instant,
    pending: tokio::sync::watch::Sender<Option<u64>>,
) where
    W: AsyncWrite + Unpin,
{
    let mut pulse = tokio::time::interval(Duration::from_secs(1));
    pulse.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break };
                if matches!(cmd, Cmd::Logout) {
                    let _ = stop.send(true);
                    let _ = write.shutdown().await;
                    return;
                }
                let Some(msg) = cmd_msg(cmd) else { continue };
                let Ok(frame) = encode_client(&msg) else { break };
                if !write_one(&mut write, TCP_CONTROL, &frame).await {
                    break;
                }
            }
            _ = pulse.tick() => {
                let token = clock.elapsed().as_micros() as u64;
                let _ = pending.send(Some(token));
                let Ok(frame) = encode_client(&ClientMsg::Ping { token }) else {
                    break;
                };
                if !write_one(&mut write, TCP_CONTROL, &frame).await {
                    break;
                }
            }
            packet = voice_rx.recv() => {
                match packet {
                    Some(packet) => {
                        if !write_one(&mut write, TCP_VOICE, &packet).await {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
    let _ = dead.send(true);
}

fn cmd_msg(cmd: Cmd) -> Option<ClientMsg> {
    Some(match cmd {
        Cmd::Chat(text) => ClientMsg::Chat { text },
        Cmd::Mute(muted) => ClientMsg::Mute { muted },
        Cmd::Deafen(deafened) => ClientMsg::Deafen { deafened },
        Cmd::Join(channel) => ClientMsg::Join { channel },
        Cmd::Leave => ClientMsg::Leave,
        Cmd::Logout => return None,
    })
}

async fn read_tcp_in<R>(
    mut read: R,
    mut acc: Vec<u8>,
    events: UnboundedSender<Event>,
    voice_in: std::sync::mpsc::SyncSender<Vec<u8>>,
    dead: tokio::sync::watch::Sender<bool>,
    mut stop: tokio::sync::watch::Receiver<bool>,
    clock: std::time::Instant,
    pending: tokio::sync::watch::Receiver<Option<u64>>,
    delay: tokio::sync::watch::Sender<Option<u32>>,
) where
    R: AsyncRead + Unpin,
{
    let mut tmp = [0u8; 2048];
    loop {
        if *stop.borrow() {
            return;
        }
        let read = tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
                continue;
            }
            n = read.read(&mut tmp) => n,
        };
        let Ok(n) = read else {
            break;
        };
        if n == 0 {
            break;
        }
        acc.extend_from_slice(&tmp[..n]);
        if !drain_tcp_in(&mut acc, &events, &voice_in, clock, &pending, &delay) {
            break;
        }
    }
    if !*stop.borrow() {
        let _ = dead.send(true);
    }
}

fn drain_tcp_in(
    acc: &mut Vec<u8>,
    events: &UnboundedSender<Event>,
    voice_in: &std::sync::mpsc::SyncSender<Vec<u8>>,
    clock: std::time::Instant,
    pending: &tokio::sync::watch::Receiver<Option<u64>>,
    delay: &tokio::sync::watch::Sender<Option<u32>>,
) -> bool {
    loop {
        match decode_tagged(acc) {
            Ok(Some((tag, payload, n))) => {
                let owned = payload.to_vec();
                acc.drain(..n);
                if tag == TCP_VOICE {
                    let _ = voice_in.try_send(owned);
                    continue;
                }
                if tag != TCP_CONTROL {
                    return false;
                }
                let mut body = owned;
                if !drain(&mut body, events, |token| {
                    note_pong(clock, pending, delay, token);
                }) || !body.is_empty()
                {
                    return false;
                }
            }
            Ok(None) => return true,
            Err(_) => return false,
        }
    }
}

async fn watch_tcp(
    mut dead: tokio::sync::watch::Receiver<bool>,
    events: UnboundedSender<Event>,
) {
    if dead.changed().await.is_ok() && *dead.borrow() {
        let _ = events.send(Event::Disconnected);
    }
}

fn classify(err: impl std::fmt::Display) -> LinkError {
    let text = err.to_string().to_ascii_lowercase();
    if text.contains("fingerprint") {
        LinkError::Pin
    } else {
        LinkError::Unreachable
    }
}

fn bind_for(addr: SocketAddr) -> &'static str {
    if addr.is_ipv6() {
        "[::]:0"
    } else {
        "0.0.0.0:0"
    }
}

fn addresses(host: &str, port: u16) -> Result<Vec<SocketAddr>, LinkError> {
    let host = match host {
        "0.0.0.0" => "127.0.0.1",
        "::" | "[::]" => "::1",
        other => other,
    };
    let mut seen = Vec::new();
    for addr in (host, port)
        .to_socket_addrs()
        .map_err(|_| LinkError::Address)?
    {
        if !seen.contains(&addr) {
            seen.push(addr);
        }
    }
    if seen.is_empty() {
        Err(LinkError::Address)
    } else {
        Ok(seen)
    }
}

fn peers(members: Vec<Member>) -> Vec<Peer> {
    members
        .into_iter()
        .map(|member| Peer {
            user_id: member.user_id,
            name: member.name,
            muted: member.muted,
            card: member.card,
        })
        .collect()
}

async fn write_msg(send: &mut quinn::SendStream, msg: &ClientMsg) -> Result<(), LinkError> {
    let frame = encode_client(msg).map_err(|_| LinkError::Unreachable)?;
    send.write_all(&frame)
        .await
        .map_err(|_| LinkError::Unreachable)
}

async fn read_msg(recv: &mut quinn::RecvStream, buf: &mut Vec<u8>) -> Result<ServerMsg, LinkError> {
    let mut tmp = [0u8; 2048];
    loop {
        if let Some((msg, n)) = decode_server_frame(buf).map_err(|_| LinkError::Unreachable)? {
            buf.drain(..n);
            return Ok(msg);
        }
        let n = tokio::time::timeout(Duration::from_secs(4), recv.read(&mut tmp))
            .await
            .map_err(|_| LinkError::Timeout)?
            .map_err(|_| LinkError::Unreachable)?
            .ok_or(LinkError::Unreachable)?;
        buf.extend_from_slice(&tmp[..n]);
    }
}

async fn run(
    endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    mut buf: Vec<u8>,
    mut cmd_rx: UnboundedReceiver<Cmd>,
    events: UnboundedSender<Event>,
    mut voice_out: tokio::sync::mpsc::Receiver<Vec<u8>>,
    voice_in: std::sync::mpsc::SyncSender<Vec<u8>>,
    delay: tokio::sync::watch::Sender<Option<u32>>,
) {
    let mut tmp = [0u8; 2048];
    let mut intentional = false;
    let mut voice_open = true;
    let mut pulse = tokio::time::interval(Duration::from_secs(1));
    pulse.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = pulse.tick() => {
                let ms = u32::try_from(conn.rtt().as_millis()).unwrap_or(u32::MAX);
                let _ = delay.send(Some(ms.min(9_999)));
            }
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break };
                if !handle_cmd(&mut send, cmd, &mut intentional).await {
                    break;
                }
            }
            read = recv.read(&mut tmp) => {
                match read {
                    Ok(Some(n)) if n > 0 => {
                        buf.extend_from_slice(&tmp[..n]);
                        if !drain(&mut buf, &events, |_| {}) {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            datagram = conn.read_datagram() => {
                match datagram {
                    Ok(packet) => {
                        let _ = voice_in.try_send(packet.to_vec());
                    }
                    Err(_) => break,
                }
            }
            packet = voice_out.recv(), if voice_open => {
                match packet {
                    Some(packet) => {
                        let _ = conn.send_datagram(bytes::Bytes::from(packet));
                    }
                    None => voice_open = false,
                }
            }
        }
    }
    if intentional {
        // Dropping the socket is not enough. The server keeps the member until
        // this close frame arrives, or until the idle timer gives up.
        conn.close(quinn::VarInt::from_u32(0), b"bye");
        let _ = tokio::time::timeout(Duration::from_millis(400), endpoint.wait_idle()).await;
    } else {
        let _ = events.send(Event::Disconnected);
    }
}

async fn handle_cmd(send: &mut quinn::SendStream, cmd: Cmd, intentional: &mut bool) -> bool {
    let msg = match cmd {
        Cmd::Chat(text) => ClientMsg::Chat { text },
        Cmd::Mute(muted) => ClientMsg::Mute { muted },
        Cmd::Deafen(deafened) => ClientMsg::Deafen { deafened },
        Cmd::Join(channel) => ClientMsg::Join { channel },
        Cmd::Leave => ClientMsg::Leave,
        Cmd::Logout => {
            *intentional = true;
            return false;
        }
    };
    write_msg(send, &msg).await.is_ok()
}

fn note_pong(
    clock: std::time::Instant,
    pending: &tokio::sync::watch::Receiver<Option<u64>>,
    delay: &tokio::sync::watch::Sender<Option<u32>>,
    token: u64,
) {
    if pending.borrow().is_none_or(|sent| sent != token) {
        return;
    }
    let now = clock.elapsed().as_micros() as u64;
    if token > now {
        return;
    }
    let ms = u32::try_from((now - token) / 1000).unwrap_or(u32::MAX);
    let _ = delay.send(Some(ms.min(9_999)));
}

fn drain(
    buf: &mut Vec<u8>,
    events: &UnboundedSender<Event>,
    mut on_pong: impl FnMut(u64),
) -> bool {
    loop {
        match decode_server_frame(buf) {
            Ok(Some((msg, n))) => {
                buf.drain(..n);
                let event = match msg {
                    ServerMsg::Joined {
                        channel,
                        members,
                        rooms,
                    } => Event::Joined {
                        channel,
                        members: peers(members),
                        rooms,
                    },
                    ServerMsg::PeerJoined {
                        user_id,
                        name,
                        card,
                    } => Event::PeerJoined {
                        user_id,
                        name,
                        card,
                    },
                    ServerMsg::PeerLeft { user_id } => Event::PeerLeft { user_id },
                    ServerMsg::Chat { user_id, text } => Event::Chat { user_id, text },
                    ServerMsg::Past { name, text, mine } => Event::Past { name, text, mine },
                    ServerMsg::Muted { user_id, muted } => Event::Muted { user_id, muted },
                    ServerMsg::Left => Event::Left,
                    ServerMsg::Reject { reason } => Event::Rejected { reason },
                    ServerMsg::Pong { token } => {
                        on_pong(token);
                        continue;
                    }
                    _ => continue,
                };
                if events.send(event).is_err() {
                    return false;
                }
            }
            Ok(None) => return true,
            Err(_) => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Server;

    async fn up() -> (u16, [u8; 32], [u8; 32], tokio::task::JoinHandle<()>) {
        let server = Server::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let port = server.port();
        let fingerprint = server.fingerprint();
        let pass = server.pass();
        let task = tokio::spawn(async move { server.serve().await });
        (port, fingerprint, pass, task)
    }

    fn invite(port: u16, fingerprint: [u8; 32], pass: [u8; 32]) -> String {
        crate::identity::invite_url("127.0.0.1", port, &fingerprint, &pass)
    }

    #[tokio::test]
    async fn join_shows_you_and_a_bad_invite_fails() {
        match connect("nope", "Mara", 1, [0; 32]).await {
            Err(LinkError::Invite) => {}
            Err(err) => panic!("expected a bad invite, got {err:?}"),
            Ok(_) => panic!("bad invite connected"),
        }

        let (port, fingerprint, pass, _task) = up().await;
        let mut bad = fingerprint;
        bad[0] ^= 0xff;
        match connect(&invite(port, bad, pass), "Mara", 1, [0; 32]).await {
            Err(LinkError::Pin) => {}
            Err(err) => panic!("expected a refused pin, got {err:?}"),
            Ok(_) => panic!("wrong fingerprint connected"),
        }

        let url = invite(port, fingerprint, pass);
        let live = connect(&url, "Mara", 1, [0; 32]).await.unwrap();
        assert_eq!(live.channel, "Lobby");
        assert_eq!(live.members.len(), 1);
        assert_eq!(live.members[0].name, "Mara");
        assert_eq!(live.members[0].user_id, live.me);

        let unspecified = crate::identity::invite_url("0.0.0.0", port, &fingerprint, &pass);
        let again = connect(&unspecified, "Mara", 3, [0; 32]).await.unwrap();
        assert_eq!(again.members[0].name, "Mara");
    }

    #[tokio::test]
    async fn a_second_client_is_announced_and_logout_drops_them() {
        let (port, fingerprint, pass, _task) = up().await;
        let url = invite(port, fingerprint, pass);
        let mut mara = connect(&url, "Mara", 1, [0; 32]).await.unwrap();
        let jonas = connect(&url, "Jonas", 2, [0; 32]).await.unwrap();
        assert_eq!(jonas.members.len(), 2);

        let event = tokio::time::timeout(Duration::from_secs(2), mara.events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            event,
            Event::PeerJoined {
                user_id: jonas.me,
                name: "Jonas".into(),
                card: 2,
            }
        );

        jonas.link.chat("pushing mid");
        let chat = tokio::time::timeout(Duration::from_secs(2), mara.events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            chat,
            Event::Chat {
                user_id: jonas.me,
                text: "pushing mid".into(),
            }
        );

        jonas.link.logout();
        let left = tokio::time::timeout(Duration::from_secs(2), mara.events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(left, Event::PeerLeft { user_id: jonas.me });
    }

    #[tokio::test]
    async fn a_missing_channel_is_refused_and_lobby_stays() {
        let (port, fingerprint, pass, _task) = up().await;
        let url = invite(port, fingerprint, pass);
        let mut live = connect(&url, "Mara", 1, [0; 32]).await.unwrap();
        live.link.join("Nope");
        let refused = tokio::time::timeout(Duration::from_secs(2), live.events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            refused,
            Event::Rejected {
                reason: "no such channel".into(),
            }
        );
        live.link.chat("still here");
        let chat = tokio::time::timeout(Duration::from_secs(2), live.events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            chat,
            Event::Chat {
                user_id: live.me,
                text: "still here".into(),
            }
        );
    }

    #[tokio::test]
    async fn a_host_opened_channel_can_be_joined() {
        let server = Server::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        server.allow("Ranked");
        let port = server.port();
        let fingerprint = server.fingerprint();
        let pass = server.pass();
        tokio::spawn(async move { server.serve().await });
        let url = invite(port, fingerprint, pass);
        let mut live = connect(&url, "Mara", 1, [0; 32]).await.unwrap();
        live.link.join("Ranked");
        let joined = tokio::time::timeout(Duration::from_secs(2), live.events.recv())
            .await
            .unwrap()
            .unwrap();
        match joined {
            Event::Joined { channel, rooms, .. } => {
                assert_eq!(channel, "Ranked");
                assert_eq!(rooms, vec!["Lobby".to_string(), "Ranked".to_string()]);
            }
            other => panic!("expected the host's channel, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_voice_packet_arrives_with_the_real_speaker() {
        let (port, fingerprint, pass, _task) = up().await;
        let url = invite(port, fingerprint, pass);
        let mara = connect(&url, "Mara", 1, [0; 32]).await.unwrap();
        let jonas = connect(&url, "Jonas", 2, [0; 32]).await.unwrap();

        let packet = ryme_proto::encode_voice(&ryme_proto::VoicePacket {
            speaker: 99,
            seq: 7,
            timestamp: 7 * 480,
            flags: 0,
            payload: vec![1, 2, 3, 4],
        })
        .unwrap();
        mara.voice.send(packet);

        let start = std::time::Instant::now();
        let got = loop {
            if let Some(bytes) = jonas.voice.try_recv() {
                break bytes;
            }
            if start.elapsed() > Duration::from_secs(2) {
                panic!("voice did not arrive");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let voice = ryme_proto::decode_voice(&got).unwrap();
        assert_eq!(voice.speaker, mara.me);
        assert_eq!(voice.seq, 7);
        assert_eq!(voice.payload, vec![1, 2, 3, 4]);
        assert!(mara.voice.try_recv().is_none());
        assert!(!mara.tcp);
    }

    async fn next_delay(delay: &mut tokio::sync::watch::Receiver<Option<u32>>) -> u32 {
        if let Some(ms) = *delay.borrow() {
            return ms;
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                delay.changed().await.expect("delay watch");
                if let Some(ms) = *delay.borrow_and_update() {
                    return ms;
                }
            }
        })
        .await
        .expect("delay sample")
    }

    #[tokio::test]
    async fn quic_reports_its_own_round_trip() {
        let (port, fingerprint, pass, _task) = up().await;
        let mut live = connect(&invite(port, fingerprint, pass), "Mara", 1, [0; 32])
            .await
            .unwrap();
        assert!(!live.tcp);
        let ms = next_delay(&mut live.delay).await;
        assert!(ms <= 9_999);
    }

    #[tokio::test]
    async fn tcp_echoes_a_round_trip_without_voice() {
        let (port, fingerprint, pass, _task) = up().await;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let mut live = connect_tcp(addr, fingerprint, "Mara", 1, pass, [0; 32])
            .await
            .unwrap();
        assert!(live.tcp);
        let ms = next_delay(&mut live.delay).await;
        assert!(ms <= 9_999);
    }

    #[tokio::test]
    async fn tcp_carries_voice_to_and_from_quic() {
        let (port, fingerprint, pass, _task) = up().await;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let tcp = connect_tcp(addr, fingerprint, "Ear", 2, pass, [0; 32])
            .await
            .unwrap();
        assert!(tcp.tcp);
        let url = invite(port, fingerprint, pass);
        let quic = connect(&url, "Mouth", 1, [0; 32]).await.unwrap();
        assert!(!quic.tcp);

        let from_quic = ryme_proto::encode_voice(&ryme_proto::VoicePacket {
            speaker: 0,
            seq: 3,
            timestamp: 3 * 480,
            flags: 0,
            payload: vec![9, 9],
        })
        .unwrap();
        quic.voice.send(from_quic);
        let heard = wait_voice(&tcp).await;
        let heard = ryme_proto::decode_voice(&heard).unwrap();
        assert_eq!(heard.speaker, quic.me);
        assert_eq!(heard.payload, vec![9, 9]);

        let from_tcp = ryme_proto::encode_voice(&ryme_proto::VoicePacket {
            speaker: 0,
            seq: 4,
            timestamp: 4 * 480,
            flags: 0,
            payload: vec![8, 8],
        })
        .unwrap();
        tcp.voice.send(from_tcp);
        let heard = wait_voice(&quic).await;
        let heard = ryme_proto::decode_voice(&heard).unwrap();
        assert_eq!(heard.speaker, tcp.me);
        assert_eq!(heard.payload, vec![8, 8]);
    }

    async fn wait_voice(live: &Live) -> Vec<u8> {
        let start = std::time::Instant::now();
        loop {
            if let Some(bytes) = live.voice.try_recv() {
                return bytes;
            }
            if start.elapsed() > Duration::from_secs(2) {
                panic!("voice did not arrive");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn the_local_socket_matches_the_address_family() {
        assert_eq!(bind_for(SocketAddr::from(([127, 0, 0, 1], 1))), "0.0.0.0:0");
        assert_eq!(
            bind_for(SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 1))),
            "[::]:0"
        );
    }

    #[test]
    fn a_literal_keeps_its_family_and_an_unspecified_host_is_local() {
        let v4 = addresses("127.0.0.1", 1542).unwrap();
        assert!(v4.iter().all(|addr| addr.is_ipv4()));
        let local = addresses("0.0.0.0", 1542).unwrap();
        assert_eq!(local, vec![SocketAddr::from(([127, 0, 0, 1], 1542))]);
    }

    #[tokio::test]
    async fn a_retired_invite_is_refused_and_a_resume_replaces_the_session() {
        let (port, fingerprint, pass, _task) = up().await;
        let url = invite(port, fingerprint, pass);
        let mut bad = pass;
        bad[0] ^= 0xff;
        let retired = crate::identity::invite_url("127.0.0.1", port, &fingerprint, &bad);
        match connect(&retired, "Mara", 1, [0; 32]).await {
            Err(LinkError::Rejected(reason)) => assert_eq!(reason, "invite was retired"),
            Err(err) => panic!("expected a retired invite, got {err:?}"),
            Ok(_) => panic!("a retired invite was admitted"),
        }

        let first = connect(&url, "Mara", 4, [0; 32]).await.unwrap();
        let second = connect(&url, "Mara", 4, first.resume).await.unwrap();
        assert_ne!(second.me, first.me);
        assert_ne!(second.resume, first.resume);
    }
}
