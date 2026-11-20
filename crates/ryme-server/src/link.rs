use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
        muted: bool,
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
    /// The call left TCP. Voice uses the same packet on the QUIC socket.
    Moved {
        user_id: u32,
        resume: [u8; 32],
    },
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
/// Both transports write the same packet into `incoming`. `outgoing` can move
/// from the TCP queue to QUIC without the audio thread noticing.
#[derive(Clone)]
pub struct VoicePipe {
    incoming: Arc<Inbox>,
    outgoing: Arc<Mutex<Outbox>>,
    /// False while the room is changing. Queued frames are dropped instead of
    /// arriving in the next room.
    pass: Arc<AtomicBool>,
}

/// Frames the audio thread has not played yet. A full inbox drops the oldest.
struct Inbox {
    cap: usize,
    items: std::sync::Mutex<std::collections::VecDeque<(Instant, Vec<u8>)>>,
}

impl Inbox {
    fn new(cap: usize) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            cap: cap.max(1),
            items: std::sync::Mutex::new(std::collections::VecDeque::new()),
        })
    }

    fn push(&self, packet: Vec<u8>) {
        let mut items = self.items.lock().unwrap();
        let now = Instant::now();
        while items
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > crate::queue::VOICE_MAX_AGE)
        {
            items.pop_front();
        }
        while items.len() >= self.cap {
            items.pop_front();
        }
        items.push_back((now, packet));
    }

    fn pop(&self) -> Option<Vec<u8>> {
        let mut items = self.items.lock().unwrap();
        let now = Instant::now();
        while let Some((at, packet)) = items.pop_front() {
            if now.duration_since(at) <= crate::queue::VOICE_MAX_AGE {
                return Some(packet);
            }
        }
        None
    }
}

#[derive(Clone)]
enum Outbox {
    /// QUIC. A full queue drops the packet the audio thread just handed over.
    Wait(tokio::sync::mpsc::Sender<Vec<u8>>),
    /// TCP. A full queue drops the oldest frame and keeps the newest.
    Fresh(crate::queue::NewestSender<Vec<u8>>),
}

/// The bench waits on this. The audio thread uses [`VoicePipe::send`] instead,
/// which never blocks.
pub struct VoiceQueue {
    inner: Outbox,
}

impl VoiceQueue {
    pub async fn send(&self, packet: Vec<u8>) -> Result<(), ()> {
        match &self.inner {
            Outbox::Wait(tx) => tx.send(packet).await.map_err(|_| ()),
            Outbox::Fresh(tx) => {
                tx.offer(packet);
                Ok(())
            }
        }
    }
}

impl VoicePipe {
    pub fn send(&self, packet: Vec<u8>) {
        if !self.pass.load(Ordering::Relaxed) {
            return;
        }
        match &*self.outgoing.lock().unwrap() {
            Outbox::Wait(tx) => {
                let _ = tx.try_send(packet);
            }
            Outbox::Fresh(tx) => tx.offer(packet),
        }
    }

    /// A sender the bench can move onto its own task. On QUIC, waiting here
    /// means a slow path is not counted as sent. On TCP the queue keeps the
    /// newest frames and this returns as soon as the frame is accepted.
    pub fn queue(&self) -> VoiceQueue {
        VoiceQueue {
            inner: self.outgoing.lock().unwrap().clone(),
        }
    }

    pub fn try_recv(&self) -> Option<Vec<u8>> {
        self.incoming.pop()
    }
}

#[derive(Clone)]
pub struct Link {
    cmd: Arc<Mutex<UnboundedSender<Cmd>>>,
    events: UnboundedSender<Event>,
    incoming: Arc<Inbox>,
    outgoing: Arc<Mutex<Outbox>>,
    resume: Arc<Mutex<[u8; 32]>>,
    channel: Arc<Mutex<String>>,
    /// The TCP socket dying after QUIC has the seat is not a dropped call.
    quiet: Arc<AtomicBool>,
    voice_pass: Arc<AtomicBool>,
    stop: tokio::sync::watch::Sender<bool>,
    delay: tokio::sync::watch::Sender<Option<u32>>,
}

impl Link {
    fn command(&self, cmd: Cmd) {
        if let Cmd::Join(channel) = &cmd {
            *self.channel.lock().unwrap() = channel.clone();
        }
        if matches!(cmd, Cmd::Leave | Cmd::Join(_)) {
            self.voice_pass.store(false, Ordering::Relaxed);
        }
        if matches!(cmd, Cmd::Leave) {
            self.channel.lock().unwrap().clear();
        }
        if matches!(cmd, Cmd::Logout) {
            let _ = self.stop.send(true);
        }
        let _ = self.cmd.lock().unwrap().send(cmd);
    }

    pub fn chat(&self, text: impl Into<String>) {
        self.command(Cmd::Chat(text.into()));
    }

    pub fn mute(&self, muted: bool) {
        self.command(Cmd::Mute(muted));
    }

    pub fn deafen(&self, deafened: bool) {
        self.command(Cmd::Deafen(deafened));
    }

    pub fn join(&self, channel: impl Into<String>) {
        self.command(Cmd::Join(channel.into()));
    }

    pub fn leave(&self) {
        self.command(Cmd::Leave);
    }

    pub fn logout(&self) {
        self.command(Cmd::Logout);
    }

    /// Voice may leave the machine again. Called once the asked room is confirmed.
    pub fn release_voice(&self) {
        self.voice_pass.store(true, Ordering::Relaxed);
    }

    /// Stop every dial this call still has open.
    pub fn cancel(&self) {
        let _ = self.stop.send(true);
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
const WRITE_WAIT: Duration = Duration::from_secs(15);

/// One transport handshake, run on its own. A cancelled race is not a result.
#[derive(Debug)]
pub struct HandshakeReport {
    pub transport: &'static str,
    pub address: SocketAddr,
    pub outcome: &'static str,
    pub millis: u128,
}

pub async fn probe_invite(invite: &str) -> Result<Vec<HandshakeReport>, LinkError> {
    let parsed = parse_invite(invite).map_err(|_| LinkError::Invite)?;
    let addrs = addresses(&parsed.host, parsed.port).await?;
    let mut reports = Vec::new();
    for addr in addrs {
        let started = Instant::now();
        let quic = dial(addr, parsed.fingerprint).await;
        reports.push(HandshakeReport {
            transport: "quic",
            address: addr,
            outcome: match &quic {
                Ok(_) => "ok",
                Err(err) => outcome(err),
            },
            millis: started.elapsed().as_millis(),
        });
        if let Ok((endpoint, conn)) = quic {
            let media_started = Instant::now();
            let media = probe_media(&conn, parsed.pass).await;
            reports.push(HandshakeReport {
                transport: "media",
                address: addr,
                outcome: match &media {
                    Ok(()) => "ok",
                    Err(err) => outcome(err),
                },
                millis: media_started.elapsed().as_millis(),
            });
            conn.close(quinn::VarInt::from_u32(0), b"probe");
            endpoint.close(quinn::VarInt::from_u32(0), b"probe");
        }
        let started = Instant::now();
        let tcp = dial_tcp(addr, parsed.fingerprint).await;
        reports.push(HandshakeReport {
            transport: "tcp",
            address: addr,
            outcome: match &tcp {
                Ok(_) => "ok",
                Err(err) => outcome(err),
            },
            millis: started.elapsed().as_millis(),
        });
        drop(tcp);
    }
    Ok(reports)
}

fn outcome(err: &LinkError) -> &'static str {
    match err {
        LinkError::Timeout => "timeout",
        LinkError::Pin => "pin",
        LinkError::Address => "address",
        LinkError::Invite => "invite",
        LinkError::Rejected(_) => "rejected",
        LinkError::Unreachable => "unreachable",
    }
}

fn diag(addr: SocketAddr, transport: &str, result: &str, millis: u128) {
    if std::env::var_os("RYME_DIAG").is_some() {
        eprintln!("ryme: {transport} {addr} {result} {millis}ms");
    }
}

enum Opened {
    /// The QUIC handshake finished. `tcp` is still dialing until media is proved.
    Quic {
        endpoint: quinn::Endpoint,
        conn: quinn::Connection,
        tcp: tokio::task::JoinHandle<
            Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, LinkError>,
        >,
    },
    /// TCP answered first. `quic` is still dialing.
    Tcp {
        stream: tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
        quic: tokio::task::JoinHandle<Result<(quinn::Endpoint, quinn::Connection), LinkError>>,
    },
}

pub async fn connect(
    invite: &str,
    name: &str,
    card: u64,
    resume: [u8; 32],
) -> Result<Live, LinkError> {
    connect_room(invite, name, card, resume, Some("Lobby")).await
}

/// `room` is the channel to enter with the hello. `None` stays connected and
/// outside every room, so a restore cannot transmit in Lobby first.
pub async fn connect_room(
    invite: &str,
    name: &str,
    card: u64,
    resume: [u8; 32],
    room: Option<&str>,
) -> Result<Live, LinkError> {
    let parsed = parse_invite(invite).map_err(|_| LinkError::Invite)?;
    let addrs = addresses(&parsed.host, parsed.port).await?;
    let mut last = LinkError::Unreachable;
    let room = room.map(str::to_string);
    for addr in addrs {
        match open(addr, parsed.fingerprint).await {
            Ok(Opened::Quic {
                endpoint,
                conn,
                tcp,
            }) => {
                match finish_quic(
                    endpoint,
                    conn,
                    name,
                    card,
                    parsed.pass,
                    resume,
                    room.clone(),
                )
                .await
                {
                    Ok(live) => {
                        tcp.abort();
                        return Ok(live);
                    }
                    Err(LinkError::Rejected(reason)) => {
                        tcp.abort();
                        return Err(LinkError::Rejected(reason));
                    }
                    Err(LinkError::Pin) => {
                        tcp.abort();
                        return Err(LinkError::Pin);
                    }
                    Err(err) => {
                        return match tcp.await {
                            Ok(Ok(stream)) => {
                                finish_tcp(stream, name, card, parsed.pass, resume, room).await
                            }
                            _ => Err(err),
                        };
                    }
                }
            }
            Ok(Opened::Tcp { stream, quic }) => {
                let live =
                    finish_tcp(stream, name, card, parsed.pass, resume, room.clone()).await?;
                let link = live.link.clone();
                tokio::spawn(pursue_quic(
                    addr,
                    parsed.fingerprint,
                    name.to_string(),
                    card,
                    parsed.pass,
                    Some(quic),
                    link,
                ));
                return Ok(live);
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
    finish_tcp(stream, name, card, pass, resume, Some("Lobby".into())).await
}

async fn open(addr: SocketAddr, fingerprint: [u8; 32]) -> Result<Opened, LinkError> {
    let quic_started = Instant::now();
    let mut quic = tokio::spawn(dial(addr, fingerprint));
    let tcp_started = Instant::now();
    let mut tcp = tokio::spawn(async move {
        tokio::time::sleep(TCP_AFTER).await;
        dial_tcp(addr, fingerprint).await
    });
    tokio::select! {
        q = &mut quic => match q {
            Ok(Ok((endpoint, conn))) => {
                diag(addr, "quic", "ok", quic_started.elapsed().as_millis());
                Ok(Opened::Quic {
                    endpoint,
                    conn,
                    tcp,
                })
            }
            Ok(Err(LinkError::Pin)) => {
                tcp.abort();
                diag(addr, "quic", "pin", quic_started.elapsed().as_millis());
                Err(LinkError::Pin)
            }
            Ok(Err(err)) => {
                diag(addr, "quic", outcome(&err), quic_started.elapsed().as_millis());
                match tcp.await {
                    Ok(Ok(stream)) => {
                        diag(addr, "tcp", "ok", tcp_started.elapsed().as_millis());
                        Ok(Opened::Tcp {
                            stream,
                            quic: tokio::spawn(async { Err(err) }),
                        })
                    }
                    Ok(Err(LinkError::Pin)) => Err(LinkError::Pin),
                    Ok(Err(tcp_err)) => {
                        diag(addr, "tcp", outcome(&tcp_err), tcp_started.elapsed().as_millis());
                        Err(err)
                    }
                    Err(_) => Err(err),
                }
            }
            Err(_) => Err(LinkError::Unreachable),
        },
        t = &mut tcp => match t {
            Ok(Ok(stream)) => {
                diag(addr, "tcp", "ok", tcp_started.elapsed().as_millis());
                diag(addr, "quic", "pending", quic_started.elapsed().as_millis());
                Ok(Opened::Tcp { stream, quic })
            }
            Ok(Err(LinkError::Pin)) => {
                quic.abort();
                diag(addr, "tcp", "pin", tcp_started.elapsed().as_millis());
                Err(LinkError::Pin)
            }
            Ok(Err(tcp_err)) => {
                diag(addr, "tcp", outcome(&tcp_err), tcp_started.elapsed().as_millis());
                match quic.await {
                    Ok(Ok((endpoint, conn))) => {
                        diag(addr, "quic", "ok", quic_started.elapsed().as_millis());
                        Ok(Opened::Quic {
                            endpoint,
                            conn,
                            tcp: tokio::spawn(async { Err(tcp_err) }),
                        })
                    }
                    Ok(Err(err)) => {
                        diag(addr, "quic", outcome(&err), quic_started.elapsed().as_millis());
                        Err(err)
                    }
                    Err(_) => Err(LinkError::Unreachable),
                }
            }
            Err(_) => match quic.await {
                Ok(Ok((endpoint, conn))) => Ok(Opened::Quic {
                    endpoint,
                    conn,
                    tcp: tokio::spawn(async { Err(LinkError::Unreachable) }),
                }),
                Ok(Err(err)) => Err(err),
                Err(_) => Err(LinkError::Unreachable),
            },
        },
    }
}

fn seat(
    cmd: UnboundedSender<Cmd>,
    events: UnboundedSender<Event>,
    incoming: Arc<Inbox>,
    outgoing: Outbox,
    resume: [u8; 32],
    channel: String,
    delay: tokio::sync::watch::Sender<Option<u32>>,
    stop: tokio::sync::watch::Sender<bool>,
) -> (Link, VoicePipe) {
    let outgoing = Arc::new(Mutex::new(outgoing));
    let link = Link {
        cmd: Arc::new(Mutex::new(cmd)),
        events,
        incoming: Arc::clone(&incoming),
        outgoing: Arc::clone(&outgoing),
        resume: Arc::new(Mutex::new(resume)),
        channel: Arc::new(Mutex::new(channel)),
        quiet: Arc::new(AtomicBool::new(false)),
        voice_pass: Arc::new(AtomicBool::new(true)),
        stop,
        delay,
    };
    let voice = VoicePipe {
        incoming,
        outgoing,
        pass: Arc::clone(&link.voice_pass),
    };
    (link, voice)
}

async fn finish_quic(
    endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    name: &str,
    card: u64,
    pass: [u8; 32],
    resume: [u8; 32],
    room: Option<String>,
) -> Result<Live, LinkError> {
    let (mut send, mut recv) = conn.open_bi().await.map_err(|_| LinkError::Unreachable)?;
    if prove_media(&conn, &mut send, &mut recv, pass)
        .await
        .is_err()
    {
        drop_quic(&endpoint, &conn);
        return Err(LinkError::Unreachable);
    }

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
    let (me, resume, welcome_rooms) = match read_msg(&mut recv, &mut buf).await? {
        ServerMsg::Welcome {
            user_id,
            resume,
            rooms,
        } => (user_id, resume, rooms),
        ServerMsg::Reject { reason } => return Err(LinkError::Rejected(reason)),
        _ => return Err(LinkError::Unreachable),
    };

    let (channel, members, rooms) = if let Some(room) = room {
        write_msg(&mut send, &ClientMsg::Join { channel: room }).await?;
        match read_msg(&mut recv, &mut buf).await? {
            ServerMsg::Joined {
                channel,
                members,
                rooms,
            } => (channel, peers(members), rooms),
            ServerMsg::Reject { reason } => return Err(LinkError::Rejected(reason)),
            _ => return Err(LinkError::Unreachable),
        }
    } else {
        (String::new(), Vec::new(), welcome_rooms)
    };

    let (cmd_tx, cmd_rx) = unbounded_channel();
    let (event_tx, events) = unbounded_channel();
    let (voice_out_tx, voice_out_rx) = crate::queue::newest(TCP_VOICE_QUEUE);
    let voice_in = Inbox::new(64);
    let (delay_tx, delay) = tokio::sync::watch::channel(None);
    let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
    let (link, voice) = seat(
        cmd_tx,
        event_tx.clone(),
        voice_in,
        Outbox::Fresh(voice_out_tx),
        resume,
        channel.clone(),
        delay_tx.clone(),
        stop_tx,
    );
    tokio::spawn(run(
        endpoint,
        conn,
        send,
        recv,
        buf,
        cmd_rx,
        event_tx,
        voice_out_rx,
        Arc::clone(&link.incoming),
        delay_tx,
        Arc::clone(&link.voice_pass),
    ));

    Ok(Live {
        me,
        channel,
        members,
        rooms,
        link,
        events,
        voice,
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
    room: Option<String>,
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
    let (me, resume, welcome_rooms) = match read_control(&mut read, &mut acc).await? {
        ServerMsg::Welcome {
            user_id,
            resume,
            rooms,
        } => (user_id, resume, rooms),
        ServerMsg::Reject { reason } => return Err(LinkError::Rejected(reason)),
        _ => return Err(LinkError::Unreachable),
    };
    let (channel, members, rooms) = if let Some(room) = room {
        write_control(&mut write, &ClientMsg::Join { channel: room }).await?;
        match read_control(&mut read, &mut acc).await? {
            ServerMsg::Joined {
                channel,
                members,
                rooms,
            } => (channel, peers(members), rooms),
            ServerMsg::Reject { reason } => return Err(LinkError::Rejected(reason)),
            _ => return Err(LinkError::Unreachable),
        }
    } else {
        (String::new(), Vec::new(), welcome_rooms)
    };

    let (cmd_tx, cmd_rx) = unbounded_channel();
    let (event_tx, events) = unbounded_channel();
    let (voice_out_tx, voice_out_rx) = crate::queue::newest(TCP_VOICE_QUEUE);
    let voice_in = Inbox::new(64);
    let (dead_tx, dead_rx) = tokio::sync::watch::channel(false);
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let clock = std::time::Instant::now();
    let (pending_tx, pending_rx) = tokio::sync::watch::channel(Vec::<u64>::new());
    let (delay_tx, delay) = tokio::sync::watch::channel(None);
    let (link, voice) = seat(
        cmd_tx,
        event_tx.clone(),
        Arc::clone(&voice_in),
        Outbox::Fresh(voice_out_tx),
        resume,
        channel.clone(),
        delay_tx.clone(),
        stop_tx.clone(),
    );
    tokio::spawn(write_tcp_out(
        write,
        voice_out_rx,
        cmd_rx,
        dead_tx.clone(),
        stop_tx,
        clock,
        pending_tx,
        Arc::clone(&link.voice_pass),
    ));
    tokio::spawn(read_tcp_in(
        read,
        acc,
        event_tx.clone(),
        voice_in,
        dead_tx,
        stop_rx,
        clock,
        pending_rx,
        delay_tx,
    ));
    tokio::spawn(watch_tcp(dead_rx, event_tx, Arc::clone(&link.quiet)));

    Ok(Live {
        me,
        channel,
        members,
        rooms,
        link,
        events,
        voice,
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
    crate::queue::limit_send_buffer(&stream);
    let config = tls_client_config(fingerprint).map_err(|_| LinkError::Unreachable)?;
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    let name =
        rustls::pki_types::ServerName::try_from("ryme").map_err(|_| LinkError::Unreachable)?;
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

async fn write_limited<W>(write: &mut W, tag: u8, payload: &[u8]) -> bool
where
    W: AsyncWrite + Unpin,
{
    matches!(
        tokio::time::timeout(WRITE_WAIT, write_one(write, tag, payload)).await,
        Ok(true)
    )
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
    mut voice_rx: crate::queue::NewestReceiver<Vec<u8>>,
    mut cmd_rx: UnboundedReceiver<Cmd>,
    dead: tokio::sync::watch::Sender<bool>,
    stop: tokio::sync::watch::Sender<bool>,
    clock: std::time::Instant,
    pending: tokio::sync::watch::Sender<Vec<u64>>,
    voice_pass: Arc<AtomicBool>,
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
                    if let Ok(frame) = encode_client(&ClientMsg::Logout) {
                        let _ = write_limited(&mut write, TCP_CONTROL, &frame).await;
                    }
                    let _ = write.shutdown().await;
                    return;
                }
                let Some(msg) = cmd_msg(cmd) else { continue };
                let Ok(frame) = encode_client(&msg) else { break };
                if !write_limited(&mut write, TCP_CONTROL, &frame).await {
                    break;
                }
            }
            _ = pulse.tick() => {
                let token = clock.elapsed().as_micros() as u64;
                let mut tokens = pending.borrow().clone();
                tokens.push(token);
                if tokens.len() > 4 {
                    tokens.remove(0);
                }
                let _ = pending.send(tokens);
                let Ok(frame) = encode_client(&ClientMsg::Ping { token }) else {
                    break;
                };
                if !write_limited(&mut write, TCP_CONTROL, &frame).await {
                    break;
                }
            }
            packet = voice_rx.recv_within(crate::queue::VOICE_MAX_AGE) => {
                match packet {
                    Some(packet) => {
                        if !voice_pass.load(Ordering::Relaxed) {
                            continue;
                        }
                        // A timed-out write may already have put bytes on the
                        // stream. The connection closes instead of sending
                        // another frame after that.
                        if !write_limited(&mut write, TCP_VOICE, &packet).await {
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
    voice_in: std::sync::Arc<Inbox>,
    dead: tokio::sync::watch::Sender<bool>,
    mut stop: tokio::sync::watch::Receiver<bool>,
    clock: std::time::Instant,
    pending: tokio::sync::watch::Receiver<Vec<u64>>,
    delay: tokio::sync::watch::Sender<Option<u32>>,
) where
    R: AsyncRead + Unpin,
{
    let mut tmp = [0u8; 2048];
    if !acc.is_empty() && !drain_tcp_in(&mut acc, &events, &voice_in, clock, &pending, &delay) {
        let _ = dead.send(true);
        return;
    }
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
    voice_in: &Inbox,
    clock: std::time::Instant,
    pending: &tokio::sync::watch::Receiver<Vec<u64>>,
    delay: &tokio::sync::watch::Sender<Option<u32>>,
) -> bool {
    loop {
        match decode_tagged(acc) {
            Ok(Some((tag, payload, n))) => {
                let owned = payload.to_vec();
                acc.drain(..n);
                if tag == TCP_VOICE {
                    voice_in.push(owned);
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
    quiet: Arc<AtomicBool>,
) {
    if dead.changed().await.is_ok() && *dead.borrow() && !quiet.load(Ordering::SeqCst) {
        let _ = events.send(Event::Disconnected);
    }
}

const QUIC_RETRY: Duration = Duration::from_secs(2);

/// Keep dialing QUIC after TCP has the call. Move the seat once datagrams have
/// been seen in both directions and the server has answered hello and join.
async fn pursue_quic(
    addr: SocketAddr,
    fingerprint: [u8; 32],
    name: String,
    card: u64,
    pass: [u8; 32],
    mut inflight: Option<
        tokio::task::JoinHandle<Result<(quinn::Endpoint, quinn::Connection), LinkError>>,
    >,
    link: Link,
) {
    let mut misses = 0u32;
    loop {
        if *link.stop.borrow() {
            if let Some(task) = inflight.take() {
                task.abort();
            }
            return;
        }
        let dialed = if let Some(task) = inflight.take() {
            match task.await {
                Ok(result) => result,
                Err(_) => Err(LinkError::Unreachable),
            }
        } else {
            dial(addr, fingerprint).await
        };
        if *link.stop.borrow() {
            if let Ok((endpoint, conn)) = &dialed {
                conn.close(quinn::VarInt::from_u32(0), b"bye");
                endpoint.close(quinn::VarInt::from_u32(0), b"bye");
            }
            return;
        }
        match dialed {
            Ok((endpoint, conn)) => {
                match move_to_quic(endpoint, conn, &name, card, pass, &link).await {
                    Ok(()) => return,
                    Err(LinkError::Pin) | Err(LinkError::Rejected(_)) => return,
                    Err(_) => {}
                }
            }
            Err(LinkError::Pin) => return,
            Err(_) => {}
        }
        misses = misses.saturating_add(1);
        let wait = QUIC_RETRY.saturating_mul(1 << misses.min(4));
        let mut stop = link.stop.subscribe();
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
            }
            _ = tokio::time::sleep(wait.min(Duration::from_secs(30))) => {}
        }
    }
}

fn both_ways(conn: &quinn::Connection) -> bool {
    let stats = conn.stats();
    stats.udp_rx.datagrams > 0 && stats.udp_tx.datagrams > 0
}

fn drop_quic(endpoint: &quinn::Endpoint, conn: &quinn::Connection) {
    conn.close(quinn::VarInt::from_u32(0), b"retry");
    endpoint.close(quinn::VarInt::from_u32(0), b"retry");
}

async fn move_to_quic(
    endpoint: quinn::Endpoint,
    conn: quinn::Connection,
    name: &str,
    card: u64,
    pass: [u8; 32],
    link: &Link,
) -> Result<(), LinkError> {
    if !both_ways(&conn) {
        drop_quic(&endpoint, &conn);
        return Err(LinkError::Unreachable);
    }
    let (mut send, mut recv) = match conn.open_bi().await {
        Ok(streams) => streams,
        Err(_) => {
            drop_quic(&endpoint, &conn);
            return Err(LinkError::Unreachable);
        }
    };
    let ticket = *link.resume.lock().unwrap();
    // Hello replaces the TCP seat. Do it only after a voice datagram has
    // echoed, so a failed check leaves the working call alone.
    if prove_media(&conn, &mut send, &mut recv, pass)
        .await
        .is_err()
    {
        drop_quic(&endpoint, &conn);
        return Err(LinkError::Unreachable);
    }
    link.quiet.store(true, Ordering::SeqCst);
    if write_msg(
        &mut send,
        &ClientMsg::Hello {
            name: name.to_string(),
            card,
            pass,
            resume: ticket,
        },
    )
    .await
    .is_err()
    {
        link.quiet.store(false, Ordering::SeqCst);
        drop_quic(&endpoint, &conn);
        return Err(LinkError::Unreachable);
    }
    let mut buf = Vec::new();
    let (me, resume) = match read_msg(&mut recv, &mut buf).await {
        Ok(ServerMsg::Welcome {
            user_id, resume, ..
        }) => (user_id, resume),
        Ok(ServerMsg::Reject { reason }) => {
            link.quiet.store(false, Ordering::SeqCst);
            drop_quic(&endpoint, &conn);
            return Err(LinkError::Rejected(reason));
        }
        _ => {
            drop_quic(&endpoint, &conn);
            let _ = link.events.send(Event::Disconnected);
            return Err(LinkError::Unreachable);
        }
    };
    let channel = link.channel.lock().unwrap().clone();
    let joined = if !channel.is_empty() {
        if write_msg(&mut send, &ClientMsg::Join { channel })
            .await
            .is_err()
        {
            link.quiet.store(false, Ordering::SeqCst);
            drop_quic(&endpoint, &conn);
            let _ = link.events.send(Event::Disconnected);
            return Err(LinkError::Unreachable);
        }
        match read_msg(&mut recv, &mut buf).await {
            Ok(ServerMsg::Joined {
                channel,
                members,
                rooms,
            }) => Some(Event::Joined {
                channel,
                members: peers(members),
                rooms,
            }),
            Ok(ServerMsg::Reject { reason }) => {
                let _ = link.events.send(Event::Rejected { reason });
                None
            }
            _ => {
                link.quiet.store(false, Ordering::SeqCst);
                drop_quic(&endpoint, &conn);
                let _ = link.events.send(Event::Disconnected);
                return Err(LinkError::Unreachable);
            }
        }
    } else {
        None
    };
    let (voice_tx, voice_rx) = crate::queue::newest(TCP_VOICE_QUEUE);
    let (cmd_tx, cmd_rx) = unbounded_channel();
    *link.outgoing.lock().unwrap() = Outbox::Fresh(voice_tx);
    *link.cmd.lock().unwrap() = cmd_tx;
    *link.resume.lock().unwrap() = resume;
    tokio::spawn(run(
        endpoint,
        conn,
        send,
        recv,
        buf,
        cmd_rx,
        link.events.clone(),
        voice_rx,
        Arc::clone(&link.incoming),
        link.delay.clone(),
        Arc::clone(&link.voice_pass),
    ));
    if let Some(event) = joined {
        let _ = link.events.send(event);
    }
    let _ = link.events.send(Event::Moved {
        user_id: me,
        resume,
    });
    Ok(())
}

async fn probe_media(conn: &quinn::Connection, pass: [u8; 32]) -> Result<(), LinkError> {
    let (mut send, mut recv) = conn.open_bi().await.map_err(|_| LinkError::Unreachable)?;
    prove_media(conn, &mut send, &mut recv, pass).await
}

async fn prove_media(
    conn: &quinn::Connection,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    pass: [u8; 32],
) -> Result<(), LinkError> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos() as u64)
        .unwrap_or(1);
    write_msg(send, &ClientMsg::Probe { pass, nonce }).await?;
    let mut buf = Vec::new();
    match read_msg(recv, &mut buf).await? {
        ServerMsg::ProbeAck { nonce: got } if got == nonce => {}
        _ => return Err(LinkError::Unreachable),
    }
    let packet = ryme_proto::encode_probe(nonce);
    conn.send_datagram(bytes::Bytes::from(packet))
        .map_err(|_| LinkError::Unreachable)?;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let left = deadline.saturating_duration_since(Instant::now());
        let Ok(Ok(got)) = tokio::time::timeout(left, conn.read_datagram()).await else {
            return Err(LinkError::Timeout);
        };
        if ryme_proto::decode_probe(&got) == Some(nonce) {
            return Ok(());
        }
    }
    Err(LinkError::Timeout)
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

fn resolve_addresses(host: &str, port: u16) -> Result<Vec<SocketAddr>, LinkError> {
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

async fn addresses(host: &str, port: u16) -> Result<Vec<SocketAddr>, LinkError> {
    let host = host.to_string();
    tokio::task::spawn_blocking(move || resolve_addresses(&host, port))
        .await
        .map_err(|_| LinkError::Address)?
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
    mut voice_out: crate::queue::NewestReceiver<Vec<u8>>,
    voice_in: std::sync::Arc<Inbox>,
    delay: tokio::sync::watch::Sender<Option<u32>>,
    voice_pass: Arc<AtomicBool>,
) {
    let mut tmp = [0u8; 2048];
    let mut intentional = false;
    let mut voice_open = true;
    if !drain(&mut buf, &events, |_| {}) {
        return;
    }
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
                        voice_in.push(packet.to_vec());
                    }
                    Err(_) => break,
                }
            }
            packet = voice_out.recv_within(crate::queue::VOICE_MAX_AGE), if voice_open => {
                match packet {
                    Some(packet) => {
                        if voice_pass.load(Ordering::Relaxed) {
                            let _ = conn.send_datagram(bytes::Bytes::from(packet));
                        }
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
            let _ = write_msg(send, &ClientMsg::Logout).await;
            return false;
        }
    };
    write_msg(send, &msg).await.is_ok()
}

fn note_pong(
    clock: std::time::Instant,
    pending: &tokio::sync::watch::Receiver<Vec<u64>>,
    delay: &tokio::sync::watch::Sender<Option<u32>>,
    token: u64,
) {
    if !pending.borrow().contains(&token) {
        return;
    }
    let now = clock.elapsed().as_micros() as u64;
    if token > now {
        return;
    }
    let ms = u32::try_from((now - token) / 1000).unwrap_or(u32::MAX);
    let _ = delay.send(Some(ms.min(9_999)));
}

fn drain(buf: &mut Vec<u8>, events: &UnboundedSender<Event>, mut on_pong: impl FnMut(u64)) -> bool {
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
                        muted,
                    } => Event::PeerJoined {
                        user_id,
                        name,
                        card,
                        muted,
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

    #[test]
    fn a_full_inbox_keeps_the_newest_frame() {
        let inbox = Inbox::new(2);
        inbox.push(vec![1]);
        inbox.push(vec![2]);
        inbox.push(vec![3]);
        assert_eq!(inbox.pop(), Some(vec![2]));
        assert_eq!(inbox.pop(), Some(vec![3]));
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
                muted: false,
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
    async fn a_long_chat_is_refused_and_the_call_stays() {
        let (port, fingerprint, pass, _task) = up().await;
        let url = invite(port, fingerprint, pass);
        let name = "你".repeat(20);
        let mut live = connect(&url, &name, 1, [0; 32]).await.unwrap();
        assert_eq!(live.members[0].name, name);

        live.link.chat("   ");
        let refused = tokio::time::timeout(Duration::from_secs(2), live.events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            refused,
            Event::Rejected {
                reason: "message was not sent".into(),
            }
        );

        live.link.chat("a".repeat(401));
        let again = tokio::time::timeout(Duration::from_secs(2), live.events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(again, Event::Rejected { .. }),
            "a long line dropped the call: {again:?}"
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

    #[tokio::test]
    async fn a_tcp_call_moves_when_quic_flows_both_ways() {
        let (port, fingerprint, pass, _task) = up().await;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let mut live = connect_tcp(addr, fingerprint, "Mara", 7, pass, [0; 32])
            .await
            .unwrap();
        assert!(live.tcp);
        let link = live.link.clone();
        tokio::spawn(pursue_quic(
            addr,
            fingerprint,
            "Mara".into(),
            7,
            pass,
            None,
            link,
        ));
        let moved = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match live.events.recv().await {
                    Some(Event::Moved { user_id, resume }) => break (user_id, resume),
                    Some(Event::Disconnected) => panic!("the call dropped while moving"),
                    Some(_) => {}
                    None => panic!("events closed"),
                }
            }
        })
        .await
        .expect("quic did not take the call");
        assert_ne!(moved.0, live.me);
        assert_ne!(moved.1, [0; 32]);

        let url = invite(port, fingerprint, pass);
        let mut other = connect(&url, "Jonas", 8, [0; 32]).await.unwrap();
        let packet = ryme_proto::encode_voice(&ryme_proto::VoicePacket {
            speaker: 0,
            seq: 1,
            timestamp: 480,
            flags: 0,
            payload: vec![4, 4],
        })
        .unwrap();
        other.voice.send(packet);
        let heard = ryme_proto::decode_voice(&wait_voice(&live).await).unwrap();
        assert_eq!(heard.payload, vec![4, 4]);

        live.link.chat("still here");
        let chat = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match other.events.recv().await {
                    Some(Event::Chat { user_id, text }) => break (user_id, text),
                    Some(_) => {}
                    None => panic!("events closed"),
                }
            }
        })
        .await
        .expect("chat");
        assert_eq!(chat.0, moved.0);
        assert_eq!(chat.1, "still here");
    }

    #[tokio::test]
    async fn leaving_on_tcp_is_not_undone_when_quic_connects() {
        let (port, fingerprint, pass, _task) = up().await;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let mut live = connect_tcp(addr, fingerprint, "Mara", 7, pass, [0; 32])
            .await
            .unwrap();
        live.link.leave();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if matches!(live.events.recv().await, Some(Event::Left)) {
                    break;
                }
            }
        })
        .await
        .expect("left");
        let link = live.link.clone();
        tokio::spawn(pursue_quic(
            addr,
            fingerprint,
            "Mara".into(),
            7,
            pass,
            None,
            link,
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match live.events.recv().await {
                    Some(Event::Moved { .. }) => break,
                    Some(Event::Joined { .. }) => panic!("promotion put the caller back in a room"),
                    Some(Event::Disconnected) => panic!("the call dropped"),
                    Some(_) => {}
                    None => panic!("events closed"),
                }
            }
        })
        .await
        .expect("move");
        let url = invite(port, fingerprint, pass);
        let other = connect(&url, "Jonas", 8, [0; 32]).await.unwrap();
        let packet = ryme_proto::encode_voice(&ryme_proto::VoicePacket {
            speaker: 0,
            seq: 1,
            timestamp: 480,
            flags: 0,
            payload: vec![4, 4],
        })
        .unwrap();
        other.voice.send(packet);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(live.voice.try_recv().is_none());
    }

    #[tokio::test]
    async fn a_failed_quic_attempt_leaves_the_tcp_call_up() {
        let (port, fingerprint, pass, _task) = up().await;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let mut live = connect_tcp(addr, fingerprint, "Mara", 9, pass, [0; 32])
            .await
            .unwrap();
        let link = live.link.clone();
        tokio::spawn(pursue_quic(
            SocketAddr::from(([127, 0, 0, 1], 1)),
            fingerprint,
            "Mara".into(),
            9,
            pass,
            None,
            link,
        ));
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!matches!(live.events.try_recv(), Ok(Event::Disconnected)));
        let url = invite(port, fingerprint, pass);
        let mut other = connect(&url, "Jonas", 10, [0; 32]).await.unwrap();
        live.link.chat("still here");
        let chat = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match other.events.recv().await {
                    Some(Event::Chat { text, .. }) => break text,
                    Some(_) => {}
                    None => panic!("events closed"),
                }
            }
        })
        .await
        .expect("chat");
        assert_eq!(chat, "still here");
    }

    #[tokio::test]
    async fn tcp_callers_keep_a_recent_voice_frame() {
        let (port, fingerprint, pass, _task) = up().await;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let mouth = connect_tcp(addr, fingerprint, "Mouth", 1, pass, [0; 32])
            .await
            .unwrap();
        let ear = connect_tcp(addr, fingerprint, "Ear", 2, pass, [0; 32])
            .await
            .unwrap();
        let last = 200u32;
        let mut heard = 0u32;
        for seq in 1..=last {
            let packet = ryme_proto::encode_voice(&ryme_proto::VoicePacket {
                speaker: 0,
                seq,
                timestamp: seq.wrapping_mul(480),
                flags: 0,
                payload: vec![1, 2, 3, 4],
            })
            .unwrap();
            mouth.voice.send(packet);
            while let Some(bytes) = ear.voice.try_recv() {
                let voice = ryme_proto::decode_voice(&bytes).unwrap();
                heard = heard.max(voice.seq);
            }
            if seq % 10 == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if let Some(bytes) = ear.voice.try_recv() {
                let voice = ryme_proto::decode_voice(&bytes).unwrap();
                heard = heard.max(voice.seq);
                if heard + 15 >= last {
                    break;
                }
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        assert!(
            heard + 15 >= last,
            "newest voice heard was {heard}, sent {last}"
        );
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
        let v4 = resolve_addresses("127.0.0.1", 1542).unwrap();
        assert!(v4.iter().all(|addr| addr.is_ipv4()));
        let local = resolve_addresses("0.0.0.0", 1542).unwrap();
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

    async fn voice_holds(mut live: Live, frames_per_sec: u64, secs: u64) -> Result<Live, Duration> {
        let packet = ryme_proto::encode_voice(&ryme_proto::VoicePacket {
            speaker: 0,
            seq: 1,
            timestamp: 480,
            flags: 0,
            payload: vec![1, 2, 3, 4],
        })
        .unwrap();
        let start = std::time::Instant::now();
        let gap = Duration::from_nanos(1_000_000_000 / frames_per_sec.max(1));
        let stop = Duration::from_secs(secs);
        while start.elapsed() < stop {
            if let Ok(Event::Disconnected) = live.events.try_recv() {
                return Err(start.elapsed());
            }
            live.voice.send(packet.clone());
            tokio::time::sleep(gap).await;
        }
        if let Ok(Event::Disconnected) = live.events.try_recv() {
            return Err(start.elapsed());
        }
        Ok(live)
    }

    #[tokio::test]
    async fn tcp_voice_at_speaking_rate_stays_up() {
        let (port, fingerprint, pass, _task) = up().await;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let live = connect_tcp(addr, fingerprint, "Mara", 1, pass, [0; 32])
            .await
            .unwrap();
        let live = match voice_holds(live, 100, 3).await {
            Ok(live) => live,
            Err(after) => panic!("tcp voice at 100/s dropped after {after:?}"),
        };
        let mut live = match voice_holds(live, 200, 3).await {
            Ok(live) => live,
            Err(after) => panic!("tcp voice at 200/s dropped after {after:?}"),
        };
        live.link.chat("still here");
        let chat = tokio::time::timeout(Duration::from_secs(2), live.events.recv())
            .await
            .expect("chat after a fast voice stream")
            .expect("session ended");
        assert!(matches!(chat, Event::Chat { .. }), "got {chat:?}");
    }
}
