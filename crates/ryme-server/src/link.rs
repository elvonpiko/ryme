use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use ryme_proto::{decode_server_frame, encode_client, parse_invite, ClientMsg, Member, ServerMsg};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::identity::client_config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub user_id: u32,
    pub name: String,
    pub muted: bool,
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
    },
    PeerLeft {
        user_id: u32,
    },
    Chat {
        user_id: u32,
        text: String,
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
    Rejected(String),
}

pub async fn connect(invite: &str, name: &str) -> Result<Live, LinkError> {
    let parsed = parse_invite(invite).map_err(|_| LinkError::Invite)?;
    let addr = resolve(&parsed.host, parsed.port)?;
    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())
        .map_err(|_| LinkError::Unreachable)?;
    endpoint.set_default_client_config(
        client_config(parsed.fingerprint).map_err(|_| LinkError::Unreachable)?,
    );
    let connecting = endpoint
        .connect(addr, "ryme")
        .map_err(|_| LinkError::Unreachable)?;
    let conn = tokio::time::timeout(Duration::from_secs(4), connecting)
        .await
        .map_err(|_| LinkError::Unreachable)?
        .map_err(|_| LinkError::Unreachable)?;
    let (mut send, mut recv) = conn.open_bi().await.map_err(|_| LinkError::Unreachable)?;

    write_msg(
        &mut send,
        &ClientMsg::Hello {
            name: name.to_string(),
        },
    )
    .await?;
    let mut buf = Vec::new();
    let me = match read_msg(&mut recv, &mut buf).await? {
        ServerMsg::Welcome { user_id } => user_id,
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
    })
}

fn resolve(host: &str, port: u16) -> Result<SocketAddr, LinkError> {
    let host = match host {
        "0.0.0.0" => "127.0.0.1",
        "::" | "[::]" => "::1",
        other => other,
    };
    (host, port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
        .ok_or(LinkError::Unreachable)
}

fn peers(members: Vec<Member>) -> Vec<Peer> {
    members
        .into_iter()
        .map(|member| Peer {
            user_id: member.user_id,
            name: member.name,
            muted: member.muted,
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
            .map_err(|_| LinkError::Unreachable)?
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
) {
    let mut tmp = [0u8; 2048];
    let mut intentional = false;
    let mut voice_open = true;
    loop {
        tokio::select! {
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
                        if !drain(&mut buf, &events) {
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

fn drain(buf: &mut Vec<u8>, events: &UnboundedSender<Event>) -> bool {
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
                    ServerMsg::PeerJoined { user_id, name } => Event::PeerJoined { user_id, name },
                    ServerMsg::PeerLeft { user_id } => Event::PeerLeft { user_id },
                    ServerMsg::Chat { user_id, text } => Event::Chat { user_id, text },
                    ServerMsg::Muted { user_id, muted } => Event::Muted { user_id, muted },
                    ServerMsg::Left => Event::Left,
                    ServerMsg::Reject { reason } => Event::Rejected { reason },
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

    async fn up() -> (u16, [u8; 32], tokio::task::JoinHandle<()>) {
        let server = Server::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let port = server.port();
        let fingerprint = server.fingerprint();
        let task = tokio::spawn(async move { server.serve().await });
        (port, fingerprint, task)
    }

    fn invite(port: u16, fingerprint: [u8; 32]) -> String {
        crate::identity::invite_url("127.0.0.1", port, &fingerprint)
    }

    #[tokio::test]
    async fn join_shows_you_and_a_bad_invite_fails() {
        match connect("nope", "Mara").await {
            Err(LinkError::Invite) => {}
            Err(err) => panic!("expected a bad invite, got {err:?}"),
            Ok(_) => panic!("bad invite connected"),
        }

        let (port, fingerprint, _task) = up().await;
        let mut bad = fingerprint;
        bad[0] ^= 0xff;
        match connect(&invite(port, bad), "Mara").await {
            Err(LinkError::Unreachable) => {}
            Err(err) => panic!("expected a refused pin, got {err:?}"),
            Ok(_) => panic!("wrong fingerprint connected"),
        }

        let url = invite(port, fingerprint);
        let live = connect(&url, "Mara").await.unwrap();
        assert_eq!(live.channel, "Lobby");
        assert_eq!(live.members.len(), 1);
        assert_eq!(live.members[0].name, "Mara");
        assert_eq!(live.members[0].user_id, live.me);

        let unspecified = crate::identity::invite_url("0.0.0.0", port, &fingerprint);
        let again = connect(&unspecified, "Mara").await.unwrap();
        assert_eq!(again.members[0].name, "Mara");
    }

    #[tokio::test]
    async fn a_second_client_is_announced_and_logout_drops_them() {
        let (port, fingerprint, _task) = up().await;
        let url = invite(port, fingerprint);
        let mut mara = connect(&url, "Mara").await.unwrap();
        let jonas = connect(&url, "Jonas").await.unwrap();
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
        let (port, fingerprint, _task) = up().await;
        let url = invite(port, fingerprint);
        let mut live = connect(&url, "Mara").await.unwrap();
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
        tokio::spawn(async move { server.serve().await });
        let url = invite(port, fingerprint);
        let mut live = connect(&url, "Mara").await.unwrap();
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
        let (port, fingerprint, _task) = up().await;
        let url = invite(port, fingerprint);
        let mara = connect(&url, "Mara").await.unwrap();
        let jonas = connect(&url, "Jonas").await.unwrap();

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
    }
}
