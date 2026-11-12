use std::net::SocketAddr;
use std::time::Duration;

use crate::identity::client_config;
use crate::Server;
use bytes::Bytes;
use ryme_proto::{
    decode_server_frame, decode_voice, encode_client, encode_voice, ClientMsg, ServerMsg,
    VoicePacket,
};

async fn dial(
    port: u16,
    fingerprint: [u8; 32],
) -> (
    quinn::Endpoint,
    quinn::Connection,
    quinn::SendStream,
    quinn::RecvStream,
) {
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(client_config(fingerprint).unwrap());
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let conn = endpoint.connect(addr, "ryme").unwrap().await.unwrap();
    let (send, recv) = conn.open_bi().await.unwrap();
    (endpoint, conn, send, recv)
}

async fn send_msg(send: &mut quinn::SendStream, msg: &ClientMsg) {
    send.write_all(&encode_client(msg).unwrap()).await.unwrap();
}

async fn next_msg(recv: &mut quinn::RecvStream, buf: &mut Vec<u8>) -> ServerMsg {
    let mut tmp = [0u8; 2048];
    loop {
        if let Some((msg, n)) = decode_server_frame(buf).unwrap() {
            buf.drain(..n);
            return msg;
        }
        let n = tokio::time::timeout(Duration::from_secs(2), recv.read(&mut tmp))
            .await
            .expect("timed out waiting for a server frame")
            .unwrap()
            .expect("server closed the control stream");
        buf.extend_from_slice(&tmp[..n]);
    }
}

#[tokio::test]
async fn two_clients_share_voice_until_one_mutes() {
    let server = Server::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = server.port();
    let fingerprint = server.fingerprint();
    assert!(server.invite("127.0.0.1").contains(&format!(":{port}/#")));
    tokio::spawn(async move {
        server.serve().await;
    });

    let (_a_end, a_conn, mut a_send, mut a_recv) = dial(port, fingerprint).await;
    let (_b_end, b_conn, mut b_send, mut b_recv) = dial(port, fingerprint).await;
    let mut a_buf = Vec::new();
    let mut b_buf = Vec::new();

    send_msg(
        &mut a_send,
        &ClientMsg::Hello {
            name: "Mara".into(),
            card: 1,
        },
    )
    .await;
    send_msg(
        &mut b_send,
        &ClientMsg::Hello {
            name: "Jonas".into(),
            card: 2,
        },
    )
    .await;
    assert!(matches!(
        next_msg(&mut a_recv, &mut a_buf).await,
        ServerMsg::Welcome { user_id: 1 }
    ));
    assert!(matches!(
        next_msg(&mut b_recv, &mut b_buf).await,
        ServerMsg::Welcome { user_id: 2 }
    ));

    send_msg(
        &mut a_send,
        &ClientMsg::Join {
            channel: "Lobby".into(),
        },
    )
    .await;
    send_msg(
        &mut b_send,
        &ClientMsg::Join {
            channel: "Lobby".into(),
        },
    )
    .await;
    assert!(matches!(
        next_msg(&mut a_recv, &mut a_buf).await,
        ServerMsg::Joined { .. }
    ));
    assert!(matches!(
        next_msg(&mut b_recv, &mut b_buf).await,
        ServerMsg::Joined { .. }
    ));
    assert!(matches!(
        next_msg(&mut a_recv, &mut a_buf).await,
        ServerMsg::PeerJoined { name, .. } if name == "Jonas"
    ));

    send_msg(
        &mut a_send,
        &ClientMsg::Chat {
            text: "pushing mid".into(),
        },
    )
    .await;
    assert!(matches!(
        next_msg(&mut b_recv, &mut b_buf).await,
        ServerMsg::Chat { text, .. } if text == "pushing mid"
    ));

    let packet = encode_voice(&VoicePacket {
        speaker: 99,
        seq: 1,
        timestamp: 960,
        flags: 0,
        payload: vec![7, 7, 7],
    })
    .unwrap();
    a_conn.send_datagram(Bytes::from(packet)).unwrap();
    let heard = tokio::time::timeout(Duration::from_secs(2), b_conn.read_datagram())
        .await
        .unwrap()
        .unwrap();
    let heard = decode_voice(&heard).unwrap();
    assert_eq!(heard.speaker, 1);
    assert_eq!(heard.payload, vec![7, 7, 7]);

    send_msg(&mut a_send, &ClientMsg::Mute { muted: true }).await;
    assert!(matches!(
        next_msg(&mut b_recv, &mut b_buf).await,
        ServerMsg::Muted { muted: true, .. }
    ));
    let dropped = encode_voice(&VoicePacket {
        speaker: 1,
        seq: 2,
        timestamp: 1920,
        flags: 0,
        payload: vec![8],
    })
    .unwrap();
    a_conn.send_datagram(Bytes::from(dropped)).unwrap();
    let none = tokio::time::timeout(Duration::from_millis(250), b_conn.read_datagram()).await;
    assert!(none.is_err(), "muted voice was relayed");
}

#[tokio::test]
async fn a_wrong_fingerprint_does_not_connect() {
    let server = Server::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = server.port();
    tokio::spawn(async move {
        server.serve().await;
    });

    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(client_config([0u8; 32]).unwrap());
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        endpoint.connect(addr, "ryme").unwrap(),
    )
    .await
    .expect("handshake hung");
    assert!(result.is_err());
}
