//! Speakers for the Pi measurement. Eighty payload bytes, one packet every
//! 10 ms, the same shape as the numbers on ryme.chat. A card is required by
//! the current hello; it is not sent anywhere except this process.
use std::time::Duration;

use ryme_proto::{encode_voice, VoicePacket};
use ryme_server::connect;

#[tokio::main]
async fn main() {
    let invite = std::env::var("RYME_INVITE").unwrap_or_else(|_| {
        std::env::args().nth(1).expect("invite")
    });
    let speakers: usize = std::env::args()
        .nth(if std::env::var("RYME_INVITE").is_ok() { 1 } else { 2 })
        .expect("speakers")
        .parse()
        .unwrap();
    let seconds: u64 = std::env::args()
        .nth(if std::env::var("RYME_INVITE").is_ok() { 2 } else { 3 })
        .unwrap_or_else(|| "12".into())
        .parse()
        .unwrap();

    let mut pending = Vec::with_capacity(speakers);
    for index in 0..speakers {
        let invite = invite.clone();
        pending.push(tokio::spawn(async move {
            connect(&invite, &format!("p{index}"), (index as u64) + 1, [0; 32])
                .await
                .expect("join")
        }));
    }
    let mut room = Vec::with_capacity(speakers);
    for joined in pending {
        room.push(joined.await.expect("task"));
    }
    eprintln!("joined {speakers}");

    let mut seq = 0u32;
    let until = tokio::time::Instant::now() + Duration::from_secs(seconds);
    while tokio::time::Instant::now() < until {
        let packet = encode_voice(&VoicePacket {
            speaker: 0,
            seq,
            timestamp: seq.wrapping_mul(480),
            flags: 0,
            payload: vec![0; 80],
        })
        .expect("voice");
        for person in &room {
            person.voice.send(packet.clone());
        }
        seq = seq.wrapping_add(1);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    eprintln!("frames {seq}");
    for person in &room {
        person.link.logout();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
}
