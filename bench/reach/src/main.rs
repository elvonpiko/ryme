//! One speaker and one listener on a real path.
//!
//! Both run in this process. The server is the invite's host. A received
//! packet was forwarded to the other participant. Loss is packets handed to
//! the socket minus packets the listener read. Delay is that same interval.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ryme_proto::{decode_voice, encode_voice, VoicePacket};
use ryme_server::connect;

#[tokio::main]
async fn main() {
    let invite = std::env::args().nth(1).expect("invite");
    let seconds: u64 = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "12".into())
        .parse()
        .expect("seconds");
    let interval_ms: u64 = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "10".into())
        .parse()
        .expect("interval ms");

    let started = Instant::now();
    let listener = connect(&invite, "ear", 1, [0; 32])
        .await
        .expect("listener");
    let speaker = connect(&invite, "mouth", 2, [0; 32])
        .await
        .expect("speaker");
    let connect_ms = started.elapsed().as_millis();

    let marks: Arc<Mutex<HashMap<u32, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    let voice = speaker.voice.queue();
    let marked = marks.clone();
    let sending = tokio::spawn(async move {
        let mut seq = 0u32;
        let until = Instant::now() + Duration::from_secs(seconds);
        while Instant::now() < until {
            let packet = encode_voice(&VoicePacket {
                speaker: 0,
                seq,
                timestamp: seq.wrapping_mul(480),
                flags: 0,
                payload: vec![0; 80],
            })
            .expect("voice");
            marked.lock().unwrap().insert(seq, Instant::now());
            if voice.send(packet).await.is_err() {
                break;
            }
            seq = seq.wrapping_add(1);
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        }
        seq
    });

    let mut delays = Vec::new();
    let mut got = std::collections::HashSet::new();
    let listen_until = Instant::now() + Duration::from_secs(seconds + 2);
    while Instant::now() < listen_until {
        while let Some(packet) = listener.voice.try_recv() {
            let Ok(voice) = decode_voice(&packet) else {
                continue;
            };
            if !got.insert(voice.seq) {
                continue;
            }
            if let Some(sent) = marks.lock().unwrap().get(&voice.seq) {
                delays.push(sent.elapsed().as_secs_f64() * 1000.0);
            }
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let sent = sending.await.expect("speaker");
    listener.link.logout();
    speaker.link.logout();
    tokio::time::sleep(Duration::from_millis(200)).await;

    delays.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let received = got.len() as u32;
    let lost = sent.saturating_sub(received);
    let (low, mid, high) = summary(&delays);
    println!("connected_ms {connect_ms}");
    println!("sent {sent}");
    println!("received {received}");
    println!("lost {lost}");
    println!("delay_ms min={low:.1} p50={mid:.1} max={high:.1}");
}

fn summary(delays: &[f64]) -> (f64, f64, f64) {
    if delays.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mid = delays[delays.len() / 2];
    (delays[0], mid, delays[delays.len() - 1])
}
