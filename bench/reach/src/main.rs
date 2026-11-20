//! One speaker and one listener on a real path.
//!
//! Both run in this process. The server is the invite's host. `queued` is
//! frames accepted by the send queue. `socket_sent` is frames written to the
//! socket. Loss is socket writes the listener did not read. No audio is a
//! failed run. `RYME_TRANSPORT=tcp` stays on TLS.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ryme_proto::{decode_voice, encode_voice, parse_invite, VoicePacket};
use ryme_server::{connect, connect_tcp_invite};

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

    let parsed = parse_invite(&invite).expect("invite");
    println!("host {}", parsed.host);
    let tcp_only = std::env::var("RYME_TRANSPORT")
        .ok()
        .is_some_and(|value| value.eq_ignore_ascii_case("tcp"));
    let started = Instant::now();
    let listener = if tcp_only {
        connect_tcp_invite(&invite, "ear", 1, [0; 32]).await
    } else {
        connect(&invite, "ear", 1, [0; 32]).await
    }
    .expect("listener");
    let speaker = if tcp_only {
        connect_tcp_invite(&invite, "mouth", 2, [0; 32]).await
    } else {
        connect(&invite, "mouth", 2, [0; 32]).await
    }
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
    let queued_attempt = sending.await.expect("speaker");
    let _ = queued_attempt;
    listener.link.logout();
    speaker.link.logout();
    tokio::time::sleep(Duration::from_millis(200)).await;

    delays.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let (queued, socket_sent) = speaker.voice.counts();
    let received = got.len() as u64;
    let (low, mid, high) = summary(&delays);
    println!("transport {}", if tcp_only { "tcp" } else { "auto" });
    println!("connected_ms {connect_ms}");
    println!("elapsed_ms {}", started.elapsed().as_millis());
    println!("queued {queued}");
    println!("socket_sent {socket_sent}");
    println!("received {received}");
    match delivery(socket_sent, received) {
        Ok(lost) => {
            println!("lost {lost}");
            if delays.is_empty() {
                println!("delay_ms none");
            } else {
                println!("delay_ms min={low:.1} p50={mid:.1} max={high:.1}");
            }
        }
        Err(reason) => {
            eprintln!("{reason}");
            std::process::exit(1);
        }
    }
}

fn delivery(socket_sent: u64, received: u64) -> Result<u64, &'static str> {
    if socket_sent == 0 || received == 0 {
        Err("no audio arrived")
    } else {
        Ok(socket_sent.saturating_sub(received))
    }
}

#[cfg(test)]
mod tests {
    use super::delivery;

    #[test]
    fn silence_is_a_failed_run() {
        assert!(delivery(0, 0).is_err());
        assert!(delivery(4, 0).is_err());
        assert_eq!(delivery(4, 3).unwrap(), 1);
    }
}

fn summary(delays: &[f64]) -> (f64, f64, f64) {
    if delays.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mid = delays[delays.len() / 2];
    (delays[0], mid, delays[delays.len() - 1])
}
