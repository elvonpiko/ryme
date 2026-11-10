use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ryme_proto::{decode_client_frame, encode_server, ClientMsg, ProtoError, ServerMsg};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::hub::{Hub, Step};

#[derive(Clone)]
struct Pipes {
    control: UnboundedSender<ServerMsg>,
    voice: UnboundedSender<Bytes>,
}

pub(crate) struct World {
    hub: Hub,
    pipes: HashMap<u32, Pipes>,
}

impl World {
    fn new() -> Self {
        Self {
            hub: Hub::new(),
            pipes: HashMap::new(),
        }
    }

    fn handle(&mut self, user: Option<u32>, msg: ClientMsg, pipes: &Pipes) -> Option<u32> {
        let step = self.hub.handle(user, msg);
        if let Some(id) = step.user_id {
            self.pipes.insert(id, pipes.clone());
        }
        self.dispatch(&step, pipes);
        step.user_id
    }

    fn voice(&mut self, from: u32, packet: Bytes) {
        let targets = self.hub.voice_targets(from, &packet);
        for id in targets {
            if let Some(pipes) = self.pipes.get(&id) {
                let _ = pipes.voice.send(packet.clone());
            }
        }
    }

    fn disconnect(&mut self, id: u32) {
        let notices = self.hub.disconnect(id);
        self.pipes.remove(&id);
        for (peer, msg) in notices {
            if let Some(pipes) = self.pipes.get(&peer) {
                let _ = pipes.control.send(msg);
            }
        }
    }

    fn dispatch(&self, step: &Step, self_pipes: &Pipes) {
        for msg in &step.to_self {
            let _ = self_pipes.control.send(msg.clone());
        }
        for (peer, msg) in &step.to_others {
            if let Some(pipes) = self.pipes.get(peer) {
                let _ = pipes.control.send(msg.clone());
            }
        }
    }
}

pub async fn run(conn: quinn::Connection, world: Arc<Mutex<World>>) {
    let Ok((send, mut recv)) = conn.accept_bi().await else {
        return;
    };

    let (control_tx, control_rx) = unbounded_channel();
    let (voice_tx, voice_rx) = unbounded_channel();
    let pipes = Pipes {
        control: control_tx,
        voice: voice_tx,
    };
    let writer = tokio::spawn(write_out(send, conn.clone(), control_rx, voice_rx));

    let mut user_id = None;
    let mut buf = Vec::new();
    let mut tmp = [0u8; 2048];
    loop {
        tokio::select! {
            read = recv.read(&mut tmp) => {
                match read {
                    Ok(Some(n)) if n > 0 => {
                        buf.extend_from_slice(&tmp[..n]);
                        match drain_frames(&mut buf, &world, &mut user_id, &pipes) {
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
                        if let Some(id) = user_id {
                            world.lock().expect("hub").voice(id, packet);
                        }
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
    let _ = writer.await;
}

fn drain_frames(
    buf: &mut Vec<u8>,
    world: &Arc<Mutex<World>>,
    user_id: &mut Option<u32>,
    pipes: &Pipes,
) -> Result<bool, ProtoError> {
    loop {
        match decode_client_frame(buf)? {
            Some((msg, n)) => {
                buf.drain(..n);
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
    mut control_rx: UnboundedReceiver<ServerMsg>,
    mut voice_rx: UnboundedReceiver<Bytes>,
) {
    loop {
        tokio::select! {
            msg = control_rx.recv() => {
                let Some(msg) = msg else { break };
                let Ok(frame) = encode_server(&msg) else { break };
                if send.write_all(&frame).await.is_err() {
                    break;
                }
            }
            packet = voice_rx.recv() => {
                let Some(packet) = packet else { break };
                if conn.send_datagram(packet).is_err() {
                    break;
                }
            }
        }
    }
}

pub(crate) fn world() -> Arc<Mutex<World>> {
    Arc::new(Mutex::new(World::new()))
}
