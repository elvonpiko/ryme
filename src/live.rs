use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ryme_server::{connect, Event, Link, LinkError, Peer};

use crate::ear::Ear;
use slint::{ComponentHandle, ModelRc, VecModel, Weak};

use crate::local::{self, Saved};
use crate::{Channel, Line, MainWindow, Member};

const TINTS: [[u8; 3]; 6] = [
    [0xc9, 0xc6, 0xbf],
    [0xa7, 0xa3, 0xb3],
    [0x8d, 0x93, 0x8c],
    [0xb0, 0x9a, 0x93],
    [0x96, 0xa0, 0xad],
    [0xc4, 0xb8, 0xa0],
];

#[derive(Clone)]
struct Room {
    me: u32,
    channel: String,
    members: Vec<Peer>,
    rooms: Vec<String>,
    lines: Vec<(String, String, bool)>,
    in_channel: bool,
    speaking: HashMap<u32, bool>,
}

impl Room {
    fn new(me: u32, channel: String, members: Vec<Peer>, rooms: Vec<String>) -> Self {
        let rooms = if rooms.is_empty() {
            vec![channel.clone()]
        } else {
            rooms
        };
        Self {
            me,
            channel,
            members,
            rooms,
            lines: Vec::new(),
            in_channel: true,
            speaking: HashMap::new(),
        }
    }

    fn apply(&mut self, event: Event) {
        match event {
            Event::Joined {
                channel,
                members,
                rooms,
            } => {
                self.channel = channel;
                self.members = members;
                self.in_channel = true;
                if !rooms.is_empty() {
                    self.rooms = rooms;
                }
            }
            Event::PeerJoined { user_id, name } => {
                if !self.members.iter().any(|peer| peer.user_id == user_id) {
                    self.members.push(Peer {
                        user_id,
                        name,
                        muted: false,
                    });
                }
            }
            Event::PeerLeft { user_id } => {
                self.members.retain(|peer| peer.user_id != user_id);
                self.speaking.remove(&user_id);
            }
            Event::Chat { user_id, text } => {
                let mine = user_id == self.me;
                let author = if mine {
                    "You".to_string()
                } else {
                    self.members
                        .iter()
                        .find(|peer| peer.user_id == user_id)
                        .map(|peer| peer.name.clone())
                        .unwrap_or_else(|| "Someone".to_string())
                };
                self.lines.push((author, text, mine));
            }
            Event::Muted { user_id, muted } => {
                if let Some(peer) = self.members.iter_mut().find(|peer| peer.user_id == user_id) {
                    peer.muted = muted;
                }
            }
            Event::Left => {
                self.in_channel = false;
                self.members.clear();
                self.speaking.clear();
            }
            Event::Rejected { .. } | Event::Disconnected => {}
        }
    }
}

struct Slot {
    link: Option<Link>,
    ear: Option<Ear>,
    generation: u64,
    channel: String,
    resume_channel: String,
    ptt: bool,
}

enum Ask {
    None,
    Logout,
    Update,
}

pub struct Shutdown {
    slot: Arc<Mutex<Slot>>,
}

impl Shutdown {
    /// Tell the server this member is gone, and leave the socket up long enough
    /// for that close to leave the machine.
    pub fn hang_up(&self) -> bool {
        let guard = self.slot.lock().unwrap();
        if let Some(link) = guard.link.as_ref() {
            link.logout();
            true
        } else {
            false
        }
    }
}

pub fn attach(ui: &MainWindow, rt: &tokio::runtime::Handle) -> Shutdown {
    let slot = Arc::new(Mutex::new(Slot {
        link: None,
        ear: None,
        generation: 0,
        channel: String::new(),
        resume_channel: String::new(),
        ptt: ui.get_ptt(),
    }));
    let ask = Arc::new(Mutex::new(Ask::None));
    let notices = Arc::new(AtomicU64::new(0));
    if let Some(saved) = local::load() {
        ui.set_display_name(saved.name.clone().into());
        ui.set_invite(saved.invite.clone().into());
        if saved.resume {
            slot.lock().unwrap().resume_channel = saved.channel;
            begin(ui, &slot, rt, &notices);
        }
    }

    ui.on_submit_join({
        let ui_weak = ui.as_weak();
        let slot = slot.clone();
        let rt = rt.clone();
        let notices = notices.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                begin(&ui, &slot, &rt, &notices);
            }
        }
    });

    ui.on_send_chat({
        let slot = slot.clone();
        move |text| send(&slot, |link| link.chat(text.to_string()))
    });

    ui.on_set_mute({
        let slot = slot.clone();
        move |muted| {
            if let Some(ear) = slot.lock().unwrap().ear.as_ref() {
                ear.set_muted(muted);
            }
            send(&slot, |link| link.mute(muted));
        }
    });

    ui.on_set_deafen({
        let slot = slot.clone();
        move |deafened| {
            if let Some(ear) = slot.lock().unwrap().ear.as_ref() {
                ear.set_deaf(deafened);
            }
            send(&slot, |link| link.deafen(deafened));
        }
    });

    ui.on_set_ptt({
        let slot = slot.clone();
        move |on| {
            let mut guard = slot.lock().unwrap();
            guard.ptt = on;
            if let Some(ear) = guard.ear.as_ref() {
                ear.set_ptt(on);
            }
        }
    });

    ui.on_leave_channel({
        let slot = slot.clone();
        move || send(&slot, |link| link.leave())
    });

    ui.on_pick_channel({
        let slot = slot.clone();
        move |name| send(&slot, |link| link.join(name.to_string()))
    });

    ui.on_logout({
        let weak = ui.as_weak();
        let ask = ask.clone();
        move || {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            if !ui.get_inside() || ui.get_confirming() {
                return;
            }
            *ask.lock().unwrap() = Ask::Logout;
            ui.set_confirm_danger(true);
            pose(
                &ui,
                "Log out",
                "Leave this server? Ryme will not reconnect on its own.",
                "Log out",
            );
        }
    });

    ui.on_request_close({
        let weak = ui.as_weak();
        move || {
            if let Some(ui) = weak.upgrade() {
                let _ = ui.window().hide();
            }
        }
    });

    ui.on_apply_update({
        let weak = ui.as_weak();
        let ask = ask.clone();
        move || {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            if ui.get_confirming() || ui.get_update_label().is_empty() || ui.get_update_busy() {
                return;
            }
            let in_call = ui.get_inside() && ui.get_joined() >= 0;
            let body = if in_call {
                "Ryme restarts and this call drops."
            } else {
                "Ryme restarts."
            };
            let title = ui.get_update_label().to_string();
            *ask.lock().unwrap() = Ask::Update;
            ui.set_confirm_danger(false);
            pose(&ui, &title, body, "Update");
        }
    });

    ui.on_confirm_no({
        let weak = ui.as_weak();
        let ask = ask.clone();
        move || {
            *ask.lock().unwrap() = Ask::None;
            if let Some(ui) = weak.upgrade() {
                ui.set_confirming(false);
            }
        }
    });

    ui.on_confirm_yes({
        let weak = ui.as_weak();
        let ask = ask.clone();
        let slot = slot.clone();
        let rt = rt.clone();
        let notices = notices.clone();
        move || {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            ui.set_confirming(false);
            let pending = std::mem::replace(&mut *ask.lock().unwrap(), Ask::None);
            match pending {
                Ask::Logout => {
                    let channel = slot.lock().unwrap().channel.clone();
                    local::store(&Saved {
                        name: ui.get_display_name().to_string(),
                        invite: ui.get_invite().to_string(),
                        channel,
                        resume: false,
                    });
                    bump(&slot);
                    clear(&ui);
                }
                Ask::Update => {
                    ui.set_update_busy(true);
                    let weak = weak.clone();
                    let rt = rt.clone();
                    let notices = notices.clone();
                    crate::update::launch(rt.clone(), move |message| {
                        let Some(ui) = weak.upgrade() else {
                            return;
                        };
                        ui.set_update_busy(false);
                        note(&ui, &rt, &notices, &message);
                    });
                }
                Ask::None => {}
            }
        }
    });

    let weak = ui.as_weak();
    let ask = ask.clone();
    crate::update::watch(rt.clone(), move |version| {
        let Some(ui) = weak.upgrade() else {
            return;
        };
        let title = format!("{version} is ready");
        ui.set_update_label(title.as_str().into());
        if ui.get_inside() || ui.get_confirming() {
            return;
        }
        *ask.lock().unwrap() = Ask::Update;
        ui.set_confirm_danger(false);
        pose(&ui, &title, "Ryme restarts.", "Update");
    });
    Shutdown { slot }
}

fn begin(
    ui: &MainWindow,
    slot: &Arc<Mutex<Slot>>,
    rt: &tokio::runtime::Handle,
    notices: &Arc<AtomicU64>,
) {
    let name = ui.get_display_name().to_string();
    let invite = ui.get_invite().to_string();
    let gen = bump(slot);
    ui.set_joining(true);
    ui.set_toast("".into());
    let weak = ui.as_weak();
    let slot = slot.clone();
    let rt = rt.clone();
    let task_rt = rt.clone();
    let notices = notices.clone();
    rt.spawn(async move {
        match connect(&invite, &name).await {
            Ok(live) => {
                let resume_channel = {
                    let mut guard = slot.lock().unwrap();
                    if guard.generation != gen {
                        live.link.logout();
                        return;
                    }
                    guard.link = Some(live.link.clone());
                    guard.channel = live.channel.clone();
                    std::mem::take(&mut guard.resume_channel)
                };
                if !resume_channel.is_empty() && resume_channel != live.channel {
                    live.link.join(resume_channel.clone());
                }
                remember(&name, &invite, &live.channel, true);
                let mut room = Room::new(live.me, live.channel.clone(), live.members, live.rooms);
                show(&weak, &slot, gen, &room, true);
                let (speak_tx, mut speak_rx) = tokio::sync::mpsc::unbounded_channel();
                let ear = crate::ear::open(live.voice, live.me, speak_tx);
                let (mic, speakers) = (ear.mic, ear.speakers);
                {
                    let mut guard = slot.lock().unwrap();
                    if guard.generation != gen {
                        return;
                    }
                    ear.set_ptt(guard.ptt);
                    guard.ear = Some(ear);
                }
                if !mic || !speakers {
                    let text = match (mic, speakers) {
                        (false, false) => "No microphone or speakers.",
                        (false, true) => "No microphone, so the room will not hear you.",
                        _ => "No speakers, so you will not hear the room.",
                    };
                    let notices = notices.clone();
                    let rt = task_rt.clone();
                    let _ = weak.upgrade_in_event_loop(move |ui| {
                        note(&ui, &rt, &notices, text);
                    });
                }
                let mut events = live.events;
                let mut speak_open = true;
                loop {
                    tokio::select! {
                        heard = speak_rx.recv(), if speak_open => {
                            match heard {
                                Some((id, on)) => {
                                    if on {
                                        room.speaking.insert(id, true);
                                    } else {
                                        room.speaking.remove(&id);
                                    }
                                    let snapshot = room.clone();
                                    let weak = weak.clone();
                                    let slot = slot.clone();
                                    let _ = weak.upgrade_in_event_loop(move |ui| {
                                        if slot.lock().unwrap().generation != gen {
                                            return;
                                        }
                                        ui.set_channels(channels(&snapshot));
                                    });
                                }
                                None => speak_open = false,
                            }
                            continue;
                        }
                        event = events.recv() => {
                            let Some(event) = event else { break };
                            // The body below handles one control event.
                            if slot.lock().unwrap().generation != gen {
                                break;
                            }
                            if matches!(event, Event::Disconnected) {
                                let ours = {
                                    let mut guard = slot.lock().unwrap();
                                    if guard.generation == gen {
                                        guard.link = None;
                                        guard.ear = None;
                                        guard.generation += 1;
                                        true
                                    } else {
                                        false
                                    }
                                };
                                if ours {
                                    let notices = notices.clone();
                                    let rt = task_rt.clone();
                                    let _ = weak.upgrade_in_event_loop(move |ui| {
                                        clear(&ui);
                                        note(&ui, &rt, &notices, "The server closed the connection.");
                                    });
                                }
                                break;
                            }
                            if let Event::Rejected { reason } = &event {
                                let notices = notices.clone();
                                let rt = task_rt.clone();
                                let reason = reason.clone();
                                let _ = weak.upgrade_in_event_loop(move |ui| {
                                    note(&ui, &rt, &notices, &reason);
                                });
                            }
                            let joined = matches!(event, Event::Joined { .. });
                            let left = matches!(event, Event::Left);
                            room.apply(event);
                            if joined {
                                slot.lock().unwrap().channel = room.channel.clone();
                                remember(&name, &invite, &room.channel, true);
                            }
                            if let Some(ear) = slot.lock().unwrap().ear.as_ref() {
                                ear.set_live(room.in_channel);
                            }
                            if left {
                                room.speaking.clear();
                            }
                            show(&weak, &slot, gen, &room, false);
                        }
                    }
                }
            }
            Err(err) => {
                if slot.lock().unwrap().generation != gen {
                    return;
                }
                let message = explain(err);
                let notices = notices.clone();
                let rt = task_rt.clone();
                let slot = slot.clone();
                let _ = weak.upgrade_in_event_loop(move |ui| {
                    if slot.lock().unwrap().generation != gen {
                        return;
                    }
                    ui.set_joining(false);
                    ui.set_inside(false);
                    note(&ui, &rt, &notices, &message);
                });
            }
        }
    });
}

fn pose(ui: &MainWindow, title: &str, body: &str, accept: &str) {
    ui.set_confirm_title(title.into());
    ui.set_confirm_body(body.into());
    ui.set_confirm_accept(accept.into());
    ui.set_confirming(true);
}

fn note(ui: &MainWindow, rt: &tokio::runtime::Handle, notices: &Arc<AtomicU64>, text: &str) {
    let token = notices.fetch_add(1, Ordering::Relaxed) + 1;
    ui.set_toast_danger(true);
    ui.set_toast(text.into());
    let notices = notices.clone();
    let weak = ui.as_weak();
    rt.spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        if notices.load(Ordering::Relaxed) != token {
            return;
        }
        let _ = weak.upgrade_in_event_loop(move |ui| {
            if notices.load(Ordering::Relaxed) == token {
                ui.set_toast("".into());
            }
        });
    });
}

fn remember(name: &str, invite: &str, channel: &str, resume: bool) {
    local::store(&Saved {
        name: name.to_string(),
        invite: invite.to_string(),
        channel: channel.to_string(),
        resume,
    });
}

fn bump(slot: &Arc<Mutex<Slot>>) -> u64 {
    let mut guard = slot.lock().unwrap();
    if let Some(link) = guard.link.take() {
        link.logout();
    }
    guard.ear = None;
    guard.generation += 1;
    guard.generation
}

fn send(slot: &Arc<Mutex<Slot>>, f: impl FnOnce(&Link)) {
    if let Some(link) = slot.lock().unwrap().link.as_ref() {
        f(link);
    }
}

fn show(weak: &Weak<MainWindow>, slot: &Arc<Mutex<Slot>>, gen: u64, room: &Room, entered: bool) {
    let room = room.clone();
    let slot = slot.clone();
    let _ = weak.upgrade_in_event_loop(move |ui| {
        if slot.lock().unwrap().generation != gen {
            return;
        }
        ui.set_joining(false);
        if entered {
            ui.set_toast("".into());
        }
        ui.set_inside(true);
        ui.set_joined(place(&room));
        ui.set_channels(channels(&room));
        ui.set_lines(lines(&room));
        if let Some(me) = room.members.iter().find(|peer| peer.user_id == room.me) {
            ui.set_muted(me.muted);
        }
    });
}

fn clear(ui: &MainWindow) {
    ui.set_inside(false);
    ui.set_joining(false);
    ui.set_joined(-1);
    ui.set_panel(0);
    ui.set_muted(false);
    ui.set_deafened(false);
    ui.set_channels(ModelRc::new(VecModel::from(Vec::<Channel>::new())));
    ui.set_lines(ModelRc::new(VecModel::from(Vec::<Line>::new())));
}

fn place(room: &Room) -> i32 {
    if !room.in_channel {
        return -1;
    }
    room.rooms
        .iter()
        .position(|name| name == &room.channel)
        .map(|index| index as i32)
        .unwrap_or(0)
}

fn channels(room: &Room) -> ModelRc<Channel> {
    let names = if room.rooms.is_empty() {
        if room.channel.is_empty() {
            Vec::new()
        } else {
            vec![room.channel.clone()]
        }
    } else {
        room.rooms.clone()
    };
    let rows = names
        .into_iter()
        .map(|name| {
            let members = if room.in_channel && name == room.channel {
                room.members
                    .iter()
                    .map(|peer| {
                        member(
                            peer,
                            room.speaking.get(&peer.user_id).copied().unwrap_or(false),
                        )
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            Channel {
                name: name.into(),
                members: ModelRc::new(VecModel::from(members)),
            }
        })
        .collect::<Vec<_>>();
    ModelRc::new(VecModel::from(rows))
}

fn lines(room: &Room) -> ModelRc<Line> {
    let lines = room
        .lines
        .iter()
        .map(|(author, body, mine)| Line {
            author: author.clone().into(),
            body: body.clone().into(),
            mine: *mine,
        })
        .collect::<Vec<_>>();
    ModelRc::new(VecModel::from(lines))
}

fn member(peer: &Peer, speaking: bool) -> Member {
    let initial = peer
        .name
        .chars()
        .next()
        .map(|glyph| glyph.to_uppercase().collect::<String>())
        .unwrap_or_else(|| "?".to_string());
    let [red, green, blue] = TINTS[(peer.user_id as usize) % TINTS.len()];
    Member {
        name: peer.name.clone().into(),
        initial: initial.into(),
        tint: slint::Color::from_rgb_u8(red, green, blue),
        muted: peer.muted,
        speaking,
    }
}

fn explain(err: LinkError) -> String {
    match err {
        LinkError::Invite => "That invite is not a ryme link.".into(),
        LinkError::Unreachable => "Could not reach that server.".into(),
        LinkError::Rejected(reason) => reason,
    }
}
