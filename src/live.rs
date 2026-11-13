use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ryme_proto::parse_invite;
use ryme_server::{connect, Event, Link, LinkError, Peer};

use crate::ear::{Ear, Mix};
use crate::local::{self, Level, Saved};
use slint::{ComponentHandle, ModelRc, VecModel, Weak};

use crate::{Band, Channel, Line, MainWindow, Member, Seat};

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
            Event::PeerJoined {
                user_id,
                name,
                card,
            } => {
                if !self.members.iter().any(|peer| peer.user_id == user_id) {
                    self.members.push(Peer {
                        user_id,
                        name,
                        muted: false,
                        card,
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
                const CHAT_KEEP: usize = 200;
                if self.lines.len() > CHAT_KEEP {
                    let extra = self.lines.len() - CHAT_KEEP;
                    self.lines.drain(..extra);
                }
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
    preview: Option<crate::ear::Preview>,
    input: String,
    output: String,
    generation: u64,
    channel: String,
    resume_channel: String,
    ptt: bool,
    denoise: bool,
    muted: bool,
    deaf: bool,
    resume_secret: [u8; 32],
    server: [u8; 32],
    roster: HashMap<u32, u64>,
    levels: HashMap<u64, Level>,
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
    let audio = local::load_audio();
    crate::ear::set_talk_key(audio.key);
    ui.set_talk_key(audio.key.to_string().into());
    ui.set_input_name(short_device(&audio.input).into());
    ui.set_output_name(short_device(&audio.output).into());
    local::forget_servers();
    let staged = local::take_staged_invite();
    let slot = Arc::new(Mutex::new(Slot {
        link: None,
        ear: None,
        preview: None,
        input: audio.input,
        output: audio.output,
        generation: 0,
        channel: String::new(),
        resume_channel: String::new(),
        ptt: ui.get_ptt(),
        denoise: ui.get_denoise(),
        muted: ui.get_muted(),
        deaf: ui.get_deafened(),
        resume_secret: [0; 32],
        server: [0; 32],
        roster: HashMap::new(),
        levels: HashMap::new(),
    }));
    let ask = Arc::new(Mutex::new(Ask::None));
    let notices = Arc::new(AtomicU64::new(0));
    let mut resuming = false;
    if let Some(saved) = local::load() {
        ui.set_display_name(saved.name.clone().into());
        if staged.is_none() {
            ui.set_invite(saved.invite.clone().into());
        }
        ui.set_denoise(saved.denoise);
        ui.set_muted(saved.muted);
        ui.set_deafened(saved.deaf);
        ui.set_ptt(saved.ptt);
        {
            let mut guard = slot.lock().unwrap();
            guard.denoise = saved.denoise;
            guard.muted = saved.muted;
            guard.deaf = saved.deaf;
            guard.ptt = saved.ptt;
            guard.resume_secret = saved.ticket;
        }
        if saved.resume && staged.is_none() {
            resuming = true;
            slot.lock().unwrap().resume_channel = saved.channel;
            begin(ui, &slot, rt, &notices, false, 0);
        }
    }
    if let Some(url) = staged {
        ui.set_invite(url.into());
    }

    ui.on_submit_join({
        let ui_weak = ui.as_weak();
        let slot = slot.clone();
        let rt = rt.clone();
        let notices = notices.clone();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                begin(&ui, &slot, &rt, &notices, false, 0);
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
            {
                let mut guard = slot.lock().unwrap();
                guard.muted = muted;
                if let Some(ear) = guard.ear.as_ref() {
                    ear.set_muted(muted);
                }
            }
            keep_switches(&slot);
            send(&slot, |link| link.mute(muted));
        }
    });

    ui.on_set_deafen({
        let slot = slot.clone();
        move |deafened| {
            {
                let mut guard = slot.lock().unwrap();
                guard.deaf = deafened;
                if let Some(ear) = guard.ear.as_ref() {
                    ear.set_deaf(deafened);
                }
            }
            keep_switches(&slot);
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
            drop(guard);
            keep_switches(&slot);
        }
    });

    ui.on_set_denoise({
        let slot = slot.clone();
        move |on| {
            let mut guard = slot.lock().unwrap();
            guard.denoise = on;
            if let Some(ear) = guard.ear.as_ref() {
                ear.set_denoise(on);
            }
            drop(guard);
            keep_switches(&slot);
        }
    });

    ui.on_v_key({
        let slot = slot.clone();
        move |down| {
            if let Some(ear) = slot.lock().unwrap().ear.as_ref() {
                ear.set_vkey(down);
            }
        }
    });

    ui.on_tune({
        let slot = slot.clone();
        let weak = ui.as_weak();
        move |gain| adjust(&slot, &weak, |level| level.gain = gain.clamp(0.0, 2.0))
    });

    ui.on_hush({
        let slot = slot.clone();
        let weak = ui.as_weak();
        move |quiet| adjust(&slot, &weak, |level| level.quiet = quiet)
    });

    ui.on_leave_channel({
        let slot = slot.clone();
        move || {
            if let Some(ear) = slot.lock().unwrap().ear.as_ref() {
                ear.set_live(false);
            }
            send(&slot, |link| link.leave())
        }
    });

    ui.on_pick_channel({
        let slot = slot.clone();
        move |name| {
            {
                let mut guard = slot.lock().unwrap();
                guard.channel = name.to_string();
                if let Some(ear) = guard.ear.as_ref() {
                    ear.set_live(false);
                }
            }
            send(&slot, |link| link.join(name.to_string()))
        }
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

    ui.on_check_update({
        let weak = ui.as_weak();
        let ask = ask.clone();
        let rt = rt.clone();
        move || {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            if ui.get_confirming() || ui.get_update_busy() {
                return;
            }
            if !ui.get_update_label().is_empty() {
                let title = ui.get_update_label().to_string();
                offer(&ui, &ask, &title);
                return;
            }
            ui.set_update_note("".into());
            ui.set_update_busy(true);
            let weak = weak.clone();
            let ask = ask.clone();
            rt.spawn(async move {
                let status = match tokio::task::spawn_blocking(crate::update::check).await {
                    Ok(status) => status,
                    Err(_) => crate::update::Found::Failed("Could not check for an update.".into()),
                };
                let _ = weak.upgrade_in_event_loop(move |ui| {
                    ui.set_update_busy(false);
                    match status {
                        crate::update::Found::Ready(version) => {
                            let title = format!("{version} is ready");
                            ui.set_update_label(title.as_str().into());
                            ui.set_update_note_danger(false);
                            ui.set_update_note("".into());
                            if !ui.get_confirming() {
                                offer(&ui, &ask, &title);
                            }
                        }
                        crate::update::Found::Current => {
                            ui.set_update_note_danger(false);
                            ui.set_update_note("Up to date.".into());
                        }
                        crate::update::Found::Failed(message) => {
                            ui.set_update_note_danger(true);
                            ui.set_update_note(message.into());
                        }
                    }
                });
            });
        }
    });

    ui.on_confirm_no({
        let weak = ui.as_weak();
        let ask = ask.clone();
        move || {
            let pending = std::mem::replace(&mut *ask.lock().unwrap(), Ask::None);
            if let Some(ui) = weak.upgrade() {
                ui.set_confirming(false);
                if matches!(pending, Ask::Update) {
                    ui.set_update_note_danger(false);
                    ui.set_update_note(ui.get_update_label());
                }
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
                    let (channel, denoise) = {
                        let guard = slot.lock().unwrap();
                        (guard.channel.clone(), guard.denoise)
                    };
                    let (muted, deaf, ptt) = {
                        let mut guard = slot.lock().unwrap();
                        guard.resume_secret = [0; 32];
                        (guard.muted, guard.deaf, guard.ptt)
                    };
                    local::store(&Saved {
                        name: ui.get_display_name().to_string(),
                        invite: ui.get_invite().to_string(),
                        channel,
                        resume: false,
                        denoise,
                        muted,
                        deaf,
                        ptt,
                        ticket: [0; 32],
                    });
                    bump(&slot);
                    clear(&ui);
                    listen_local(&slot);
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

    ui.on_cycle_input({
        let ui_weak = ui.as_weak();
        let slot = slot.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            cycle_device(&ui, &slot, true);
        }
    });
    ui.on_cycle_output({
        let ui_weak = ui.as_weak();
        let slot = slot.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            cycle_device(&ui, &slot, false);
        }
    });
    ui.on_test_speakers({
        let slot = slot.clone();
        move || {
            let guard = slot.lock().unwrap();
            if let Some(ear) = guard.ear.as_ref() {
                ear.play_tone();
            } else if let Some(preview) = guard.preview.as_ref() {
                preview.play_tone();
            }
        }
    });
    ui.on_choose_key({
        let ui_weak = ui.as_weak();
        let slot = slot.clone();
        move |text| {
            let Some(letter) = text.chars().next() else {
                return;
            };
            if !letter.is_ascii_alphabetic() {
                if let Some(ui) = ui_weak.upgrade() {
                    ui.set_toast("Choose a letter.".into());
                    ui.set_toast_danger(false);
                }
                return;
            }
            crate::ear::set_talk_key(letter);
            let key = crate::ear::talk_key();
            if let Some(ui) = ui_weak.upgrade() {
                ui.set_talk_key(key.to_string().into());
                ui.set_capture_key(false);
            }
            let guard = slot.lock().unwrap();
            local::store_audio(&local::AudioPref {
                input: guard.input.clone(),
                output: guard.output.clone(),
                key,
            });
        }
    });

    let meter_slot = slot.clone();
    let meter_ui = ui.as_weak();
    rt.spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let level = {
                let guard = meter_slot.lock().unwrap();
                guard
                    .ear
                    .as_ref()
                    .map(|ear| ear.level())
                    .or_else(|| guard.preview.as_ref().map(|preview| preview.level()))
                    .unwrap_or(0)
            };
            let keyed = crate::ear::key_is_down();
            let _ = meter_ui.upgrade_in_event_loop(move |ui| {
                ui.set_mic_level(level as f32 / 100.0);
                ui.set_keyed(keyed);
                #[cfg(target_os = "linux")]
                crate::linux_tray::set_status(&ui.get_mic_line());
            });
        }
    });

    if !resuming {
        listen_local(&slot);
    }

    Shutdown { slot }
}

fn short_device(name: &str) -> String {
    let name = if name == "default" { "Default" } else { name };
    let count = name.chars().count();
    if count > 16 {
        let short: String = name.chars().take(15).collect();
        format!("{short}…")
    } else {
        name.to_string()
    }
}

fn cycle_device(ui: &MainWindow, slot: &Arc<Mutex<Slot>>, input: bool) {
    let names = if input {
        crate::ear::input_names()
    } else {
        crate::ear::output_names()
    };
    if names.is_empty() {
        return;
    }
    let mut guard = slot.lock().unwrap();
    let current = if input {
        guard.input.clone()
    } else {
        guard.output.clone()
    };
    let index = names.iter().position(|name| name == &current).unwrap_or(0);
    let next = names[(index + 1) % names.len()].clone();
    if input {
        guard.input = next.clone();
        if let Some(ear) = guard.ear.as_ref() {
            ear.set_input(&next);
        }
        if let Some(preview) = guard.preview.as_ref() {
            preview.set_input(&next);
        }
        ui.set_input_name(short_device(&next).into());
    } else {
        guard.output = next.clone();
        if let Some(ear) = guard.ear.as_ref() {
            ear.set_output(&next);
        }
        if let Some(preview) = guard.preview.as_ref() {
            preview.set_output(&next);
        }
        ui.set_output_name(short_device(&next).into());
    }
    local::store_audio(&local::AudioPref {
        input: guard.input.clone(),
        output: guard.output.clone(),
        key: crate::ear::talk_key(),
    });
}

fn listen_local(slot: &Arc<Mutex<Slot>>) {
    let mut guard = slot.lock().unwrap();
    if guard.ear.is_some() || guard.preview.is_some() {
        return;
    }
    let input = guard.input.clone();
    let output = guard.output.clone();
    guard.preview = Some(crate::ear::Preview::start(&input, &output));
}

fn begin(
    ui: &MainWindow,
    slot: &Arc<Mutex<Slot>>,
    rt: &tokio::runtime::Handle,
    notices: &Arc<AtomicU64>,
    retry: bool,
    attempt: u32,
) {
    let name = ui.get_display_name().to_string();
    let invite = ui.get_invite().to_string();
    let muted = ui.get_muted();
    let deafened = ui.get_deafened();
    let ptt = ui.get_ptt();
    {
        let mut guard = slot.lock().unwrap();
        guard.muted = muted;
        guard.deaf = deafened;
        guard.ptt = ptt;
    }
    let gen = bump(slot);
    ui.set_reconnecting(retry);
    ui.set_joining(true);
    ui.set_toast("".into());
    let weak = ui.as_weak();
    let slot = slot.clone();
    let rt = rt.clone();
    let task_rt = rt.clone();
    let notices = notices.clone();
    rt.spawn(async move {
        let server = parse_invite(&invite)
            .map(|parsed| parsed.fingerprint)
            .unwrap_or([0; 32]);
        {
            let mut guard = slot.lock().unwrap();
            guard.server = server;
            guard.levels = local::load_levels(&server);
        }
        let resume_secret = slot.lock().unwrap().resume_secret;
        match connect(&invite, &name, local::card(), resume_secret).await {
            Ok(live) => {
                let resume_channel = {
                    let mut guard = slot.lock().unwrap();
                    if guard.generation != gen {
                        live.link.logout();
                        return;
                    }
                    guard.link = Some(live.link.clone());
                    guard.resume_secret = live.resume;
                    let resume = std::mem::take(&mut guard.resume_channel);
                    guard.channel = if resume.is_empty() {
                        live.channel.clone()
                    } else {
                        resume.clone()
                    };
                    resume
                };
                let pending = !resume_channel.is_empty() && resume_channel != live.channel;
                if pending {
                    live.link.join(resume_channel.clone());
                }
                if muted {
                    live.link.mute(true);
                }
                if deafened {
                    live.link.deafen(true);
                }
                let (denoise, ptt, muted_now, deaf_now) = {
                    let guard = slot.lock().unwrap();
                    (guard.denoise, guard.ptt, guard.muted, guard.deaf)
                };
                let ticket = slot.lock().unwrap().resume_secret;
                remember(
                    &name,
                    &invite,
                    &live.channel,
                    true,
                    denoise,
                    muted_now,
                    deaf_now,
                    ptt,
                    ticket,
                );
                let mut room = Room::new(live.me, live.channel.clone(), live.members, live.rooms);
                if muted {
                    if let Some(me) = room.members.iter_mut().find(|peer| peer.user_id == room.me) {
                        me.muted = true;
                    }
                }
                show(&weak, &slot, gen, &room, true);
                let (speak_tx, mut speak_rx) = tokio::sync::mpsc::unbounded_channel();
                let (input, output) = {
                    let guard = slot.lock().unwrap();
                    (guard.input.clone(), guard.output.clone())
                };
                let ear = crate::ear::open(
                    live.voice,
                    live.me,
                    speak_tx,
                    crate::ear::Gate {
                        muted,
                        deaf: deafened,
                        ptt,
                        denoise,
                    },
                    &input,
                    &output,
                );
                let (mic, speakers) = (ear.mic, ear.speakers);
                {
                    let mut guard = slot.lock().unwrap();
                    if guard.generation != gen {
                        return;
                    }
                    ear.set_ptt(guard.ptt);
                    ear.set_denoise(guard.denoise);
                    ear.set_muted(guard.muted);
                    ear.set_deaf(guard.deaf);
                    if !pending {
                        ear.set_live(true);
                    }
                    ear.set_levels(hearing(&guard.roster, &guard.levels));
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
                                        let levels = {
                                            let guard = slot.lock().unwrap();
                                            if guard.generation != gen {
                                                return;
                                            }
                                            guard.levels.clone()
                                        };
                                        ui.set_channels(channels(&snapshot));
                                        ui.set_bands(bands(&snapshot, &levels));
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
                                let held = {
                                    let mut guard = slot.lock().unwrap();
                                    if guard.generation != gen {
                                        None
                                    } else {
                                        guard.link = None;
                                        let ear = guard.ear.take();
                                        guard.generation += 1;
                                        guard.resume_channel = guard.channel.clone();
                                        Some((ear, guard.generation))
                                    }
                                };
                                let Some((ear, retry_gen)) = held else {
                                    break;
                                };
                                drop(ear);
                                let weak = weak.clone();
                                let slot = slot.clone();
                                let notices = notices.clone();
                                let _ = weak.upgrade_in_event_loop(move |ui| {
                                    ui.set_panel(0);
                                    ui.set_reconnecting(true);
                                    ui.set_joining(false);
                                    ui.set_toast("".into());
                                });
                                pursue(&task_rt, &weak, &slot, &notices, 0, retry_gen);
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
                                let switches = {
                                    let mut guard = slot.lock().unwrap();
                                    guard.channel = room.channel.clone();
                                    (
                                        guard.denoise,
                                        guard.muted,
                                        guard.deaf,
                                        guard.ptt,
                                    )
                                };
                                let ticket = slot.lock().unwrap().resume_secret;
                                remember(
                                    &name,
                                    &invite,
                                    &room.channel,
                                    true,
                                    switches.0,
                                    switches.1,
                                    switches.2,
                                    switches.3,
                                    ticket,
                                );
                            }
                            let want = slot.lock().unwrap().channel.clone();
                            if let Some(ear) = slot.lock().unwrap().ear.as_ref() {
                                ear.set_live(room.in_channel && room.channel == want);
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
                let next = attempt.saturating_add(1);
                if retry
                    && matches!(err, LinkError::Unreachable | LinkError::Timeout)
                    && pace(next).is_some()
                {
                    let weak = weak.clone();
                    let slot = slot.clone();
                    let notices = notices.clone();
                    let _ = weak.upgrade_in_event_loop(move |ui| {
                        ui.set_reconnecting(true);
                        ui.set_joining(false);
                    });
                    pursue(&task_rt, &weak, &slot, &notices, next, gen);
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
                    clear(&ui);
                    listen_local(&slot);
                    note(&ui, &rt, &notices, &message);
                });
            }
        }
    });
}

fn offer(ui: &MainWindow, ask: &Mutex<Ask>, title: &str) {
    let in_call = ui.get_inside() && ui.get_joined() >= 0;
    let body = if in_call {
        "Ryme will restart to update, and this call drops."
    } else {
        "Ryme will restart to update."
    };
    *ask.lock().unwrap() = Ask::Update;
    ui.set_confirm_danger(false);
    pose(ui, title, body, "Update");
}

fn pose(ui: &MainWindow, title: &str, body: &str, accept: &str) {
    ui.set_confirm_title(title.into());
    ui.set_confirm_body(body.into());
    ui.set_confirm_accept(accept.into());
    ui.set_confirming(true);
}

fn note(ui: &MainWindow, rt: &tokio::runtime::Handle, notices: &Arc<AtomicU64>, text: &str) {
    toast(ui, rt, notices, text, true);
}

fn toast(
    ui: &MainWindow,
    rt: &tokio::runtime::Handle,
    notices: &Arc<AtomicU64>,
    text: &str,
    danger: bool,
) {
    let token = notices.fetch_add(1, Ordering::Relaxed) + 1;
    ui.set_toast_danger(danger);
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

fn remember(
    name: &str,
    invite: &str,
    channel: &str,
    resume: bool,
    denoise: bool,
    muted: bool,
    deaf: bool,
    ptt: bool,
    ticket: [u8; 32],
) {
    local::store(&Saved {
        name: name.to_string(),
        invite: invite.to_string(),
        channel: channel.to_string(),
        resume,
        denoise,
        muted,
        deaf,
        ptt,
        ticket,
    });
}

fn keep_switches(slot: &Mutex<Slot>) {
    let Some(mut saved) = local::load() else {
        return;
    };
    let guard = slot.lock().unwrap();
    saved.denoise = guard.denoise;
    saved.muted = guard.muted;
    saved.deaf = guard.deaf;
    saved.ptt = guard.ptt;
    drop(guard);
    local::store(&saved);
}

fn adjust(slot: &Arc<Mutex<Slot>>, weak: &Weak<MainWindow>, change: impl FnOnce(&mut Level)) {
    let Some(ui) = weak.upgrade() else {
        return;
    };
    let id = ui.get_picked();
    if id < 0 {
        return;
    }
    let mut guard = slot.lock().unwrap();
    let Some(card) = guard.roster.get(&(id as u32)).copied() else {
        return;
    };
    let level = guard.levels.entry(card).or_insert(Level {
        gain: 1.0,
        quiet: false,
    });
    change(level);
    local::store_levels(&guard.server, &guard.levels);
    let mixes = hearing(&guard.roster, &guard.levels);
    if let Some(ear) = guard.ear.as_ref() {
        ear.set_levels(mixes);
    }
}

fn bump(slot: &Arc<Mutex<Slot>>) -> u64 {
    let mut guard = slot.lock().unwrap();
    if let Some(link) = guard.link.take() {
        link.logout();
    }
    guard.ear = None;
    guard.preview = None;
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
        let levels = {
            let mut guard = slot.lock().unwrap();
            if guard.generation != gen {
                return;
            }
            guard.roster = room
                .members
                .iter()
                .map(|peer| (peer.user_id, peer.card))
                .collect();
            let mixes = hearing(&guard.roster, &guard.levels);
            if let Some(ear) = guard.ear.as_ref() {
                ear.set_levels(mixes);
            }
            guard.levels.clone()
        };
        ui.set_joining(false);
        ui.set_reconnecting(false);
        if entered {
            ui.set_toast("".into());
        }
        ui.set_inside(true);
        ui.set_joined(place(&room));
        ui.set_channels(channels(&room));
        ui.set_bands(bands(&room, &levels));
        ui.set_lines(lines(&room));
        let picked = ui.get_picked();
        if picked >= 0 {
            if let Some(peer) = room
                .members
                .iter()
                .find(|peer| peer.user_id == picked as u32)
            {
                let level = levels.get(&peer.card).copied().unwrap_or(Level {
                    gain: 1.0,
                    quiet: false,
                });
                ui.set_picked_name(peer.name.clone().into());
                ui.set_picked_quiet(level.quiet);
                ui.set_picked_gain(level.gain);
            } else {
                ui.set_picked(-1);
            }
        }
        if let Some(me) = room.members.iter().find(|peer| peer.user_id == room.me) {
            ui.set_muted(me.muted);
        }
    });
}

fn hearing(roster: &HashMap<u32, u64>, levels: &HashMap<u64, Level>) -> HashMap<u32, Mix> {
    roster
        .iter()
        .filter_map(|(id, card)| {
            levels.get(card).map(|level| {
                (
                    *id,
                    Mix {
                        gain: level.gain,
                        quiet: level.quiet,
                    },
                )
            })
        })
        .collect()
}

fn clear(ui: &MainWindow) {
    ui.set_inside(false);
    ui.set_joining(false);
    ui.set_reconnecting(false);
    ui.set_joined(-1);
    ui.set_panel(0);
    ui.set_muted(false);
    ui.set_deafened(false);
    ui.set_channels(ModelRc::new(VecModel::from(Vec::<Channel>::new())));
    ui.set_lines(ModelRc::new(VecModel::from(Vec::<Line>::new())));
    ui.set_bands(ModelRc::new(VecModel::from(Vec::<Band>::new())));
    ui.set_picked(-1);
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

fn bands(room: &Room, levels: &HashMap<u64, Level>) -> ModelRc<Band> {
    if !room.in_channel {
        return ModelRc::new(VecModel::from(Vec::new()));
    }
    let seats: Vec<Seat> = room
        .members
        .iter()
        .map(|peer| {
            let level = levels.get(&peer.card).copied().unwrap_or(Level {
                gain: 1.0,
                quiet: false,
            });
            let initial = peer
                .name
                .chars()
                .next()
                .map(|glyph| glyph.to_uppercase().collect::<String>())
                .unwrap_or_else(|| "?".to_string());
            let [red, green, blue] = TINTS[(peer.user_id as usize) % TINTS.len()];
            Seat {
                name: peer.name.clone().into(),
                initial: initial.into(),
                tint: slint::Color::from_rgb_u8(red, green, blue),
                muted: peer.muted,
                speaking: room.speaking.get(&peer.user_id).copied().unwrap_or(false),
                id: peer.user_id as i32,
                quiet: level.quiet,
                mine: peer.user_id == room.me,
                gain: level.gain,
            }
        })
        .collect();
    let rows = seats
        .chunks(5)
        .map(|row| Band {
            seats: ModelRc::new(VecModel::from(row.to_vec())),
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
        LinkError::Timeout => "The server did not answer. Check that UDP can get through.".into(),
        LinkError::Pin => "This invite does not match that server. Ask for a new link.".into(),
        LinkError::Address => "That address did not resolve.".into(),
        LinkError::Rejected(reason) => reason,
    }
}

// A dropped call gets three attempts back. The new hello carries the previous
// session secret, so the server closes the old one instead of waiting it out.
const ATTEMPTS: u32 = 3;
const QUIET_WAIT: Duration = Duration::from_secs(1);
const AGAIN_WAIT: Duration = Duration::from_secs(4);

fn pace(attempt: u32) -> Option<Duration> {
    if attempt >= ATTEMPTS {
        return None;
    }
    Some(if attempt == 0 { QUIET_WAIT } else { AGAIN_WAIT })
}

fn pursue(
    rt: &tokio::runtime::Handle,
    weak: &Weak<MainWindow>,
    slot: &Arc<Mutex<Slot>>,
    notices: &Arc<AtomicU64>,
    attempt: u32,
    gen: u64,
) {
    let Some(wait) = pace(attempt) else {
        return;
    };
    let weak = weak.clone();
    let slot = slot.clone();
    let task_rt = rt.clone();
    let notices = notices.clone();
    rt.spawn(async move {
        tokio::time::sleep(wait).await;
        let _ = weak.upgrade_in_event_loop(move |ui| {
            if slot.lock().unwrap().generation != gen {
                return;
            }
            begin(&ui, &slot, &task_rt, &notices, true, attempt);
        });
    });
}

#[cfg(test)]
mod tests {
    use super::{explain, pace, Room, AGAIN_WAIT, ATTEMPTS, QUIET_WAIT};
    use ryme_server::{Event, LinkError};

    #[test]
    fn a_drop_is_tried_three_times_and_then_stops() {
        assert_eq!(pace(0), Some(QUIET_WAIT));
        assert_eq!(pace(1), Some(AGAIN_WAIT));
        assert_eq!(pace(ATTEMPTS - 1), Some(AGAIN_WAIT));
        assert_eq!(pace(ATTEMPTS), None);
        assert!(QUIET_WAIT < AGAIN_WAIT);
    }

    #[test]
    fn chat_history_stays_bounded() {
        let mut room = Room::new(1, "Lobby".into(), Vec::new(), vec!["Lobby".into()]);
        for n in 0..250 {
            room.apply(Event::Chat {
                user_id: 1,
                text: format!("{n}"),
            });
        }
        assert_eq!(room.lines.len(), 200);
        assert_eq!(room.lines[0].1, "50");
        assert_eq!(room.lines.last().unwrap().1, "249");
    }

    #[test]
    fn connection_failures_say_what_went_wrong() {
        assert!(explain(LinkError::Pin).contains("does not match"));
        assert!(explain(LinkError::Timeout).contains("UDP"));
        assert!(explain(LinkError::Address).contains("resolve"));
        assert!(explain(LinkError::Unreachable).contains("Could not reach"));
    }
}
