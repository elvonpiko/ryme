use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ryme_proto::{parse_invite, Invite, DEFAULT_PORT};
use ryme_server::{connect_room, Event, Link, LinkError, Peer};

use crate::intent::{self, Aim};

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
                let same = self.in_channel && self.channel == channel;
                self.channel = channel;
                self.members = members;
                self.in_channel = true;
                if !same {
                    self.lines.clear();
                }
                if !rooms.is_empty() {
                    self.rooms = rooms;
                }
            }
            Event::PeerJoined {
                user_id,
                name,
                card,
                muted,
            } => {
                if let Some(peer) = self.members.iter_mut().find(|peer| peer.user_id == user_id) {
                    peer.muted = muted;
                } else {
                    self.members.push(Peer {
                        user_id,
                        name,
                        muted,
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
                self.keep_lines();
            }
            Event::Past { name, text, mine } => {
                let author = if mine {
                    "You".to_string()
                } else if name.is_empty() {
                    "Someone".to_string()
                } else {
                    name
                };
                self.lines.push((author, text, mine));
                self.keep_lines();
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
            Event::Moved { user_id, .. } => {
                if let Some(peer) = self.members.iter_mut().find(|peer| peer.user_id == self.me) {
                    peer.user_id = user_id;
                }
                self.speaking.remove(&self.me);
                self.me = user_id;
            }
        }
    }

    fn for_speaking(&self) -> Self {
        Self {
            me: self.me,
            channel: self.channel.clone(),
            members: self.members.clone(),
            rooms: self.rooms.clone(),
            lines: Vec::new(),
            in_channel: self.in_channel,
            speaking: self.speaking.clone(),
        }
    }

    fn keep_lines(&mut self) {
        const CHAT_KEEP: usize = 200;
        if self.lines.len() > CHAT_KEEP {
            let extra = self.lines.len() - CHAT_KEEP;
            self.lines.drain(..extra);
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
    /// A join that has not been confirmed. The mic stays on the confirmed room.
    pending_channel: Option<String>,
    resume_channel: String,
    ptt: bool,
    denoise: bool,
    speakerphone: bool,
    muted: bool,
    deaf: bool,
    resume_secret: [u8; 32],
    /// Fingerprint of the server that issued `resume_secret`.
    ticket_server: [u8; 32],
    aim: Aim,
    pref_tick: u64,
    tcp: bool,
    server: [u8; 32],
    roster: HashMap<u32, u64>,
    levels: HashMap<u64, Level>,
}

enum Ask {
    None,
    Logout,
    Update,
    /// Leave the live server and open this invite.
    Switch(String),
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

    /// Flip the local mute and tell the server. The tray uses this when the
    /// card is hidden.
    pub fn toggle_mute(&self) {
        let mut guard = self.slot.lock().unwrap();
        guard.muted = !guard.muted;
        guard.pref_tick = guard.pref_tick.wrapping_add(1);
        if let Some(ear) = guard.ear.as_ref() {
            ear.set_muted(guard.muted);
        }
        if guard.muted {
            crate::ear::release_talk();
        }
        if let Some(link) = guard.link.as_ref() {
            link.mute(guard.muted);
        }
    }
}

pub fn attach(ui: &MainWindow, rt: &tokio::runtime::Handle) -> Shutdown {
    let audio = local::load_audio();
    crate::ear::set_talk_key(audio.key);
    ui.set_talk_key(audio.key.to_string().into());
    ui.set_speakerphone(audio.speakerphone);
    ui.set_input_name(short_device(&audio.input, true).into());
    ui.set_output_name(short_device(&audio.output, false).into());
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
        pending_channel: None,
        resume_channel: String::new(),
        ptt: ui.get_ptt(),
        denoise: ui.get_denoise(),
        speakerphone: audio.speakerphone,
        muted: ui.get_muted(),
        deaf: ui.get_deafened(),
        resume_secret: [0; 32],
        ticket_server: [0; 32],
        aim: Aim::Outside,
        pref_tick: 0,
        tcp: false,
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
            guard.ticket_server = parse_invite(&saved.invite)
                .map(|parsed| parsed.fingerprint)
                .unwrap_or([0; 32]);
            if saved.channel.is_empty() {
                guard.aim = Aim::Outside;
            } else {
                guard.aim = Aim::Room(saved.channel.clone());
            }
        }
        if saved.resume && staged.is_none() {
            resuming = true;
            let mut guard = slot.lock().unwrap();
            if let Aim::Room(name) = &guard.aim {
                guard.resume_channel = name.clone();
            }
            drop(guard);
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
                {
                    let mut guard = slot.lock().unwrap();
                    if matches!(guard.aim, Aim::Outside) {
                        guard.aim = Aim::Room("Lobby".into());
                    }
                }
                begin(&ui, &slot, &rt, &notices, false, 0);
            }
        }
    });

    ui.on_send_chat({
        let slot = slot.clone();
        let ui_weak = ui.as_weak();
        let rt = rt.clone();
        let notices = notices.clone();
        move |text| {
            let text = match fit_chat(&text) {
                Ok(text) => text,
                Err(reason) => {
                    if let Some(ui) = ui_weak.upgrade() {
                        note(&ui, &rt, &notices, reason);
                    }
                    return;
                }
            };
            send(&slot, |link| link.chat(text));
        }
    });

    ui.on_copy_status({
        let ui_weak = ui.as_weak();
        let rt = rt.clone();
        let notices = notices.clone();
        move || {
            let Some(ui) = ui_weak.upgrade() else {
                return;
            };
            let rtt = if ui.get_ping_on() {
                ui.get_ping_text().to_string()
            } else {
                "not connected".into()
            };
            let text = format!(
                "Ryme {}\n{}\nServer RTT: {}\nAudio: {}\nMicrophone: {}\nSpeakers: {}",
                env!("CARGO_PKG_VERSION"),
                ui.get_mic_line(),
                rtt,
                ui.get_health_line(),
                ui.get_input_name(),
                ui.get_output_name()
            );
            let copied = copy_text(&text);
            toast(
                &ui,
                &rt,
                &notices,
                if copied { "Copied." } else { "Could not copy." },
                !copied,
            );
        }
    });

    ui.on_copy_link({
        let ui_weak = ui.as_weak();
        let rt = rt.clone();
        let notices = notices.clone();
        move |link| {
            let copied = copy_text(link.as_str());
            if let Some(ui) = ui_weak.upgrade() {
                toast(
                    &ui,
                    &rt,
                    &notices,
                    if copied { "Copied." } else { "Could not copy." },
                    !copied,
                );
            }
        }
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

    ui.on_set_speakerphone({
        let slot = slot.clone();
        move |on| {
            let mut guard = slot.lock().unwrap();
            guard.speakerphone = on;
            if let Some(ear) = guard.ear.as_ref() {
                ear.set_speakerphone(on);
            }
            local::store_audio(&local::AudioPref {
                input: guard.input.clone(),
                output: guard.output.clone(),
                key: crate::ear::talk_key(),
                speakerphone: on,
            });
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
            let (denoise, muted, deaf, ptt, ticket) = {
                let mut guard = slot.lock().unwrap();
                guard.aim = Aim::Outside;
                guard.channel.clear();
                guard.resume_channel.clear();
                guard.pending_channel = None;
                if let Some(ear) = guard.ear.as_ref() {
                    ear.set_live(false);
                }
                (
                    guard.denoise,
                    guard.muted,
                    guard.deaf,
                    guard.ptt,
                    guard.resume_secret,
                )
            };
            send(&slot, |link| link.leave());
            if let Some(saved) = local::load() {
                remember(
                    &saved.name,
                    &saved.invite,
                    "",
                    true,
                    denoise,
                    muted,
                    deaf,
                    ptt,
                    ticket,
                );
            }
        }
    });

    ui.on_pick_channel({
        let slot = slot.clone();
        move |name| {
            {
                let mut guard = slot.lock().unwrap();
                let asked = name.to_string();
                if guard.channel == asked
                    && guard.pending_channel.is_none()
                    && intent::admits(&guard.aim, &asked)
                {
                    return;
                }
                guard.aim = Aim::Room(name.to_string());
                guard.pending_channel = Some(name.to_string());
                if let Some(ear) = guard.ear.as_ref() {
                    ear.set_live(false);
                }
            }
            send(&slot, |link| link.join(name.to_string()))
        }
    });

    ui.on_open_invite({
        let weak = ui.as_weak();
        let ask = ask.clone();
        let slot = slot.clone();
        let rt = rt.clone();
        let notices = notices.clone();
        move |url| {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            open_invite(&ui, &slot, &rt, &notices, &ask, &url);
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
                    let (channel, denoise, ticket) = {
                        let mut guard = slot.lock().unwrap();
                        guard.resume_channel.clear();
                        guard.aim = Aim::Outside;
                        guard.channel.clear();
                        (String::new(), guard.denoise, guard.resume_secret)
                    };
                    let (muted, deaf, ptt) = {
                        let guard = slot.lock().unwrap();
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
                        ticket,
                    });
                    bump(&slot);
                    clear(&ui);
                    listen_local(&slot);
                }
                Ask::Switch(url) => {
                    ui.set_invite(url.clone().into());
                    let (denoise, muted, deaf, ptt) = {
                        let mut guard = slot.lock().unwrap();
                        guard.resume_channel.clear();
                        guard.resume_secret = [0; 32];
                        guard.ticket_server = [0; 32];
                        guard.aim = Aim::Room("Lobby".into());
                        (guard.denoise, guard.muted, guard.deaf, guard.ptt)
                    };
                    local::store(&Saved {
                        name: ui.get_display_name().to_string(),
                        invite: url,
                        channel: String::new(),
                        resume: false,
                        denoise,
                        muted,
                        deaf,
                        ptt,
                        ticket: [0; 32],
                    });
                    clear(&ui);
                    begin(&ui, &slot, &rt, &notices, false, 0);
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
            let mut guard = slot.lock().unwrap();
            if let Some(ear) = guard.ear.as_ref() {
                ear.play_tone();
                return;
            }
            if guard.preview.is_none() {
                let input = guard.input.clone();
                let output = guard.output.clone();
                guard.preview = Some(crate::ear::Preview::start(&input, &output));
            }
            if let Some(preview) = guard.preview.as_ref() {
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
                speakerphone: guard.speakerphone,
            });
        }
    });

    let meter_slot = slot.clone();
    let meter_ui = ui.as_weak();
    let meter_rt = rt.clone();
    let meter_notices = notices.clone();
    rt.spawn(async move {
        let mut seen_pref = 0u64;
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut switched = None;
            let level = {
                let mut guard = meter_slot.lock().unwrap();
                if let Some((input, applied)) = device_fault(&guard) {
                    if input {
                        guard.input = applied.clone();
                    } else {
                        guard.output = applied.clone();
                    }
                    local::store_audio(&local::AudioPref {
                        input: guard.input.clone(),
                        output: guard.output.clone(),
                        key: crate::ear::talk_key(),
                        speakerphone: guard.speakerphone,
                    });
                    switched = Some((input, applied));
                }
                guard
                    .ear
                    .as_ref()
                    .map(|ear| ear.level())
                    .or_else(|| guard.preview.as_ref().map(|preview| preview.level()))
                    .unwrap_or(0)
            };
            let no_mic = meter_slot
                .lock()
                .unwrap()
                .ear
                .as_ref()
                .is_some_and(|ear| !ear.mic);
            let overruns = crate::ear::take_capture_overruns();
            let keyed = crate::ear::key_is_down();
            let (pref, muted_now) = {
                let guard = meter_slot.lock().unwrap();
                (guard.pref_tick, guard.muted)
            };
            let paint_mute = pref != seen_pref;
            if paint_mute {
                seen_pref = pref;
            }
            let toast_rt = meter_rt.clone();
            let toast_notices = meter_notices.clone();
            let _ = meter_ui.upgrade_in_event_loop(move |ui| {
                if !ui.window().is_visible() {
                    crate::ear::release_talk();
                }
                ui.set_health_line(crate::ear::health_line().into());
                ui.set_mic_level(level as f32 / 100.0);
                ui.set_keyed(keyed);
                ui.set_input_fault(no_mic);
                if paint_mute {
                    ui.set_muted(muted_now);
                }
                if overruns > 0 {
                    toast(
                        &ui,
                        &toast_rt,
                        &toast_notices,
                        "The microphone dropped a block.",
                        true,
                    );
                }
                if let Some((input, applied)) = switched {
                    if input {
                        ui.set_input_name(short_device(&applied, true).into());
                    } else {
                        ui.set_output_name(short_device(&applied, false).into());
                    }
                    let text = if input {
                        "That microphone did not open."
                    } else {
                        "Those speakers did not open."
                    };
                    toast(&ui, &toast_rt, &toast_notices, text, true);
                }
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

fn device_fault(guard: &Slot) -> Option<(bool, String)> {
    if let Some(ear) = guard.ear.as_ref() {
        if ear.take_input_fault() {
            return Some((true, ear.applied_input()));
        }
        if ear.take_output_fault() {
            return Some((false, ear.applied_output()));
        }
        return None;
    }
    if let Some(preview) = guard.preview.as_ref() {
        if preview.take_input_fault() {
            return Some((true, preview.applied_input()));
        }
        if preview.take_output_fault() {
            return Some((false, preview.applied_output()));
        }
    }
    None
}

fn fit_name(name: &str) -> String {
    let mut out = String::new();
    let mut chars = 0usize;
    for ch in name.trim().chars() {
        if ch.is_control() {
            continue;
        }
        if chars == ryme_proto::MAX_NAME {
            break;
        }
        if out.len() + ch.len_utf8() > ryme_proto::NAME_BYTES {
            break;
        }
        out.push(ch);
        chars += 1;
    }
    out
}

fn fit_chat(text: &str) -> Result<String, &'static str> {
    let flat: String = text
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect();
    let trimmed = flat.trim();
    if trimmed.is_empty() {
        return Err("That line was empty.");
    }
    let mut out = String::new();
    for ch in trimmed.chars() {
        if out.len() + ch.len_utf8() > ryme_proto::MAX_CHAT {
            return Err("That line is longer than 400 bytes.");
        }
        out.push(ch);
    }
    Ok(out)
}

fn short_device(name: &str, input: bool) -> String {
    let name = crate::ear::device_caption(name, input);
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
        ui.set_input_name(short_device(&next, true).into());
    } else {
        guard.output = next.clone();
        if let Some(ear) = guard.ear.as_ref() {
            ear.set_output(&next);
        }
        if let Some(preview) = guard.preview.as_ref() {
            preview.set_output(&next);
        }
        ui.set_output_name(short_device(&next, false).into());
    }
    local::store_audio(&local::AudioPref {
        input: guard.input.clone(),
        output: guard.output.clone(),
        key: crate::ear::talk_key(),
        speakerphone: guard.speakerphone,
    });
}

fn listen_local(slot: &Arc<Mutex<Slot>>) {
    let mut guard = slot.lock().unwrap();
    guard.preview = None;
}

fn begin(
    ui: &MainWindow,
    slot: &Arc<Mutex<Slot>>,
    rt: &tokio::runtime::Handle,
    notices: &Arc<AtomicU64>,
    retry: bool,
    attempt: u32,
) {
    let name = fit_name(&ui.get_display_name());
    if name.is_empty() {
        note(ui, rt, notices, "Add a name.");
        return;
    }
    ui.set_display_name(name.clone().into());
    let invite = ui.get_invite().to_string();
    let (muted, deafened, ptt) = {
        let guard = slot.lock().unwrap();
        (guard.muted, guard.deaf, guard.ptt)
    };
    ui.set_muted(muted);
    ui.set_deafened(deafened);
    ui.set_ptt(ptt);
    let gen = bump(slot);
    ui.set_reconnecting(retry);
    ui.set_joining(true);
    ui.set_ping_on(false);
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
        let (resume_secret, room) = {
            let guard = slot.lock().unwrap();
            let secret = intent::ticket_for(guard.ticket_server, server, guard.resume_secret);
            let room = match &guard.aim {
                Aim::Room(name) => Some(name.clone()),
                Aim::Outside => None,
            };
            (secret, room)
        };
        match connect_room(
            &invite,
            &name,
            local::card_for(&server),
            resume_secret,
            room.as_deref(),
        )
        .await
        {
            Ok(live) => {
                {
                    let mut guard = slot.lock().unwrap();
                    if guard.generation != gen {
                        live.link.logout();
                        return;
                    }
                    guard.link = Some(live.link.clone());
                    guard.tcp = live.tcp;
                    guard.resume_secret = live.resume;
                    guard.ticket_server = server;
                    guard.channel = live.channel.clone();
                    guard.resume_channel.clear();
                    guard.pending_channel = None;
                };
                let pending = !intent::admits(&slot.lock().unwrap().aim, &live.channel);
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
                if live.channel.is_empty() {
                    room.in_channel = false;
                }
                if muted {
                    if let Some(me) = room.members.iter_mut().find(|peer| peer.user_id == room.me) {
                        me.muted = true;
                    }
                }
                show(&weak, &slot, gen, &room, true, false);
                let (speak_tx, mut speak_rx) = tokio::sync::mpsc::unbounded_channel();
                let (input, output) = {
                    let guard = slot.lock().unwrap();
                    (guard.input.clone(), guard.output.clone())
                };
                let tcp = live.tcp;
                let ear = crate::ear::open(
                    live.voice,
                    live.me,
                    speak_tx,
                    crate::ear::Gate {
                        muted,
                        deaf: deafened,
                        ptt,
                        denoise,
                        speakerphone: slot.lock().unwrap().speakerphone,
                    },
                    &input,
                    &output,
                    tcp,
                );
                let (mic, speakers) = (ear.mic, ear.speakers);
                {
                    let mut guard = slot.lock().unwrap();
                    if guard.generation != gen {
                        return;
                    }
                    ear.set_ptt(guard.ptt);
                    ear.set_denoise(guard.denoise);
                    ear.set_speakerphone(guard.speakerphone);
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
                let mut delay = live.delay;
                let mut speak_open = true;
                let born = std::time::Instant::now();
                let mut last_ms = None::<u32>;
                let mut last_at = std::time::Instant::now();
                let mut age_tick = tokio::time::interval(Duration::from_secs(1));
                age_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = age_tick.tick() => {
                            let Some(ms) = last_ms else { continue };
                            let age = last_at.elapsed();
                            let tcp = slot.lock().unwrap().tcp;
                            let weak = weak.clone();
                            let _ = weak.upgrade_in_event_loop(move |ui| {
                                show_ping(&ui, ms, tcp, age);
                            });
                        }
                        changed = delay.changed() => {
                            if changed.is_err() {
                                continue;
                            }
                            let Some(ms) = *delay.borrow_and_update() else {
                                continue;
                            };
                            last_ms = Some(ms);
                            last_at = std::time::Instant::now();
                            let tcp = slot.lock().unwrap().tcp;
                            let weak = weak.clone();
                            let _ = weak.upgrade_in_event_loop(move |ui| {
                                show_ping(&ui, ms, tcp, Duration::ZERO);
                            });
                        }
                        heard = speak_rx.recv(), if speak_open => {
                            match heard {
                                Some((id, on)) => {
                                    if on {
                                        room.speaking.insert(id, true);
                                    } else {
                                        room.speaking.remove(&id);
                                    }
                                    let snapshot = room.for_speaking();
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
                                        if let Some(link) = guard.link.take() {
                                            link.cancel();
                                        }
                                        let ear = guard.ear.take();
                                        guard.generation += 1;
                                        guard.resume_channel = match &guard.aim {
                                            Aim::Room(name) => name.clone(),
                                            Aim::Outside => String::new(),
                                        };
                                        if let Aim::Room(name) = &guard.aim {
                                            guard.channel = name.clone();
                                        }
                                        Some((ear, guard.generation))
                                    }
                                };
                                let Some((ear, retry_gen)) = held else {
                                    break;
                                };
                                drop(ear);
                                let young = born.elapsed() < QUICK_DROP;
                                let weak = weak.clone();
                                let slot = slot.clone();
                                let notices = notices.clone();
                                let Some(next) = next_try(attempt, young) else {
                                    let rt = task_rt.clone();
                                    let _ = weak.upgrade_in_event_loop(move |ui| {
                                        clear(&ui);
                                        listen_local(&slot);
                                        note(&ui, &rt, &notices, "The call dropped.");
                                    });
                                    break;
                                };
                                let _ = weak.upgrade_in_event_loop(move |ui| {
                                    ui.set_panel(0);
                                    ui.set_reconnecting(true);
                                    ui.set_joining(false);
                                    ui.set_ping_on(false);
                                    ui.set_toast("".into());
                                });
                                pursue(&task_rt, &weak, &slot, &notices, next, retry_gen);
                                break;
                            }
                            if let Event::Rejected { reason } = &event {
                                let notices = notices.clone();
                                let rt = task_rt.clone();
                                let reason = reason.clone();
                                let restore = {
                                    let mut guard = slot.lock().unwrap();
                                    guard.pending_channel = None;
                                    if !guard.channel.is_empty() {
                                        guard.aim = Aim::Room(guard.channel.clone());
                                    } else {
                                        guard.aim = Aim::Outside;
                                    }
                                    (guard.muted, guard.deaf, guard.ptt)
                                };
                                if let Some(ear) = slot.lock().unwrap().ear.as_ref() {
                                    ear.set_muted(restore.0);
                                    ear.set_deaf(restore.1);
                                    ear.set_ptt(restore.2);
                                }
                                let _ = weak.upgrade_in_event_loop(move |ui| {
                                    note(&ui, &rt, &notices, &reason);
                                });
                            }
                            if let Event::Moved { user_id, resume } = &event {
                                let mut guard = slot.lock().unwrap();
                                guard.tcp = false;
                                guard.resume_secret = *resume;
                                if let Some(ear) = guard.ear.as_ref() {
                                    ear.set_me(*user_id);
                                    ear.set_tcp(false);
                                }
                                let link = guard.link.clone();
                                let muted = guard.muted;
                                let deaf = guard.deaf;
                                drop(guard);
                                if let Some(link) = link {
                                    if muted {
                                        link.mute(true);
                                    }
                                    if deaf {
                                        link.deafen(true);
                                    }
                                }
                            }
                            let joined = matches!(event, Event::Joined { .. });
                            let moved = matches!(event, Event::Moved { .. });
                            let left = matches!(event, Event::Left);
                            let fresh = matches!(&event, Event::Chat { user_id, .. } if *user_id != room.me);
                            room.apply(event);
                            if joined {
                                let switches = {
                                    let mut guard = slot.lock().unwrap();
                                    guard.channel = room.channel.clone();
                                    guard.pending_channel = None;
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
                            if moved {
                                let (channel, denoise, muted, deaf, ptt, ticket) = {
                                    let guard = slot.lock().unwrap();
                                    (
                                        guard.channel.clone(),
                                        guard.denoise,
                                        guard.muted,
                                        guard.deaf,
                                        guard.ptt,
                                        guard.resume_secret,
                                    )
                                };
                                remember(
                                    &name, &invite, &channel, true, denoise, muted, deaf, ptt,
                                    ticket,
                                );
                            }
                            let open = {
                                let guard = slot.lock().unwrap();
                                room.in_channel && intent::admits(&guard.aim, &room.channel)
                            };
                            if open {
                                if let Some(link) = slot.lock().unwrap().link.as_ref() {
                                    link.release_voice();
                                }
                            }
                            if let Some(ear) = slot.lock().unwrap().ear.as_ref() {
                                ear.set_live(open);
                            }
                            if left {
                                room.speaking.clear();
                            }
                            show(&weak, &slot, gen, &room, false, fresh);
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

/// What a second invite should do. A link names a server, not a room.
enum Arrival {
    /// This is the server already on the call. Bring the window forward.
    Stay,
    /// A different server, and the call is live. Ask before leaving.
    Ask,
    /// Not in a call. Put the link in the field.
    Fill,
    /// The text is not an invite.
    Reject,
}

fn arrival(inside: bool, same: bool, ok: bool) -> Arrival {
    if !ok {
        return Arrival::Reject;
    }
    if inside && same {
        Arrival::Stay
    } else if inside {
        Arrival::Ask
    } else {
        Arrival::Fill
    }
}

fn place_label(invite: &Invite) -> String {
    let host = if invite.host.contains(':') {
        format!("[{}]", invite.host)
    } else {
        invite.host.clone()
    };
    if invite.port == DEFAULT_PORT {
        host
    } else {
        format!("{host}:{}", invite.port)
    }
}

fn open_invite(
    ui: &MainWindow,
    slot: &Arc<Mutex<Slot>>,
    rt: &tokio::runtime::Handle,
    notices: &Arc<AtomicU64>,
    ask: &Mutex<Ask>,
    url: &str,
) {
    let url = url.trim();
    let Ok(parsed) = parse_invite(url) else {
        note(ui, rt, notices, "That link is not an invite.");
        return;
    };
    let session = ui.get_inside() || ui.get_joining();
    let same = session && {
        let guard = slot.lock().unwrap();
        guard.server != [0; 32] && guard.server == parsed.fingerprint
    };
    match arrival(ui.get_inside(), same, true) {
        Arrival::Reject => note(ui, rt, notices, "That link is not an invite."),
        Arrival::Stay => {
            let mut pending = ask.lock().unwrap();
            if matches!(*pending, Ask::Switch(_)) {
                *pending = Ask::None;
                drop(pending);
                ui.set_confirming(false);
            }
        }
        Arrival::Ask => {
            let place = place_label(&parsed);
            *ask.lock().unwrap() = Ask::Switch(url.to_string());
            ui.set_toast("".into());
            ui.set_confirm_danger(true);
            pose(
                ui,
                "Switch server",
                &format!("Leave this call and join {place}?"),
                "Switch",
            );
        }
        Arrival::Fill => {
            if ui.get_joining() && same {
                return;
            }
            ui.set_toast("".into());
            ui.set_invite(url.into());
            ui.set_panel(0);
            if ui.get_joining() {
                let mut guard = slot.lock().unwrap();
                guard.resume_channel.clear();
                guard.resume_secret = [0; 32];
                guard.ticket_server = [0; 32];
                guard.aim = Aim::Room("Lobby".into());
                drop(guard);
                begin(ui, slot, rt, notices, false, 0);
            }
        }
    }
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

const PING_EASY: u32 = 80;

fn show_ping(ui: &MainWindow, ms: u32, tcp: bool, age: Duration) {
    let ms = ms.min(9_999);
    let path = if tcp { "TCP" } else { "QUIC" };
    let stale = age >= Duration::from_secs(3);
    let text = if stale {
        format!("stale · {path}")
    } else if age >= Duration::from_secs(1) {
        format!("{ms} ms · {path} · {}s", age.as_secs())
    } else {
        format!("{ms} ms · {path}")
    };
    ui.set_ping_text(text.into());
    ui.set_ping_hot(stale || ms > PING_EASY);
    ui.set_ping_on(true);
}

fn show(
    weak: &Weak<MainWindow>,
    slot: &Arc<Mutex<Slot>>,
    gen: u64,
    room: &Room,
    entered: bool,
    fresh: bool,
) {
    let room = room.clone();
    let slot = slot.clone();
    let _ = weak.upgrade_in_event_loop(move |ui| {
        let (levels, tcp) = {
            let mut guard = slot.lock().unwrap();
            if guard.generation != gen {
                return;
            }
            guard.roster = room
                .members
                .iter()
                .map(|peer| (peer.user_id, peer.card))
                .collect();
            let tcp = guard.tcp;
            let mixes = hearing(&guard.roster, &guard.levels);
            if let Some(ear) = guard.ear.as_ref() {
                ear.set_levels(mixes);
            }
            (guard.levels.clone(), tcp)
        };
        ui.set_tcp(tcp);
        ui.set_joining(false);
        ui.set_reconnecting(false);
        if entered {
            ui.set_toast("".into());
            ui.set_chat_dot(false);
        }
        if fresh && ui.get_panel() != 2 {
            ui.set_chat_dot(true);
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
    ui.set_chat_dot(false);
    ui.set_inside(false);
    ui.set_joining(false);
    ui.set_reconnecting(false);
    ui.set_tcp(false);
    ui.set_ping_on(false);
    ui.set_joined(-1);
    ui.set_panel(0);
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
            link: first_link(body).into(),
            body: chat_body(body).into(),
            mine: *mine,
        })
        .collect::<Vec<_>>();
    ModelRc::new(VecModel::from(lines))
}

/// The first http, https, or steam address in a line. Empty when there is none.
fn first_link(text: &str) -> String {
    const SCHEMES: [&str; 3] = ["https://", "http://", "steam://"];
    let folded = text.to_ascii_lowercase();
    let Some(start) = SCHEMES
        .iter()
        .filter_map(|scheme| folded.find(scheme))
        .min()
    else {
        return String::new();
    };
    let rest = &text[start..];
    let end = rest
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace())
        .map(|(index, _)| index)
        .unwrap_or(rest.len());
    let url = rest[..end].trim_end_matches(|ch: char| {
        matches!(
            ch,
            '.' | ',' | ';' | ':' | '!' | '?' | ')' | ']' | '"' | '\''
        )
    });
    let folded_url = url.to_ascii_lowercase();
    if SCHEMES
        .iter()
        .any(|scheme| folded_url.starts_with(scheme) && url.len() > scheme.len())
    {
        url.to_string()
    } else {
        String::new()
    }
}

fn copy_text(text: &str) -> bool {
    arboard::Clipboard::new()
        .and_then(|mut clipboard| clipboard.set_text(text))
        .is_ok()
}

/// Break a long token so the small card can wrap it. The mark is invisible.
fn chat_body(text: &str) -> String {
    const RUN: usize = 16;
    let mut out = String::new();
    let mut run = 0usize;
    for ch in text.chars() {
        if ch.is_whitespace() {
            run = 0;
            out.push(ch);
            continue;
        }
        if run == RUN {
            out.push('\u{200b}');
            run = 0;
        }
        out.push(ch);
        run += 1;
    }
    out
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
        LinkError::Timeout => {
            "The server did not answer. Check that UDP or TCP can get through.".into()
        }
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
const QUICK_DROP: Duration = Duration::from_secs(8);

fn pace(attempt: u32) -> Option<Duration> {
    if attempt >= ATTEMPTS {
        return None;
    }
    Some(if attempt == 0 { QUIET_WAIT } else { AGAIN_WAIT })
}

/// A call that dies immediately uses up one of the three tries. A call that
/// lasted comes back once, from the start of that count.
fn next_try(attempt: u32, young: bool) -> Option<u32> {
    let next = if young { attempt.saturating_add(1) } else { 0 };
    pace(next).map(|_| next)
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
    use super::{
        arrival, chat_body, explain, first_link, next_try, pace, place_label, Arrival, Room,
        AGAIN_WAIT, ATTEMPTS, QUIET_WAIT,
    };
    use ryme_proto::Invite;
    use ryme_server::{Event, LinkError};

    #[test]
    fn a_rejected_switch_leaves_the_confirmed_room_live() {
        let stage = crate::intent::Aim::Room("Stage".into());
        assert!(!crate::intent::admits(&stage, "Lobby"));
        assert!(crate::intent::admits(&stage, "Stage"));
        assert!(!crate::intent::admits(
            &crate::intent::Aim::Outside,
            "Stage"
        ));
    }

    #[test]
    fn a_drop_is_tried_three_times_and_then_stops() {
        assert_eq!(pace(0), Some(QUIET_WAIT));
        assert_eq!(pace(1), Some(AGAIN_WAIT));
        assert_eq!(pace(ATTEMPTS - 1), Some(AGAIN_WAIT));
        assert_eq!(pace(ATTEMPTS), None);
        assert!(QUIET_WAIT < AGAIN_WAIT);
        assert_eq!(next_try(0, true), Some(1));
        assert_eq!(next_try(1, true), Some(2));
        assert_eq!(next_try(2, true), None);
        assert_eq!(next_try(2, false), Some(0));
    }

    #[test]
    fn a_long_token_can_wrap_inside_the_card() {
        assert_eq!(chat_body("hello there"), "hello there");
        let shown = chat_body("https://example.com/this/is/a/very/long/path");
        let longest = shown
            .split(|ch: char| ch.is_whitespace() || ch == '\u{200b}')
            .map(|piece| piece.chars().count())
            .max()
            .unwrap_or(0);
        assert!(longest <= 16, "a run of {longest} still cannot wrap");
        assert!(shown.contains('\u{200b}'));
    }

    #[test]
    fn a_link_is_the_address_without_the_trailing_period() {
        assert_eq!(first_link("be right back"), "");
        assert_eq!(
            first_link("see https://store.steampowered.com/app/570."),
            "https://store.steampowered.com/app/570"
        );
        assert_eq!(first_link("steam://run/730"), "steam://run/730");
        assert_eq!(first_link("https://"), "");
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
    fn joining_a_channel_replaces_the_thread_with_its_history() {
        let mut room = Room::new(1, "Lobby".into(), Vec::new(), vec!["Lobby".into()]);
        room.apply(Event::Chat {
            user_id: 2,
            text: "gone".into(),
        });
        room.apply(Event::Joined {
            channel: "Lobby".into(),
            members: Vec::new(),
            rooms: vec!["Lobby".into(), "Stage".into()],
        });
        assert_eq!(room.lines.len(), 1);
        room.apply(Event::Joined {
            channel: "Stage".into(),
            members: Vec::new(),
            rooms: vec!["Lobby".into(), "Stage".into()],
        });
        assert!(room.lines.is_empty());
        room.apply(Event::Past {
            name: "Mara".into(),
            text: "brb".into(),
            mine: false,
        });
        room.apply(Event::Past {
            name: "Mara".into(),
            text: "back".into(),
            mine: true,
        });
        assert_eq!(room.lines.len(), 2);
        assert_eq!(room.lines[0].0, "Mara");
        assert_eq!(room.lines[1].0, "You");
    }

    #[test]
    fn a_second_link_stays_asks_or_fills() {
        assert!(matches!(arrival(true, true, true), Arrival::Stay));
        assert!(matches!(arrival(true, false, true), Arrival::Ask));
        assert!(matches!(arrival(false, false, true), Arrival::Fill));
        assert!(matches!(arrival(false, true, true), Arrival::Fill));
        assert!(matches!(arrival(true, true, false), Arrival::Reject));
    }

    #[test]
    fn a_switch_names_the_other_server() {
        let invite = Invite {
            host: "fra.ryme.chat".into(),
            port: ryme_proto::DEFAULT_PORT,
            fingerprint: [0; 32],
            pass: [0; 32],
        };
        assert_eq!(place_label(&invite), "fra.ryme.chat");
        let other = Invite {
            host: "2001:db8::1".into(),
            port: 443,
            ..invite
        };
        assert_eq!(place_label(&other), "[2001:db8::1]:443");
    }

    #[test]
    fn connection_failures_say_what_went_wrong() {
        assert!(explain(LinkError::Pin).contains("does not match"));
        assert!(explain(LinkError::Timeout).contains("UDP"));
        assert!(explain(LinkError::Address).contains("resolve"));
        assert!(explain(LinkError::Unreachable).contains("Could not reach"));
    }
}
