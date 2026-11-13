use std::collections::HashMap;

use ryme_proto::{decode_voice, ClientMsg, Member, ServerMsg};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub user_id: Option<u32>,
    /// Previous session this hello took the place of. The connection for that
    /// id has to be closed; removing the roster entry is not enough.
    pub replace: Option<u32>,
    pub to_self: Vec<ServerMsg>,
    pub to_others: Vec<(u32, ServerMsg)>,
}

pub const MAX_PEOPLE: usize = 32;
pub const MAX_ROOMS: usize = 16;

struct User {
    name: String,
    channel: Option<String>,
    muted: bool,
    deafened: bool,
    card: u64,
    resume: [u8; 32],
}

pub struct Hub {
    next_id: u32,
    users: HashMap<u32, User>,
    channels: Vec<String>,
    pass: [u8; 32],
    locked: bool,
    banned: std::collections::HashSet<u64>,
}

impl Hub {
    pub fn new(pass: [u8; 32]) -> Self {
        Self {
            next_id: 1,
            users: HashMap::new(),
            channels: vec!["Lobby".to_string()],
            pass,
            locked: false,
            banned: std::collections::HashSet::new(),
        }
    }

    pub fn set_locked(&mut self, locked: bool) {
        self.locked = locked;
    }

    pub fn ban(&mut self, card: u64) {
        if card != 0 {
            self.banned.insert(card);
        }
    }

    pub fn unban(&mut self, card: u64) {
        self.banned.remove(&card);
    }

    pub fn cards(&self) -> Vec<(u32, u64)> {
        self.users
            .iter()
            .map(|(id, user)| (*id, user.card))
            .collect()
    }

    pub fn banned_cards(&self) -> Vec<u64> {
        let mut cards: Vec<u64> = self.banned.iter().copied().collect();
        cards.sort_unstable();
        cards
    }

    /// The host decides which rooms exist. A join cannot invent one.
    pub fn allow(&mut self, name: &str) {
        let name = name.trim();
        if name.is_empty() || name.len() > ryme_proto::MAX_CHANNEL || name.contains(['\n', '\r']) {
            return;
        }
        if self.channels.iter().any(|room| room == name) {
            return;
        }
        if self.channels.len() >= MAX_ROOMS {
            return;
        }
        self.channels.push(name.to_string());
    }

    pub fn handle(&mut self, user: Option<u32>, msg: ClientMsg) -> Step {
        match msg {
            ClientMsg::Hello {
                name,
                card,
                pass,
                resume,
            } => self.hello(user, name, card, pass, resume),
            ClientMsg::Join { channel } => self.join(user, channel),
            ClientMsg::Leave => self.leave(user),
            ClientMsg::Chat { text } => self.chat(user, text),
            ClientMsg::Mute { muted } => self.mute(user, muted),
            ClientMsg::Deafen { deafened } => self.deafen(user, deafened),
        }
    }

    /// Who else should receive this datagram. Invalid, muted, or idle packets go nowhere.
    pub fn voice_targets(&self, from: u32, packet: &[u8]) -> Vec<u32> {
        if decode_voice(packet).is_err() {
            return Vec::new();
        }
        let Some(user) = self.users.get(&from) else {
            return Vec::new();
        };
        if user.muted {
            return Vec::new();
        }
        let Some(channel) = user.channel.as_deref() else {
            return Vec::new();
        };
        let mut targets: Vec<u32> = self
            .users
            .iter()
            .filter(|(id, other)| **id != from && other.channel.as_deref() == Some(channel))
            .map(|(id, _)| *id)
            .collect();
        targets.sort_unstable();
        targets
    }

    pub fn disconnect(&mut self, id: u32) -> Vec<(u32, ServerMsg)> {
        let Some(user) = self.users.remove(&id) else {
            return Vec::new();
        };
        let Some(channel) = user.channel else {
            return Vec::new();
        };
        self.peers(&channel, id)
            .into_iter()
            .map(|peer| (peer, ServerMsg::PeerLeft { user_id: id }))
            .collect()
    }

    fn hello(
        &mut self,
        user: Option<u32>,
        name: String,
        card: u64,
        pass: [u8; 32],
        resume: [u8; 32],
    ) -> Step {
        if user.is_some() {
            return reject(user, "already here");
        }
        if !crate::identity::same(&pass, &self.pass) {
            return reject(user, "invite was retired");
        }
        if card == 0 {
            return reject(user, "bad card");
        }
        if self.banned.contains(&card) {
            return reject(user, "not invited");
        }
        let replacing = resume != [0; 32]
            && self.users.values().any(|existing| {
                existing.card == card && crate::identity::same(&existing.resume, &resume)
            });
        if self.locked && !replacing {
            return reject(user, "room is locked");
        }
        let fresh = match crate::identity::random_32() {
            Ok(fresh) => fresh,
            Err(_) => return reject(user, "unavailable"),
        };
        let mut replace = None;
        let mut to_others = Vec::new();
        if resume != [0; 32] {
            let old = self
                .users
                .iter()
                .find(|(_, existing)| {
                    existing.card == card && crate::identity::same(&existing.resume, &resume)
                })
                .map(|(id, _)| *id);
            if let Some(old) = old {
                to_others.extend(self.disconnect(old));
                replace = Some(old);
            }
        }
        if self.users.values().any(|existing| existing.card == card) {
            return reject(user, "already here");
        }
        if self.users.len() >= MAX_PEOPLE {
            return reject(user, "server is full");
        }
        let id = self.next_id;
        self.next_id += 1;
        self.users.insert(
            id,
            User {
                name,
                channel: None,
                muted: false,
                deafened: false,
                card,
                resume: fresh,
            },
        );
        Step {
            user_id: Some(id),
            replace,
            to_self: vec![ServerMsg::Welcome {
                user_id: id,
                resume: fresh,
            }],
            to_others,
        }
    }

    fn join(&mut self, user: Option<u32>, channel: String) -> Step {
        let Some(id) = user else {
            return reject(None, "say hello first");
        };
        let Some(existing) = self.users.get(&id) else {
            return reject(Some(id), "unknown");
        };
        if !self.channels.iter().any(|room| room == &channel) {
            return reject(Some(id), "no such channel");
        }
        if existing.channel.as_deref() == Some(channel.as_str()) {
            let members = self.members(&channel);
            let rooms = self.channels.clone();
            return Step {
                user_id: Some(id),
                replace: None,
                to_self: vec![ServerMsg::Joined {
                    channel,
                    members,
                    rooms,
                }],
                to_others: Vec::new(),
            };
        }
        let previous = existing.channel.clone();
        let name = existing.name.clone();
        let card = existing.card;
        if let Some(user) = self.users.get_mut(&id) {
            user.channel = Some(channel.clone());
        }
        let mut to_others = Vec::new();
        if let Some(previous) = previous {
            to_others.extend(
                self.peers(&previous, id)
                    .into_iter()
                    .map(|peer| (peer, ServerMsg::PeerLeft { user_id: id })),
            );
        }
        to_others.extend(self.peers(&channel, id).into_iter().map(|peer| {
            (
                peer,
                ServerMsg::PeerJoined {
                    user_id: id,
                    name: name.clone(),
                    card,
                },
            )
        }));
        let members = self.members(&channel);
        let rooms = self.channels.clone();
        tracing::info!(user_id = id, %channel, "joined");
        Step {
            user_id: Some(id),
            replace: None,
            to_self: vec![ServerMsg::Joined {
                channel,
                members,
                rooms,
            }],
            to_others,
        }
    }

    fn leave(&mut self, user: Option<u32>) -> Step {
        let Some(id) = self.known(user) else {
            return reject(user, "not in a channel");
        };
        let Some(channel) = self.users.get(&id).and_then(|u| u.channel.clone()) else {
            return reject(Some(id), "not in a channel");
        };
        if let Some(user) = self.users.get_mut(&id) {
            user.channel = None;
        }
        tracing::info!(user_id = id, "left");
        Step {
            user_id: Some(id),
            replace: None,
            to_self: vec![ServerMsg::Left],
            to_others: self
                .peers(&channel, id)
                .into_iter()
                .map(|peer| (peer, ServerMsg::PeerLeft { user_id: id }))
                .collect(),
        }
    }

    fn chat(&mut self, user: Option<u32>, text: String) -> Step {
        let Some(id) = self.known(user) else {
            return reject(user, "not in a channel");
        };
        let Some(channel) = self.users.get(&id).and_then(|u| u.channel.clone()) else {
            return reject(Some(id), "not in a channel");
        };
        let msg = ServerMsg::Chat { user_id: id, text };
        let mut to_others = Vec::new();
        let mut to_self = Vec::new();
        for peer in self.peers(&channel, id) {
            to_others.push((peer, msg.clone()));
        }
        to_self.push(msg);
        Step {
            user_id: Some(id),
            replace: None,
            to_self,
            to_others,
        }
    }

    fn mute(&mut self, user: Option<u32>, muted: bool) -> Step {
        let Some(id) = self.known(user) else {
            return reject(user, "say hello first");
        };
        let Some(user) = self.users.get_mut(&id) else {
            return reject(Some(id), "unknown");
        };
        user.muted = muted;
        let channel = user.channel.clone();
        let msg = ServerMsg::Muted { user_id: id, muted };
        let to_others = channel
            .map(|channel| {
                self.peers(&channel, id)
                    .into_iter()
                    .map(|peer| (peer, msg.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Step {
            user_id: Some(id),
            replace: None,
            to_self: vec![msg],
            to_others,
        }
    }

    fn deafen(&mut self, user: Option<u32>, deafened: bool) -> Step {
        let Some(id) = self.known(user) else {
            return reject(user, "say hello first");
        };
        if let Some(user) = self.users.get_mut(&id) {
            user.deafened = deafened;
        }
        Step {
            user_id: Some(id),
            replace: None,
            to_self: Vec::new(),
            to_others: Vec::new(),
        }
    }

    fn known(&self, user: Option<u32>) -> Option<u32> {
        user.filter(|id| self.users.contains_key(id))
    }

    fn members(&self, channel: &str) -> Vec<Member> {
        let mut members: Vec<Member> = self
            .users
            .iter()
            .filter(|(_, user)| user.channel.as_deref() == Some(channel))
            .map(|(id, user)| Member {
                user_id: *id,
                name: user.name.clone(),
                muted: user.muted,
                card: user.card,
            })
            .collect();
        members.sort_by_key(|member| member.user_id);
        members
    }

    fn peers(&self, channel: &str, except: u32) -> Vec<u32> {
        let mut peers: Vec<u32> = self
            .users
            .iter()
            .filter(|(id, user)| **id != except && user.channel.as_deref() == Some(channel))
            .map(|(id, _)| *id)
            .collect();
        peers.sort_unstable();
        peers
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new([0; 32])
    }
}

fn reject(user_id: Option<u32>, reason: &str) -> Step {
    tracing::info!(user_id, %reason, "rejected");
    Step {
        user_id,
        replace: None,
        to_self: vec![ServerMsg::Reject {
            reason: reason.to_string(),
        }],
        to_others: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ryme_proto::{encode_voice, VoicePacket};

    fn hello(hub: &mut Hub, name: &str) -> u32 {
        let step = hub.handle(
            None,
            ClientMsg::Hello {
                name: name.into(),
                card: card_of(name),
                pass: [7; 32],
                resume: [0; 32],
            },
        );
        step.user_id.unwrap()
    }

    fn card_of(name: &str) -> u64 {
        name.bytes().fold(1u64, |acc, byte| {
            acc.wrapping_mul(131).wrapping_add(byte as u64)
        })
    }

    fn voice(seq: u32) -> Vec<u8> {
        encode_voice(&VoicePacket {
            speaker: 0,
            seq,
            timestamp: seq * 960,
            flags: 0,
            payload: vec![seq as u8],
        })
        .unwrap()
    }

    #[test]
    fn hello_assigns_ids_and_refuses_a_second_one() {
        let mut hub = Hub::new([7; 32]);
        let mara = hello(&mut hub, "Mara");
        let jonas = hello(&mut hub, "Jonas");
        assert_eq!(mara, 1);
        assert_eq!(jonas, 2);
        let again = hub.handle(
            Some(mara),
            ClientMsg::Hello {
                name: "Mara".into(),
                card: card_of("Mara"),
                pass: [7; 32],
                resume: [0; 32],
            },
        );
        assert_eq!(
            again.to_self,
            vec![ServerMsg::Reject {
                reason: "already here".into()
            }]
        );
        assert!(again.to_others.is_empty());
    }

    #[test]
    fn join_tells_the_people_already_there() {
        let mut hub = Hub::new([7; 32]);
        let mara = hello(&mut hub, "Mara");
        let jonas = hello(&mut hub, "Jonas");
        let first = hub.handle(
            Some(mara),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        assert!(first.to_others.is_empty());
        assert_eq!(
            first.to_self,
            vec![ServerMsg::Joined {
                channel: "Lobby".into(),
                members: vec![Member {
                    user_id: mara,
                    name: "Mara".into(),
                    muted: false,
                    card: card_of("Mara"),
                }],
                rooms: vec!["Lobby".into()],
            }]
        );

        let second = hub.handle(
            Some(jonas),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        assert_eq!(
            second.to_others,
            vec![(
                mara,
                ServerMsg::PeerJoined {
                    user_id: jonas,
                    name: "Jonas".into(),
                    card: card_of("Jonas"),
                }
            )]
        );
        match &second.to_self[0] {
            ServerMsg::Joined { members, .. } => {
                assert_eq!(members.len(), 2);
                assert_eq!(members[0].user_id, mara);
                assert_eq!(members[1].user_id, jonas);
            }
            other => panic!("expected join, got {other:?}"),
        }
    }

    #[test]
    fn rejoining_the_same_channel_does_not_announce_again() {
        let mut hub = Hub::new([7; 32]);
        let mara = hello(&mut hub, "Mara");
        let _ = hub.handle(
            Some(mara),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        let again = hub.handle(
            Some(mara),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        assert!(again.to_others.is_empty());
    }

    #[test]
    fn an_unknown_channel_does_not_get_created() {
        let mut hub = Hub::new([7; 32]);
        let mara = hello(&mut hub, "Mara");
        let _ = hub.handle(
            Some(mara),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        let refused = hub.handle(
            Some(mara),
            ClientMsg::Join {
                channel: "Loby".into(),
            },
        );
        assert_eq!(
            refused.to_self,
            vec![ServerMsg::Reject {
                reason: "no such channel".into(),
            }]
        );
        assert!(refused.to_others.is_empty());
        let jonas = hello(&mut hub, "Jonas");
        let _ = hub.handle(
            Some(jonas),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        assert_eq!(hub.voice_targets(mara, &voice(1)), vec![jonas]);
    }

    #[test]
    fn switching_channels_leaves_the_old_one() {
        let mut hub = Hub::new([7; 32]);
        let mara = hello(&mut hub, "Mara");
        let jonas = hello(&mut hub, "Jonas");
        let _ = hub.handle(
            Some(mara),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        let _ = hub.handle(
            Some(jonas),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        hub.allow("Ranked");
        let moved = hub.handle(
            Some(jonas),
            ClientMsg::Join {
                channel: "Ranked".into(),
            },
        );
        assert_eq!(
            moved.to_others,
            vec![(mara, ServerMsg::PeerLeft { user_id: jonas })]
        );
        assert!(hub.voice_targets(mara, &voice(1)) == Vec::<u32>::new());
    }

    #[test]
    fn voice_reaches_channel_mates_only() {
        let mut hub = Hub::new([7; 32]);
        let mara = hello(&mut hub, "Mara");
        let jonas = hello(&mut hub, "Jonas");
        let kit = hello(&mut hub, "Kit");
        let _ = hub.handle(
            Some(mara),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        let _ = hub.handle(
            Some(jonas),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        hub.allow("Ranked");
        let _ = hub.handle(
            Some(kit),
            ClientMsg::Join {
                channel: "Ranked".into(),
            },
        );

        assert_eq!(hub.voice_targets(mara, &voice(1)), vec![jonas]);
        assert_eq!(hub.voice_targets(jonas, &voice(2)), vec![mara]);
        assert!(hub.voice_targets(kit, &voice(3)).is_empty());
        assert!(hub.voice_targets(mara, &[1, 2, 3]).is_empty());
        assert!(hub.voice_targets(99, &voice(4)).is_empty());
    }

    #[test]
    fn muted_voice_is_dropped_and_deafen_still_receives() {
        let mut hub = Hub::new([7; 32]);
        let mara = hello(&mut hub, "Mara");
        let jonas = hello(&mut hub, "Jonas");
        let _ = hub.handle(
            Some(mara),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        let _ = hub.handle(
            Some(jonas),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );

        let deaf = hub.handle(Some(jonas), ClientMsg::Deafen { deafened: true });
        assert!(deaf.to_self.is_empty());
        assert!(deaf.to_others.is_empty());
        assert_eq!(hub.voice_targets(mara, &voice(1)), vec![jonas]);

        let mute = hub.handle(Some(mara), ClientMsg::Mute { muted: true });
        assert_eq!(
            mute.to_others,
            vec![(
                jonas,
                ServerMsg::Muted {
                    user_id: mara,
                    muted: true
                }
            )]
        );
        assert!(hub.voice_targets(mara, &voice(2)).is_empty());

        let chat = hub.handle(Some(mara), ClientMsg::Chat { text: "brb".into() });
        assert_eq!(
            chat.to_others,
            vec![(
                jonas,
                ServerMsg::Chat {
                    user_id: mara,
                    text: "brb".into()
                }
            )]
        );
        assert_eq!(
            chat.to_self,
            chat.to_others
                .iter()
                .map(|(_, msg)| msg.clone())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn chat_and_voice_before_a_channel_are_refused() {
        let mut hub = Hub::new([7; 32]);
        let before = hub.handle(
            None,
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        assert_eq!(
            before.to_self,
            vec![ServerMsg::Reject {
                reason: "say hello first".into()
            }]
        );
        let mara = hello(&mut hub, "Mara");
        let chat = hub.handle(Some(mara), ClientMsg::Chat { text: "hey".into() });
        assert_eq!(
            chat.to_self,
            vec![ServerMsg::Reject {
                reason: "not in a channel".into()
            }]
        );
        assert!(hub.voice_targets(mara, &voice(1)).is_empty());
    }

    #[test]
    fn leave_and_disconnect_announce_once() {
        let mut hub = Hub::new([7; 32]);
        let mara = hello(&mut hub, "Mara");
        let jonas = hello(&mut hub, "Jonas");
        let _ = hub.handle(
            Some(mara),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        let _ = hub.handle(
            Some(jonas),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );

        let left = hub.handle(Some(jonas), ClientMsg::Leave);
        assert_eq!(left.to_self, vec![ServerMsg::Left]);
        assert_eq!(
            left.to_others,
            vec![(mara, ServerMsg::PeerLeft { user_id: jonas })]
        );
        assert!(hub.voice_targets(mara, &voice(1)).is_empty());

        let _ = hub.handle(
            Some(jonas),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        let gone = hub.disconnect(jonas);
        assert_eq!(gone, vec![(mara, ServerMsg::PeerLeft { user_id: jonas })]);
        assert!(hub.disconnect(jonas).is_empty());
        assert!(hub.voice_targets(mara, &voice(2)).is_empty());
    }

    #[test]
    fn a_wrong_token_is_refused_and_admits_nobody() {
        let mut hub = Hub::new([7; 32]);
        let refused = hub.handle(
            None,
            ClientMsg::Hello {
                name: "Mara".into(),
                card: 4,
                pass: [8; 32],
                resume: [0; 32],
            },
        );
        assert_eq!(refused.user_id, None);
        assert_eq!(
            refused.to_self,
            vec![ServerMsg::Reject {
                reason: "invite was retired".into()
            }]
        );
        let admitted = hub.handle(
            None,
            ClientMsg::Hello {
                name: "Mara".into(),
                card: 4,
                pass: [7; 32],
                resume: [0; 32],
            },
        );
        assert_eq!(admitted.user_id, Some(1));
    }

    #[test]
    fn the_resume_secret_replaces_only_that_session() {
        let mut hub = Hub::new([7; 32]);
        let first = hub.handle(
            None,
            ClientMsg::Hello {
                name: "Mara".into(),
                card: 4,
                pass: [7; 32],
                resume: [0; 32],
            },
        );
        let (id, secret) = match &first.to_self[0] {
            ServerMsg::Welcome { user_id, resume } => (*user_id, *resume),
            other => panic!("expected a welcome, got {other:?}"),
        };
        let _ = hub.handle(
            Some(id),
            ClientMsg::Join {
                channel: "Lobby".into(),
            },
        );
        let again = hub.handle(
            None,
            ClientMsg::Hello {
                name: "Mara".into(),
                card: 4,
                pass: [7; 32],
                resume: secret,
            },
        );
        assert_eq!(again.replace, Some(id));
        assert_ne!(again.user_id, Some(id));
        let stolen = hub.handle(
            None,
            ClientMsg::Hello {
                name: "Mara".into(),
                card: 4,
                pass: [7; 32],
                resume: [9; 32],
            },
        );
        assert_eq!(
            stolen.to_self,
            vec![ServerMsg::Reject {
                reason: "already here".into()
            }]
        );
    }

    #[test]
    fn the_roster_stops_at_the_control_frame() {
        let mut hub = Hub::new([7; 32]);
        for index in 0..MAX_PEOPLE {
            let step = hub.handle(
                None,
                ClientMsg::Hello {
                    name: format!("p{index}"),
                    card: (index as u64) + 1,
                    pass: [7; 32],
                    resume: [0; 32],
                },
            );
            assert!(step.user_id.is_some());
        }
        let extra = hub.handle(
            None,
            ClientMsg::Hello {
                name: "extra".into(),
                card: 99,
                pass: [7; 32],
                resume: [0; 32],
            },
        );
        assert_eq!(
            extra.to_self,
            vec![ServerMsg::Reject {
                reason: "server is full".into()
            }]
        );

        let members: Vec<ryme_proto::Member> = (0..MAX_PEOPLE as u32)
            .map(|id| ryme_proto::Member {
                user_id: id,
                name: "n".repeat(ryme_proto::MAX_NAME),
                muted: true,
                card: u64::MAX,
            })
            .collect();
        let rooms: Vec<String> = (0..MAX_ROOMS)
            .map(|index| format!("room-{index:02}"))
            .collect();
        let frame = ryme_proto::encode_server(&ServerMsg::Joined {
            channel: "c".repeat(ryme_proto::MAX_CHANNEL),
            members,
            rooms,
        })
        .unwrap();
        assert!(
            frame.len() <= ryme_proto::MAX_FRAME,
            "joined frame is {} bytes",
            frame.len()
        );
    }

    #[test]
    fn a_lock_and_a_ban_refuse_new_people() {
        let mut hub = Hub::new([7; 32]);
        let first = hub.handle(
            None,
            ClientMsg::Hello {
                name: "Mara".into(),
                card: 4,
                pass: [7; 32],
                resume: [0; 32],
            },
        );
        let ServerMsg::Welcome { resume, .. } = &first.to_self[0] else {
            panic!("welcome");
        };
        let resume = *resume;
        hub.set_locked(true);
        let locked = hub.handle(
            None,
            ClientMsg::Hello {
                name: "Ned".into(),
                card: 5,
                pass: [7; 32],
                resume: [0; 32],
            },
        );
        assert_eq!(
            locked.to_self,
            vec![ServerMsg::Reject {
                reason: "room is locked".into()
            }]
        );
        let back = hub.handle(
            None,
            ClientMsg::Hello {
                name: "Mara".into(),
                card: 4,
                pass: [7; 32],
                resume,
            },
        );
        assert!(back.user_id.is_some());
        hub.set_locked(false);
        hub.ban(5);
        let banned = hub.handle(
            None,
            ClientMsg::Hello {
                name: "Ned".into(),
                card: 5,
                pass: [7; 32],
                resume: [0; 32],
            },
        );
        assert_eq!(
            banned.to_self,
            vec![ServerMsg::Reject {
                reason: "not invited".into()
            }]
        );
    }
}
