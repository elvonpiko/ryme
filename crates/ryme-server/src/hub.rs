use std::collections::HashMap;

use ryme_proto::{decode_voice, ClientMsg, Member, ServerMsg};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub user_id: Option<u32>,
    pub to_self: Vec<ServerMsg>,
    pub to_others: Vec<(u32, ServerMsg)>,
}

struct User {
    name: String,
    channel: Option<String>,
    muted: bool,
    deafened: bool,
}

pub struct Hub {
    next_id: u32,
    users: HashMap<u32, User>,
}

impl Hub {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            users: HashMap::new(),
        }
    }

    pub fn handle(&mut self, user: Option<u32>, msg: ClientMsg) -> Step {
        match msg {
            ClientMsg::Hello { name } => self.hello(user, name),
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

    fn hello(&mut self, user: Option<u32>, name: String) -> Step {
        if user.is_some() {
            return reject(user, "already here");
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
            },
        );
        Step {
            user_id: Some(id),
            to_self: vec![ServerMsg::Welcome { user_id: id }],
            to_others: Vec::new(),
        }
    }

    fn join(&mut self, user: Option<u32>, channel: String) -> Step {
        let Some(id) = user else {
            return reject(None, "say hello first");
        };
        let Some(existing) = self.users.get(&id) else {
            return reject(Some(id), "unknown");
        };
        if existing.channel.as_deref() == Some(channel.as_str()) {
            let members = self.members(&channel);
            return Step {
                user_id: Some(id),
                to_self: vec![ServerMsg::Joined { channel, members }],
                to_others: Vec::new(),
            };
        }
        let previous = existing.channel.clone();
        let name = existing.name.clone();
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
                },
            )
        }));
        let members = self.members(&channel);
        Step {
            user_id: Some(id),
            to_self: vec![ServerMsg::Joined { channel, members }],
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
        Step {
            user_id: Some(id),
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
        Self::new()
    }
}

fn reject(user_id: Option<u32>, reason: &str) -> Step {
    Step {
        user_id,
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
        let step = hub.handle(None, ClientMsg::Hello { name: name.into() });
        step.user_id.unwrap()
    }

    fn voice(seq: u32) -> Vec<u8> {
        encode_voice(&VoicePacket {
            flags: 1,
            seq,
            timestamp: seq * 960,
            payload: vec![seq as u8],
        })
        .unwrap()
    }

    #[test]
    fn hello_assigns_ids_and_refuses_a_second_one() {
        let mut hub = Hub::new();
        let mara = hello(&mut hub, "Mara");
        let jonas = hello(&mut hub, "Jonas");
        assert_eq!(mara, 1);
        assert_eq!(jonas, 2);
        let again = hub.handle(
            Some(mara),
            ClientMsg::Hello {
                name: "Mara".into(),
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
        let mut hub = Hub::new();
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
                }],
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
        let mut hub = Hub::new();
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
    fn switching_channels_leaves_the_old_one() {
        let mut hub = Hub::new();
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
        let mut hub = Hub::new();
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
        let mut hub = Hub::new();
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
        let mut hub = Hub::new();
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
        let mut hub = Hub::new();
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
}
