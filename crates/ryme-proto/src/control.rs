use serde::{Deserialize, Serialize};

use crate::ProtoError;

pub const MAX_NAME: usize = 32;
/// A name may be 32 characters. CJK is more than one byte, so the byte cap is wider.
pub const NAME_BYTES: usize = 128;
pub const MAX_CHANNEL: usize = 32;
pub const MAX_CHAT: usize = 400;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub user_id: u32,
    pub name: String,
    pub muted: bool,
    /// Stable id from that person's own client. A session id changes on every
    /// join; this one does not, so a local volume can follow them.
    pub card: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientMsg {
    /// `pass` is the invitation token. `resume` is zero on a first hello.
    /// A later hello repeats the secret from Welcome to take that session's place.
    Hello {
        name: String,
        card: u64,
        pass: [u8; 32],
        resume: [u8; 32],
    },
    Join {
        channel: String,
    },
    Leave,
    Chat {
        text: String,
    },
    Mute {
        muted: bool,
    },
    Deafen {
        deafened: bool,
    },
    /// A token the server echoes. It is not a voice packet.
    Ping {
        token: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerMsg {
    Welcome {
        user_id: u32,
        /// Known only to this client. Present it on the next hello to replace
        /// this session instead of sitting beside it.
        resume: [u8; 32],
    },
    Reject {
        reason: String,
    },
    Joined {
        channel: String,
        members: Vec<Member>,
        rooms: Vec<String>,
    },
    Left,
    PeerJoined {
        user_id: u32,
        name: String,
        card: u64,
    },
    PeerLeft {
        user_id: u32,
    },
    Chat {
        user_id: u32,
        text: String,
    },
    Muted {
        user_id: u32,
        muted: bool,
    },
    /// The token from `Ping`, unchanged.
    Pong {
        token: u64,
    },
    /// A line from the channel, sent when someone joins. `mine` is that person's own earlier line.
    Past {
        name: String,
        text: String,
        mine: bool,
    },
}

/// A display name: trimmed, no control characters, at most 32 characters and
/// [`NAME_BYTES`] bytes.
pub fn clean_name(value: &str) -> Result<String, ProtoError> {
    let trimmed = value.trim();
    let chars = trimmed.chars().count();
    if trimmed.is_empty()
        || chars > MAX_NAME
        || trimmed.len() > NAME_BYTES
        || trimmed.chars().any(|c| c.is_control())
    {
        return Err(ProtoError::Invalid);
    }
    Ok(trimmed.to_string())
}

pub fn clean_label(value: &str, max: usize) -> Result<String, ProtoError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > max || trimmed.chars().any(|c| c.is_control()) {
        return Err(ProtoError::Invalid);
    }
    Ok(trimmed.to_string())
}

pub fn normalize_client(msg: ClientMsg) -> Result<ClientMsg, ProtoError> {
    Ok(match msg {
        ClientMsg::Hello {
            name,
            card,
            pass,
            resume,
        } => ClientMsg::Hello {
            name: clean_name(&name)?,
            card,
            pass,
            resume,
        },
        ClientMsg::Join { channel } => ClientMsg::Join {
            channel: clean_label(&channel, MAX_CHANNEL)?,
        },
        ClientMsg::Chat { text } => ClientMsg::Chat {
            text: clean_label(&text, MAX_CHAT)?,
        },
        other => other,
    })
}

pub fn normalize_server(msg: ServerMsg) -> Result<ServerMsg, ProtoError> {
    Ok(match msg {
        ServerMsg::Reject { reason } => ServerMsg::Reject {
            reason: clean_label(&reason, MAX_CHAT)?,
        },
        ServerMsg::Joined {
            channel,
            members,
            rooms,
        } => ServerMsg::Joined {
            channel: clean_label(&channel, MAX_CHANNEL)?,
            members,
            rooms: rooms
                .into_iter()
                .map(|room| clean_label(&room, MAX_CHANNEL))
                .collect::<Result<Vec<_>, _>>()?,
        },
        ServerMsg::PeerJoined {
            user_id,
            name,
            card,
        } => ServerMsg::PeerJoined {
            user_id,
            name: clean_name(&name)?,
            card,
        },
        ServerMsg::Chat { user_id, text } => ServerMsg::Chat {
            user_id,
            text: clean_label(&text, MAX_CHAT)?,
        },
        ServerMsg::Past { name, text, mine } => ServerMsg::Past {
            name: clean_name(&name)?,
            text: clean_label(&text, MAX_CHAT)?,
            mine,
        },
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_a_name() {
        assert_eq!(clean_name("  Mara  ").unwrap(), "Mara");
        let cjk = "你".repeat(20);
        assert_eq!(clean_name(&cjk).unwrap(), cjk);
        assert_eq!(
            clean_name(&"你".repeat(MAX_NAME + 1)).unwrap_err(),
            ProtoError::Invalid
        );
    }

    #[test]
    fn rejects_empty_control_and_long_labels() {
        assert_eq!(
            clean_label("   ", MAX_NAME).unwrap_err(),
            ProtoError::Invalid
        );
        assert_eq!(clean_label("", MAX_NAME).unwrap_err(), ProtoError::Invalid);
        assert_eq!(
            clean_label("a\nb", MAX_NAME).unwrap_err(),
            ProtoError::Invalid
        );
        let long = "a".repeat(MAX_NAME + 1);
        assert_eq!(
            clean_label(&long, MAX_NAME).unwrap_err(),
            ProtoError::Invalid
        );
        assert!(clean_label(&"a".repeat(MAX_NAME), MAX_NAME).is_ok());
        assert!(clean_label(&"a".repeat(MAX_CHAT), MAX_CHAT).is_ok());
        assert_eq!(
            clean_label(&"a".repeat(MAX_CHAT + 1), MAX_CHAT).unwrap_err(),
            ProtoError::Invalid
        );
    }

    #[test]
    fn hello_normalizes_the_stored_name() {
        let msg = normalize_client(ClientMsg::Hello {
            name: "  Kit ".into(),
            card: 7,
            pass: [1; 32],
            resume: [0; 32],
        })
        .unwrap();
        assert_eq!(
            msg,
            ClientMsg::Hello {
                name: "Kit".into(),
                card: 7,
                pass: [1; 32],
                resume: [0; 32],
            }
        );
    }
}
