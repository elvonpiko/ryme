use serde::{Deserialize, Serialize};

use crate::ProtoError;

pub const MAX_NAME: usize = 32;
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
    Hello { name: String, card: u64 },
    Join { channel: String },
    Leave,
    Chat { text: String },
    Mute { muted: bool },
    Deafen { deafened: bool },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerMsg {
    Welcome {
        user_id: u32,
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
        ClientMsg::Hello { name, card } => ClientMsg::Hello {
            name: clean_label(&name, MAX_NAME)?,
            card,
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
            name: clean_label(&name, MAX_NAME)?,
            card,
        },
        ServerMsg::Chat { user_id, text } => ServerMsg::Chat {
            user_id,
            text: clean_label(&text, MAX_CHAT)?,
        },
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_a_name() {
        assert_eq!(clean_label("  Mara  ", MAX_NAME).unwrap(), "Mara");
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
        })
        .unwrap();
        assert_eq!(
            msg,
            ClientMsg::Hello {
                name: "Kit".into(),
                card: 7
            }
        );
    }
}
