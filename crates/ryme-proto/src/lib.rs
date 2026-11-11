//! Two paths share a connection and nothing else.
//!
//! Control frames (hello, join, chat) are length-prefixed and reliable.
//! Voice is one datagram: a 14-byte header plus an Opus payload. The server
//! stamps the speaker and forwards the bytes. It does not decode them.

mod control;
mod frame;
mod invite;
mod voice;

pub use control::{ClientMsg, Member, ServerMsg, MAX_CHANNEL, MAX_CHAT, MAX_NAME};
pub use frame::{
    decode_client_frame, decode_server_frame, encode_client, encode_server, MAX_FRAME,
};
pub use invite::{parse_invite, Invite};
pub use voice::{
    decode_voice, encode_voice, stamp_speaker, VoicePacket, MAX_VOICE_PAYLOAD, VOICE_ENCRYPTED,
    VOICE_HEADER, VOICE_VERSION,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtoError {
    #[error("truncated")]
    Truncated,
    #[error("too large")]
    TooLarge,
    #[error("bad version")]
    Version,
    #[error("invalid")]
    Invalid,
    #[error("codec")]
    Codec,
}
