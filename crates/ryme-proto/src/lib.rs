//! Two paths share a connection and nothing else.
//!
//! Control frames (hello, join, chat) are length-prefixed and reliable.
//! Voice is one datagram: a 14-byte header plus an Opus payload. The server
//! stamps the speaker and forwards the bytes. It does not decode them.
//!
//! The QUIC connection is encrypted, and the server is where that encryption
//! ends. The server handles the encoded audio. Participants are not
//! end-to-end encrypted to each other.

mod control;
mod frame;
mod invite;
mod tcp;
mod voice;

pub use control::{ClientMsg, Member, ServerMsg, MAX_CHANNEL, MAX_CHAT, MAX_NAME, NAME_BYTES};
pub use frame::{
    decode_client_frame, decode_server_frame, encode_client, encode_server, take_client_frame,
    ClientFrame, MAX_FRAME,
};
pub use invite::{format_invite, parse_invite, Invite, DEFAULT_PORT};
pub use tcp::{decode_tagged, encode_tagged, TCP_CONTROL, TCP_MAX, TCP_VOICE};
pub use voice::{
    decode_probe, decode_voice, encode_probe, encode_voice, stamp_speaker, voice_ok, VoicePacket,
    MAX_VOICE_PAYLOAD, PROBE_MAGIC, VOICE_ENCRYPTED, VOICE_HEADER, VOICE_VERSION,
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
