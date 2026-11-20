/// The published release. The crate version stays 0.1.0; the client compares
/// this tag with GitHub.
pub const RELEASE: &str = "0.1.44";

mod hub;
mod identity;
mod link;
mod queue;
mod server;
mod session;
mod sums;

#[cfg(test)]
mod net_test;

pub use identity::{client_config, Door, Identity};
pub use link::{
    connect, connect_room, probe_invite, Event, HandshakeReport, Link, LinkError, Live, Peer,
    VoicePipe, VoiceQueue,
};
pub use server::Server;
pub use sums::{sign_sums, verify_sums};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("{0}")]
    Message(String),
}
