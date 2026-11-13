mod hub;
mod identity;
mod link;
mod server;
mod session;
mod sums;

#[cfg(test)]
mod net_test;

pub use identity::{client_config, Door, Identity};
pub use link::{connect, Event, Link, LinkError, Live, Peer, VoicePipe};
pub use server::Server;
pub use sums::{sign_sums, verify_sums};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("{0}")]
    Message(String),
}
