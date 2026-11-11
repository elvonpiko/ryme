mod hub;
mod identity;
mod link;
mod server;
mod session;

#[cfg(test)]
mod net_test;

pub use identity::{client_config, Identity};
pub use link::{connect, Event, Link, LinkError, Live, Peer, VoicePipe};
pub use server::Server;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("{0}")]
    Message(String),
}
