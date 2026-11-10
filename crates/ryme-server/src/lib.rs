mod hub;
mod identity;
mod server;
mod session;

#[cfg(test)]
mod net_test;

pub use identity::client_config;
pub use server::Server;

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("{0}")]
    Message(String),
}
