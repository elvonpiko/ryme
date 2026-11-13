//! The QUIC connection is encrypted to this process. This process is where
//! that encryption ends: it forwards the encoded audio and does not mix it.
//! Participants are not end-to-end encrypted to each other.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::identity::{self, Door, Identity};
use crate::session;
use crate::ServerError;

pub struct Server {
    endpoint: quinn::Endpoint,
    world: Arc<std::sync::Mutex<session::World>>,
    fingerprint: [u8; 32],
    pass: [u8; 32],
    host_key: [u8; 32],
    host_addr: Option<SocketAddr>,
}

impl Server {
    pub fn bind(addr: SocketAddr) -> Result<Self, ServerError> {
        Self::bind_identity(addr, Identity::generate()?, Door::generate()?, None, None)
    }

    pub fn bind_identity(
        addr: SocketAddr,
        identity: Identity,
        door: Door,
        host_addr: Option<SocketAddr>,
        data: Option<PathBuf>,
    ) -> Result<Self, ServerError> {
        let endpoint = listen(addr, &identity)?;
        Ok(Self {
            endpoint,
            world: session::world(door.pass, data),
            fingerprint: identity.fingerprint,
            pass: door.pass,
            host_key: door.host,
            host_addr,
        })
    }

    pub fn port(&self) -> u16 {
        self.endpoint
            .local_addr()
            .map(|addr| addr.port())
            .unwrap_or(0)
    }

    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    pub fn pass(&self) -> [u8; 32] {
        self.pass
    }

    pub fn invite(&self, host: &str) -> String {
        self.invite_on(host, self.port())
    }

    pub fn invite_on(&self, host: &str, port: u16) -> String {
        identity::invite_url(host, port, &self.fingerprint, &self.pass)
    }

    pub async fn serve(&self) {
        if let Some(addr) = self.host_addr {
            let world = Arc::clone(&self.world);
            let key = self.host_key;
            tokio::spawn(async move {
                session::listen_host(addr, key, world).await;
            });
        }
        while let Some(incoming) = self.endpoint.accept().await {
            let peer = incoming.remote_address();
            if !session::claim_connection() {
                tracing::info!(%peer, "connection refused, server is full");
                continue;
            }
            let world = Arc::clone(&self.world);
            tokio::spawn(async move {
                let result = incoming.await;
                let Ok(conn) = result else {
                    session::release_connection();
                    tracing::info!(%peer, "handshake rejected");
                    return;
                };
                session::run(conn, world).await;
                session::release_connection();
            });
        }
    }

    /// Add a room the host is willing to run. Lobby already exists.
    pub fn allow(&self, name: &str) {
        session::allow(&self.world, name);
    }
}

fn listen(addr: SocketAddr, identity: &Identity) -> Result<quinn::Endpoint, ServerError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = CertificateDer::from(identity.cert_der.clone());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.key_der.clone()));
    let crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(|err| ServerError::Message(err.to_string()))?;
    let quic = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
        .map_err(|err| ServerError::Message(err.to_string()))?;
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(quic));
    server.transport_config(Arc::new(identity::datagram_transport()));
    quinn::Endpoint::server(server, addr).map_err(|err| ServerError::Message(err.to_string()))
}
