//! The QUIC connection is encrypted to this process. This process is where
//! that encryption ends: it forwards the encoded audio and does not mix it.
//! Participants are not end-to-end encrypted to each other.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::identity::{self, Door, Identity};
use crate::session;
use crate::ServerError;

pub struct Server {
    endpoint: quinn::Endpoint,
    tcp: tokio::net::TcpListener,
    tls: Arc<rustls::ServerConfig>,
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
        let port = endpoint
            .local_addr()
            .map_err(|err| ServerError::Message(err.to_string()))?
            .port();
        let tcp = listen_tcp(SocketAddr::new(addr.ip(), port))?;
        let tls = Arc::new(tls_config(&identity)?);
        Ok(Self {
            endpoint,
            tcp,
            tls,
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
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::clone(&self.tls));
        loop {
            tokio::select! {
                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
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
                accepted = self.tcp.accept() => {
                    let Ok((stream, peer)) = accepted else { continue };
                    if !session::claim_connection() {
                        tracing::info!(%peer, "connection refused, server is full");
                        continue;
                    }
                    let world = Arc::clone(&self.world);
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        let handshake = async {
                            stream.set_nodelay(true).map_err(|_| ())?;
                            match tokio::time::timeout(Duration::from_secs(5), acceptor.accept(stream)).await {
                                Ok(Ok(tls)) => Ok(tls),
                                _ => Err(()),
                            }
                        };
                        match handshake.await {
                            Ok(tls) => session::run_tcp(peer, tls, world).await,
                            Err(()) => tracing::info!(%peer, "tcp handshake rejected"),
                        }
                        session::release_connection();
                    });
                }
            }
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

fn listen_tcp(addr: SocketAddr) -> Result<tokio::net::TcpListener, ServerError> {
    let listener = std::net::TcpListener::bind(addr)
        .map_err(|err| ServerError::Message(err.to_string()))?;
    listener
        .set_nonblocking(true)
        .map_err(|err| ServerError::Message(err.to_string()))?;
    tokio::net::TcpListener::from_std(listener).map_err(|err| ServerError::Message(err.to_string()))
}

/// Same certificate as the QUIC listener. TLS 1.3 only, matching the client pin.
fn tls_config(identity: &Identity) -> Result<rustls::ServerConfig, ServerError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = CertificateDer::from(identity.cert_der.clone());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.key_der.clone()));
    rustls::ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|err| ServerError::Message(err.to_string()))?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map_err(|err| ServerError::Message(err.to_string()))
}
