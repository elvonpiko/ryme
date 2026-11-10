use std::net::SocketAddr;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::identity::{self, Identity};
use crate::session;
use crate::ServerError;

pub struct Server {
    endpoint: quinn::Endpoint,
    world: Arc<std::sync::Mutex<session::World>>,
    fingerprint: [u8; 32],
}

impl Server {
    pub fn bind(addr: SocketAddr) -> Result<Self, ServerError> {
        let identity = Identity::generate()?;
        let endpoint = listen(addr, &identity)?;
        Ok(Self {
            endpoint,
            world: session::world(),
            fingerprint: identity.fingerprint,
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

    pub fn invite(&self, host: &str) -> String {
        identity::invite_url(host, self.port(), &self.fingerprint)
    }

    pub async fn serve(&self) {
        while let Some(incoming) = self.endpoint.accept().await {
            let world = Arc::clone(&self.world);
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else {
                    return;
                };
                session::run(conn, world).await;
            });
        }
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
