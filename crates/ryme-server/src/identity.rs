use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error, SignatureScheme};
use sha2::{Digest, Sha256};

use crate::ServerError;

pub struct Identity {
    pub cert_der: Vec<u8>,
    pub key_der: Vec<u8>,
    pub fingerprint: [u8; 32],
}

impl Identity {
    pub fn generate() -> Result<Self, ServerError> {
        let key_pair =
            rcgen::KeyPair::generate().map_err(|err| ServerError::Message(err.to_string()))?;
        let params = rcgen::CertificateParams::new(vec!["ryme".to_string()])
            .map_err(|err| ServerError::Message(err.to_string()))?;
        let cert = params
            .self_signed(&key_pair)
            .map_err(|err| ServerError::Message(err.to_string()))?;
        let cert_der = cert.der().to_vec();
        let key_der = key_pair.serialize_der();
        let fingerprint = fingerprint_of(&cert_der);
        Ok(Self {
            cert_der,
            key_der,
            fingerprint,
        })
    }
}

pub fn fingerprint_of(cert_der: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(cert_der);
    let mut fingerprint = [0u8; 32];
    fingerprint.copy_from_slice(digest.as_ref());
    fingerprint
}

pub fn invite_url(host: &str, port: u16, fingerprint: &[u8; 32]) -> String {
    format!("ryme://{host}:{port}/#{}", hex_encode(fingerprint))
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[derive(Debug)]
pub struct FingerprintVerifier {
    fingerprint: [u8; 32],
    provider: Arc<CryptoProvider>,
}

impl FingerprintVerifier {
    pub fn new(fingerprint: [u8; 32]) -> Self {
        Self {
            fingerprint,
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        }
    }
}

impl ServerCertVerifier for FingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        if fingerprint_of(end_entity.as_ref()) == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(Error::General("ryme fingerprint mismatch".into()))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(Error::General("ryme does not use tls 1.2".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub fn client_config(fingerprint: [u8; 32]) -> Result<quinn::ClientConfig, ServerError> {
    let provider = rustls::crypto::ring::default_provider();
    let crypto = rustls::ClientConfig::builder_with_provider(provider.into())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|err| ServerError::Message(err.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(FingerprintVerifier::new(fingerprint)))
        .with_no_client_auth();
    let quic = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .map_err(|err| ServerError::Message(err.to_string()))?;
    let mut client = quinn::ClientConfig::new(Arc::new(quic));
    client.transport_config(Arc::new(datagram_transport()));
    Ok(client)
}

pub fn datagram_transport() -> quinn::TransportConfig {
    let mut transport = quinn::TransportConfig::default();
    transport.datagram_receive_buffer_size(Some(1024 * 1024));
    transport.max_idle_timeout(Some(std::time::Duration::from_secs(60).try_into().unwrap()));
    transport
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::ServerName;
    use ryme_proto::parse_invite;

    #[test]
    fn generated_invites_pin_that_certificate() {
        let first = Identity::generate().unwrap();
        let second = Identity::generate().unwrap();
        assert_ne!(first.fingerprint, second.fingerprint);

        let invite = invite_url("127.0.0.1", 7777, &first.fingerprint);
        let parsed = parse_invite(&invite).unwrap();
        assert_eq!(parsed.host, "127.0.0.1");
        assert_eq!(parsed.port, 7777);
        assert_eq!(parsed.fingerprint, first.fingerprint);
    }

    #[test]
    fn verifier_rejects_a_different_certificate() {
        let identity = Identity::generate().unwrap();
        let verifier = FingerprintVerifier::new(identity.fingerprint);
        let name = ServerName::try_from("ryme").unwrap();
        let now = UnixTime::now();
        let own = CertificateDer::from(identity.cert_der);
        assert!(verifier
            .verify_server_cert(&own, &[], &name, &[], now)
            .is_ok());

        let other = Identity::generate().unwrap();
        let other_der = CertificateDer::from(other.cert_der);
        assert!(verifier
            .verify_server_cert(&other_der, &[], &name, &[], now)
            .is_err());
    }
}
