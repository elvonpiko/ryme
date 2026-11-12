use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error, SignatureScheme};
use sha2::{Digest, Sha256};

use crate::ServerError;

pub struct Identity {
    pub cert_der: Vec<u8>,
    pub key_der: Vec<u8>,
    pub fingerprint: [u8; 32],
}

const CERT_FILE: &str = "cert.der";
const KEY_FILE: &str = "key.der";

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

    /// Load the host certificate, or create it the first time this directory is used.
    pub fn load_or_create(dir: &Path) -> Result<Self, ServerError> {
        match (file_exists(dir, CERT_FILE), file_exists(dir, KEY_FILE)) {
            (false, false) => {
                let identity = Self::generate()?;
                identity.store(dir)?;
                Ok(identity)
            }
            (true, true) => Self::load(dir),
            _ => Err(ServerError::Message(
                "identity is incomplete; keep cert.der and key.der together".into(),
            )),
        }
    }

    /// Replace the host certificate. Links pinned to the previous one stop connecting.
    pub fn rotate(dir: &Path) -> Result<Self, ServerError> {
        let identity = Self::generate()?;
        identity.store(dir)?;
        Ok(identity)
    }

    fn load(dir: &Path) -> Result<Self, ServerError> {
        let cert_der = read_file(&dir.join(CERT_FILE))?;
        let key_der = read_file(&dir.join(KEY_FILE))?;
        check_pair(&cert_der, &key_der)?;
        Ok(Self {
            cert_der: cert_der.clone(),
            key_der,
            fingerprint: fingerprint_of(&cert_der),
        })
    }

    fn store(&self, dir: &Path) -> Result<(), ServerError> {
        fs::create_dir_all(dir).map_err(io_err)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(io_err)?;
        }
        write_private(&dir.join(KEY_FILE), &self.key_der)?;
        write_private(&dir.join(CERT_FILE), &self.cert_der)?;
        Ok(())
    }
}

fn file_exists(dir: &Path, name: &str) -> bool {
    dir.join(name).is_file()
}

fn read_file(path: &Path) -> Result<Vec<u8>, ServerError> {
    fs::read(path).map_err(io_err)
}

fn check_pair(cert_der: &[u8], key_der: &[u8]) -> Result<(), ServerError> {
    if cert_der.is_empty() || key_der.is_empty() {
        return Err(ServerError::Message("identity is empty".into()));
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = CertificateDer::from(cert_der.to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der.to_vec()));
    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .map(|_| ())
        .map_err(|err| ServerError::Message(format!("identity: {err}")))
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<(), ServerError> {
    let tmp = temp_path(path);
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp).map_err(io_err)?;
    file.write_all(bytes).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    fs::rename(&tmp, path).map_err(io_err)?;
    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

fn io_err(err: std::io::Error) -> ServerError {
    ServerError::Message(err.to_string())
}

pub fn fingerprint_of(cert_der: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(cert_der);
    let mut fingerprint = [0u8; 32];
    fingerprint.copy_from_slice(digest.as_ref());
    fingerprint
}

pub fn invite_url(host: &str, port: u16, fingerprint: &[u8; 32]) -> String {
    let host = host.trim_matches(|c| c == '[' || c == ']');
    if host.contains(':') {
        format!("ryme://[{host}]:{port}/#{}", hex_encode(fingerprint))
    } else {
        format!("ryme://{host}:{port}/#{}", hex_encode(fingerprint))
    }
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
    // Mute, a left channel, and a missing microphone send no voice. A keepalive
    // inside the idle window is what keeps that session connected. A process
    // that died without a close still leaves the lobby, but only for this long.
    transport.keep_alive_interval(Some(std::time::Duration::from_secs(4)));
    transport.max_idle_timeout(Some(std::time::Duration::from_secs(15).try_into().unwrap()));
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

    #[test]
    fn a_restart_reuses_the_certificate() {
        let dir = scratch("keep");
        let first = Identity::load_or_create(&dir).unwrap();
        let second = Identity::load_or_create(&dir).unwrap();
        assert_eq!(first.fingerprint, second.fingerprint);
        assert_eq!(fs::read(dir.join("cert.der")).unwrap(), first.cert_der);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let key_mode = fs::metadata(dir.join("key.der"))
                .unwrap()
                .permissions()
                .mode();
            let dir_mode = fs::metadata(&dir).unwrap().permissions().mode();
            assert_eq!(key_mode & 0o777, 0o600);
            assert_eq!(dir_mode & 0o777, 0o700);
        }

        let rotated = Identity::rotate(&dir).unwrap();
        assert_ne!(rotated.fingerprint, first.fingerprint);
        let loaded = Identity::load_or_create(&dir).unwrap();
        assert_eq!(loaded.fingerprint, rotated.fingerprint);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_partial_identity_is_kept_and_refused() {
        let dir = scratch("partial");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("cert.der"), b"leftover").unwrap();
        match Identity::load_or_create(&dir) {
            Err(err) => assert!(err.to_string().contains("incomplete")),
            Ok(_) => panic!("partial identity was accepted"),
        }
        assert!(dir.join("cert.der").is_file());
        assert!(!dir.join("key.der").is_file());
        let _ = fs::remove_dir_all(&dir);
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("ryme-id-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }
}
