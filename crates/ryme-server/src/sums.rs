//! Signature over SHA256SUMS. The checksum file and the signature travel
//! together, so the signature is what makes the checksum a publisher's.

use ring::signature::{Ed25519KeyPair, KeyPair, UnparsedPublicKey, ED25519};

/// Public half of the release signing key. The private PKCS#8 document is the
/// `RYME_UPDATE_PKCS8` secret and is not stored in this repository.
pub const UPDATE_PUBLIC: [u8; 32] = [
    0x42, 0x8c, 0xd7, 0x50, 0xcc, 0xd6, 0xbb, 0xc3, 0x05, 0x4a, 0x51, 0x2f, 0xdf, 0x98, 0x0d, 0x7e,
    0x74, 0xd2, 0xef, 0xc7, 0x1d, 0x3f, 0xa2, 0xbe, 0x3f, 0x23, 0xe1, 0xe3, 0x39, 0xa3, 0xff, 0x22,
];

pub fn verify_sums(message: &[u8], signature: &[u8]) -> bool {
    let key = UnparsedPublicKey::new(&ED25519, &UPDATE_PUBLIC);
    key.verify(message, signature).is_ok()
}

pub fn sign_sums(pkcs8: &[u8], message: &[u8]) -> Result<Vec<u8>, String> {
    // OpenSSL writes a v1 document: the seed, and no public key. ring's strict
    // parser accepts only v2. The public key embedded in the client is the check.
    let key = Ed25519KeyPair::from_pkcs8_maybe_unchecked(pkcs8)
        .map_err(|_| "bad signing key".to_string())?;
    if key.public_key().as_ref() != UPDATE_PUBLIC {
        return Err("bad signing key".to_string());
    }
    Ok(key.sign(message).as_ref().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::KeyPair;

    #[test]
    fn a_signature_from_its_own_key_verifies_and_a_stranger_does_not() {
        let doc = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let key = Ed25519KeyPair::from_pkcs8(doc.as_ref()).unwrap();
        let message = b"abc  ryme\n";
        let signature = key.sign(message);
        let public = key.public_key().as_ref();
        let own = UnparsedPublicKey::new(&ED25519, public);
        assert!(own.verify(message, signature.as_ref()).is_ok());
        assert!(!verify_sums(message, signature.as_ref()));
        assert!(!verify_sums(message, &[0; 64]));
        assert!(sign_sums(doc.as_ref(), message).is_err());
    }

    #[test]
    fn the_configured_release_key_matches_the_public_key() {
        if std::env::var("RYME_SIGN_CHECK").ok().as_deref() != Some("1") {
            return;
        }
        let Some(home) = std::env::var_os("HOME") else {
            return;
        };
        let path = std::path::PathBuf::from(home).join(".config/ryme/update.pkcs8");
        let Ok(pkcs8) = std::fs::read(path) else {
            return;
        };
        let message = b"abc  ryme\n";
        let signature = sign_sums(&pkcs8, message).expect("release key");
        assert!(verify_sums(message, &signature));
    }
}
