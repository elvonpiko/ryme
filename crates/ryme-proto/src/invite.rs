use crate::ProtoError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invite {
    pub host: String,
    pub port: u16,
    pub fingerprint: [u8; 32],
}

pub fn parse_invite(raw: &str) -> Result<Invite, ProtoError> {
    let rest = raw
        .trim()
        .strip_prefix("ryme://")
        .ok_or(ProtoError::Invalid)?;
    let (addr, hex_fp) = rest.split_once("/#").ok_or(ProtoError::Invalid)?;
    let (host, port) = addr.rsplit_once(':').ok_or(ProtoError::Invalid)?;
    let host = host.trim_matches(|c| c == '[' || c == ']');
    if host.is_empty() || host.chars().any(|c| c.is_whitespace() || c == '/') {
        return Err(ProtoError::Invalid);
    }
    let port: u16 = port.parse().map_err(|_| ProtoError::Invalid)?;
    if port == 0 {
        return Err(ProtoError::Invalid);
    }
    Ok(Invite {
        host: host.to_string(),
        port,
        fingerprint: decode_hex(hex_fp)?,
    })
}

fn decode_hex(s: &str) -> Result<[u8; 32], ProtoError> {
    let bytes = s.as_bytes();
    if bytes.len() != 64 {
        return Err(ProtoError::Invalid);
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let hi = nibble(bytes[i * 2])?;
        let lo = nibble(bytes[i * 2 + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn nibble(b: u8) -> Result<u8, ProtoError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(ProtoError::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(host: &str) -> String {
        format!("ryme://{host}:7777/#{}", "ab".repeat(32))
    }

    #[test]
    fn parses_a_pinned_invite() {
        let invite = parse_invite(&sample("127.0.0.1")).unwrap();
        assert_eq!(invite.host, "127.0.0.1");
        assert_eq!(invite.port, 7777);
        assert_eq!(invite.fingerprint, [0xab; 32]);
    }

    #[test]
    fn accepts_upper_hex_and_surrounding_space() {
        let raw = format!("  ryme://voice.example:443/#{}  ", "AB".repeat(32));
        let invite = parse_invite(&raw).unwrap();
        assert_eq!(invite.host, "voice.example");
        assert_eq!(invite.port, 443);
        assert_eq!(invite.fingerprint, [0xab; 32]);
    }

    #[test]
    fn parses_a_bracketed_ipv6_host() {
        let invite = parse_invite(&sample("[2001:db8::1]")).unwrap();
        assert_eq!(invite.host, "2001:db8::1");
        assert_eq!(invite.port, 7777);
    }

    #[test]
    fn rejects_bad_invites() {
        assert!(parse_invite("https://example").is_err());
        assert!(parse_invite("ryme://127.0.0.1:7777").is_err());
        assert!(parse_invite("ryme://127.0.0.1:7777/#abcd").is_err());
        assert!(parse_invite("ryme://127.0.0.1:0/#aabb").is_err());
        assert!(parse_invite(&format!("ryme://:7777/#{}", "aa".repeat(32))).is_err());
        assert!(parse_invite(&format!("ryme://bad host:1/#{}", "aa".repeat(32))).is_err());
    }
}
