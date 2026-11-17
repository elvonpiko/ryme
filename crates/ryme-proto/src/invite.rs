use crate::ProtoError;

pub const DEFAULT_PORT: u16 = 1542;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invite {
    pub host: String,
    pub port: u16,
    /// Certificate pin. This answers which server it is.
    pub fingerprint: [u8; 32],
    /// Admission token. This answers whether the caller may enter.
    pub pass: [u8; 32],
}

/// The line the server prints. Port 1542 is left off. The fragment stays in the
/// browser, so ryme.chat never receives the pin or the token.
pub fn format_invite(host: &str, port: u16, fingerprint: &[u8; 32], pass: &[u8; 32]) -> String {
    let host = host.trim_matches(|c| c == '[' || c == ']');
    let place = if host.contains(':') {
        if port == DEFAULT_PORT {
            format!("[{host}]")
        } else {
            format!("[{host}]:{port}")
        }
    } else if port == DEFAULT_PORT {
        host.to_string()
    } else {
        format!("{host}:{port}")
    };
    format!("https://ryme.chat/#{place}/{}", encode_token(fingerprint, pass))
}

pub fn parse_invite(raw: &str) -> Result<Invite, ProtoError> {
    let raw = raw.trim();
    let body = raw
        .strip_prefix("https://ryme.chat/#")
        .or_else(|| raw.strip_prefix("https://www.ryme.chat/#"))
        .or_else(|| raw.strip_prefix("ryme://"))
        .ok_or(ProtoError::Invalid)?;
    let (place, token) = body.split_once('/').ok_or(ProtoError::Invalid)?;
    if token.is_empty() || token.contains('/') || token.contains('#') {
        return Err(ProtoError::Invalid);
    }
    let (host, port) = split_place(place)?;
    let (fingerprint, pass) = decode_token(token)?;
    Ok(Invite {
        host,
        port,
        fingerprint,
        pass,
    })
}

fn split_place(place: &str) -> Result<(String, u16), ProtoError> {
    if place.chars().any(|c| c.is_whitespace()) {
        return Err(ProtoError::Invalid);
    }
    if let Some(rest) = place.strip_prefix('[') {
        let (host, after) = rest.split_once(']').ok_or(ProtoError::Invalid)?;
        if host.is_empty() || !host.contains(':') {
            return Err(ProtoError::Invalid);
        }
        let port = if after.is_empty() {
            DEFAULT_PORT
        } else {
            parse_port(after.strip_prefix(':').ok_or(ProtoError::Invalid)?)?
        };
        return Ok((host.to_string(), port));
    }
    if place.is_empty() || place.contains(':') && place.matches(':').count() > 1 {
        return Err(ProtoError::Invalid);
    }
    if let Some((host, port)) = place.rsplit_once(':') {
        if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) {
            if host.is_empty() {
                return Err(ProtoError::Invalid);
            }
            return Ok((host.to_string(), parse_port(port)?));
        }
    }
    Ok((place.to_string(), DEFAULT_PORT))
}

fn parse_port(text: &str) -> Result<u16, ProtoError> {
    let port: u16 = text.parse().map_err(|_| ProtoError::Invalid)?;
    if port == 0 {
        Err(ProtoError::Invalid)
    } else {
        Ok(port)
    }
}

fn encode_token(fingerprint: &[u8; 32], pass: &[u8; 32]) -> String {
    let mut raw = [0u8; 64];
    raw[..32].copy_from_slice(fingerprint);
    raw[32..].copy_from_slice(pass);
    const ALPHA: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(86);
    let mut i = 0;
    while i + 3 <= 64 {
        let n = ((raw[i] as u32) << 16) | ((raw[i + 1] as u32) << 8) | raw[i + 2] as u32;
        out.push(ALPHA[((n >> 18) & 63) as usize] as char);
        out.push(ALPHA[((n >> 12) & 63) as usize] as char);
        out.push(ALPHA[((n >> 6) & 63) as usize] as char);
        out.push(ALPHA[(n & 63) as usize] as char);
        i += 3;
    }
    let n = (raw[63] as u32) << 16;
    out.push(ALPHA[((n >> 18) & 63) as usize] as char);
    out.push(ALPHA[((n >> 12) & 63) as usize] as char);
    out
}

fn decode_token(token: &str) -> Result<([u8; 32], [u8; 32]), ProtoError> {
    let bytes = token.as_bytes();
    if bytes.len() != 86 {
        return Err(ProtoError::Invalid);
    }
    let mut val = [0u8; 86];
    for (i, byte) in bytes.iter().copied().enumerate() {
        val[i] = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return Err(ProtoError::Invalid),
        };
    }
    let mut raw = [0u8; 64];
    let mut o = 0;
    let mut i = 0;
    while i + 4 <= 84 {
        let n = ((val[i] as u32) << 18)
            | ((val[i + 1] as u32) << 12)
            | ((val[i + 2] as u32) << 6)
            | val[i + 3] as u32;
        raw[o] = (n >> 16) as u8;
        raw[o + 1] = (n >> 8) as u8;
        raw[o + 2] = n as u8;
        o += 3;
        i += 4;
    }
    let n = ((val[84] as u32) << 18) | ((val[85] as u32) << 12);
    raw[63] = (n >> 16) as u8;
    let mut fingerprint = [0u8; 32];
    let mut pass = [0u8; 32];
    fingerprint.copy_from_slice(&raw[..32]);
    pass.copy_from_slice(&raw[32..]);
    Ok((fingerprint, pass))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_default_port_roundtrips_without_the_port() {
        let url = format_invite("voice.example", DEFAULT_PORT, &[0xab; 32], &[0xcd; 32]);
        assert!(url.starts_with("https://ryme.chat/#voice.example/"));
        assert!(!url.contains(":1542"));
        let invite = parse_invite(&url).unwrap();
        assert_eq!(invite.host, "voice.example");
        assert_eq!(invite.port, DEFAULT_PORT);
        assert_eq!(invite.fingerprint, [0xab; 32]);
        assert_eq!(invite.pass, [0xcd; 32]);
        let opened = url.replacen("https://ryme.chat/#", "ryme://", 1);
        assert_eq!(parse_invite(&opened).unwrap(), invite);
    }

    #[test]
    fn another_port_and_ipv6_stay_in_the_link() {
        let url = format_invite("203.0.113.4", 443, &[1; 32], &[2; 32]);
        assert!(url.contains("203.0.113.4:443/"));
        assert_eq!(parse_invite(&url).unwrap().port, 443);

        let v6 = format_invite("2001:db8::1", DEFAULT_PORT, &[3; 32], &[4; 32]);
        assert!(v6.contains("#[2001:db8::1]/"));
        let invite = parse_invite(&v6).unwrap();
        assert_eq!(invite.host, "2001:db8::1");
        assert_eq!(invite.port, DEFAULT_PORT);

        let odd = format_invite("2001:db8::1", 9, &[3; 32], &[4; 32]);
        assert_eq!(parse_invite(&odd).unwrap().port, 9);
    }

    #[test]
    fn www_and_surrounding_space_are_the_same_invite() {
        let url = format!("  {}  ", format_invite("h", 1542, &[9; 32], &[8; 32]));
        let web = url.trim().replacen("https://", "https://www.", 1);
        assert_eq!(parse_invite(&url).unwrap(), parse_invite(&web).unwrap());
    }

    #[test]
    fn rejects_a_bad_link() {
        assert!(parse_invite("https://example").is_err());
        assert!(parse_invite("ryme://voice.example").is_err());
        assert!(parse_invite("https://ryme.chat/#voice.example/").is_err());
        assert!(parse_invite("https://ryme.chat/#voice.example/aaaa").is_err());
        assert!(parse_invite("https://ryme.chat/#:1542/aaaa").is_err());
        assert!(parse_invite("https://ryme.chat/#bad host/aaaa").is_err());
        let old = format!(
            "ryme://127.0.0.1:1542/#{}.{}",
            "ab".repeat(32),
            "cd".repeat(32)
        );
        assert!(parse_invite(&old).is_err());
    }
}
