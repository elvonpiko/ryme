use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use ryme_server::{Door, Identity, Server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .init();
    let data = PathBuf::from(std::env::var("RYME_DATA").unwrap_or_else(|_| "ryme-data".into()));
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--sign-sums") {
        let sums = args.get(1).cloned().ok_or_else(|| {
            anyhow::anyhow!("usage: ryme-server --sign-sums SHA256SUMS key.pkcs8")
        })?;
        let key_path = args.get(2).cloned().ok_or_else(|| {
            anyhow::anyhow!("usage: ryme-server --sign-sums SHA256SUMS key.pkcs8")
        })?;
        let key = std::fs::read(&key_path)?;
        let message = std::fs::read(&sums)?;
        let signature =
            ryme_server::sign_sums(&key, &message).map_err(|err| anyhow::anyhow!(err))?;
        let sig = format!("{sums}.sig");
        std::fs::write(&sig, hex_of(&signature))?;
        println!("{sig}");
        return Ok(());
    }
    match host_request(&args)? {
        Some(line) => {
            let reply = ask_host(&data, &line)?;
            println!("{reply}");
            if reply.starts_with("error ") {
                std::process::exit(1);
            }
            return Ok(());
        }
        None => {}
    }
    let bind = std::env::var("RYME_BIND").unwrap_or_else(|_| "0.0.0.0:1542".to_string());
    let addr: SocketAddr = bind.parse()?;
    let rotate = args.iter().any(|arg| arg == "--new-invite");
    if rotate {
        if let Ok(reply) = ask_host(&data, "rotate") {
            println!("{reply}");
            if reply.starts_with("error ") {
                std::process::exit(1);
            }
            return Ok(());
        }
    }
    let identity = Identity::load_or_create(&data)?;
    let door = if rotate {
        tracing::info!(
            "new invite, previous links no longer connect; the certificate is unchanged"
        );
        Door::rotate(&data)?
    } else {
        Door::load_or_create(&data)?
    };
    let host_addr = ryme_server::admin_addr(&data);
    let server = Server::bind_identity(addr, identity, door, Some(host_addr), Some(data.clone()))?;
    for name in channels_from(&data) {
        server.allow(&name);
    }
    let (host, port) = published(addr);
    tracing::info!(
        %addr,
        %host,
        port,
        path = %data.display(),
        release = ryme_server::RELEASE,
        "listening on udp and tcp"
    );
    println!("{}", server.invite_on(&host, port));
    server.remember_public(&host, port);
    server.serve().await;
    Ok(())
}

/// One host command from the process that has the data directory. The key
/// stays on this machine. `drop` ends a session. `ban` refuses that card
/// until `unban`. `lock` refuses new people; someone already in can reconnect.
fn host_request(args: &[String]) -> anyhow::Result<Option<String>> {
    if args.is_empty()
        || args
            .iter()
            .any(|arg| arg == "--new-invite" || arg == "--sign-sums")
    {
        return Ok(None);
    }
    let mut rest = args.iter();
    let Some(arg) = rest.next() else {
        return Ok(None);
    };
    let line = if arg == "--drop" {
        let id = rest
            .next()
            .ok_or_else(|| anyhow::anyhow!("usage: ryme-server --drop ID"))?;
        format!("drop {id}")
    } else if let Some(id) = arg.strip_prefix("--drop=") {
        format!("drop {id}")
    } else if arg == "--lock" {
        "lock".into()
    } else if arg == "--unlock" {
        "unlock".into()
    } else if arg == "--ban" {
        let card = rest
            .next()
            .ok_or_else(|| anyhow::anyhow!("usage: ryme-server --ban CARD"))?;
        format!("ban {card}")
    } else if let Some(card) = arg.strip_prefix("--ban=") {
        format!("ban {card}")
    } else if arg == "--unban" {
        let card = rest
            .next()
            .ok_or_else(|| anyhow::anyhow!("usage: ryme-server --unban CARD"))?;
        format!("unban {card}")
    } else if let Some(card) = arg.strip_prefix("--unban=") {
        format!("unban {card}")
    } else if arg == "--rotate" {
        "rotate".into()
    } else {
        return Err(anyhow::anyhow!(
            "unknown argument {arg}. host commands are --drop, --lock, --unlock, --ban, --unban, --rotate"
        ));
    };
    if rest.next().is_some() {
        return Err(anyhow::anyhow!("unexpected extra argument"));
    }
    Ok(Some(line))
}

fn ask_host(data: &Path, line: &str) -> anyhow::Result<String> {
    let bytes = std::fs::read(data.join("host.key"))?;
    let key: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("host key is unreadable"))?;
    let addr = ryme_server::admin_addr(data);
    let mut sock = std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(2))?;
    sock.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;
    use std::io::{Read, Write};
    sock.write_all(&key)?;
    sock.write_all(line.as_bytes())?;
    if !line.ends_with('\n') {
        sock.write_all(b"\n")?;
    }
    let mut reply = String::new();
    sock.read_to_string(&mut reply)?;
    let reply = reply.trim().to_string();
    if reply.is_empty() {
        return Err(anyhow::anyhow!("the server did not answer"));
    }
    Ok(reply)
}

fn hex_of(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// One channel name per line in `<data>/channels`. Lobby is always open.
fn channels_from(dir: &Path) -> Vec<String> {
    let Ok(text) = std::fs::read_to_string(dir.join("channels")) else {
        return Vec::new();
    };
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter(|line| line.len() <= ryme_proto::MAX_CHANNEL && !line.contains(['\n', '\r']))
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_missing_host_argument_is_rejected() {
        let err = host_request(&["--drop".into()]).unwrap_err();
        assert!(err.to_string().contains("usage"));
        assert!(host_request(&["--nope".into()]).is_err());
        assert_eq!(
            host_request(&["--lock".into()]).unwrap().as_deref(),
            Some("lock")
        );
        assert!(host_request(&["--new-invite".into()]).unwrap().is_none());
    }

    #[test]
    fn a_channels_file_lists_extra_rooms() {
        let dir = std::env::temp_dir().join(format!("ryme-channels-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("channels"), "# note\n\nRanked\n  Night  \n").unwrap();
        assert_eq!(
            channels_from(&dir),
            vec!["Ranked".to_string(), "Night".to_string()]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_public_address_replaces_the_bind_address() {
        let bind: SocketAddr = "0.0.0.0:1542".parse().unwrap();
        temp_env("RYME_PUBLIC", None);
        assert_eq!(published(bind), ("0.0.0.0".into(), 1542));
        temp_env("RYME_PUBLIC", Some("voice.example"));
        assert_eq!(published(bind), ("voice.example".into(), 1542));
        temp_env("RYME_PUBLIC", Some("voice.example:41234"));
        assert_eq!(published(bind), ("voice.example".into(), 41234));
        temp_env("RYME_PUBLIC", Some("[2001:db8::5]:9999"));
        assert_eq!(published(bind), ("2001:db8::5".into(), 9999));
        temp_env("RYME_PUBLIC", Some("2001:db8::5"));
        assert_eq!(published(bind), ("2001:db8::5".into(), 1542));
        temp_env("RYME_PUBLIC", None);
    }

    fn temp_env(key: &str, value: Option<&str>) {
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

/// `RYME_PUBLIC` is `host` or `host:port`. The host is what friends connect to.
/// An address with colons, such as `2001:db8::1`, is a host and keeps the bind port.
/// `[2001:db8::1]:41234` sets both.
fn published(bind: SocketAddr) -> (String, u16) {
    let Ok(raw) = std::env::var("RYME_PUBLIC") else {
        return (public_host(bind), bind.port());
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return (public_host(bind), bind.port());
    }
    if let Some(rest) = raw.strip_prefix('[') {
        if let Some((host, port)) = rest.split_once("]:") {
            if let Ok(port) = port.parse::<u16>() {
                if port != 0 && !host.is_empty() {
                    return (host.to_string(), port);
                }
            }
        }
    }
    if let Some((host, port)) = raw.rsplit_once(':') {
        if !host.is_empty() && !host.contains(':') {
            if let Ok(port) = port.parse::<u16>() {
                if port != 0 {
                    return (host.to_string(), port);
                }
            }
        }
    }
    (raw.to_string(), bind.port())
}

fn public_host(addr: SocketAddr) -> String {
    match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => "0.0.0.0".to_string(),
        IpAddr::V6(ip) if ip.is_unspecified() => "::".to_string(),
        ip => ip.to_string(),
    }
}
