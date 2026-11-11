use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use ryme_server::{Identity, Server};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .init();
    let bind = std::env::var("RYME_BIND").unwrap_or_else(|_| "0.0.0.0:1542".to_string());
    let addr: SocketAddr = bind.parse()?;
    let data = PathBuf::from(std::env::var("RYME_DATA").unwrap_or_else(|_| "ryme-data".into()));
    let rotate = std::env::args().any(|arg| arg == "--new-invite");
    let identity = if rotate {
        tracing::info!("new invite, previous links no longer connect");
        Identity::rotate(&data)?
    } else {
        Identity::load_or_create(&data)?
    };
    let server = Server::bind_identity(addr, identity)?;
    for name in channels_from(&data) {
        server.allow(&name);
    }
    let (host, port) = published(addr);
    tracing::info!(%addr, %host, port, path = %data.display(), "listening");
    println!("{}", server.invite_on(&host, port));
    server.serve().await;
    Ok(())
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
