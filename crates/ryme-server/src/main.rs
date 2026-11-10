use std::net::{IpAddr, SocketAddr};

use ryme_server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bind = std::env::var("RYME_BIND").unwrap_or_else(|_| "0.0.0.0:7777".to_string());
    let addr: SocketAddr = bind.parse()?;
    let server = Server::bind(addr)?;
    println!("{}", server.invite(&public_host(addr)));
    server.serve().await;
    Ok(())
}

fn public_host(addr: SocketAddr) -> String {
    match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => "0.0.0.0".to_string(),
        IpAddr::V6(ip) if ip.is_unspecified() => "::".to_string(),
        ip => ip.to_string(),
    }
}
