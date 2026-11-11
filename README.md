<p align="center">
  <img src="ui/icon.svg" alt="ryme" width="104">
</p>

# ryme

A quiet room for voices. One server, one invite, and the same card on Windows, Mac, and Linux. Written in Rust.

[ryme.chat](https://ryme.chat)

0.1.0 is the first release. Ryme is still under development.

The connection is encrypted, and the invite pins the server. The machine you run handles the audio. A leaked invite is replaced with `--new-invite`.

## Server

On Linux, including a Raspberry Pi:

```sh
curl -fsSL https://ryme.chat/install | bash
```

Set `RYME_PUBLIC` to the address friends can reach, then run `ryme-server`. Rooms, Docker, and tunnels are documented on [ryme.chat](https://ryme.chat).

## Clients

The [latest release](https://github.com/elvonpiko/ryme/releases/latest) has a Windows setup, a Mac disk image, and a Linux package. ryme.chat offers the one for this computer.

## Contributing

Issues and pull requests are welcome. Keep a change small.

The card is `ui/app.slint`. The server is the `ryme-server` crate. `cargo test -p ryme-proto -p ryme-server` checks the protocol and the server.
