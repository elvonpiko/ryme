<p align="center">
  <img src="ui/icon.svg" alt="ryme" width="104">
</p>

# ryme

A quiet room for voices. Self-hosted voice chat for friends. One person runs the server. Everyone else installs the client and opens the invite.

[ryme.chat](https://ryme.chat)

0.1.19 is this release. The client offers an update when the tag matches.

## Where this is

The next tag is a small invited beta: admission is checked, the microphone starts in a known state, and the limits below are written down. It is not a public beta.

A public beta needs a clean-machine install, an update of an already-running app, and audio tried on ordinary headsets, Bluetooth, and laptop speakers. A broad launch needs a new person to download, join, and recover without help, and the website to show that same path. None of those are claimed yet.

## Privacy

The connection is encrypted to the server. The server ends that encryption and forwards the encoded audio. People in the room are not end-to-end encrypted to each other. The server does not mix or record the call. It logs its own startup and connection failures.

The invite pins the server and carries a separate admission token. `--new-invite` retires that token and leaves the certificate in place. `--drop <id>` disconnects one session. `--lock` refuses new people; someone already in can reconnect, and `--unlock` opens the room. `--ban <card>` refuses that card until `--unban`. Those commands run on the machine that has the server's data directory. The host key stays there. Everyone with the invite is equal. There are no roles.

## Known limits

- Push-to-talk is one letter, V unless you choose another. Windows can see that key in other applications. macOS and a focused Linux window see it while Ryme is focused. Linux also reads the keyboard device when the session allows it. Wayland often does not. If the key cannot be read, push-to-talk stays closed.
- Soften noise is a quiet-part expander. It is not a neural denoiser.
- Phones and tablets are not supported.
- Headset unplug, sleep and wake, and upgrading a running app need a machine. Automated tests cover admission, a muted microphone, and a slow receiver.

## Security

See [SECURITY.md](SECURITY.md). The license is MIT. See [LICENSE](LICENSE).

## Server

Written in Rust. On Linux, including a Raspberry Pi.

The short start is the image [ghcr.io/elvonpiko/ryme-server](https://github.com/elvonpiko/ryme/pkgs/container/ryme-server), tagged with the release. Or install the binary and run it:

```sh
curl -fsSL https://ryme.chat/install | bash
```

`RYME_PUBLIC` is the one host friends use: the public IP, or a domain that points at it. The server listens on UDP 1542 and prints `ryme://that-host:1542/#…`. The setup, the firewall, and a home router are on [ryme.chat](https://ryme.chat/docs/docker).

## Clients

The [latest release](https://github.com/elvonpiko/ryme/releases/latest) has a Windows setup, a Mac disk image, and a Linux package. ryme.chat offers the one for this computer.

## Contributing

Issues and pull requests are welcome. Keep a change small.

The card is `ui/app.slint`. The server is the `ryme-server` crate. `cargo test -p ryme-proto -p ryme-server` checks the protocol and the server. `cargo test -p ryme` checks the client. Set `RYME_SOUND=1` to also open the default sound device.
