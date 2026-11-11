#!/bin/sh
# Build a .deb from the Linux client. Arguments: version binary icon256 output
set -eu

version=${1#v}
binary=$2
icon=$3
output=$4

root=$(mktemp -d)
cleanup() { rm -rf "$root"; }
trap cleanup EXIT

mkdir -p \
  "$root/DEBIAN" \
  "$root/usr/bin" \
  "$root/usr/share/applications" \
  "$root/usr/share/icons/hicolor/48x48/apps" \
  "$root/usr/share/icons/hicolor/256x256/apps"

install -m 0755 "$binary" "$root/usr/bin/ryme"
install -m 0644 packaging/linux/ryme.desktop "$root/usr/share/applications/ryme.desktop"
install -m 0644 packaging/icon/ryme-48.png "$root/usr/share/icons/hicolor/48x48/apps/ryme.png"
install -m 0644 "$icon" "$root/usr/share/icons/hicolor/256x256/apps/ryme.png"

cat > "$root/DEBIAN/control" <<EOF
Package: ryme
Version: ${version}
Section: net
Priority: optional
Architecture: amd64
Maintainer: Ryme <elvonpiko@gmail.com>
Homepage: https://ryme.chat
Depends: libc6 (>= 2.34), libgcc-s1, libasound2 (>= 1.2.2) | libasound2t64, libfontconfig1 | libfontconfig1t64, libx11-6, libx11-xcb1, libxcursor1, libxi6, libxrender1, libxkbcommon0, libxkbcommon-x11-0, libegl1, libgl1, libwayland-client0, libwayland-egl1
Description: A quiet room for voices.
 Friends join with a name and one invite link.
EOF

dpkg-deb --root-owner-group --build "$root" "$output"
