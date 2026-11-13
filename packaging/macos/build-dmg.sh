#!/bin/sh
# Build a drag-to-Applications disk image.
# Arguments: version binary label output-dir icon1024
set -eu

version=${1#v}
binary=$2
label=$3
outdir=$4
icon=$5

stage=$(mktemp -d)
icons=$(mktemp -d)
cleanup() { rm -rf "$stage" "$icons"; }
trap cleanup EXIT

app="$stage/Ryme.app"
mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources"
install -m 0755 "$binary" "$app/Contents/MacOS/Ryme"
sed "s/VERSION/${version}/g" packaging/macos/Info.plist > "$app/Contents/Info.plist"

iset="$icons/ryme.iconset"
mkdir -p "$iset"
for spec in \
  "16 icon_16x16.png" \
  "32 icon_16x16@2x.png" \
  "32 icon_32x32.png" \
  "64 icon_32x32@2x.png" \
  "128 icon_128x128.png" \
  "256 icon_128x128@2x.png" \
  "256 icon_256x256.png" \
  "512 icon_256x256@2x.png" \
  "512 icon_512x512.png" \
  "1024 icon_512x512@2x.png"
do
  size=${spec%% *}
  name=${spec#* }
  sips -z "$size" "$size" "$icon" --out "$iset/$name" >/dev/null
done
iconutil -c icns "$iset" -o "$app/Contents/Resources/ryme.icns"
if [ -n "${APPLE_SIGNING_IDENTITY:-}" ]; then
  codesign --force --options runtime --timestamp --sign "$APPLE_SIGNING_IDENTITY" "$app"
  codesign --verify --strict --verbose=2 "$app"
fi
ln -s /Applications "$stage/Applications"

hdiutil create \
  -volname Ryme \
  -srcfolder "$stage" \
  -ov \
  -format UDZO \
  "$outdir/Ryme-mac-${label}.dmg"
