#!/bin/sh
# Submit a disk image to Apple's notary service and staple the ticket.
# Apple documents this for software distributed outside the App Store:
# https://developer.apple.com/documentation/security/notarizing-macos-software-before-distribution
set -eu

dmg=${1:?disk image}
if [ -z "${APPLE_ID:-}" ] || [ -z "${APPLE_TEAM_ID:-}" ] || [ -z "${APPLE_APP_PASSWORD:-}" ]; then
  echo "Apple notarization is not configured. The disk image is not notarized."
  exit 0
fi

xcrun notarytool submit "$dmg" \
  --apple-id "$APPLE_ID" \
  --team-id "$APPLE_TEAM_ID" \
  --password "$APPLE_APP_PASSWORD" \
  --wait
xcrun stapler staple "$dmg"
