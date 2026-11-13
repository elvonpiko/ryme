#!/bin/sh
# Import a Developer ID certificate for codesign. No-op when the secret is absent.
set -eu

if [ -z "${APPLE_CERTIFICATE_P12_BASE64:-}" ] || [ -z "${APPLE_CERTIFICATE_PASSWORD:-}" ]; then
  echo "Developer ID certificate is not configured. The disk image will not be signed." >&2
  exit 0
fi

keychain="$RUNNER_TEMP/ryme.keychain"
password="$(openssl rand -hex 16)"
printf '%s' "$APPLE_CERTIFICATE_P12_BASE64" | base64 -d > "$RUNNER_TEMP/developer.p12"
security create-keychain -p "$password" "$keychain"
security set-keychain-settings -lut 21600 "$keychain"
security unlock-keychain -p "$password" "$keychain"
security import "$RUNNER_TEMP/developer.p12" -k "$keychain" -P "$APPLE_CERTIFICATE_PASSWORD" -T /usr/bin/codesign -T /usr/bin/security
security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$password" "$keychain"
security list-keychains -d user -s "$keychain" $(security list-keychains -d user | tr -d '"')
rm -f "$RUNNER_TEMP/developer.p12"

identity=${APPLE_SIGNING_IDENTITY:-}
if [ -z "$identity" ]; then
  identity=$(security find-identity -v -p codesigning "$keychain" | awk -F'"' 'NR==1 { print $2 }')
fi
if [ -z "$identity" ]; then
  echo "The certificate did not contain a codesigning identity." >&2
  exit 1
fi
printf '%s\n' "$identity"
