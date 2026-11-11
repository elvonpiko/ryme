#!/bin/sh
# Root only long enough to hand the data directory to the ryme user.
set -eu
data="${RYME_DATA:-/var/lib/ryme}"
mkdir -p "$data"
if [ "$(id -u)" = "0" ]; then
  chown -R ryme:ryme "$data"
  exec setpriv \
    --no-new-privs \
    --reuid=ryme \
    --regid=ryme \
    --init-groups \
    --inh-caps=-all \
    --bounding-set=-all \
    /usr/local/bin/ryme-server "$@"
fi
exec /usr/local/bin/ryme-server "$@"
