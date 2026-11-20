#!/bin/sh
# Same measurement as the numbers on ryme.chat.
#
# Run this on the Pi, against the container address, never the public tunnel.
# The server only forwards. Each speaker sends an 80-byte voice packet every
# 10 milliseconds for 12 seconds. CPU is (utime + stime of every thread) divided
# by the measured wall time of that run. Memory is VmRSS. docker stats
# reports 0 for this container.
#
#   cargo build --release --manifest-path bench/load/Cargo.toml
#   ./bench/measure.sh 'ryme://172.28.0.10:1542/#<fingerprint>'
#
# Playit can stay running. It is not on this path. Publish a new landing
# number only from a run of this script.

set -eu

invite=${1:?container invite, ryme://172.28.0.10:1542/#...}
bin=${2:-bench/load/target/release/ryme-load}
host=$(printf '%s' "$invite" | sed -n 's#^ryme://\([^:/]*\).*#\1#p')
case "$host" in
  172.28.0.10 | 127.0.0.1) ;;
  *)
    echo "Refusing $host. The landing number is the direct path, not the tunnel." >&2
    exit 2
    ;;
esac

pid=$(docker top ryme-server -eo pid | awk 'NR>1 { print $1; exit }')
clk=$(getconf CLK_TCK)
ticks() {
  awk '{ u += $14; s += $15 } END { print u + s + 0 }' /proc/"$1"/task/*/stat
}
rss() {
  awk '/VmRSS/ { print $2 }' /proc/"$1"/status
}

report() {
  name=$1
  t0=$2
  t1=$3
  kb=$4
  seconds=$5
  python3 - "$name" "$t0" "$t1" "$clk" "$kb" "$seconds" <<'PY'
import sys
name, t0, t1, clk, kb, seconds = sys.argv[1:]
elapsed = float(seconds)
if elapsed <= 0:
    elapsed = 0.001
cpu = (int(t1) - int(t0)) / int(clk) / elapsed * 100
print(f"{name} cpu={cpu:.2f}% rss_kb={kb} seconds={elapsed:.2f}")
PY
}

now_s() {
  python3 -c 'import time; print(f"{time.time():.6f}")'
}

echo "pid=$pid idle"
t0=$(ticks "$pid")
s0=$(now_s)
sleep 12
s1=$(now_s)
t1=$(ticks "$pid")
report idle "$t0" "$t1" "$(rss "$pid")" "$(python3 -c "print($s1 - $s0)")"

run() {
  n=$1
  t0=$(ticks "$pid")
  s0=$(now_s)
  "$bin" "$invite" "$n" 12
  s1=$(now_s)
  t1=$(ticks "$pid")
  report "speakers=$n" "$t0" "$t1" "$(rss "$pid")" "$(python3 -c "print($s1 - $s0)")"
}

run 10
run 25
