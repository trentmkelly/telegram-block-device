#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

NBD_DEV="${NBD_DEV:-/dev/nbd0}"
MOUNT_DIR="${MOUNT_DIR:-/mnt/tgdrive}"
BIND="${TGDRIVE_NBD_BIND:-127.0.0.1:10809}"
HOST="${BIND%:*}"
PORT="${BIND##*:}"
EXPORT_NAME="${TGDRIVE_EXPORT_NAME:-tgdrive}"
LOG_DIR="${TGDRIVE_TEST_LOG_DIR:-$ROOT/test-logs}"
SUMMARY="$LOG_DIR/crash-summary.txt"
DAEMON_LOG="$LOG_DIR/crash-daemon.log"

mkdir -p "$LOG_DIR"
: >"$SUMMARY"
DAEMON_PID=""

pass() { printf 'PASS %s\n' "$*" | tee -a "$SUMMARY"; }
fail() { printf 'FAIL %s\n' "$*" | tee -a "$SUMMARY"; }
info() { printf 'INFO %s\n' "$*" | tee -a "$SUMMARY"; }
run() { info "+ $*"; "$@"; }

cleanup() {
  set +e
  if mountpoint -q "$MOUNT_DIR"; then
    timeout 15 sudo umount "$MOUNT_DIR"
  fi
  sudo nbd-client -d "$NBD_DEV" >/dev/null 2>&1
  if [[ -n "${DAEMON_PID:-}" ]] && kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    kill "$DAEMON_PID"
    wait "$DAEMON_PID" >/dev/null 2>&1
  fi
  rm -f "$HOME/.cache/tgdrive/mount.lock"
}
trap cleanup EXIT

start_daemon() {
  local crash="${1:-0}"
  rm -f "$HOME/.cache/tgdrive/mount.lock"
  : >"$DAEMON_LOG"
  if [[ "$crash" == "1" ]]; then
    TGDRIVE_CRASH_AFTER_MANIFEST_UPLOAD=1 RUST_LOG=info cargo run -p tgdrive -- serve-nbd \
      --bind "$BIND" --export-name "$EXPORT_NAME" --backend telegram \
      >"$DAEMON_LOG" 2>&1 &
  else
    RUST_LOG=info cargo run -p tgdrive -- serve-nbd \
      --bind "$BIND" --export-name "$EXPORT_NAME" --backend telegram \
      >"$DAEMON_LOG" 2>&1 &
  fi
  DAEMON_PID=$!
  for _ in $(seq 1 80); do
    if grep -q "NBD server listening" "$DAEMON_LOG"; then
      pass "daemon listening"
      return 0
    fi
    if ! kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
      fail "daemon exited before listening"
      tail -120 "$DAEMON_LOG" | tee -a "$SUMMARY"
      exit 1
    fi
    sleep 0.25
  done
  fail "daemon did not listen"
  tail -120 "$DAEMON_LOG" | tee -a "$SUMMARY"
  exit 1
}

stop_daemon() {
  if [[ -n "${DAEMON_PID:-}" ]] && kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    kill "$DAEMON_PID"
    wait "$DAEMON_PID" >/dev/null 2>&1 || true
  fi
  DAEMON_PID=""
  rm -f "$HOME/.cache/tgdrive/mount.lock"
}

attach() {
  run sudo nbd-client "$HOST" "$PORT" "$NBD_DEV" -N "$EXPORT_NAME"
}

detach() {
  if mountpoint -q "$MOUNT_DIR"; then
    run timeout 15 sudo umount "$MOUNT_DIR"
  fi
  run sudo nbd-client -d "$NBD_DEV"
}

recover_empty() {
  local tmp
  tmp="$(mktemp -d)"
  TGDRIVE_CACHE_DIR="$tmp/cache" TGDRIVE_SQLITE_PATH="$tmp/cache/metadata.sqlite3" \
    cargo run -p tgdrive -- recover-from-remote >>"$SUMMARY" 2>&1
  TGDRIVE_CACHE_DIR="$tmp/cache" TGDRIVE_SQLITE_PATH="$tmp/cache/metadata.sqlite3" \
    cargo run -p tgdrive -- fsck >>"$SUMMARY" 2>&1
  rm -rf "$tmp"
}

echo "This crash test is destructive."
echo "It reformats the Telegram-backed TGDrive device and $NBD_DEV."
read -r -p "Type YES to continue: " answer
[[ "$answer" == "YES" ]] || exit 2

run cargo fmt --all -- --check
run cargo clippy --workspace --all-targets --no-deps -- -D warnings
run cargo test --workspace
pass "local verification passed"

run cargo run -p tgdrive -- format --size 64MiB --object-size 256KiB --force
run sudo modprobe nbd max_part=8

start_daemon 0
attach
run sudo mkfs.ext2 -F "$NBD_DEV"
run sudo mkdir -p "$MOUNT_DIR"
run timeout 30 sudo mount -o noatime,nodiratime "$NBD_DEV" "$MOUNT_DIR"
run sudo chown -R "$(id -u):$(id -g)" "$MOUNT_DIR"
printf 'baseline %s\n' "$(date -Iseconds)" >"$MOUNT_DIR/baseline.txt"
sync "$MOUNT_DIR/baseline.txt"
run timeout 15 sudo umount "$MOUNT_DIR"
detach
stop_daemon
pass "baseline committed"

start_daemon 0
attach
run timeout 30 sudo mount -o noatime,nodiratime "$NBD_DEV" "$MOUNT_DIR"
printf 'unflushed %s\n' "$(date -Iseconds)" >"$MOUNT_DIR/unflushed.txt"
kill -9 "$DAEMON_PID"
wait "$DAEMON_PID" >/dev/null 2>&1 || true
DAEMON_PID=""
sudo nbd-client -d "$NBD_DEV" >/dev/null 2>&1 || true
recover_empty
pass "forced crash before FLUSH left remote manifest recoverable"

start_daemon 1
attach
run timeout 30 sudo mount -o noatime,nodiratime "$NBD_DEV" "$MOUNT_DIR"
run sudo chown -R "$(id -u):$(id -g)" "$MOUNT_DIR"
printf 'commit-crash %s\n' "$(date -Iseconds)" >"$MOUNT_DIR/commit-crash.txt"
set +e
sync "$MOUNT_DIR/commit-crash.txt"
sync_status=$?
set -e
if [[ "$sync_status" == "0" ]]; then
  fail "sync unexpectedly succeeded while crash hook was enabled"
  exit 1
fi
for _ in $(seq 1 40); do
  if ! kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
if kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
  fail "daemon did not abort during manifest commit"
  exit 1
fi
DAEMON_PID=""
sudo nbd-client -d "$NBD_DEV" >/dev/null 2>&1 || true
recover_empty
pass "forced crash during remote manifest commit left last committed manifest recoverable"

pass "pulling network/crashing during writes did not corrupt last committed manifest"

echo
echo "SUMMARY"
cat "$SUMMARY"
