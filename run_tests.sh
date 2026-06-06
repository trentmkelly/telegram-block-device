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
SIZE="${TGDRIVE_TEST_SIZE:-64MiB}"
OBJECT_SIZE="${TGDRIVE_TEST_OBJECT_SIZE:-256KiB}"
LOG_DIR="${TGDRIVE_TEST_LOG_DIR:-$ROOT/test-logs}"
DAEMON_LOG="$LOG_DIR/daemon.log"
SUMMARY="$LOG_DIR/summary.txt"
PASSPHRASE_FILE="$LOG_DIR/luks-passphrase"
CRYPT_NAME="${TGDRIVE_CRYPT_NAME:-tgdrive_crypt_test}"

mkdir -p "$LOG_DIR"
: >"$SUMMARY"

DAEMON_PID=""

pass() {
  printf 'PASS %s\n' "$*" | tee -a "$SUMMARY"
}

fail() {
  printf 'FAIL %s\n' "$*" | tee -a "$SUMMARY"
}

info() {
  printf 'INFO %s\n' "$*" | tee -a "$SUMMARY"
}

run() {
  info "+ $*"
  "$@"
}

cleanup() {
  set +e
  if mountpoint -q "$MOUNT_DIR"; then
    timeout 15 sudo umount "$MOUNT_DIR"
  fi
  if [[ -e "/dev/mapper/$CRYPT_NAME" ]]; then
    sudo cryptsetup close "$CRYPT_NAME"
  fi
  if [[ -e "$NBD_DEV" ]]; then
    sudo nbd-client -d "$NBD_DEV" >/dev/null 2>&1
  fi
  if [[ -n "${DAEMON_PID:-}" ]] && kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    kill "$DAEMON_PID"
    wait "$DAEMON_PID" >/dev/null 2>&1
  fi
  rm -f "$HOME/.cache/tgdrive/mount.lock"
}
trap cleanup EXIT

require() {
  command -v "$1" >/dev/null 2>&1 || {
    fail "missing command: $1"
    exit 1
  }
}

start_daemon() {
  rm -f "$HOME/.cache/tgdrive/mount.lock"
  : >"$DAEMON_LOG"
  RUST_LOG=info cargo run -p tgdrive -- serve-nbd \
    --bind "$BIND" \
    --export-name "$EXPORT_NAME" \
    --backend telegram \
    >"$DAEMON_LOG" 2>&1 &
  DAEMON_PID=$!

  for _ in $(seq 1 80); do
    if grep -q "NBD server listening" "$DAEMON_LOG"; then
      pass "tgdrive serve-nbd is listening on $BIND"
      return 0
    fi
    if ! kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
      fail "tgdrive serve-nbd exited early"
      tail -120 "$DAEMON_LOG" | tee -a "$SUMMARY"
      exit 1
    fi
    sleep 0.25
  done
  fail "tgdrive serve-nbd did not become ready"
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

attach_nbd() {
  run sudo nbd-client "$HOST" "$PORT" "$NBD_DEV" -N "$EXPORT_NAME"
  local size
  size="$(lsblk -b -dn -o SIZE "$NBD_DEV")"
  [[ "$size" == "67108864" ]] || {
    fail "$NBD_DEV size was $size, expected 67108864"
    exit 1
  }
  pass "$NBD_DEV is attached with 64 MiB size"
}

detach_nbd() {
  if mountpoint -q "$MOUNT_DIR"; then
    run sudo umount "$MOUNT_DIR"
  fi
  run sudo nbd-client -d "$NBD_DEV"
}

mount_ext() {
  run sudo mkdir -p "$MOUNT_DIR"
  run timeout 30 sudo mount -o noatime,nodiratime "$NBD_DEV" "$MOUNT_DIR"
  findmnt "$MOUNT_DIR" -o SOURCE,FSTYPE,OPTIONS --noheadings | tee -a "$SUMMARY"
  pass "$NBD_DEV mounted at $MOUNT_DIR"
}

write_read_check() {
  local label="$1"
  run sudo chown -R "$(id -u):$(id -g)" "$MOUNT_DIR"
  printf 'tgdrive %s %s\n' "$label" "$(date -Iseconds)" >"$MOUNT_DIR/$label.txt"
  sync "$MOUNT_DIR/$label.txt"
  grep -q "tgdrive $label" "$MOUNT_DIR/$label.txt"
  pass "file write/sync/read succeeded for $label"
}

echo "This test is destructive."
echo "It will reformat the Telegram-backed TGDrive device and $NBD_DEV."
echo "Telegram may return FLOOD_WAIT during ext4/LUKS flushes; the daemon will wait and retry."
echo "Logs: $LOG_DIR"
read -r -p "Type YES to continue: " answer
if [[ "$answer" != "YES" ]]; then
  echo "aborted"
  exit 2
fi

require cargo
require nbd-client
require lsblk
require findmnt
require mkfs.ext2
require mkfs.ext4
require cryptsetup

run cargo fmt --all -- --check
run cargo clippy --workspace --all-targets --no-deps -- -D warnings
run cargo test --workspace
pass "local Rust verification passed"

run cargo run -p tgdrive -- resolve-channel
pass "tgdrive resolve-channel resolved TGDrive"

run cargo run -p tgdrive -- format --size "$SIZE" --object-size "$OBJECT_SIZE" --force
pass "remote TGDrive device formatted as $SIZE with $OBJECT_SIZE objects"

run cargo run -p tgdrive -- recover-from-remote
pass "recover-from-remote rebuilt local metadata"

run cargo run -p tgdrive -- fsck
pass "fsck verified current manifest"

cleanup
run sudo modprobe nbd max_part=8
lsmod | grep -q '^nbd '
pass "nbd kernel module loaded"

start_daemon
attach_nbd

run sudo mkfs.ext2 -F "$NBD_DEV"
pass "mkfs.ext2 completed"

mount_ext
write_read_check ext2

run sudo umount "$MOUNT_DIR"
run sudo mount -o noatime,nodiratime "$NBD_DEV" "$MOUNT_DIR"
grep -q 'tgdrive ext2' "$MOUNT_DIR/ext2.txt"
pass "unmount/remount preserved ext2 file"

detach_nbd
stop_daemon

start_daemon
attach_nbd
run timeout 30 sudo mount -o noatime,nodiratime "$NBD_DEV" "$MOUNT_DIR"
grep -q 'tgdrive ext2' "$MOUNT_DIR/ext2.txt"
write_read_check restart
pass "daemon restart plus WAL/remote replay preserved data"

detach_nbd
stop_daemon

RECOVER_CACHE="$(mktemp -d)"
TGDRIVE_CACHE_DIR="$RECOVER_CACHE/cache" \
TGDRIVE_SQLITE_PATH="$RECOVER_CACHE/cache/metadata.sqlite3" \
  cargo run -p tgdrive -- recover-from-remote
TGDRIVE_CACHE_DIR="$RECOVER_CACHE/cache" \
TGDRIVE_SQLITE_PATH="$RECOVER_CACHE/cache/metadata.sqlite3" \
  cargo run -p tgdrive -- fsck
rm -rf "$RECOVER_CACHE"
pass "empty local cache directory recovered from Telegram metadata"

start_daemon
attach_nbd
run sudo mkfs.ext4 -F -E lazy_itable_init=1,lazy_journal_init=1 "$NBD_DEV"
pass "mkfs.ext4 completed"
mount_ext
write_read_check ext4
detach_nbd
stop_daemon

start_daemon
attach_nbd
dd if=/dev/urandom of="$PASSPHRASE_FILE" bs=32 count=1 status=none
chmod 600 "$PASSPHRASE_FILE"
run sudo cryptsetup luksFormat --batch-mode "$NBD_DEV" "$PASSPHRASE_FILE"
run sudo cryptsetup open "$NBD_DEV" "$CRYPT_NAME" --key-file "$PASSPHRASE_FILE"
run sudo mkfs.ext4 -F /dev/mapper/"$CRYPT_NAME"
run sudo mount -o noatime,nodiratime,commit=60 /dev/mapper/"$CRYPT_NAME" "$MOUNT_DIR"
write_read_check luks
run sudo umount "$MOUNT_DIR"
run sudo cryptsetup close "$CRYPT_NAME"
detach_nbd
stop_daemon
pass "LUKS-on-TGDrive completed"

run cargo run -p tgdrive -- fsck
pass "final fsck verified manifest"

echo
echo "SUMMARY"
cat "$SUMMARY"
