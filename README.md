# Telegram Block Device

This was vibe coded and had not been thoroughly reviewed for safety. Use it at
your own risk.

Telegram Block Device is a Linux block device backed by a private Telegram
channel. It speaks NBD locally, stores remote data as Telegram channel objects,
and treats Telegram as slow durable object storage rather than as a filesystem.

The daemon is intended to log in through Telegram MTProto, resolve a configured
storage channel, and recover its remote manifest from pinned channel metadata.
Local state is only a cache: a fresh machine should be able to rebuild its index
from the channel, hydrate blocks on demand, and regain access after a new
Telegram login.

Writes are staged locally, committed through copy-on-write object uploads, and
made durable remotely when the block device receives a flush. Reads use direct
message ID lookups and an aggressive local cache so ordinary Linux filesystems
can run on top of the exported device.

The storage layer deliberately avoids native encryption. It behaves like a
normal block device, so tools such as LUKS, ext4, and other standard Linux block
stack components can be layered above it.

## Build

Requirements: Linux, Rust, `nbd-client`, and Telegram MTProto API credentials
from `my.telegram.org`.

```bash
cargo build --workspace
```

Create a local `.env` file:

```bash
TELEGRAM_API_ID=...
TELEGRAM_API_HASH=...
TELEGRAM_CHANNEL_ID=-100...
TELEGRAM_CHANNEL_TITLE=TGDrive
TELEGRAM_SESSION_PATH=.tgdrive.session
```

Then authenticate and resolve the channel access hash:

```bash
cargo run -p tgdrive -- login
cargo run -p tgdrive -- resolve-channel
```

## Use

Format the remote block device metadata:

```bash
cargo run -p tgdrive -- format --size 64MiB --object-size 256KiB --force
```

Start the NBD export:

```bash
cargo run -p tgdrive -- serve-nbd --bind 127.0.0.1:10809 --export-name tgdrive --backend telegram
```

Attach and format from another shell:

```bash
sudo modprobe nbd max_part=8
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N tgdrive
sudo mkfs.ext4 -F /dev/nbd0
sudo mount -o noatime,nodiratime,commit=60 /dev/nbd0 /mnt/tgdrive
```

For encryption, put LUKS above `/dev/nbd0` before creating the filesystem.

Useful checks:

```bash
cargo run -p tgdrive -- recover-from-remote
cargo run -p tgdrive -- fsck
```
